from typing import List

import core.influxdb
import core.realm
import data.element_configs
import lib.clientperf
import logging

from datetime import datetime, timedelta
from dateutil import tz

options = lib.element_dev.options

def calculate_total_from_metrics(metrics, start, duration="1h", resolution="1m"):
    ms = 1000000
    duration_secs = core.utc.parse_time(duration)
    resolution_secs = core.utc.parse_time(resolution)
    start_ms = int(start.timestamp()) * ms
    total = 0
    for ts in range(start_ms, start_ms + (duration_secs * ms), resolution_secs * ms):
        total += metrics[ts] * resolution_secs
    return total


async def monitor_error_rate(element_config_ids: List[str] = None, duration="2h") -> None:
    resolution = "1m"
    if not element_config_ids:
        element_config_ids = lib.clientperf.get_whitelisted_element_config_ids()
    ec_ids = sorted(element_config_ids)
    ecs = await data.element_configs.read_multi(ec_ids)
    resolution_secs = core.utc.parse_time(resolution)
    print(f"processing: {ec_ids}, resolution_secs: {resolution_secs}")
    for id in ec_ids:
        load_metrics = (await core.influxdb.read("var:element_load_%s" % id,
            duration, resolution=resolution)).get("", {})
        load_error_metrics = (await core.influxdb.read(
            "var:element_load_error_%s" % id,
            duration, resolution=resolution)).get("", {})
        print(f"\nid: {ecs[id].name}({id}), load_metrics: {len(load_metrics)}, load_errors: {len(load_error_metrics)}")
        # for k, v in load_metrics.items():
        #     print("load  ts: %s, rate: %s, number: %s" % (k, v, v * resolution_secs))
        # for k, v in load_error_metrics.items():
        #     print("error ts: %s, rate: %s, number: %s" % (k, v, v * resolution_secs))
        if len(load_metrics) < 2 or len(load_error_metrics) < 2:
            logging.NOCOMMIT("skipping element id: %s, loads: %s, error: %s" %
                (id, len(load_metrics), len(load_error_metrics)))
            continue

        now = datetime.now()
        start = (now.replace(minute=0, second=0) - timedelta(hours=1))
        loads = calculate_total_from_metrics(load_metrics, start)
        errors = calculate_total_from_metrics(load_error_metrics, start)
        if loads == 0:
            logging.NOCOMMIT("skipping element id: %s - 0 loads", id)
            continue

        end = start + timedelta(minutes=59)
        to_zone = tz.gettz("America/Los_Angeles")

        print(f"loads: {loads}, errors: {errors}, time: %s-%s" % (start.astimezone(tz=to_zone).strftime("%I:%M%p %Z"), end.astimezone(tz=to_zone).strftime("%I:%M%p %Z")))
        ratio = (errors / loads) * 100
        if ratio > options.elements_error_ratio_threshold:
            element_config = await data.element_configs.read(id)
            config_url = "%(intranet_home)slookup?id=%(config_id)s" % {
                "intranet_home": core.realm.spec().http.intranet_home,
                "config_id": element_config.id
            }
            error_browser_url = (
                "%(intranet_home)selements-load-error-browser"
                "?element_config_id=%(config_id)s" % {
                    "intranet_home": core.realm.spec().http.intranet_home,
                    "config_id": element_config.id
                }
            )
            message_args = {
                "config_name": element_config.name,
                "config_url": config_url,
                "error_browser_url": error_browser_url,
                "config_id": element_config.id,
                "error_rate": round(ratio),
                "threshold": options.elements_error_ratio_threshold,
                "load_cnt": loads,
                "load_error_cnt": errors,
            }
            html_message = (
                "%(config_name)s "
                "(<a href='%(config_url)s'><code>%(config_id)s</code></a>) "
                "<a href='%(error_browser_url)s'> error rate "
                "(%(error_rate)s%%) </a> was higher than threshold "
                "(%(threshold)s%%). "
                "Failed approximately %(load_error_cnt)s "
                "out of %(load_cnt)s attempts ") % message_args
            print("\n", html_message)  # NOCOMMIT
