from typing import Dict, List

import core.influxdb
import core.realm
import data.element_configs
import lib.clientperf
import logging
import proto.elements

from datetime import datetime, timedelta
from dateutil import tz

options = lib.element_dev.options


async def monitor_error_rate(
    element_config_ids: List[str] = None,
    duration: str="2h",
    verbose: bool=True
) -> None:
    resolution = "1m"
    if not element_config_ids:
        element_config_ids = lib.clientperf.get_whitelisted_element_config_ids()
    ec_ids = sorted(element_config_ids)
    ecs = await data.element_configs.read_multi(ec_ids)
    resolution_secs = core.utc.parse_time(resolution)
    duration_secs = core.utc.parse_time(duration)

    def calculate_total_from_metrics(
        metrics: Dict[float, float],
        start: datetime
    ):
        duration = "1h"
        resolution = "1m"
        ms = 1000000
        duration_secs = core.utc.parse_time(duration)
        resolution_secs = core.utc.parse_time(resolution)
        start_ms = int(start.timestamp()) * ms
        total = 0
        end = start_ms + (duration_secs * ms)
        increment = resolution_secs * ms
        for ts in range(start_ms, end, increment):
            total += metrics[ts] * resolution_secs
        return total

    async def log_high_error_ratio_warning(
        ec: proto.elements.ElementConfig,
        loads: float,
        errors: float,
        ratio: float,
        start: datetime
    ) -> None:
        config_url = "%(intranet_home)slookup?id=%(config_id)s" % {
            "intranet_home": core.realm.spec().http.intranet_home,
            "config_id": ec.id
        }
        error_browser_url = (
            "%(intranet_home)selements-load-error-browser"
            "?element_config_id=%(config_id)s" % {
                "intranet_home": core.realm.spec().http.intranet_home,
                "config_id": ec.id
            }
        )
        end = start + timedelta(minutes=59)
        to_zone = tz.gettz("America/Los_Angeles")
        message_args = {
            "config_id": ec.id,
            "config_name": ec.name,
            "config_url": config_url,
            "end_time": end.astimezone(tz=to_zone).strftime("%I:%M%p %Z"),
            "error_browser_url": error_browser_url,
            "error_rate": round(ratio),
            "load_cnt": loads,
            "load_error_cnt": errors,
            "start_time": start.astimezone(tz=to_zone).strftime("%I:%M%p %Z"),
            "threshold": options.elements_error_ratio_threshold,
        }
        html_message = (
            "%(config_name)s "
            "(<a href='%(config_url)s'><code>%(config_id)s</code></a>) "
            "<a href='%(error_browser_url)s'> error rate "
            "(%(error_rate)s%%) </a> was higher than threshold "
            "(%(threshold)s%%). "
            "Failed %(load_error_cnt)d "
            "out of %(load_cnt)d attempts "
            "from %(start_time)s - %(end_time)s") % message_args
        logging.NOCOMMIT("msg: %s", html_message)
        # await data.element_configs.post_to_element_log(html_message)

    for id, ec in ecs.items():
        load_metrics = (await core.influxdb.read("var:element_load_%s" % id,
            duration, resolution=resolution)).get("", {})
        load_error_metrics = (await core.influxdb.read(
            "var:element_load_error_LOAAjA15rGy", "2h", resolution="1m"
            duration, resolution=resolution)).get("", {})
        if verbose:
            logging.info(f"ec: {id}, len(load_metrics): {len(load_metrics)}, "
                f"len(load_error_metrics): {len(load_error_metrics)}")

        # calculate the most recent exact hour that is >= 60 minutes ago
        now = datetime.now()
        start = (now.replace(minute=0, second=0) - timedelta(hours=1))

        while start > now - timedelta(seconds=duration_secs):
            loads = calculate_total_from_metrics(load_metrics, start)
            errors = calculate_total_from_metrics(load_error_metrics, start)
            if loads == 0:
                logging.NOCOMMIT("skipping element id: %s - 0 loads", id)
                continue

            end = start + timedelta(minutes=59)
            to_zone = tz.gettz("America/Los_Angeles")
            if verbose:
                logging.info(f"ec: {ec.id} loads: {loads}, errors: {errors}, time: %s-%s" %
                    (start.astimezone(tz=to_zone).strftime("%I:%M%p %Z"),
                     end.astimezone(tz=to_zone).strftime("%I:%M%p %Z")))
            ratio = (errors / loads) * 100
            if ratio > options.elements_error_ratio_threshold:
                await log_high_error_ratio_warning(ec, loads, errors, ratio, start)
            start -= timedelta(hours=1)

