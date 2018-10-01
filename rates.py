from typing import List

import lib.clientperf
import core.influxdb
import datetime
import core.realm
import data.element_configs

options = lib.element_dev.options


async def monitor_error_rate(element_config_ids: List[str] = None) -> None:
    duration = "2h"
    resolution = "1h"
    if not element_config_ids:
        element_config_ids = lib.clientperf.get_whitelisted_element_config_ids()
    for id in element_config_ids:
        print("\nid:", id)

        def count_events(start, end, hertz):
            return round(((end - start) / 1000000) * hertz)

        load_metrics = (await core.influxdb.read("var:element_load_%s" % id,
            duration, resolution=resolution)).get("", {})
        load_error_metrics = (await core.influxdb.read(
            "var:element_load_error_%s" % id,
            duration, resolution=resolution)).get("", {})
        if len(load_metrics) == 0 or len(load_error_metrics) == 0:
            continue
        keys = sorted(load_metrics.keys())[-2:]
        e_keys = sorted(load_error_metrics.keys())[-2:]
        if len(keys) != 2 or keys != e_keys:
            continue
        now = datetime.datetime.now(None).timestamp() * 1000000
        loaded = (count_events(keys[0], keys[1], load_metrics[keys[0]]) +
            count_events(keys[1], now, load_metrics[keys[1]]))
        errors = (count_events(keys[0], keys[1], load_error_metrics[keys[0]]) +
            count_events(keys[1], now, load_error_metrics[keys[1]]))
        ratio = (errors / loaded) * 100
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
                "load_cnt": round(loaded),
                "load_error_cnt": round(errors),
                "minutes": round(((now - keys[0]) / 1000000) / 60),
            }
            html_message = (
                "%(config_name)s "
                "(<a href='%(config_url)s'><code>%(config_id)s</code></a>) "
                "<a href='%(error_browser_url)s'> error rate "
                "(%(error_rate)s%%) </a> was higher than threshold "
                "(%(threshold)s%%). "
                "Failed approximately %(load_error_cnt)s out of %(load_cnt)s attempts "
                "in the last %(minutes)s minutes.") % message_args
            print("\n", html_message)  # NOCOMMIT
