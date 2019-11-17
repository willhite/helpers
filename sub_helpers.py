from typing import List

import core.http
import core.io
import data.users
import datetime
import json
import lib.integrations
import logging
import proto
import services

# def make_listview_subscription(thread_id:str, listview: str):
#     quip = a(data.company.get_by_domain("quip.com"))[0]
#     dw = a(data.users.get_by_email_login("dwillhite@quip.com"))
#     set_tracer_user(dw)
#     sub = thread.salesforce_record_subscriptions[0]
#     org_id = "00D4P000001dcb5UAA"
#     record_type = "Opportunity"
#     record_id = "0064P00000lQXedQAG"
#     fr = a(lib.sfdc_cache.get_full_cdc_record(org_id, quip, record_id, record_type))
#     lib.sfdc_record_subscriptions.filter_record_space_includes_record(sub.record_space.filter_record_space, fr)
#     fields = {f.name: f for f in fr.fields}
#     record_field = fields["Amount"]
#     record_space_filter = sub.record_space.filter_record_space.filters[0]
#     record_space_filter.comparison_value_field.currency_value.value = 100000.0
#     lib.sfdc_field_comparators.compare(record_field, record_space_filter.comparison_value_field, record_space_filter.operator)
#     path = (f"sobjects/{subscription.record_type}/listviews/{listview_id}"
#             "/results?limit=2000")
#     listview = await lib.sfdc_api.make_rest_call_as_user(
#         subscription.salesforce_org_id, user, path)
#     column_index = next(i for i, column in enumerate(listview["columns"])
#                         if column["fieldNameOrPath"] == "Id")


fitzroy_org_id = "00D3i000000swFpEAI"
bryan_org_id = "00D4P000001dcb5UAA"
org62_org_id = "00D000000000062EAA"
barry_report_id = "00O300000098euGEAQ"
barry_id = "bWcAEA9EKx3"
merwan_id = "PRIAEAGK65v"
quip_id = "DYTAcAiIFUr" if core.realm.any_prod() else "FTGAcAzRzdg"
dw_id = "efOAEA5zZKu" if core.realm.any_prod() else "dKWAEApvV4V"
anna_thread_id = "FXCAAA4wjYT" if core.realm.any_prod() else "aXEAAApr9IC"
anna_id= "efPAEAVKk4k"
object_type = "Opportunity"
report_id = "00O0M00000A4aHZUAZ" if core.realm.any_prod() else "00O3i0000025PLcEAM"
id_column_name = "Opportunity Name"


async def make_subscription_with_report_record_space(
        thread_id: str, org_id, report_id: str, id_column_name: str, object_type: str):
    user = await core.tracer.get_user()
    record_space = proto.salesforce.RecordSpace()
    report_record_space = proto.salesforce.RecordSpace.ReportRecordSpace(
        report_id=report_id, id_column_name=id_column_name)
    record_space.report_record_space.CopyFrom(report_record_space)
    alerts = get_default_alerts()
    report_alert_rules = [proto.salesforce.ReportAlertRule(
        diff=proto.salesforce.ReportAlertRule.Diff(
            notify_removal=True,
            notify_addition=True,
        )
    )]
    await lib.sfdc_record_subscriptions.create_subscription(thread_id, object_type,
        org_id, user, record_space, alerts, name="Migrated Subscription",
        report_alert_rules=report_alert_rules)


async def fetch_report(report_id: str, org_id: str):
    url = lib.sfdc_export.report_url(org_id, report_id, details=True)
    user = await core.tracer.get_user()
    json_string = await lib.sfdc_export.fetch_data_for_url(url, user.id)
    report = lib.sfdc_export._parse_report_json(json_string, url, details=True)
    return report


async def make_subscription_with_listview_record_space(
        thread_id: str, org_id, listview_id: str, id_column_name: str, object_type: str):
    user = await core.tracer.get_user()
    record_space = proto.salesforce.RecordSpace()
    listview_record_space = proto.salesforce.RecordSpace.ListviewRecordSpace(
        listview_id=listview_id)
    record_space.listview_record_space.CopyFrom(listview_record_space)
    alerts = get_default_alerts()

    await lib.sfdc_record_subscriptions.create_subscription(thread_id, object_type,
        org_id, user, record_space, alerts, name="Migrated Subscription",
        report_alert_rules=[])


async def fetch_listview(listview_id: str, org_id: str, object_type: str):
    path = (f"sobjects/{object_type}/listviews/{listview_id}"
        "/results?limit=2000")
    user = await core.tracer.get_user()
    listview = await lib.sfdc_api.make_rest_call_as_user(org_id, user, path)
    logging.NOCOMMIT(f"listview: {listview}")


async def info_for_threads(thread_ids: List[str]) -> None:
    for id in thread_ids:
        thread = await data.threads.read(id)
        members = await data.thread_members.read_by_root_id(thread.id)
        user_ids = [m.user_id for m in members.values()]
        users = await data.users.read_multi(user_ids)
        names = sorted([u.name for u in users.values()])
        namelist = ", ".join(names)
        print(f"{thread.id}, \"{thread.title}\", members: {namelist}")


def get_default_alerts() -> List[proto.salesforce.Alert]:
    return [
        proto.salesforce.Alert(
            alert_type=proto.salesforce.RULE_BASED_ALERT,
            rules = [
                proto.salesforce.FilterRule(
                    field_name="Amount",
                    relational_op=proto.salesforce.CHANGED_BY_VALUE,
                    change_value=50000.0,
                    comparison_value_field=proto.salesforce.Record.Field(
                        type=proto.salesforce.CURRENCY,
                        name="Amount",
                    )
                )
            ],
        ),
        proto.salesforce.Alert(
            alert_type=proto.salesforce.RULE_BASED_ALERT,
            rules = [
                proto.salesforce.FilterRule(
                    field_name="Next_Steps__c",
                    relational_op=proto.salesforce.CHANGED,
                    comparison_value_field=proto.salesforce.Record.Field(
                        type=proto.salesforce.TEXTAREA,
                        name="Next_Steps__c",
                    )
                )
            ]
        ),
        proto.salesforce.Alert(
            alert_type=proto.salesforce.RULE_BASED_ALERT,
            rules = [
                proto.salesforce.FilterRule(
                    field_name="Fiscal",
                    relational_op=proto.salesforce.CHANGED,
                    comparison_value_field=proto.salesforce.Record.Field(
                        type=proto.salesforce.STRING,
                        name="Fiscal",
                    )
                )
            ]
        ),
        proto.salesforce.Alert(
            alert_type=proto.salesforce.RULE_BASED_ALERT,
            rules=[
                proto.salesforce.FilterRule(
                    field_name="StageName",
                    relational_op=proto.salesforce.CHANGED,
                    comparison_value_field=proto.salesforce.Record.Field(
                        type=proto.salesforce.PICKLIST,
                        name="StageName",
                    )
                )
            ]
        ),
    ]

async def monitor_subscriptions() -> None:
    thread_ids = await lib.sfdc_record_subscriptions.all_subscribed_thread_ids()
    threads_dict = await data.threads.read_multi(thread_ids)
    threads = list(threads_dict.values())
    pairs = []
    for thread in threads:
        for s in thread.salesforce_record_subscriptions:
            pairs.append((thread.id, s.subscription_id,))

    for tid, sid in sorted(pairs):
        thread = threads_dict[tid]
        s = next((sub for sub in thread.salesforce_record_subscriptions if sub.subscription_id == sid), None)
        if not s:
            print(f"thread: {tid}, missing sub: {sid}")
            continue
        rs = s.record_space
        if rs.HasField("report_record_space"):
            print(f"REPORT thread: {thread.title}({tid}), sid: {sid}, enabled: {s.enabled}, time: {core.utc.datetime_from_usec(rs.report_record_space.cache_updated_usec)}")
        elif rs.HasField("listview_record_space"):
            print(f"LISTVIEW thread: {thread.title}({tid}), sid: {sid}, enabled: {s.enabled}, time: {core.utc.datetime_from_usec(rs.listview_record_space.cache_updated_usec)}")
        elif rs.HasField("explicit_record_space"):
            print(f"EXPLICIT thread: {thread.title}({tid}), sid: {sid}, enabled: {s.enabled}")
        elif rs.HasField("filter_record_space"):
            print(f"FILTER thread: {thread.title}({tid}), sid: {sid}, enabled: {s.enabled}")
        else:
            print(f"UNKNOWN tid: {tid}, sub: {s}")

