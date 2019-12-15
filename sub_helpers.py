from typing import Dict, List, Set

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
isotope_org_id = "00DB0000000YNS1MAO"
barry_report_id = "00O300000098euGEAQ"
barry_id = "bWcAEA9EKx3"
merwan_id = "PRIAEAGK65v"
quip_id = "DYTAcAiIFUr" if core.realm.any_prod() else "FTGAcAzRzdg"
sfdc_id = "fPQAcAPz5E9"
dw_id = "efOAEA5zZKu" if core.realm.any_prod() else "dKWAEApvV4V"
anna_thread_id = "FXCAAA4wjYT" if core.realm.any_prod() else "aXEAAApr9IC"
anna_id= "efPAEAVKk4k"
object_type = "Opportunity"
report_id = "00O0M00000A4aHZUAZ" if core.realm.any_prod() else "00O3i0000025PLcEAM"
id_column_name = "Opportunity Name"

async def make_isotope_subscriptions(thread_id: str):
    await make_subscription_with_listview_record_space(
        "All Features", thread_id, isotope_org_id, "00BB0000003TqxzMAC", "Feature__c")
    await make_subscription_with_listview_record_space(
        "All Issues", thread_id, isotope_org_id, "00BB0000003TqxkMAC", "Issue__c")
    await make_subscription_with_listview_record_space(
        "All Milestones", thread_id, isotope_org_id, "00BB0000003TqxuMAC", "Milestone__c")
    await make_subscription_with_listview_record_space(
        "All Teams", thread_id, isotope_org_id, "00BB0000003TqxfMAC", "Team__c")
    await make_subscription_with_listview_record_space(
        "All Projects", thread_id, isotope_org_id, "00BB0000003TqRUMA0", "Project__c")

async def make_subscription_with_report_record_space(
        name: str, thread_id: str, org_id, report_id: str, id_column_name: str,
        object_type: str):
    user = await core.tracer.get_user()
    record_space = proto.salesforce.RecordSpace()
    report_record_space = proto.salesforce.RecordSpace.ReportRecordSpace(
        report_id=report_id, id_column_name=id_column_name)
    record_space.report_record_space.CopyFrom(report_record_space)
    alerts = get_default_alerts(org_id)
    report_alert_rules = [proto.salesforce.ReportAlertRule(
        diff=proto.salesforce.ReportAlertRule.Diff(
            notify_removal=True,
            notify_addition=True,
        )
    )]
    await lib.sfdc_record_subscriptions.create_subscription(thread_id, object_type,
        org_id, user, record_space, alerts, name=name,
        report_alert_rules=report_alert_rules)


async def fetch_report(report_id: str, org_id: str):
    url = lib.sfdc_export.report_url(org_id, report_id, details=True)
    user = await core.tracer.get_user()
    json_string = await lib.sfdc_export.fetch_data_for_url(url, user.id)
    report = lib.sfdc_export._parse_report_json(json_string, url, details=True)
    return report


async def make_subscription_with_listview_record_space(
        name: str, thread_id: str, org_id, listview_id: str, object_type: str):
    user = await core.tracer.get_user()
    record_space = proto.salesforce.RecordSpace()
    listview_record_space = proto.salesforce.RecordSpace.ListviewRecordSpace(
        listview_id=listview_id)
    record_space.listview_record_space.CopyFrom(listview_record_space)
    alerts = get_default_alerts(org_id)

    await lib.sfdc_record_subscriptions.create_subscription(thread_id, object_type,
        org_id, user, record_space, alerts, name=name, report_alert_rules=[])


async def fetch_listview(listview_id: str, org_id: str, object_type: str) -> None:
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
        names = sorted([uj.name for u in users.values()])
        namelist = ", ".join(names)
        print(f"{thread.id}, \"{thread.title}\", members: {namelist}")


def get_default_alerts(org_id: str) -> List[proto.salesforce.Alert]:
    if org_id == isotope_org_id:
        return get_default_isotope_alerts()
    elif org_id == org62_org_id:
        return get_default_org62_alerts()
    else:
        return []

def get_default_isotope_alerts() -> List[proto.salesforce.Alert]:
    return [
        proto.salesforce.Alert(
            alert_type=proto.salesforce.RULE_BASED_ALERT,
            rules=[
                proto.salesforce.FilterRule(
                    field_name="Name",
                    relational_op=proto.salesforce.CHANGED,
                    comparison_value_field=proto.salesforce.Record.Field(
                        type=proto.salesforce.STRING,
                        name="Name",
                    )
                )
            ]
        ),
    ]

def get_default_org62_alerts() -> List[proto.salesforce.Alert]:
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

async def monitor_subscriptions(enabled_only: bool=True) -> Dict[str, List[str]]:
    thread_ids = await lib.sfdc_record_subscriptions.all_subscribed_thread_ids()
    threads_dict = await data.threads.read_multi(thread_ids)
    threads = list(threads_dict.values())
    pairs = []
    for thread in threads:
        for s in thread.salesforce_record_subscriptions:
            pairs.append((thread.id, s.subscription_id,))

    cached_ids = {}
    results = []
    for tid, sid in sorted(pairs):
        thread = threads_dict[tid]
        s = next((sub for sub in thread.salesforce_record_subscriptions if sub.subscription_id == sid), None)
        if not s:
            print(f"thread: {tid}, missing sub: {sid}")
            continue
        if not s.enabled and enabled_only:
            continue

        cache = await lib.sfdc_record_subscriptions.get_record_ids_for_subscription(sid)
        rs = s.record_space
        sinfo = None
        if rs.HasField("report_record_space"):
            print(f"REPORT thread: {thread.title}({tid}), sid: {sid}({len(cache)}), report_id: {rs.report_record_space.report_id}, enabled: {s.enabled}, time: {core.utc.datetime_from_usec(rs.report_record_space.cache_updated_usec)}")
            sinfo = {
                "type": "report",
                "thread_title": thread.title,
                "thread_id": tid,
                "subscription_id": sid,
                "cached_record_count": len(cache),
                "collection_id": rs.report_record_space.report_id,
                "enabled": s.enabled,
                "cache_updated": core.utc.datetime_from_usec(rs.report_record_space.cache_updated_usec),
                "updated": core.utc.datetime_from_usec(s.updated_usec),
                "created": core.utc.datetime_from_usec(s.created_usec),
            }
        elif rs.HasField("listview_record_space"):
            print(f"LISTVIEW thread: {thread.title}({tid}), sid: {sid}({len(cache)}), listview_id: {rs.listview_record_space.listview_id}, enabled: {s.enabled}, time: {core.utc.datetime_from_usec(rs.listview_record_space.cache_updated_usec)}")
            sinfo = {
                "type": "listview",
                "thread_title": thread.title,
                "thread_id": tid,
                "subscription_id": sid,
                "cached_record_count": len(cache),
                "collection_id": rs.listview_record_space.listview_id,
                "enabled": s.enabled,
                "cache_updated": core.utc.datetime_from_usec(rs.listview_record_space.cache_updated_usec),
                "updated": core.utc.datetime_from_usec(s.updated_usec),
                "created": core.utc.datetime_from_usec(s.created_usec),
            }
        elif rs.HasField("explicit_record_space"):
            print(f"EXPLICIT thread: {thread.title}({tid}), sid: {sid}({len(cache)}), enabled: {s.enabled}")
            sinfo = {
                "type": "explicit",
                "thread_title": thread.title,
                "thread_id": tid,
                "subscription_id": sid,
                "cached_record_count": len(cache),
                "collection_id": None,
                "enabled": s.enabled,
                "cache_updated": core.utc.datetime_from_usec(rs.listview_record_space.cache_updated_usec),
                "updated": core.utc.datetime_from_usec(s.updated_usec),
                "created": core.utc.datetime_from_usec(s.created_usec),
            }
        elif rs.HasField("filter_record_space"):
            print(f"FILTER thread: {thread.title}({tid}), sid: {sid}({len(cache)}), enabled: {s.enabled}")
            sinfo = {
                "type": "filter",
                "thread_title": thread.title,
                "thread_id": tid,
                "subscription_id": sid,
                "cached_record_count": len(cache),
                "collection_id": None,
                "enabled": s.enabled,
                "cache_updated": core.utc.datetime_from_usec(rs.listview_record_space.cache_updated_usec),
                "updated": core.utc.datetime_from_usec(s.updated_usec),
                "created": core.utc.datetime_from_usec(s.created_usec),
            }
        else:
            print(f"UNKNOWN tid: {tid}, sub: {s}")
        if sinfo:
            results.append(sinfo)
    return results

async def collect_cdc_cache_keys(new_style=True) -> Set[str]:
    keys = set()
    pattern = "salesforce_cdc_record:0:*:*:*" if new_style else "salesforce_cdc_record:0:??????????????????:??????????????????"
    ch = data.redis_host_map.get_chain_for_shortname("redis-0")
    cursor = b"0"
    while True:
        cursor, results = await ch.scan(cursor, pattern, 100000)
        print(f"cursor: {cursor.decode()}, len(results): {len(results)}, len(keys): {len(keys)}")
        keys.update(set(results))
        if cursor == b"0":
            break
    return keys

async def disable_cdc_consumption(company_id: str, org_id: str) -> proto.teams.Company:
    async with data.task.WriteTask(proto.db.USERS) as task:
        await task.connect_to_id(company_id)
        c = await data.company.read_in_task(task, company_id)
        sub = next((sub for sub in c.salesforce_orgs_data
                    if sub.organization_id == org_id), None)
        sub.enable_cdc_consumption = False
        return await data.company.update(task, c)

async def update_cdc_streams_on_company(
        company_id: str, org_id: str, streams: List[str]) -> proto.teams.Company:
    async with data.task.WriteTask(proto.db.USERS) as task:
        await task.connect_to_id(company_id)
        c = await data.company.read_in_task(task, company_id)
        sub = next((sub for sub in c.salesforce_orgs_data
                    if sub.organization_id == org_id), None)
        del sub.salesforce_cdc_streams[:]
        if streams:
            sub.salesforce_cdc_streams.extend(streams)
        return await data.company.update(task, c)

async def update_org_streams(company_id, org_id="00D000000000062EAA") -> proto.teams.Company:
    if org_id == fitzroy_org_id:
        streams = [
            "/data/AccountChangeEvent",
            "/data/LeadChangeEvent",
            "/data/OpportunityChangeEvent",
            # "/data/AgentWork__cChangeEvent",  # Quote
        ]
    elif org_id == isotope_org_id:
        streams = [
            "/data/Feature__ChangeEvent",
            "/data/Issue__ChangeEvent",
            "/data/Team__ChangeEvent",
            "/data/Project__ChangeEvent",
            "/data/Milestone__ChangeEvent",
        ]
    elif org_id == org62_org_id:
        streams = [
            "/data/OpportunityChangeEvent",
            "/data/LeadChangeEvent",
            "/data/AccountChangeEvent",
            "/data/CaseChangeEvent",
            "/data/ContactChangeEvent",
            "/data/ContractChangeEvent",
            "/data/ContractLineItemChangeEvent",
            # "/data/sfbase__SalesforceTeam__ChangeEvent",  # Salesforce Team
            # "/data/Apttus_Proposal__Proposal__ChangeEvent",  # Quote
            "/data/UserChangeEvent",
        ]
    else:
        raise Exception("Unrecognized id")
    return await update_cdc_streams_on_company(company_id, org_id, streams)

import lite.git
def order_commit_hashes(cherrypicks: Set[str]) -> List[str]:
    args = ["git", "log", "-n2000", "--format=format:%H"]
    commits = lite.git._run(args).split("\n")
    try:
        indexed_picks = [(commits.index(cp), cp) for cp in cherrypicks]
        picks = list(reversed(sorted(indexed_picks)))
    except Exception as exc:
        print(exc)
        return None
    return [cpt[1] for cpt in picks]
