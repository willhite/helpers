import core.http
import core.io
import data.users
import datetime
import json
import lib.integrations
import logging
import proto
import services

admin_company = None
admin_user = None
last_result = None


class StubResponse(core.http.HTTPServerResponse):
    def __init__(self, connection: "core.io.Socket") -> None:
        core.http.HTTPServerResponse.__init__(self, connection)

    async def write_json(self, value, **kwargs):
        global last_result
        last_result = value
        print(json.dumps(value, indent=4))
        return

    async def write(self, chunk, done=True, log_message=None, enable_etag=True, subcategory=None):
        print("len(chunk):", len(chunk))
        return


async def initwork(company=None):
    global admin_company, admin_user

    if company == "fb-testing":
        user_email = "gorle@thefacebook.com"  # Sandeep -- fYOAEAoMJRe
        # admin_user_email = "fbsupport@onna.com"  # Vikas -- QeLAEAB83er
        admin_user_email = "equip@atlasense.com"  # Onna Dev Team -- GLLAEAzEEU6
        admin_company_domain = "onna.com"  # Quip-Fb-Testing -- LeWAcAlW86u
    elif company == "fb":
        user_email = "khajanchi@fb.com"  # Vikas -- QeLAEAB83er
        admin_user_email = "bijnens@fb.com"  # Bruno -- aBSAEAbDgCI
        admin_company_domain = "fb.com"  # fb.com -- TXZAcAZ6Xs4
    else:  # docker
        user_email = "huy@quip.com"  # Huy Nguyen -- eJHAEATIfS7
        admin_user_email = "dwillhite@quip.com"  # Dan -- dKWAEApvV4V
        admin_company_domain = "quip.com"  # quip.com -- FTGAcAzRzdg

    admin_company = (await data.company.get_by_domain(admin_company_domain))[0]
    admin_user = await get_user(admin_user_email, company_id=admin_company.id)
    user = await data.users.get_by_email_login(user_email)


async def set_info(user_id, company_id):
    global admin_company, admin_user
    if user_id:
        admin_user = await data.users.read(user_id)
    if company_id:
        admin_company = await data.company.read(company_id)
    return admin_user, admin_company


async def get_user(email, company_id=None):
    if company_id:
        all_users = await data.users.get_multiple_by_email(email)
        users = list(filter(lambda x: x.company_id == company_id, all_users))
        if len(users) > 0:
            return users[0]
    return await data.users.data.users.get_by_email_login(admin_user.id)


async def build_request(user=None, company=None):
    company = company if company else admin_company
    user = user if user else admin_user
    r = core.http.HTTPRequest(remote_address="1.0.0.27", protocol="https")
    platform_source = proto.users.Session.PlatformSource(company_id=company.id, client_id=b"123")
    s = await data.user_sessions.start_session(user, r, None,
        proto.users.Session.PLATFORM, proto.users.Session.PLATFORM_OAUTH,
        platform_source=platform_source)
    r.user = user
    r.session = s
    return r


def admin_company_args(args):
    if args is None:
        args = {}
    if "company_id" not in args:
        args["company_id"] = [admin_company.id]
    return args


async def get_admin_users_endpoint(user_id, req=None, args=None):
    ep = services.platform.AdminUsersGet()
    r = req if req else await build_request()
    if args is None:
        args = {"include_deleted": ["1"], "threads_meta": ["1"]}
    r.arguments = admin_company_args(args)
    res = StubResponse(None)
    await ep.get(r, res, user_id)


async def get_admin_threads_endpoint(thread_id, req=None, args=None):
    ep = services.platform.AdminThreadsGet()
    r = req if req else await build_request()
    r.arguments = admin_company_args(args)
    res = StubResponse(None)
    await ep.get(r, res, thread_id)


async def post_admin_threads_list_endpoint(req=None, args=None):
    ep = services.platform.AdminThreadsList()
    r = req if req else await build_request()
    r.arguments = admin_company_args(args)
    res = StubResponse(None)
    await ep.post(r, res)


async def post_admin_threads_add_members_endpoint(req=None, args=None):
    ep = services.platform.AdminThreadsAddMembers()
    r = req if req else await build_request()
    r.arguments = admin_company_args(args)
    res = StubResponse(None)
    await ep.post(r, res)


async def delete_admin_message_delete_endpoint(req=None, args=None):
    ep = services.platform.AdminMessageDelete()
    r = req if req else await build_request()
    r.arguments = admin_company_args(args)
    res = StubResponse(None)
    await ep.post(r, res)


async def get_admin_blob_endpoint(thread_id, blob_id, req=None, args=None):
    ep = services.platform.AdminBlobGet()
    r = req if req else await build_request()
    r.arguments = admin_company_args(args)
    res = StubResponse(None)
    await ep.get(r, res, thread_id, blob_id)


async def post_admin_quarantine_endpoint(req=None, args=None):
    ep = services.platform.AdminQuarantine()
    r = req if req else await build_request()
    r.arguments = admin_company_args(args)
    res = StubResponse(None)
    await ep.post(r, res)


async def get_threads_endpoint(thread_id, req=None, args=None):
    bg = services.platform.ThreadsGet()
    r = req if req else await build_request()
    r.arguments = args if args is not None else {}
    res = StubResponse(None)
    await bg.get(r, res, thread_id)


async def set_really_scary_features(company_id, switch):
    async with data.task.WriteTask(proto.db.USERS) as task:
        await task.connect_to_id(company_id)
        c = await data.company.read_in_task(task, company_id)
        c.really_enable_scary_audit_features = switch
        await data.company.update(task, c)


# Do this before executing in bin/interactive
# dw = a(data.users.get_by_email_login("dwillhite@quip.com"))
# set_tracer_user(dw)
# # data.users.options.require_user_creation_provisioned = False
# # thread_id = await data.access.lookup_secret_path("KU9aAjA4BP2x")
# thread_id = "fbRAAAkwNPS"
async def add_integration_to_thread(thread_id):
    thread = await data.threads.read(thread_id)
    # tracer_user_id = data.users.get_robot_id(name="bootstrap")
    # tracer_user = await data.users.read(tracer_user_id)
    # core.runtime.set_tracer(data.tcache.build_tracer_from_context(
    #     proto.tracer.Context(loop_name="bootstrap_private_cluster")))
    tracer_user = await core.tracer.get_user()
    t_user_id = tracer_user.id if tracer_user else None
    logging.NOCOMMIT(f"user_id: {t_user_id}")
    integration = await lib.integrations.create_integration(
        tracer_user, thread.id,
        proto.threads.IntegrationEnum.ACCESS_TOKEN)
    integration_user = await data.users.read(integration.id)
    session = await data.user_sessions.start_session(
        integration_user, None, None, proto.users.Session.PLATFORM,
        proto.users.Session.PLATFORM_OAUTH,
        length=datetime.timedelta(days=31))
