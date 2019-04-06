import core.http
import core.io
import data.users
import json
import logging
import proto
import services

admin_company = None
admin_user = None

class StubResponse(core.http.HTTPServerResponse):
    def __init__(self, connection: "core.io.Socket") -> None:
        core.http.HTTPServerResponse.__init__(self, connection)

    async def write_json(self, value, **kwargs):
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
        admin_user_email = "equip@atlasense.com" # Onna Dev Team -- GLLAEAzEEU6
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


async def get_admin_users_endpoint(user_id, req=None, args=None):
    ug = services.platform.AdminUsersGet()
    r = req if req else await build_request()
    r.arguments = args if args is not None else {"include_deleted": ["1"], "threads_meta": ["1"], "company_id": [admin_company.id]}
    res = StubResponse(None)
    await ug.get(r, res, user_id)


async def get_admin_threads_endpoint(thread_id, req=None, args=None):
    tg = services.platform.AdminThreadsGet()
    r = req if req else await build_request()
    r.arguments = args if args is not None else {"company_id": [admin_company.id]}
    res = StubResponse(None)
    await tg.get(r, res, thread_id)


async def post_admin_threads_list_endpoint(req=None, args=None):
    tg = services.platform.AdminThreadsList()
    r = req if req else await build_request()
    r.arguments = args if args is not None else {"company_id": [admin_company.id]}
    res = StubResponse(None)
    await tg.post(r, res)


async def get_admin_blob_endpoint(thread_id, blob_id, req=None, args=None):
    bg = services.platform.AdminBlobGet()
    r = req if req else await build_request()
    r
    r.arguments = args if args is not None else {"company_id": [admin_company.id]}
    res = StubResponse(None)
    await bg.get(r, res, thread_id, blob_id)


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
