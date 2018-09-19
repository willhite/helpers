import core.http
import core.io
import data.users
import json
import logging
import proto
import services

admin_company = None
admin_user = None
user = None

# admin_company, admin_user, user = await initwork()


class StubResponse(core.http.HTTPServerResponse):
    def __init__(self, connection: "core.io.Socket") -> None:
        core.http.HTTPServerResponse.__init__(self, connection)

    async def write_json(self, value, **kwargs):
        print(json.dumps(value, indent=4))
        return


async def initwork(company=None):
    global admin_company, admin_user, user

    if company == "Quip-Fb-Testing":
        user_email = "gorle@thefacebook.com"  # Sandeep -- fYOAEAoMJRe
        admin_user_email = "khajanchi@fb.com"  # Vikas -- QeLAEAB83er
        admin_company_domain = "onna.com"  # Quip-Fb-Testing -- LeWAcAlW86u
    elif company == "fb":
        user_email = "khajanchi@fb.com"  # Vikas -- QeLAEAB83er
        admin_user_email = "bijnens@fb.com"  # Bruno -- aBSAEAbDgCI
        admin_company_domain = "fb.com"  # fb.com -- TXZAcAZ6Xs4
    else:  # docker
        user_email = "huy@quip.com"  # Huy Nguyen -- eJHAEATIfS7
        admin_user_email = "dwillhite@quip.com"  # Dan -- dKWAEApvV4V
        admin_company_domain = "quip.com"  # quip.com -- FTGAcAzRzdg

    admin_company = await data.company.get_by_domain(admin_company_domain)
    admin_user = await data.users.get_by_email_login(admin_user_email)
    user = await data.users.get_by_email_login(user_email)


async def get_user(email):
    return await data.users.data.users.get_by_email_login(admin_user.id)


async def get_admin_users_endpoint(user_id):
    ug = services.platform.AdminUsersGet()
    r = core.http.HTTPRequest(remote_address="1.0.0.27", protocol="https")
    platform_source = proto.users.Session.PlatformSource(company_id=admin_company.id, client_id=b"123")
    s = await data.user_sessions.start_session(admin_user, r, None,
        proto.users.Session.PLATFORM, proto.users.Session.PLATFORM_OAUTH,
        platform_source=platform_source)
    r.user = admin_user
    r.session = s
    r.arguments = {"include_deleted": ["1"], "threads_meta": ["112"], "company_id": [admin_company.id]}
    res = StubResponse(None)
    await ug.get(r, res, user_id)


async def get_admin_threads_endpoint(thread_id):
    tg = services.platform.AdminThreadsGet()
    r = core.http.HTTPRequest(remote_address="1.0.0.27", protocol="https")
    platform_source = proto.users.Session.PlatformSource(company_id=admin_company.id, client_id=b"123")
    s = await data.user_sessions.start_session(admin_user, r, None,
        proto.users.Session.PLATFORM, proto.users.Session.PLATFORM_OAUTH,
        platform_source=platform_source)
    r.user = admin_user
    r.session = s
    r.arguments = {"company_id": [admin_company.id]}
    res = StubResponse(None)
    await tg.get(r, res, thread_id)


async def set_really_scary_features(company_id, switch):
    logging.NOCOMMIT("company_id: %s, switch: %s", company_id, switch)
    async with data.task.WriteTask(proto.db.USERS) as task:
        await task.connect_to_id(company_id)
        c = await data.company.read_in_task(task, company_id)
        c.really_enable_scary_audit_features = switch
        await data.company.update(task, c)
