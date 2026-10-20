import os, time, re, json, hashlib, hmac, logging, urllib.parse, threading, random, uuid
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, send_file, redirect, Response
import requests as http_requests

APP_ID       = os.environ.get("LARK_APP_ID")
APP_SECRET   = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://samurai-portal.vercel.app/api/callback")

UPSTASH_REDIS_REST_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
REDIS_ENABLED = bool(UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN)

# Bearer-token auth for the external cron-job.org caller that drives
# /api/sync/refresh every 30 minutes -- separate from the admin-user session
# check already on that route, since an unattended cron job has no Feishu user
# session to present. See is_cron_authorized().
CRON_SECRET = os.environ.get("CRON_SECRET", "")
REDIS_MAX_VALUE_BYTES = 900_000

MOCK_MODE = not bool(APP_ID and APP_SECRET)

# APP_VERSION changes automatically on every new deploy: Vercel sets
# VERCEL_GIT_COMMIT_SHA (falls back to VERCEL_DEPLOYMENT_ID, then to this
# process's own start time for local/dev runs where neither is set -- so at
# minimum a server restart still counts as "the code changed"). The frontend
# polls /api/app-version and forces a re-login the moment this value no longer
# matches what a session logged in under, so nobody keeps running against a
# deploy the server has already moved past.
APP_VERSION = (os.environ.get("VERCEL_GIT_COMMIT_SHA") or os.environ.get("VERCEL_DEPLOYMENT_ID")
               or f"local-{int(time.time())}")

BASE_ID           = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID   = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID   = "tbl3wweYCpmDmDSx"
ROSTER_TABLE_ID   = "tblvA2Y4Z4j7Jwbr"   # Agent Roster (Phase 11 presence system)
AUDIT_TABLE_ID    = os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB")
FORM_ID = os.environ.get("LARK_FORM_ID", "shrcnKiM5Nd35n4nP0IP2o1Y8Lb")
ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954']

# BUG FIX (silent identity loss): submit_request() used to write the real submitter's
# name into a field called "Submitted By". That field does NOT exist anywhere in the
# live Feishu schema (confirmed against the table's actual field list) -- so every
# single submission silently dropped it in the "actual_fields" whitelist filter further
# down, and it never reached Feishu at all. That's why the fallback that was supposed
# to reliably show who really submitted a ticket (for agents whose token isn't allowed
# to create records under their own identity, so "Respondents" gets stamped with the
# app/tenant identity instead) never actually worked -- there was nothing to fall back
# to. To fix this for real, add a plain Text column to the Requests table in Feishu
# (any name is fine) and put that exact name here. Until that column exists, this
# still degrades gracefully: get_table_schema() drops the key and logs a warning
# (see submit_request), so nothing crashes, but attribution stays unreliable.
SUBMITTED_BY_FIELD_NAME = "Submitted By"

PK_ACMS = {"nabeel","hasseb","haseeb","enzo","farooq","mubeen","cruz","ehtisham",
            "usama","sehar ch","hamza malik","zohaib","eagle","leo","berlin"}
IN_ACMS  = {"holy","vihan","shivam","rudra","ravikant","ansh","rocky","bella"}

CACHE_TTL_REALTIME   = 300    
CACHE_TTL_HISTORICAL = 3600   

RATE_LIMIT_SEARCH    = (50, 60)
RATE_LIMIT_ANALYTICS = (30, 60)
RATE_LIMIT_RECORDS   = (50, 60)
# Phase 15 (Ahmed: "make it live and fast and cheap ... do not make it consume
# my plan"): the bell now polls every 5s and an open thread every 3s (Miaoda's
# Option-A live feel) — the comment routes read the Portal Comments mirror
# table (ONE indexed Feishu records/search, names pre-resolved inside the row
# payload) instead of touching Redis name caches per id, so each poll costs
# ~1-3 Redis commands TOTAL. A dedicated limiter keeps the faster cadence from
# tripping the 50/min records budget shared with the rest of the portal.
RATE_LIMIT_COMMENTS  = (150, 60)

COINS_MULTIPLIER = 100000

QUERY_FIELD_ALIASES = {
    "user_id":     ["User ID"],
    "numbering":   ["Numbering"],
    "otherapp_id": ["Otherapp ID", "Otherapp Name", "Other App ID"],
    "nid_number":  ["NID Number", "NID"],
    "bd_code":     ["Bd Code", "BD Code"],
    # Added for the Audit tab's "Check Duplicate" button (auditCheckDuplicates()
    # in the frontend) -- it fans out one /api/query call per filled identity
    # field, Agency Code included, so this needs its own alias entry here.
    "agency_code": ["Agency Code"],
}

EXCLUDED_SUBMIT_FIELDS = {
    "Numbering", "Submitted on", "Submitted on Copy", "Match ID", "Record ID Text",
    "Cleaned User ID", "Bot Color", "Bot Title", "Bot Message", "Ticket Details",
    "Duplicated Check", "Handle Time (Seconds)", "Base Points", "Formula",
    "Point Balance", "time of the requests", "Created By", "Webhook Lookup",
    "Mention this Group", "BD Nickname1", "BD Nickname2", "Respondents", "Lock Owner",
    "Assigned Member", "Assigned Time", "Completion Time",
    "Last Retry Time", "Ready to Archive", "Reward", "Approval", "Request Status"
}

# Update fields allows auditors to edit Status, Approval, Reject Reason, etc.
# BUG FIX (still applies): "Assigned Member" and "Lock Owner" are Feishu User-type
# fields that stay system-managed (assignment happens via Pull My Ticket, not by
# hand), so writing plain text to them still gets rejected by Feishu with
# "UserFieldConvFail" -- keep those excluded.
#
# "Done by" and "Mentioned Person" are ALSO User-type fields, but they're now
# picked from a real member-search control (see #tw-*-picker in the frontend and
# /api/members/search) that sends proper `[{"id": open_id}]` objects instead of
# plain text, so Feishu accepts the write -- they're deliberately no longer
# excluded here.
#
# "Status" is ALSO deliberately no longer excluded: the Audit tab's "Audition
# Action" card lets the agent set Status/Mentioned Group/Done by/Audition note
# at creation time (this ticket type is filed already-resolved), and only that
# form ever renders a "Status" field to submit in the first place.
EXCLUDED_UPDATE_FIELDS = {
    "Numbering", "Submitted on", "Submitted on Copy", "Match ID", "Record ID Text",
    "Cleaned User ID", "Bot Color", "Bot Title", "Bot Message", "Ticket Details",
    "Duplicated Check", "Handle Time (Seconds)", "Base Points", "Formula",
    "Point Balance", "time of the requests", "Created By", "Webhook Lookup",
    "Mention this Group", "BD Nickname1", "BD Nickname2", "Respondents", "Lock Owner",
    "Assigned Member",
    "Assigned Time", "Completion Time", "Last Retry Time", "Ready to Archive", "Reward"
}

# ════════════════════════════════════════════════════════════════════
# AGENCY EXCHANGE (target realization board) — config
# ════════════════════════════════════════════════════════════════════
# How many days of daily point-total snapshots we keep in Redis for the "Movers"
# (gainers/decliners) panel. Snapshots are tiny (agency_code -> total_points), one
# per day, so this is cheap even at a few hundred agencies.
EXCHANGE_HISTORY_MAX_DAYS = 35
REDIS_KEY_EXCHANGE_HISTORY_PREFIX = "xena:exchange:hist:"
REDIS_KEY_EXCHANGE_HISTORY_INDEX  = "xena:exchange:hist:index"
REDIS_KEY_EXCHANGE_LAST_SNAPSHOT  = "xena:exchange:last_snapshot_date"
REDIS_KEY_WATCHLIST_PREFIX        = "xena:exchange:watchlist:"

MONTHLY_ALLOCATOR_LIMITS = {
    "trend card": 10, "traffic card": 50, "30 mic 15 days": 999, "30 mic 30 days": 999,
    "normal short id ( 2 levels above ) 15 days": 999, "normal short id ( 2 levels above ) 30 days": 999,
    "customized short id 15 days": 999, "customized short id 30 days": 999,
    "room pin-up": 999, "welcome package 3": 15, "welcome package 2": 50,
}
ORDER_TYPE_LIMITS = {
    "main page banner": 3, "news banner": 5, "live banner": 5, "splash": 10,
}

# Full price list behind /api/agency-points/charge -- a 1:1 copy of the JS `db`
# inside the Base workflow's "Agency point calculator" handler (same order,
# because rule lookup is first-substring-match, same prices, same limits, same
# per-request "order" flag). If the sheet handler's db ever changes, change it
# here too -- the portal and the sheet MUST charge identically.
POINT_PRICE_DB = [
    {"key": "trend card", "same": 40, "other": 50, "limit": 10},
    {"key": "traffic card", "same": 30, "other": 40, "limit": 50},
    {"key": "30 mic 15 days", "same": 150, "other": 175, "limit": 999},
    {"key": "30 mic 30 days", "same": 300, "other": 350, "limit": 999},
    {"key": "normal short id ( 2 levels above ) 15 days", "same": 120, "other": 180, "limit": 999},
    {"key": "normal short id ( 2 levels above ) 30 days", "same": 240, "other": 360, "limit": 999},
    {"key": "customized short id 15 days", "same": 300, "other": 450, "limit": 999},
    {"key": "customized short id 30 days", "same": 1200, "other": 1800, "limit": 999},
    {"key": "room pin-up", "same": 100, "other": 150, "limit": 999},
    {"key": "welcome package 3", "same": 80, "other": 80, "limit": 15},
    {"key": "welcome package 2", "same": 30, "other": 30, "limit": 50},
    {"key": "golden lion package", "same": 50, "other": 50, "limit": 999},
    {"key": "white lion package", "same": 50, "other": 50, "limit": 999},
    {"key": "pink lion package", "same": 50, "other": 50, "limit": 999},
    {"key": "green warrior package", "same": 50, "other": 50, "limit": 999},
    {"key": "lion king package", "same": 50, "other": 50, "limit": 999},
    {"key": "pink love package", "same": 50, "other": 50, "limit": 999},
    # Banners: strictly PER REQUEST limits, tracked monthly -- kept at the END
    # exactly like the JS db, first-match order matters.
    {"key": "main page banner", "same": 150, "other": 150, "limit": 3, "type": "order"},
    {"key": "news banner", "same": 100, "other": 100, "limit": 5, "type": "order"},
    {"key": "live banner", "same": 150, "other": 150, "limit": 5, "type": "order"},
    {"key": "splash", "same": 1500, "other": 2000, "limit": 10, "type": "order"},
]

# ════════════════════════════════════════════════════════════════════
# CORE UTILITIES & TIMEZONE MANAGEMENT
# ════════════════════════════════════════════════════════════════════
CAIRO_OFFSET = timedelta(hours=3)

def cairo_now():
    return datetime.now(timezone.utc) + CAIRO_OFFSET

def cairo_epoch_ms(cairo_dt):
    """Converts a datetime produced by cairo_now()/derived from it (aware, but
    deliberately mislabeled tzinfo=utc while its wall-clock value is actually Cairo
    local time) back into the real, true UTC epoch millisecond it represents.
    Undoing CAIRO_OFFSET before calling .timestamp() -- on an *aware* datetime, so
    this is pure explicit arithmetic and never depends on the host machine's own
    local timezone setting -- is what makes this safe to feed straight to Feishu as
    a real epoch ms value."""
    return int((cairo_dt - CAIRO_OFFSET).timestamp() * 1000)

def local_day_window_ms(prior_days, tz_offset_min=180):
    """Returns (from_ms, to_ms_exclusive) -- exact UTC epoch-ms boundaries of the
    VIEWER'S local calendar-day window, correct for any timezone on Earth.
    tz_offset_min is minutes EAST of UTC (e.g. Cairo=+180, PK=+300, IN=+330)."""
    tz = timezone(timedelta(minutes=tz_offset_min))
    now_local = datetime.now(timezone.utc).astimezone(tz)
    midnight_today = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    from_dt = midnight_today - timedelta(days=prior_days)
    to_dt = midnight_today + timedelta(days=1)
    return int(from_dt.timestamp() * 1000), int(to_dt.timestamp() * 1000)

def _extract_epoch_ms(date_val):
    """Extract the true UTC epoch-ms from a Feishu date/datetime field value, or 0."""
    if not date_val:
        return 0
    if isinstance(date_val, list) and date_val:
        date_val = date_val[0]
    if isinstance(date_val, dict):
        date_val = date_val.get('value', date_val.get('text', ''))
    try:
        return int(float(date_val)) if date_val else 0
    except (TypeError, ValueError):
        return 0

def _safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

_token_cache = {"token": None, "expires_at": 0, "lock": threading.Lock()}

# SPEED FIX (Live Ticket Queue feels slow to open a ticket / run duplicate check):
# every http_requests.get/post/put call to Feishu was opening a brand new TCP+TLS
# connection from scratch, even on a warm serverless instance handling back-to-back
# requests to the same open.feishu.cn host. requests.Session() pools and reuses
# connections across calls on the same warm instance (the same pattern already
# proven out in /api/my-requests's own Session), cutting handshake latency on the
# hot paths below without changing any request/response behavior -- .get/.post/.put
# on a Session have the exact same signature and return value as the bare module
# functions, so this is purely a performance change.
feishu_session = http_requests.Session()
_TOKEN_REDIS_KEY = "xena:tenant_access_token"

def get_tenant_access_token():
    """
    ISSUE 1 FIX (Ticket Open Speed): the tenant access token used to live only in an
    in-process dict, which is wiped every time Vercel spins up a fresh serverless
    instance (cold start). That forced a full extra round-trip to Feishu's auth
    endpoint on almost every request -- one of the two sequential network calls behind
    every single ticket open. We now check, in order: (1) this warm instance's memory
    (fastest, ~0ms), (2) Upstash Redis (survives cold starts, ~10-30ms), and only fall
    back to actually calling Feishu's auth API when both are empty/expired.
    """
    if MOCK_MODE: return "mock_tenant_token_12345"

    with _token_cache["lock"]:
        if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
            return _token_cache["token"]

    if REDIS_ENABLED:
        cached = redis_get_json(_TOKEN_REDIS_KEY)
        if cached and time.time() < cached.get("expires_at", 0):
            with _token_cache["lock"]:
                _token_cache["token"] = cached["token"]
                _token_cache["expires_at"] = cached["expires_at"]
            return cached["token"]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = http_requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10).json()
    token = resp.get("tenant_access_token")
    expire = resp.get("expire", 7200)
    expires_at = time.time() + max(expire - 300, 60)

    with _token_cache["lock"]:
        _token_cache["token"] = token
        _token_cache["expires_at"] = expires_at

    if REDIS_ENABLED and token:
        redis_set_json(_TOKEN_REDIS_KEY, {"token": token, "expires_at": expires_at}, ttl=max(expire - 300, 60))

    return token

_app_token_cache = {"token": None, "expires_at": 0, "lock": threading.Lock()}
def get_app_access_token():
    """App-level access token, used ONLY to renew user access tokens (Feishu's
    refresh_access_token endpoint requires it in the Authorization header)."""
    if MOCK_MODE:
        return "mock_app_token"
    with _app_token_cache["lock"]:
        if _app_token_cache["token"] and time.time() < _app_token_cache["expires_at"]:
            return _app_token_cache["token"]
    resp = http_requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal",
        json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=15).json()
    token = resp.get("app_access_token")
    if not token:
        logger.error("app_token_fetch_failed", resp=resp)
        return None
    with _app_token_cache["lock"]:
        _app_token_cache["token"] = token
        _app_token_cache["expires_at"] = time.time() + int(resp.get("expire", 7200)) - 300
    return token
_refresh_lock = threading.Lock()
def _do_refresh_exchange(open_id, rt):
    """One Feishu refresh_access_token round. Returns (new_uat, new_rt,
    expires_in) or (None, None, None) on any failure."""
    app_token = get_app_access_token()
    if not app_token:
        return None, None, None
    try:
        resp = http_requests.post(
            "https://open.feishu.cn/open-apis/authen/v1/refresh_access_token",
            headers={"Authorization": f"Bearer {app_token}", "Content-Type": "application/json"},
            json={"grant_type": "refresh_token", "refresh_token": rt}, timeout=15).json()
    except Exception as e:
        logger.warn("uat_refresh_request_failed", open_id=open_id[-6:], error=str(e))
        return None, None, None
    data = resp.get("data") or {}
    new_uat = data.get("access_token")
    new_rt = data.get("refresh_token")
    if not new_uat:
        logger.warn("uat_refresh_rejected", open_id=open_id[-6:],
                    code=resp.get("code"), msg=resp.get("msg"))
        return None, None, None
    return new_uat, new_rt, int(data.get("expires_in", 7200))

def _refresh_user_token(open_id):
    """Exchange the stored OAuth refresh_token for a fresh user access token (uat).
    Keeps 'Respondents' stamped with the agent instead of the app on submits made
    hours after login. Returns the new uat, or None (caller falls back to tenant)."""
    if not open_id or not REDIS_ENABLED or MOCK_MODE:
        return None
    rt = redis_cmd("GET", f"xena:refresh_token:{open_id}")
    if not rt:
        return None
    with _refresh_lock:
        rt = redis_cmd("GET", f"xena:refresh_token:{open_id}") or rt
        new_uat, new_rt, expires_in = _do_refresh_exchange(open_id, rt)
        if not new_uat:
            # Phase 10.44: CROSS-PROCESS RACE. Vercel runs many serverless
            # invocations; two concurrent refreshes both read rt=A, the first
            # rotates it to B and Feishu INVALIDATES A on the spot -- the
            # second invocation's exchange then fails with invalid_grant and
            # used to return None here, which the callers read as "session
            # dead" and forced a mid-shift logout. If Redis now holds a
            # DIFFERENT rt, another instance already refreshed: reuse the
            # fresh uat it stored, or retry once with the newer rt.
            latest_rt = redis_cmd("GET", f"xena:refresh_token:{open_id}")
            if latest_rt and latest_rt != rt:
                cached = redis_cmd("GET", f"xena:uat:{open_id}")
                if cached:
                    logger.info("uat_refresh_race_reuse", open_id=open_id[-6:])
                    return cached
                new_uat, new_rt, expires_in = _do_refresh_exchange(open_id, latest_rt)
                if new_uat:
                    logger.info("uat_refresh_race_retried", open_id=open_id[-6:])
        if not new_uat:
            return None
        uat_ttl = max(expires_in - 300, 60)
        redis_cmd("SET", f"xena:uat:{open_id}", new_uat, "EX", uat_ttl)
        if new_rt:
            # Phase 10.14 (Ahmed): the refresh token used to be kept for only
            # 4h, which killed every session at the 4-hour mark even while the
            # agent was actively working ("the token expire so fast ... expire
            # when submit request"). Feishu's own refresh_token lives ~30
            # days; we now keep it for 24h (renewed on every refresh), which
            # comfortably covers the 12h client-side session limit -- submits
            # hours into a shift refresh silently instead of dying.
            redis_cmd("SET", f"xena:refresh_token:{open_id}", new_rt, "EX", 24 * 3600)
            # Phase 10.44: keep the open_id->email binding alive alongside the
            # refresh chain so /api/auth/refresh-session can keep verifying
            # the caller for as long as the chain itself lives.
            em = redis_cmd("GET", f"xena:uid_email:{open_id}")
            if em:
                redis_cmd("SET", f"xena:uid_email:{open_id}", em, "EX", 24 * 3600)
        return new_uat
def _get_cached_uat(open_id):
    """Freshest server-side user token for this agent, if we hold one (set at login
    and on every silent refresh). Lets the backend use a newer token than the
    browser's stale localStorage copy."""
    if not open_id or not REDIS_ENABLED or MOCK_MODE:
        return None
    return redis_cmd("GET", f"xena:uat:{open_id}")

_schema_cache = {"data": {}, "lock": threading.Lock()}
_SCHEMA_REDIS_TTL = 300

def get_table_schema(table_id, token, base_id, ttl=300):
    """
    ISSUE 1 FIX (Ticket Open Speed): same cold-start problem as the token cache above --
    this schema lookup (used to filter out read-only fields before a save/submit) used
    to hit Feishu's /fields endpoint on every cold instance. Now backed by Redis so a
    warm schema survives across serverless instances, not just within one process.
    """
    with _schema_cache["lock"]:
        cached = _schema_cache["data"].get(table_id)
        if cached and time.time() - cached["ts"] < ttl:
            return cached["fields"]

    redis_key = f"xena:schema:{table_id}"
    if REDIS_ENABLED:
        cached_fields = redis_get_json(redis_key)
        if cached_fields is not None:
            fields = set(cached_fields)
            with _schema_cache["lock"]:
                _schema_cache["data"][table_id] = {"fields": fields, "ts": time.time()}
            return fields

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/fields"
    try:
        resp = http_requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15).json()
        if resp.get("code") == 0:
            fields = set(f.get("field_name") for f in resp.get("data", {}).get("items", []))
            with _schema_cache["lock"]:
                _schema_cache["data"][table_id] = {"fields": fields, "ts": time.time()}
            if REDIS_ENABLED:
                redis_set_json(redis_key, list(fields), ttl=_SCHEMA_REDIS_TTL)
            return fields
    except Exception as e:
        logger.error("get_table_schema_failed", table_id=table_id, error=str(e))
    with _schema_cache["lock"]:
        cached = _schema_cache["data"].get(table_id)
        return cached["fields"] if cached else set()

_field_types_cache = {"data": {}, "lock": threading.Lock()}
_FIELD_TYPES_REDIS_TTL = 300

def get_field_ui_types(table_id, token, base_id, ttl=300):
    """
    BUG FIX (NumberFieldConvFail): Feishu Number-type fields (Base Points, Counter,
    Point Balance, Quantities Input, Handle Time (Seconds), etc.) reject an empty
    string "" -- but every plain <input type="number"> that the user left untouched
    submits "" as its value, and update_request()/submit_request() were forwarding
    that straight through to Feishu's PUT, which rejected the whole save with
    "NumberFieldConvFail". To fix this at the source we need to know which fields are
    actually Number-typed, so we can drop/convert them before the write -- this reuses
    Feishu's /fields endpoint (cached the same way get_table_schema is, Redis-backed so
    it survives cold starts) but keeps the per-field "ui_type" string instead of just
    the name.
    """
    with _field_types_cache["lock"]:
        cached = _field_types_cache["data"].get(table_id)
        if cached and time.time() - cached["ts"] < ttl:
            return cached["types"]

    redis_key = f"xena:fieldtypes:{table_id}"
    if REDIS_ENABLED:
        cached_types = redis_get_json(redis_key)
        if cached_types is not None:
            with _field_types_cache["lock"]:
                _field_types_cache["data"][table_id] = {"types": cached_types, "ts": time.time()}
            return cached_types

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/fields"
    try:
        resp = http_requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15).json()
        if resp.get("code") == 0:
            types = {}
            for f in resp.get("data", {}).get("items", []):
                name = f.get("field_name")
                # Fall back to the numeric "type" code (2 == Number) if "ui_type" isn't
                # present on this API version, so this still works either way.
                ui_type = f.get("ui_type") or ("Number" if f.get("type") == 2 else None)
                if name and ui_type:
                    types[name] = ui_type
            with _field_types_cache["lock"]:
                _field_types_cache["data"][table_id] = {"types": types, "ts": time.time()}
            if REDIS_ENABLED:
                redis_set_json(redis_key, types, ttl=_FIELD_TYPES_REDIS_TTL)
            return types
    except Exception as e:
        logger.error("get_field_ui_types_failed", table_id=table_id, error=str(e))
    with _field_types_cache["lock"]:
        cached = _field_types_cache["data"].get(table_id)
        return cached["types"] if cached else {}

_NUMERIC_UI_TYPES = {"Number", "Currency", "Progress", "Rating"}

# BUG FIX: "make sure you did not try to update automatic/filled/formula fields" --
# these ui_types are all system-computed by Feishu itself (formulas, lookups, rollups,
# auto-numbering, created/modified stamps). Writing to any of them is always rejected
# by Feishu regardless of value, so they're stripped from every update/submit payload
# by *type*, not just by a hand-maintained name list (EXCLUDED_UPDATE_FIELDS /
# EXCLUDED_SUBMIT_FIELDS still apply too, as a second, independent line of defense --
# useful for fields that are writable in Feishu but that we simply never want agents
# editing directly, like "Numbering").
READONLY_UI_TYPES = {
    "Formula", "Lookup", "Rollup", "AutoNumber",
    "CreatedTime", "ModifiedTime", "CreatedUser", "ModifiedUser",
    "DuplexLink", "GroupChat",
}

def strip_readonly_fields(fields, field_types):
    """Drops any key whose live ui_type marks it as Feishu-computed/automatic."""
    dropped = [k for k in fields if field_types.get(k) in READONLY_UI_TYPES]
    if dropped:
        logger.warn("stripped_readonly_fields_before_write", fields=dropped)
    return {k: v for k, v in fields.items() if field_types.get(k) not in READONLY_UI_TYPES}

def strip_invalid_attachment_fields(fields, field_types):
    """
    BUG FIX (AttachFieldConvFail): Feishu's Attachment-type fields only accept a list
    of {"file_token": "..."} objects -- never null, an empty string, or a plain string.
    A stale/legacy frontend payload (e.g. an attachment field echoed back as its old
    display text instead of being omitted) was slipping through update_request()'s
    other filters and crashing the whole write. This drops any Attachment-typed key
    whose value isn't already a well-formed token list, leaving the existing
    attachment on that record untouched (rather than trying to convert/clear it).
    """
    cleaned, dropped = {}, []
    for k, v in fields.items():
        if field_types.get(k) == "Attachment":
            if isinstance(v, list) and v and all(isinstance(x, dict) and x.get("file_token") for x in v):
                cleaned[k] = v
            else:
                dropped.append(k)
            continue
        cleaned[k] = v
    if dropped:
        logger.warn("stripped_invalid_attachment_fields", fields=dropped)
    return cleaned

def strip_invalid_user_fields(fields, field_types, actor=None, audit_on_bad_submitted_by=True):
    """
    BUG FIX (UserFieldConvFail on Create New Request): Feishu's User/Person-type
    fields only accept a list of {"id": open_id} objects (see EDITABLE_USER_FIELDS /
    twRenderMemberPicker in the frontend for "Done by" and "Mentioned Person") --
    never a plain display-name string. If ANY field the live schema reports as
    ui_type "User" shows up in the outgoing payload with a plain string (or anything
    else that isn't already a well-formed [{"id": ...}] list), Feishu rejects the
    *entire* write with "System Patch failed: UserFieldConvFail" -- which is exactly
    what was happening on every "Create New Request" submission.
    Root cause: SUBMITTED_BY_FIELD_NAME ("Submitted By") is written as a plain
    display-name string by design (see submit_request()) on the assumption that it's
    a Text column in Feishu. If that column was actually created as a Person/User
    field in the live base instead of Text, every single submission would hit this
    exact error, since final_fields[SUBMITTED_BY_FIELD_NAME] = user is always a bare
    string. Rather than let that take down the whole submission, drop just that field
    and log/audit it loudly so it's obvious the Feishu column type needs fixing --
    same "fail soft, don't fail loud" approach as strip_invalid_attachment_fields().
    """
    cleaned, dropped = {}, []
    for k, v in fields.items():
        if field_types.get(k) == "User":
            well_formed = isinstance(v, list) and v and all(isinstance(x, dict) and x.get("id") for x in v)
            if well_formed:
                cleaned[k] = v
            else:
                dropped.append(k)
            continue
        cleaned[k] = v
    if dropped:
        logger.warn("stripped_invalid_user_fields", fields=dropped)
        if audit_on_bad_submitted_by and SUBMITTED_BY_FIELD_NAME in dropped:
            audit.log(actor or "system", "SUBMITTED_BY_WRONG_FIELD_TYPE",
                      f"'{SUBMITTED_BY_FIELD_NAME}' is a Feishu User/Person field, not "
                      f"Text -- writes to it as a plain name always fail with "
                      f"UserFieldConvFail. Change this column's type to Text in Feishu "
                      f"to fix both this and slow/incomplete My Recent Requests.",
                      severity="Warning")
    return cleaned

def coerce_number_fields(fields, field_types):
    """
    For every field the live schema says is Number-like: drop it entirely if the
    incoming value is blank/None (so the write just leaves that cell alone instead of
    trying to convert "" into a number), and convert numeric-looking strings into a
    real int/float since Feishu's Number fields want an actual JSON number, not text.
    Anything that can't be parsed as a number is dropped rather than sent, so one bad
    field can't take down the whole save.
    """
    cleaned = {}
    for k, v in fields.items():
        ui_type = field_types.get(k)
        if ui_type in _NUMERIC_UI_TYPES:
            if v is None or (isinstance(v, str) and v.strip() == ""):
                continue  # leave the cell untouched rather than sending ""
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                cleaned[k] = v
                continue
            try:
                num = float(str(v).replace(",", "").strip())
                cleaned[k] = int(num) if num.is_integer() else num
            except (ValueError, TypeError):
                logger.warn("dropping_unconvertible_number_field", field=k, value=v)
                continue
        else:
            cleaned[k] = v
    return cleaned

class StructuredLogger:
    def __init__(self, name):
        self._log = logging.getLogger(name)
        if not self._log.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter('%(message)s'))
            self._log.addHandler(h)
            self._log.setLevel(logging.INFO)

    def _emit(self, level, event, **extra):
        record = {"ts": datetime.utcnow().isoformat(), "level": level, "event": event}
        record.update(extra)
        getattr(self._log, level)(json.dumps(record, default=str))

    def info(self, event, **kw):  self._emit("info", event, **kw)
    def warn(self, event, **kw):  self._emit("warning", event, **kw)
    def error(self, event, **kw): self._emit("error", event, **kw)

logger = StructuredLogger("xena")

def mask_email(email):
    if not email or "@" not in email: return email[:3] + "***" if email else ""
    local, domain = email.split("@", 1)
    return local[:2] + "***@" + domain

def mask_name(name):
    if not name: return ""
    parts = name.strip().split()
    return " ".join(p[:1] + "***" if len(p) > 1 else p for p in parts)

# Phase 10.44 (Ahmed: "the token is still expire after 3 hour ... while iam
# working on tkt"): a Redis hiccup (Upstash quota, a cross-border timeout)
# used to be indistinguishable from "session really dead" -- every failing
# GET looked like an empty cache, so one bad minute forced a full logout and
# destroyed half-filled forms. redis_cmd now tracks whether the store itself
# is reachable: an EXCEPTION marks it down, any successful round-trip (even a
# key miss, which returns result=None) marks it healthy. Session-enforcement
# code checks redis_healthy() and FAILS OPEN while the store is unreachable
# instead of logging agents out over infrastructure trouble.
_redis_health = {"down_since": 0.0, "lock": threading.Lock(),
                 "last_error": "", "last_error_ts": 0.0}
REDIS_RETRY_AFTER = 30   # seconds before a known-down store gets one probe call

def _redis_mark_down(err, now):
    with _redis_health["lock"]:
        if not _redis_health["down_since"]:
            _redis_health["down_since"] = now
        _redis_health["last_error"] = str(err)[:300]
        _redis_health["last_error_ts"] = now

def redis_cmd(*args, timeout=8):
    if not REDIS_ENABLED: return None
    now = time.time()
    with _redis_health["lock"]:
        down = _redis_health["down_since"]
        # While the store is known-down, calls are skipped outright (the
        # caller's fallback covers the functionality) -- one probe is allowed
        # through every REDIS_RETRY_AFTER so recovery is picked up.
        if down and (now - down) < REDIS_RETRY_AFTER:
            return None
    try:
        resp = http_requests.post(
            UPSTASH_REDIS_REST_URL,
            headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
            json=list(args), timeout=timeout
        )
        data = resp.json()
        result = data.get("result")
        # Phase 17: an Upstash RESPONSE-level error (daily quota exceeded, bad
        # args -- {"result": null, "error": "..."}) used to be
        # indistinguishable from a key miss, so a dead store failed silently
        # forever (the production outage behind Ahmed's "no names, 15s loads,
        # empty mirror table" report). Note: SET-NX declining a held lock also
        # returns result=None but carries NO error field -- not a failure.
        if result is None and data.get("error"):
            _redis_mark_down(data.get("error"), now)
            logger.warn("redis_cmd_error", cmd=args[0] if args else "?",
                        error=str(data.get("error"))[:300])
            return None
        with _redis_health["lock"]:
            _redis_health["down_since"] = 0.0
            _redis_health["last_error"] = ""
            _redis_health["last_error_ts"] = 0.0
        return result
    except Exception as e:
        logger.warn("redis_cmd_failed", cmd=args[0] if args else "?", error=str(e))
        _redis_mark_down(e, now)
        return None

def redis_healthy():
    """True while Redis calls are succeeding (a missing key is still a
    successful round-trip -- only exceptions/response errors mark it down)."""
    if not REDIS_ENABLED: return False
    with _redis_health["lock"]:
        return _redis_health["down_since"] == 0.0

# ─── Phase 17: in-process fallback store (Redis-optional comments) ────────
# Ahmed's 2026-10-19 production outage (Vercel deployment with Redis
# unreachable): every Redis-backed cache silently died -- the name cache
# (masked "user-xxxx" everywhere), the scan cache (15s cold base scans on
# EVERY read) and the scan-refresh lock (so the mirror sync chain NEVER ran
# and the Portal Comments table stayed empty). The fix has two halves: the
# mirror table is the durable store for comments (Miaoda's architecture --
# reads are plain Bitable queries needing no Redis at all), and this small
# per-instance memory store keeps every other Redis-backed cache working
# inside a warm instance while the store is disabled or down.
_MEM_STORE_MAX = 4000
_mem_store = {}   # key -> (expires_epoch_or_0, json_string); insertion-ordered

def _mem_get(key):
    ent = _mem_store.get(key)
    if not ent: return None
    exp, raw = ent
    if exp and time.time() > exp:
        _mem_store.pop(key, None)
        return None
    return raw

def _mem_set(key, raw, ttl=None):
    if len(_mem_store) >= _MEM_STORE_MAX and key not in _mem_store:
        for k in list(_mem_store.keys())[: _MEM_STORE_MAX // 10 or 1]:
            _mem_store.pop(k, None)
    _mem_store[key] = (time.time() + ttl if ttl else 0, raw)

def redis_get_json(key):
    raw = redis_cmd("GET", key)
    if raw:
        try: return json.loads(raw)
        except Exception: return None
    # Phase 17: per-instance fallback while the store is disabled/down (a
    # genuine key miss on a HEALTHY store must NOT read the fallback -- a key
    # deleted upstream would otherwise resurrect from memory).
    if not redis_healthy():
        raw = _mem_get(key)
        if raw:
            try: return json.loads(raw)
            except Exception: return None
    return None

def redis_set_json(key, value, ttl=None):
    try:
        payload = json.dumps(value, default=str)
    except Exception as e:
        logger.warn("redis_serialize_failed", key=key, error=str(e))
        return False
    if len(payload) > REDIS_MAX_VALUE_BYTES:
        logger.warn("redis_value_too_large", key=key, size_bytes=len(payload))
        return False
    if not redis_healthy():
        _mem_set(key, payload, ttl)   # Phase 17: instance-local fallback
        return True
    if ttl: redis_cmd("SET", key, payload, "EX", int(ttl))
    else:   redis_cmd("SET", key, payload)
    return True

_cache: dict = {}
_cache_lock = threading.Lock()

def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() < entry["expires"]:
            return entry["data"]
        if entry:
            del _cache[key]
        return None

def cache_set(key, data, ttl=CACHE_TTL_REALTIME):
    with _cache_lock:
        _cache[key] = {"data": data, "expires": time.time() + ttl}

def cache_make_key(*parts):
    raw = ":".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()

def cache_invalidate(prefix=""):
    with _cache_lock:
        keys = [k for k in list(_cache.keys()) if not prefix or k.startswith(prefix)]
        for k in keys:
            del _cache[k]

# ════════════════════════════════════════════════════════════════════
# Phase 10.27 (Ahmed): ZERO-COST PRESENCE ("online / offline + last active")
# Every authenticated API call already carries the caller's identity -- a
# before_request hook stamps it into a Redis hash (one HSET, throttled to
# one write per user per minute, so the added latency is ~zero and there is
# NO new HTTP request anywhere: the 4-minute keep-warm ping every open
# client already sends doubles as the heartbeat). The Manage Agents page
# then reads the same hash inside the EXISTING /api/admin/users response.
# Online  = seen in the last 5 minutes (keep-warm cadence is 4 min).
# Idle    = seen in the last 30 minutes.
# Offline = anything older, or never seen.
# ════════════════════════════════════════════════════════════════════
PRESENCE_KEY = "xena:presence"
PRESENCE_ONLINE_S = 5 * 60
PRESENCE_IDLE_S = 30 * 60
_presence_local: dict = {}   # email -> {"name","ts","wrote_epoch"} (fallback + write throttle)
_presence_lock = threading.Lock()

def presence_touch(email, name=""):
    """Stamp a user's last-seen time. Never raises, never blocks the request
    meaningfully: in-memory stamp always; the Redis HSET rides at most once
    per minute per user (the local dict is the throttle memory)."""
    email = (email or "").strip().lower()
    if not email or email.startswith("__role__::"):
        return
    name = (name or "").strip()
    now_iso = cairo_now().isoformat()
    do_write = False
    with _presence_lock:
        prev = _presence_local.get(email) or {}
        wrote = prev.get("wrote_epoch") or 0
        if time.time() - wrote >= 60:
            do_write = True
        _presence_local[email] = {
            "name": name or prev.get("name", ""),
            "ts": now_iso,
            "wrote_epoch": time.time() if do_write else wrote,
        }
    if do_write and REDIS_ENABLED:
        try:
            redis_cmd("HSET", PRESENCE_KEY, email,
                      json.dumps({"name": _presence_local[email]["name"], "ts": now_iso}))
        except Exception:
            pass

def presence_map():
    """email -> {"name","ts"} for every seen user: Redis hash first, the
    in-process dict fills any gap (local dev / Redis down)."""
    out = {}
    if REDIS_ENABLED:
        try:
            raw = redis_cmd("HGETALL", PRESENCE_KEY)
            pairs = raw.items() if isinstance(raw, dict) else zip((raw or [])[::2], (raw or [])[1::2])
            for k, v in pairs:
                try:
                    out[str(k)] = json.loads(v)
                except Exception:
                    pass
        except Exception:
            pass
    with _presence_lock:
        for k, v in _presence_local.items():
            if k not in out and v.get("ts"):
                out[k] = {"name": v.get("name", ""), "ts": v["ts"]}
    return out

_rate_store: dict = defaultdict(list)
_rate_lock = threading.Lock()

def rate_check(ip, max_requests, window_seconds):
    now = time.time()
    with _rate_lock:
        timestamps = _rate_store[ip]
        _rate_store[ip] = [t for t in timestamps if now - t < window_seconds]
        if len(_rate_store[ip]) >= max_requests: return False
        _rate_store[ip].append(now)
        return True

def is_cron_authorized(req):
    """True if the request carries a valid `Authorization: Bearer <CRON_SECRET>`
    header -- the auth path for the external cron-job.org caller hitting
    /api/sync/refresh. Fails closed if CRON_SECRET isn't set (never treat a
    missing secret as "no auth required")."""
    if not CRON_SECRET:
        return False
    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False
    token = auth_header[len("Bearer "):]
    return hmac.compare_digest(token, CRON_SECRET)

def rate_limit(max_req, window):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
            if not rate_check(ip, max_req, window):
                logger.warn("rate_limited", ip=ip, endpoint=request.path)
                return jsonify({"error": "Too many requests. Please wait before trying again."}), 429
            return fn(*args, **kwargs)
        return wrapper
    return decorator

# ════════════════════════════════════════════════════════════════════
# LOCAL JSON FALLBACK LOADER + RAM CACHE (Fixes "Data too large")
# ════════════════════════════════════════════════════════════════════
_local_json_cache = {}
_local_json_lock = threading.Lock()
_data_status = {}   
SELF_BASE_URL = os.environ.get("SELF_BASE_URL", "").rstrip("/")

def _candidate_data_paths(filename):
    here = os.path.abspath(__file__)                 
    api_dir = os.path.dirname(here)                  
    project_root = os.path.dirname(api_dir)          
    candidates = [
        os.path.join(project_root, 'public', 'data', filename),
        os.path.join(api_dir, 'public', 'data', filename),
        os.path.join('/var/task', 'public', 'data', filename),
        os.path.join(os.getcwd(), 'public', 'data', filename),
        os.path.join(os.getcwd(), 'xena-portal', 'public', 'data', filename),
    ]
    seen, out = set(), []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out

def _read_build_meta_generated_at():
    """Read the real build-completion timestamp fetch_data.py stamps into
    _build_meta.json. Returns None if the file is missing (older deploy that
    predates this fix) or unreadable -- callers fall back to the unreliable
    os.path.getmtime() in that case. See the comment in load_local_json()."""
    for path in _candidate_data_paths("_build_meta.json"):
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                ts = meta.get("generated_at")
                if isinstance(ts, (int, float)):
                    return ts
            except Exception as e:
                logger.warn("build_meta_read_failed", path=path, error=str(e))
    return None

def _fetch_data_over_http(filename):
    base = SELF_BASE_URL
    if not base:
        try:
            from flask import request as _req, has_request_context
            if has_request_context():
                base = _req.host_url.rstrip("/")
        except Exception:
            base = ""
    if not base:
        return None
    url = f"{base}/data/{filename}"
    try:
        resp = http_requests.get(url, timeout=20)
        if resp.status_code == 200:
            return resp.json()
        logger.warn("local_json_http_fallback_bad_status", file=filename, status=resp.status_code)
    except Exception as e:
        logger.warn("local_json_http_fallback_failed", file=filename, error=str(e))
    return None

def load_local_json(filename):
    with _local_json_lock:
        if filename in _local_json_cache:
            data, timestamp = _local_json_cache[filename]
            if time.time() - timestamp < 120:
                return data

    tried_paths = _candidate_data_paths(filename)
    for file_path in tried_paths:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with _local_json_lock:
                    _local_json_cache[filename] = (data, time.time())
                # BUG FIX ("Analytics Report flatlines at an old date and never
                # recovers"): this used to be trusted forever with no freshness check
                # at all, unlike every other snapshot source in this file (background
                # sync / Redis both check BACKGROUND_SYNC_MAX_AGE). If the external
                # build job that regenerates this file (fetch_data.py, run via cron)
                # silently stops running, the app kept serving a days-old snapshot
                # indefinitely with fetch_complete:True and no signal anything was
                # wrong. "build_mtime" was meant to catch that -- but os.path.getmtime()
                # on this file is NOT trustworthy on Vercel: its deployment packaging
                # does not preserve the real write time when bundling public/data/**
                # into the serverless function, so this always read back some fixed,
                # meaningless value (observed: Oct 2018) no matter how recently the
                # build actually ran -- meaning the snapshot was *always* seen as
                # years stale and this fast path never actually got used, silently
                # forcing a live Feishu fetch on every request regardless of deploy
                # frequency. fetch_data.py now writes its own real completion time to
                # _build_meta.json alongside the data; read that first since it's
                # generated by the script itself, not read off a filesystem attribute
                # the platform resets. Only fall back to the (unreliable) OS mtime if
                # that sidecar file is missing, e.g. an older deploy that predates it.
                build_mtime = _read_build_meta_generated_at()
                if build_mtime is None:
                    try:
                        build_mtime = os.path.getmtime(file_path)
                    except OSError:
                        build_mtime = time.time()
                _data_status[filename] = {
                    "source": "disk", "path": file_path,
                    "records": len(data) if isinstance(data, list) else None,
                    "loaded_at": time.time(), "build_mtime": build_mtime,
                }
                return data
            except Exception as e:
                logger.error("local_json_read_failed", file=filename, path=file_path, error=str(e))

    data = _fetch_data_over_http(filename)
    if data is not None:
        with _local_json_lock:
            _local_json_cache[filename] = (data, time.time())
        _data_status[filename] = {
            "source": "http_self_fetch",
            "records": len(data) if isinstance(data, list) else None,
            "loaded_at": time.time(), "build_mtime": time.time(),
        }
        return data

    _data_status[filename] = {"source": "not_found", "tried_paths": tried_paths, "loaded_at": time.time()}
    logger.warn("local_json_not_found_anywhere", file=filename, tried_paths=tried_paths)
    return None

# How old the on-disk build snapshot (public/data/*.json, regenerated by
# fetch_data.py) is allowed to get before we stop trusting it unconditionally and
# fall through to the background-sync / Redis / live-Feishu chain instead. This is
# deliberately much more lenient than BACKGROUND_SYNC_MAX_AGE (600s) -- that constant
# is for an in-memory snapshot this same process refreshes every few minutes, whereas
# the build snapshot is only regenerated whenever the external cron job runs (often
# hourly or less often). 3 hours comfortably covers a normal build cadence while still
# catching a genuinely broken/stalled cron (e.g. stuck for days) well before it does.
LOCAL_SNAPSHOT_MAX_AGE_SECONDS = 3 * 3600

def local_json_age_seconds(filename):
    """Seconds since the on-disk build snapshot was last written, or None if unknown."""
    status = _data_status.get(filename)
    if not status or "build_mtime" not in status:
        return None
    return time.time() - status["build_mtime"]

def sanitize_agency_code(code):
    if not code: return None
    code = str(code).strip()
    if not re.match(r'^\d{3,8}$', code): return None
    return code

def sanitize_text(text, max_length=200):
    if not text: return ""
    text = str(text).strip()[:max_length]
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)

def parse_float_safe(val):
    try: return float(str(val).replace(',', '').strip())
    except (ValueError, TypeError): return 0.0

MONTH_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

def parse_monthly_target_field(raw_value):
    """Parses the "Target whole check" formula column, which is NOT a single YTD
    number -- it's a 12-slot, Jan-through-Dec breakdown per agency, e.g.:
      "not exist yet | not exist yet | ... | 0 | 901 | 1519 | N/A | N/A | ... | N/A"
    Meaning per slot:
      - "not exist yet"  -> the agency didn't exist yet that month (no target expected)
      - a number (incl. 0) -> that month's real target
      - "N/A"            -> that calendar month hasn't happened yet this year

    Returns (monthly, ytd_target):
      monthly    -- list of 12 {"month": "Jan".."Dec", "status": "not_exist"|"na"|"value",
                     "value": number|None} dicts, always length 12 regardless of how many
                     tokens were actually found (missing/extra tokens are padded/truncated).
      ytd_target -- sum of the numeric slots from Jan through the current month inclusive
                     (an actual, correct year-to-date target), used everywhere this file
                     previously (incorrectly) tried to parse_float_safe() the whole raw
                     string directly, which always evaluated to 0 since it's text.

    Delimiter: tries "|" first (matches the format this was specified in), then falls
    back to "," / ";" / newlines if "|" doesn't yield ~12 tokens, so this keeps working
    if the live column ever renders with a different separator.
    """
    text = extract_field_text(raw_value).strip()
    monthly = [{"month": m, "status": "na", "value": None} for m in MONTH_ABBR]
    if not text:
        return monthly, 0.0

    tokens = [t.strip() for t in text.split('|')]
    if len([t for t in tokens if t]) < 6:
        for delim in [',', ';', '\n']:
            candidate = [t.strip() for t in text.split(delim)]
            if len([t for t in candidate if t]) >= 6:
                tokens = candidate
                break

    for i in range(min(12, len(tokens))):
        tok = tokens[i].strip()
        tok_lower = tok.lower()
        if not tok or tok_lower in ('n/a', 'na'):
            monthly[i] = {"month": MONTH_ABBR[i], "status": "na", "value": None}
        elif 'not exist' in tok_lower:
            monthly[i] = {"month": MONTH_ABBR[i], "status": "not_exist", "value": None}
        else:
            try:
                num = float(tok.replace(',', ''))
                monthly[i] = {"month": MONTH_ABBR[i], "status": "value", "value": num}
            except ValueError:
                monthly[i] = {"month": MONTH_ABBR[i], "status": "na", "value": None}

    current_month_idx = cairo_now().month - 1  # 0 = Jan
    ytd_target = sum(m["value"] for m in monthly[:current_month_idx+1] if m["status"] == "value" and m["value"])
    return monthly, ytd_target

class AuditLogger:
    def __init__(self):
        self._queue = []
        self._lock  = threading.Lock()

    def log(self, actor, action, target, details="", ip="", severity="Info", blocking=False):
        full_target = f"{target} | {details}" if details else target
        entry = {
            # NOTE: audit entries are only ever readable by admins (see the
            # is_super_admin checks on every /api/admin/* route), so we log full,
            # unmasked identifiers here rather than "F***"/"jo***@..." -- masking
            # defeated the whole point of an accountability trail for the people
            # actually authorized to read it.
            "actor": actor or "Unknown", "action": action, "target": full_target,
            "ip": ip, "severity": severity, "ts": datetime.utcnow().isoformat(),
        }
        logger.info("audit", **entry)
        if AUDIT_TABLE_ID and not MOCK_MODE:
            if blocking:
                # Used for critical, low-volume, time-sensitive events (e.g. an
                # access request) where the caller needs the Feishu write to have
                # actually landed before returning -- otherwise an admin's very
                # next poll of /api/admin/access-requests can miss it entirely.
                # This app runs on serverless (Vercel): self._queue below is
                # per-process memory and is NOT shared across invocations, so a
                # fire-and-forget background write plus an in-memory fallback can
                # silently race for minutes until a poll happens to land on an
                # instance where the async write already finished.
                self._write_feishu(entry)
            else:
                t = threading.Thread(target=self._write_feishu, args=(entry,), daemon=True)
                t.start()
        with self._lock:
            self._queue.append(entry)
            if len(self._queue) > 500: self._queue = self._queue[-500:]

    def _write_feishu(self, entry):
        try:
            tat = get_tenant_access_token()
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{AUDIT_TABLE_ID}/records"
            hdrs = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
            payload = {"fields": {
                "Timestamp": int(datetime.utcnow().timestamp() * 1000),
                "Agent": entry["actor"],
                "Action": entry["action"],
                "Target": entry["target"],
                "IP Address": entry["ip"],
                "Severity": entry["severity"]
            }}
            http_requests.post(url, headers=hdrs, json=payload, timeout=8)
        except Exception as e:
            logger.error("audit_write_failed", error=str(e))

    def get_recent(self, limit=100):
        with self._lock: return list(reversed(self._queue[-limit:]))

audit = AuditLogger()

# ════════════════════════════════════════════════════════════════════
# MOCK DB & DATA PARSERS
# ════════════════════════════════════════════════════════════════════
class MockFeishuDB:
    @staticmethod
    def generate_requests(limit=500):
        items = []
        now = datetime.utcnow()
        for i in range(limit):
            is_pk = random.choice([True, False])
            acm = random.choice(list(PK_ACMS)) if is_pk else random.choice(list(IN_ACMS))
            req_type = random.choice(["Agency Creation", "BD Creation", "Closing Agency", "Target Check", "Trend Card", "Traffic Card"])
            status = random.choice(["Done", "Done", "Done", "Rejected", "Under Investigation"])
            dt = now - timedelta(days=random.randint(0, 60))
            
            item = {
                "record_id": str(uuid.uuid4()),
                "fields": {
                    "Numbering": f"REQ-{1000+i}",
                    "Request Type": req_type,
                    "Status": status,
                    "Region": "PK" if is_pk else "IN",
                    "Acm Name (PK)": acm if is_pk else "",
                    "Acm Name (IN)": acm if not is_pk else "",
                    "Agency Type": random.choice(["Acm hunting", "BD hunting", "Walkin"]),
                    "Submitted on Copy": int(dt.timestamp() * 1000),
                    "Agency Code": str(40000 + random.randint(1, 999)),
                    "Agency Point Privilege": req_type if "Card" in req_type else "",
                    "Target Type": "Agency" if "Target" in req_type else "",
                    "Quantities Input": str(random.randint(1, 3)),
                    "Point Balance": str(random.randint(100, 5000))
                }
            }
            items.append(item)
        return items

    @staticmethod
    def generate_agency(code):
        is_pk = random.choice([True, False])
        acm = random.choice(list(PK_ACMS)) if is_pk else random.choice(list(IN_ACMS))
        total = random.randint(1000, 10000)
        used = random.randint(0, total)
        return [{
            "record_id": str(uuid.uuid4()),
            "fields": {
                "Agency Code": code,
                "Agency Name": f"Mock Agency {code}",
                "Region": "PK" if is_pk else "IN",
                "Acm": acm,
                "Base Points": str(random.randint(100, 500)),
                "Total Points": str(total),
                "Used Points": str(used),
                "Point Balance": str(total - used)
            }
        }]

def normalize_key(k):
    return " ".join(str(k).lower().strip().split())

def get_field_local(fields, *aliases):
    if not fields: return None
    for alias in aliases:
        if alias in fields and fields[alias] not in (None, "", []): 
            return fields[alias]
    for alias in aliases:
        tgt = normalize_key(alias)
        for k, v in fields.items():
            if normalize_key(k) == tgt and v not in (None, "", []):
                return v
    for alias in aliases:
        tgt = normalize_key(alias)
        for k, v in fields.items():
            if tgt in normalize_key(k) and v not in (None, "", []):
                return v
    return None

def extract_field_text(field_data):
    if not field_data: return ""
    if isinstance(field_data, (str, int, float)): return str(field_data)
    if isinstance(field_data, dict):
        for key in ['text', 'name', 'en_name', 'email', 'value', 'label', 'id']:
            if key in field_data: return str(field_data[key])
        if 'id' in field_data: return str(field_data['id'])
        return str(field_data)
    if isinstance(field_data, list):
        if len(field_data) == 0: return ""
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                extracted = False
                for key in ['text', 'name', 'en_name', 'email', 'value', 'id']:
                    if key in item:
                        texts.append(str(item[key]))
                        extracted = True
                        break
                if not extracted: texts.append(str(item))
            else: texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)

def extract_attachments(field_data, field_id=None):
    if not field_data or not isinstance(field_data, list): return []
    out = []
    for item in field_data:
        if isinstance(item, dict) and item.get("file_token"):
            url = f"/api/attachments/{item['file_token']}"
            if field_id:
                url += f"?field_id={field_id}"
            out.append({"name": item.get("name", "file"), "url": url})
    return out

def extract_field_list(field_data):
    if not field_data: return []
    if isinstance(field_data, dict):
        for key in ['text', 'name', 'en_name', 'email', 'value', 'label']:
            if key in field_data and field_data[key] not in (None, ""):
                return [str(field_data[key]).strip()]
        if 'id' in field_data and field_data['id'] not in (None, ""):
            return [str(field_data['id']).strip()]
        return [str(field_data).strip()]
    if isinstance(field_data, str):
        # Accept both comma-separated (legacy tickets) and newline-separated
        # (Agency Target Privilege's multi User ID field submits one ID per
        # line now, matching how it displays in Feishu) so old and new
        # tickets parse the same way.
        return [s.strip() for s in re.split(r'[,\n\r]+', field_data) if s.strip()]
    if isinstance(field_data, list):
        res = []
        for item in field_data:
            if not item: continue
            if isinstance(item, dict):
                extracted = False
                for key in ['text', 'name', 'en_name', 'email', 'value', 'label']:
                    if key in item and item[key] not in (None, ""):
                        res.append(str(item[key]).strip())
                        extracted = True
                        break
                if not extracted and 'id' in item and item['id'] not in (None, ""):
                    res.append(str(item['id']).strip())
                elif not extracted:
                    res.append(str(item).strip())
            else:
                res.append(str(item).strip())
        return res
    return [str(field_data).strip()]

def get_accepted_ids(fields):
    user_ids     = extract_field_list(get_field_local(fields, "User ID", "User Id"))
    rejected_ids = extract_field_list(get_field_local(fields, "Rejected Ids", "Rejected ID", "Rejected IDs", "Rejected Id"))
    rejected_set = {str(r).strip().lower() for r in rejected_ids if r}
    accepted = [uid for uid in user_ids if str(uid).strip().lower() not in rejected_set]
    return accepted

def parse_feishu_date(date_val):
    if not date_val: return None
    if isinstance(date_val, list) and len(date_val) > 0: date_val = date_val[0]
    if isinstance(date_val, dict): date_val = date_val.get('value', date_val.get('text', ''))
    try:
        if isinstance(date_val, (int, float)):
            dt_utc = datetime.fromtimestamp(date_val / 1000.0, tz=timezone.utc)
            dt_cairo = dt_utc + timedelta(hours=3)
            return dt_cairo.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
            
        date_str = str(date_val).strip()
        if date_str.isdigit():
            dt_utc = datetime.fromtimestamp(int(date_str) / 1000.0, tz=timezone.utc)
            dt_cairo = dt_utc + timedelta(hours=3)
            return dt_cairo.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        
        clean_str = date_str[:10].replace('/', '-').replace('.', '-')
        return datetime.strptime(clean_str, "%Y-%m-%d")
    except Exception:
        return None

def clean(field_data):
    return extract_field_text(field_data).strip().lower()

def compute_allocator_status(usage_dict):
    status = {}
    for item, used in usage_dict.items():
        limit = MONTHLY_ALLOCATOR_LIMITS.get(item)
        status[item] = {
            "used": used,
            "limit": limit,
            "remaining": (max(0, limit - used) if limit is not None else None)
        }
    return status

def parse_granular_string(raw_str):
    default = {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}
    if not raw_str or str(raw_str).strip() == "": return default
    if "=" not in raw_str:
        parts = [x.strip().lower() for x in raw_str.split(",") if x.strip()]
        if not parts: parts = ["all"]
        return {"target": parts, "points": parts, "analytics": parts, "query": parts}
    res = {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}
    for chunk in raw_str.split(";"):
        if "=" in chunk:
            mod, vals = chunk.split("=", 1)
            mod = mod.strip().lower()
            val_list = [v.strip().lower() for v in vals.split(",") if v.strip()]
            if not val_list: val_list = ["all"]
            if mod in res: res[mod] = val_list
    return res

# Phase 12 (Ahmed: "One toggle per member covering everything: login access +
# notifications"): a single Redis HASH holds every member's on/off state
# (field = lowercased email OR Person name, value "1"/"0"; ABSENT = enabled).
# When a member is toggled OFF, _get_user_permissions_raw returns zero
# modules: the login flow lands on the access-denied / request-access page,
# every /api endpoint denies, and the notification bell gets 403 and hides.
# The hardcoded ADMIN_USERS super-admin early-return stays EXEMPT as a
# failsafe so the team can never lock itself out. Fail-OPEN on Redis errors —
# a store hiccup must never lock the whole team out either.
ACCESS_TOGGLES_KEY = "xena:access:toggles"

def _TOGGLED_OFF_PERMS():
    return {"is_super_admin": False, "modules": [],
            "permissions": {"acms": {}, "regions": {}}, "toggled_off": True}

def _access_toggle_allows(ident):
    ident = (ident or "").strip().lower()
    if not ident:
        return True
    try:
        v = redis_cmd("HGET", ACCESS_TOGGLES_KEY, ident)
    except Exception:
        return True
    return v != "0"

def _access_toggles_map():
    """{ident: enabled_bool} for the admin users UI — one HGETALL total."""
    try:
        flat = redis_cmd("HGETALL", ACCESS_TOGGLES_KEY) or []
    except Exception:
        return {}
    out = {}
    if isinstance(flat, list):
        for i in range(0, len(flat) - 1, 2):
            out[str(flat[i])] = (str(flat[i + 1]) != "0")
    return out

def _get_user_permissions_raw(email, name):
    name_clean = name.strip().lower() if name else ""
    email_clean = email.strip().lower() if email else ""
    
    if any(admin == name_clean for admin in ADMIN_USERS):
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin", "query", "submit", "submit_new_request", "submit_my_requests", "submit_audit", "query_requests", "query_agency_list", "export_data", "tickets", "tickets_live_queue"],
            "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]},
                            "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}}
        }

    if not email_clean and not name_clean:
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

    if MOCK_MODE:
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin", "query", "submit", "submit_new_request", "submit_my_requests", "submit_audit", "query_requests", "query_agency_list", "export_data", "tickets", "tickets_live_queue"],
            "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]},
                            "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}}
        }

    cache_key = cache_make_key("perms", email_clean, name_clean)
    cached = cache_get(cache_key)
    if cached:
        # Phase 12: one Redis toggle per member covers EVERYTHING (login +
        # notifications). Checked AFTER the cache read so a flip lands on the
        # very next request — no cache busting, instant off AND instant on.
        if not _access_toggle_allows(email_clean) or not _access_toggle_allows(name_clean):
            return _TOGGLED_OFF_PERMS()
        return cached

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    try:
        res = http_requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        for item in res.get("data", {}).get("items", []):
            fields = item.get("fields", {})
            db_email = extract_field_text(fields.get("Email", "")).lower().strip()
            db_person = extract_field_text(fields.get("Person", "")).lower().strip()
            
            if (email_clean and email_clean == db_email) or (name_clean and name_clean == db_person):
                modules = [m.strip().lower() for m in extract_field_text(get_field_local(fields, "Modules")).split(",") if m.strip()]
                parsed_acms = parse_granular_string(extract_field_text(get_field_local(fields, "ACMs")))
                parsed_regions = parse_granular_string(extract_field_text(get_field_local(fields, "Regions")))
                result = {"is_super_admin": "admin" in modules, "modules": modules, "permissions": {"acms": parsed_acms, "regions": parsed_regions}}
                cache_set(cache_key, result, ttl=300)
                # Phase 12: toggle check on the DB path too — the CACHE keeps
                # the real permissions (so re-enabling is instant), the toggle
                # only decides what this request gets.
                if not _access_toggle_allows(email_clean) or not _access_toggle_allows(name_clean):
                    return _TOGGLED_OFF_PERMS()
                return result
    except Exception: pass
    
    fallback = {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}
    return fallback

def get_user_permissions(email, name):
    """Phase 10.26 (Ahmed): full-faithful Preview Agent View. While an admin
    previews another member, the frontend stamps every /api request with the
    X-Preview-As header (the target's email). When the REAL caller is a super
    admin / admin-module holder, resolve the TARGET's permissions instead --
    every data endpoint then filters exactly what the target would see
    (regions / ACMs / modules), and any mutation that slipped past the
    view-only frontend is authorized as the target too (defense in depth).

    Guards:
    - /api/admin/* always uses the real identity, so the preview console
      itself (preview-targets / preview-access) keeps working mid-preview.
    - A forged X-Preview-As from anyone WITHOUT full admin rights is ignored
      (falls through to the real identity) -- a plain preview_access holder
      can NOT escalate by pointing the header at a more privileged member.
    """
    target = ""
    try:
        if request.path.startswith("/api/") and not request.path.startswith("/api/admin/"):
            target = sanitize_text(request.headers.get("X-Preview-As", ""), 200).strip()
    except RuntimeError:
        target = ""  # no request context (scripts / tests)
    if target:
        real = _get_user_permissions_raw(email, name)
        if real.get("is_super_admin") or "admin" in real.get("modules", []):
            return _get_user_permissions_raw(target, target)
    return _get_user_permissions_raw(email, name)

def generate_executive_insights(stats, cmp_stats=None):
    insights = []
    
    kpis = stats.get("kpis", {})
    creations = kpis.get("creations", 0)
    bds = kpis.get("bds", 0)
    closings = kpis.get("closings", 0)

    if creations > 0 and bds > 0:
        ratio = creations / bds
        if ratio > 2.5:
            insights.append(f"Pipeline Analysis: Creation-to-BD ratio sits at {ratio:.1f}x, indicating highly effective Top-of-Funnel organic acquisition.")
        else:
            insights.append(f"Pipeline Analysis: Creation-to-BD ratio is {ratio:.1f}x, suggesting a BD-reliant growth strategy this period.")

    if creations > 0 and closings > 0:
        eff = (closings / creations) * 100
        insights.append(f"Closing Efficiency: Converting at {eff:.1f}% relative to new creations.")

    acm_perf = stats.get("acm_performance", {})
    if acm_perf:
        top_acm = max(acm_perf, key=acm_perf.get)
        share = (acm_perf[top_acm] / creations * 100) if creations > 0 else 0
        insights.append(f"Leadership: {top_acm} is driving {share:.1f}% of total volume, establishing a strong regional benchmark.")

    if cmp_stats:
        prev_creations = cmp_stats.get("kpis", {}).get("creations", 0)
        if prev_creations > 0:
            delta = ((creations - prev_creations) / prev_creations) * 100
            trend = "growth" if delta >= 0 else "decline"
            insights.append(f"Period Momentum: Demonstrating a {abs(delta):.1f}% {trend} in agency creations compared to the previous cycle.")

    return insights

def fetch_feishu_records(table_id, from_dt=None, max_seconds=None):
    if MOCK_MODE:
        items = MockFeishuDB.generate_requests(300)
        keys = set(items[0]["fields"].keys()) if items else set()
        return items, keys, True, ""

    tat = get_tenant_access_token()
    all_items, seen_ids, master_keys = [], set(), set()
    fetch_complete, stop_reason, consecutive_old_pages = True, "", 0

    session = http_requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records"

    # BUG FIX ("Server timeout: Data too large or loading"): this loop used to have no
    # wall-clock bound at all. When called with from_dt=None (the background cron sync
    # does exactly this), the "stop once pages look old enough" heuristic below never
    # triggers, so it would walk the *entire* table history every single cycle -- and as
    # the table has grown that eventually runs past Vercel's function execution limit,
    # which kills the request and returns a non-JSON timeout page instead of a normal
    # error (that's what "Server timeout: Data too large or loading" actually means --
    # it's the frontend's res.json() failing to parse whatever Vercel sent back).
    # Records are fetched newest-first (sort "Numbering DESC"), so a time-boxed partial
    # fetch still returns the most recent rows -- exactly what a 15-day lookback needs.
    deadline = (time.time() + max_seconds) if max_seconds else None

    page_token = None
    for _ in range(200):
        if deadline and time.time() > deadline:
            fetch_complete, stop_reason = False, "Stopped early: time budget reached (partial results, newest records only)."
            break

        params = {"page_size": 500, "automatic_fields": "true"} 
        if table_id == REQUESTS_TABLE_ID: params["sort"] = '["Numbering DESC"]'
        if page_token: params["page_token"] = page_token
        
        try:
            resp = session.get(url, params=params, timeout=20)
            if resp.status_code != 200:
                fetch_complete, stop_reason = False, f"HTTP {resp.status_code}: {resp.text}"
                break
                
            data = resp.json()
            if data.get("code") != 0:
                fetch_complete, stop_reason = False, f"Feishu Error {data.get('code')}: {data.get('msg')}"
                break
            
            block = data.get("data", {})
            items = block.get("items", [])
            if not items: break

            page_old_count, valid_dates_in_page = 0, 0
            for item in items:
                rid = item.get("record_id")
                if rid and rid not in seen_ids:
                    seen_ids.add(rid)
                    all_items.append(item)
                    master_keys.update(item.get("fields", {}).keys())
                    raw_date = get_field_local(item.get("fields", {}), "Submitted on Copy", "Submitted on", "Created Time", "Date")
                    record_dt = parse_feishu_date(raw_date)
                    if record_dt:
                        valid_dates_in_page += 1
                        if from_dt and record_dt < (from_dt - timedelta(days=1)):
                            page_old_count += 1
            
            if valid_dates_in_page > 0 and page_old_count == valid_dates_in_page:
                consecutive_old_pages += 1
            else: consecutive_old_pages = 0

            if consecutive_old_pages >= 3:
                stop_reason = "Safely reached pages with all older records."
                break

            page_token = block.get("page_token")
            if not page_token or not block.get("has_more", False): break

        except Exception as e:
            fetch_complete, stop_reason = False, str(e)
            break

    return all_items, master_keys, fetch_complete, stop_reason

REQUESTS_ANALYTICS_FIELDS = [
    "Numbering", "Submitted on Copy", "Request Type", "Status", "Region",
    "Acm Name (PK)", "Acm Name (IN)", "Acm", "Agency Type",
    "Closing Reason", "Otherapp Name", "Reject Reason", "Create Way",
]

POINTS_TABLE_FIELDS = [
    "Agency Code", "Agency Name", "Region", "Acm", "Acm Name (PK)", "Acm Name (IN)",
    "Assigned Member", "Base Points", "Bonus Points", "Total Points", "# Total Points",
    "Used Points", "Point Balance",
    # YTD Target integration (Executive Dashboard) -- "Target whole check" is the
    # formula column on the Agency Points table (tbl6LYUxGi8tlkJH) that holds each
    # agency's full-year target. Multiple aliases because Feishu formula columns are
    # sometimes renamed/duplicated across environments.
    "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check",
]

QUERY_RECORDS_FIELDS = [
    "Numbering", "Request Type", "Submitted on Copy", "Submitted on", "Respondents",
    "User ID", "Otherapp ID", "Otherapp Name", "Other App Name",
    "Acm Name (PK)", "Acm Name (IN)", "Acm", "Assigned Member", "Region",
    "Bd Code", "BD Code", "NID Number", "NID", "Status", "Request Status",
    "Reject Reason", "Rejection Reason", "Audition note", "Audition Note", "Duplicated Check",
    "Agency Code", "Agency Type", "Type of Agency",
    "Closing Reason", "Closing Agencies Reason",
]

def _fetch_bitable_shard(table_id, tat, filter_obj=None, field_names=None, timeout=30):
    session = http_requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records/search?automatic_fields=true"

    items, seen, complete, reason = [], set(), True, ""
    page_token, projection = None, field_names

    for _ in range(200):
        payload = {"page_size": 500}
        if filter_obj:  payload["filter"] = filter_obj
        if projection:  payload["field_names"] = projection
        if page_token:  payload["page_token"] = page_token
        try:
            resp = session.post(url, json=payload, timeout=timeout)
            data = resp.json()
            if data.get("code") == 1254045 and projection:
                projection, page_token = None, None
                items, seen = [], set()
                continue
            if data.get("code") != 0:
                complete, reason = False, f"Feishu Error {data.get('code')}: {data.get('msg')}"
                break
            block = data.get("data", {})
            for item in block.get("items", []):
                rid = item.get("record_id")
                if rid and rid not in seen:
                    seen.add(rid)
                    items.append(item)
            page_token = block.get("page_token")
            if not page_token or not block.get("has_more", False):
                break
        except Exception as e:
            complete, reason = False, str(e)
            break

    return items, complete, reason

REQUESTS_SHARD_COUNT = 10 
REQUESTS_LOOKBACK_DAYS_DEFAULT = 365 * 3  

def fetch_requests_sharded(from_dt=None, to_dt=None, field_names=REQUESTS_ANALYTICS_FIELDS, n_shards=REQUESTS_SHARD_COUNT, max_seconds=None):
    if MOCK_MODE:
        items = MockFeishuDB.generate_requests(300)
        keys = set(items[0]["fields"].keys()) if items else set()
        return items, keys, True, ""

    items, keys, fetch_complete, stop_reason = fetch_feishu_records(REQUESTS_TABLE_ID, from_dt=from_dt, max_seconds=max_seconds)

    filtered_items = []
    for item in items:
        if from_dt or to_dt:
            raw_date = get_field_local(item.get("fields", {}), "Submitted on Copy", "Submitted on", "Created Time")
            dt = parse_feishu_date(raw_date)
            if dt:
                if from_dt and dt < from_dt: continue
                if to_dt and dt >= to_dt: continue
        filtered_items.append(item)
        
    return filtered_items, keys, fetch_complete, stop_reason

def fetch_points_sharded(field_names=POINTS_TABLE_FIELDS):
    if MOCK_MODE:
        items = MockFeishuDB.generate_agency("All") * 10
        return items, True, ""

    tat = get_tenant_access_token()
    t0 = time.time()
    items, complete, reason = _fetch_bitable_shard(POINTS_TABLE_ID, tat, filter_obj=None, field_names=field_names)
    logger.info("points_fetch", ms=int((time.time() - t0) * 1000), rows=len(items), complete=complete, reason=reason or "")
    return items, complete, reason

def compute_target_pace_analytics(total_pts, ytd_target, monthly_target):
    """Shared target-realization math -- same formulas the Agency Exchange board
    uses (realization %, pace-based year-end projection, implied full-year target,
    downgrade-risk flag, required daily catch-up rate). Used here so the single-
    agency lookup ("Check Agency Points") can show the same analytics without a
    second network call -- it's computed from fields already fetched for this page."""
    now = cairo_now()
    elapsed_months = now.month
    year_end = now.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=0)
    days_left_in_year = max(1, (year_end.date() - now.date()).days)

    realization_pct = round((total_pts / ytd_target * 100) if ytd_target > 0 else 0, 1)
    variance = round(total_pts - ytd_target, 0)

    if ytd_target <= 0:            pacing_status = "No Target"
    elif realization_pct >= 110:   pacing_status = "Ahead"
    elif realization_pct >= 95:    pacing_status = "On Track"
    elif realization_pct >= 80:    pacing_status = "Needs Attention"
    else:                          pacing_status = "Lagging"

    projected_eoy_points = round((total_pts / elapsed_months) * 12, 0) if elapsed_months > 0 else total_pts
    current_month_target = monthly_target[elapsed_months - 1]["value"] if elapsed_months <= 12 else None

    elapsed_value_months = sum(1 for m in monthly_target[:elapsed_months] if m["status"] == "value")
    implied_full_year_target = round((ytd_target / elapsed_value_months) * 12, 0) if elapsed_value_months > 0 else 0
    will_miss_target = bool(implied_full_year_target > 0 and projected_eoy_points < implied_full_year_target)
    gap_to_full_year = max(0.0, implied_full_year_target - total_pts) if implied_full_year_target > 0 else 0.0
    required_daily_rate = round(gap_to_full_year / days_left_in_year, 2) if gap_to_full_year > 0 else 0.0

    return {
        "realization_pct": realization_pct, "variance": variance, "pacing_status": pacing_status,
        "projected_eoy_points": projected_eoy_points, "current_month_target": current_month_target,
        "implied_full_year_target": implied_full_year_target, "will_miss_target": will_miss_target,
        "gap_to_full_year": round(gap_to_full_year, 0), "required_daily_rate": required_daily_rate,
    }

def fetch_agency_data(code, query_type="points", allowed_acms=None, allowed_regs=None):
    if MOCK_MODE:
        all_records = MockFeishuDB.generate_agency(code)
    else:
        tat = get_tenant_access_token()
        headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
        points_payload = {
            "filter": {
                "conjunction": "and",
                "conditions": [{"field_name": "Agency Code", "operator": "contains", "value": [code]}]
            }
        }
        search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
        
        try:
            resp = http_requests.post(search_url, headers=headers, json=points_payload, timeout=30).json()
            if resp.get("code") != 0: return {"found": False, "error": f"Feishu API Error: {resp.get('msg')}"}
            raw_records = resp.get("data", {}).get("items", [])
            
            all_records = []
            for r in raw_records:
                r_code = extract_field_text(get_field_local(r.get("fields", {}), "Agency Code"))
                if str(r_code).strip() == str(code).strip():
                    all_records.append(r)

            if not all_records: return {"found": False, "error": f"Notice: Agency {code} not found or no records."}
        except Exception as e:
            return {"found": False, "error": f"Search timeout or connection error: {str(e)}"}

    first = all_records[0].get("fields", {})

    agency_name  = extract_field_text(get_field_local(first,"Agency Name","Name"))
    region_raw   = clean(get_field_local(first,"Region","Agency Region"))
    acm_raw      = extract_field_text(get_field_local(first,"Acm Name (PK)","Acm Name (IN)","Acm","Assigned Member"))

    if region_raw in ('', 'none'):
        if acm_raw.lower() in PK_ACMS: region_raw = 'pk'
        elif acm_raw.lower() in IN_ACMS: region_raw = 'in'

    if allowed_acms and "all" not in allowed_acms:
        if acm_raw.strip().lower() not in [a.lower() for a in allowed_acms]:
            return {"found": False, "error": f"Access Denied: Not authorized to view ACM {acm_raw}."}
    if allowed_regs and "all" not in allowed_regs:
        if region_raw.strip().lower() not in [r.lower() for r in allowed_regs]:
            return {"found": False, "error": f"Access Denied: Not authorized to view Region {region_raw.upper()}."}

    history_points, history_target = [], []
    privileges_claimed, usage_this_month = defaultdict(int), defaultdict(int)
    accepted_ids_all = []
    
    _cairo_now = cairo_now()
    cm, cy = _cairo_now.month, _cairo_now.year
    
    if MOCK_MODE:
        hist_items = MockFeishuDB.generate_requests(50)
    else:
        try:
            hist_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
            hist_resp = http_requests.post(hist_url, headers=headers, json=points_payload, timeout=30).json()
            raw_hist_items = hist_resp.get("data", {}).get("items", []) if hist_resp.get("code") == 0 else []
            
            hist_items = []
            for r in raw_hist_items:
                r_code = extract_field_text(get_field_local(r.get("fields", {}), "Agency Code"))
                if str(r_code).strip() == str(code).strip():
                    hist_items.append(r)
        except: hist_items = []

    for r in hist_items:
        hf = r.get("fields", {})
        h_date = parse_feishu_date(get_field_local(hf, "Submitted on Copy", "Submitted on", "Created Time"))
        
        if not h_date or h_date.month != cm or h_date.year != cy: continue

        req_type      = extract_field_text(get_field_local(hf, "Request Type", "Type")).strip()
        status_val    = extract_field_text(get_field_local(hf, "Status", "Request Status")).strip()
        
        target_status_val = extract_field_text(hf.get("Status", "")).strip()
        if not target_status_val:
            target_status_val = status_val

        req_type_lower = req_type.lower()
        s_lower = status_val.lower()
        target_s_lower = target_status_val.lower()

        is_target_done   = any(ok in target_s_lower for ok in ("done", "complet", "approv", "confirm"))
        is_points_done   = any(ok in s_lower for ok in ("done", "complet", "approv", "confirm"))
        is_points_reject = any(rej in s_lower for rej in ("reject", "fail", "decline"))

        target_type   = extract_field_text(get_field_local(hf, "Target Type")).strip()
        point_balance = extract_field_text(get_field_local(hf, "Point Balance")).strip()

        accepted_ids = get_accepted_ids(hf)
        if accepted_ids: accepted_ids_all.extend(accepted_ids)

        if "target" in req_type_lower:
            privilege_val = extract_field_text(get_field_local(hf, "Agency Point Privilege", "Privilege", "Agency Privilege")).strip()
            raw_counter = extract_field_text(get_field_local(hf, "Counter", "Qty", "Quantities Input")).strip()
            qty = 1
            if raw_counter:
                m = re.search(r'\d+', str(raw_counter))
                if m: qty = int(m.group())

            history_target.append({
                "date": h_date.strftime("%Y-%m-%d"), "_dt": h_date,
                "request_type": req_type, 
                "status": target_status_val,
                "privilege": privilege_val, "quantities_input": str(qty),
                "accepted_ids": accepted_ids,
            })

            if is_target_done and privilege_val:
                privileges_claimed[privilege_val] += qty

        else:
            latest_usage  = extract_field_text(get_field_local(hf, "Latest Usage Tracker")).strip()
            parsed_items = re.findall(r'🔹\s*(.*?):\s*(\d+)', latest_usage)
            
            if parsed_items: 
                privilege_val = " + ".join([f"{k.strip()} ({v})" for k, v in parsed_items])
                qty_input_val = str(sum(int(v) for k, v in parsed_items))
            else: 
                privilege_val = extract_field_text(get_field_local(hf, "Agency Point Privilege", "Privilege")).strip()
                qty_input_val = extract_field_text(get_field_local(hf, "Quantities Input", "Qty", "Counter")).strip()
                if not qty_input_val: qty_input_val = "1"

            history_points.append({
                "date": h_date.strftime("%Y-%m-%d"), "_dt": h_date,
                "request_type": req_type, "status": status_val,
                "target_type": target_type, "point_balance": point_balance,
                "privilege": privilege_val,
                "quantities_input": qty_input_val,
                "accepted_ids": accepted_ids,
            })

            if not is_points_reject:
                for item_name, item_qty in parsed_items:
                    name_clean, qty_int = item_name.strip().lower(), int(item_qty)
                    matched = False
                    for key in MONTHLY_ALLOCATOR_LIMITS.keys():
                        if key in name_clean:
                            usage_this_month[key] += qty_int
                            matched = True
                            break
                    if not matched: usage_this_month[name_clean] += qty_int

    history_points.sort(key=lambda x: (x["_dt"] is None, x["_dt"]), reverse=True)
    history_target.sort(key=lambda x: (x["_dt"] is None, x["_dt"]), reverse=True)
    for h in history_points: h.pop("_dt", None)
    for h in history_target: h.pop("_dt", None)

    allocator_status = compute_allocator_status(usage_this_month)
    
    if query_type == "points":
        total_pts = parse_float_safe(extract_field_text(get_field_local(first, '# Total Points', 'Total Points', 'Total')))
        used_pts  = parse_float_safe(extract_field_text(get_field_local(first, 'Used Points', 'Used')))
        balance   = parse_float_safe(extract_field_text(get_field_local(first, 'Point Balance', 'Balance')))
        monthly_target, ytd_target = parse_monthly_target_field(get_field_local(first, "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check"))

        if balance == 0 and total_pts > 0: balance = total_pts - used_pts

        health_score, health_status = 100, "Healthy"
        if total_pts > 0:
            utilization = used_pts / total_pts
            if utilization > 0.90: health_score, health_status = 40, "Critical"
            elif utilization > 0.70: health_score, health_status = 70, "At Risk"
            else: health_score, health_status = 95, "Healthy"
        else: 
            health_score, health_status = 0, "Inactive"

        pace = compute_target_pace_analytics(total_pts, ytd_target, monthly_target)

        # This-month activity, bucketed like a simple kanban board (Pending / Completed
        # / Rejected) for the Agency dashboard -- built entirely from `history_points`,
        # which was already fetched above for the allocator; no extra Feishu calls.
        activity_board = {"pending": [], "completed": [], "rejected": []}
        for h in history_points:
            s = (h.get("status") or "").lower()
            entry = {"date": h.get("date"), "request_type": h.get("request_type"), "privilege": h.get("privilege")}
            if any(rej in s for rej in ("reject", "fail", "decline")):
                activity_board["rejected"].append(entry)
            elif any(ok in s for ok in ("done", "complet", "approv", "confirm")):
                activity_board["completed"].append(entry)
            else:
                activity_board["pending"].append(entry)

        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw.upper(), "acm": acm_raw.title(),
            "total_points": total_pts, "used_points": used_pts,
            "point_balance": balance, "health_score": health_score,
            "health_status": health_status, "ytd_target": ytd_target, "monthly_target": monthly_target,
            "history": history_points, 
            "allocator_status": allocator_status,
            "accepted_user_ids": sorted(set(accepted_ids_all)),
            "requests": [r.get("fields", {}) for r in all_records],
            "target_analytics": pace,
            "activity_board": activity_board,
        }
    else:  
        raw_base_pts = parse_float_safe(extract_field_text(get_field_local(first, "Base Points", "base_points")))
        # Same target-realization fields as the Points branch, added here so the
        # "🎯 Agency Target Lookup" view can show the Monthly Target Breakdown too
        # (this was previously only under Points -- moved/shared per request).
        total_pts_for_target = parse_float_safe(extract_field_text(get_field_local(first, '# Total Points', 'Total Points', 'Total')))
        monthly_target, ytd_target = parse_monthly_target_field(get_field_local(first, "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check"))
        pace = compute_target_pace_analytics(total_pts_for_target, ytd_target, monthly_target)
        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw.upper(), "acm": acm_raw.title(),
            "base_points": raw_base_pts * COINS_MULTIPLIER, "health_score": 100, "health_status": "Healthy",
            "privileges_claimed": dict(privileges_claimed),  
            "history": history_target, 
            "accepted_user_ids": sorted(set(accepted_ids_all)),
            "requests": [r.get("fields", {}) for r in all_records],
            "total_points": total_pts_for_target, "ytd_target": ytd_target, "monthly_target": monthly_target,
            "target_analytics": pace,
        }

def _build_field_map_safe(item: dict) -> dict:
    fields = item.get("fields", {})
    raw_date   = get_field_local(fields,"Submitted on Copy","Submitted on","Created Time")
    raw_type   = get_field_local(fields,"Request Type","Request type","Type","Category")
    raw_status = get_field_local(fields,"Status","Request Status","Agency Status","State")
    raw_region = get_field_local(fields,"Region","Agency Region")
    raw_acm_pk = get_field_local(fields,"Acm Name (PK)")
    raw_acm_in = get_field_local(fields,"Acm Name (IN)")
    raw_acm_fb = get_field_local(fields,"Acm","Assigned Member")
    raw_a_type = get_field_local(fields,"Agency Type","Type of Agency")
    raw_cl_rsn = get_field_local(fields,"Closing Reason","Closing Agencies Reason")
    raw_o_app  = get_field_local(fields,"Otherapp Name","Other App Name","Other Apps")
    raw_rj_rsn = get_field_local(fields,"Reject Reason","Rejection Reason")
    raw_cr_way = get_field_local(fields,"Create Way","Creation Type")

    return {
        "date":      parse_feishu_date(raw_date),
        "req_type":  clean(raw_type),
        "status":    clean(raw_status),
        "region":    clean(raw_region),
        "acm_pk":    clean(raw_acm_pk),
        "acm_in":    clean(raw_acm_in),
        "acm_fb":    clean(raw_acm_fb),
        "a_type":    clean(raw_a_type),
        "cl_rsn":    clean(raw_cl_rsn),
        "o_app":     clean(raw_o_app),
        "rj_rsns":   extract_field_list(raw_rj_rsn),
        "cr_ways":   extract_field_list(raw_cr_way),
    }

def run_analytics(all_items, from_dt, to_dt, region_filter, acm_filter, type_filter,
                  allowed_acms, allowed_regs):
    stats = {
        "kpis": {"creations":0,"bds":0,"closings":0},
        "creation_status": {"Done":0,"Rejected":0,"Under Investigation":0},
        "bd_status":       {"Done":0,"Rejected":0,"Under Investigation":0},
        "closing_status":  {"Done":0,"Rejected":0,"Under Investigation":0},
        "acm_performance":{}, "creation_types":{}, "agency_types":{},
        "other_apps":{}, "reject_reasons":{}, "closing_reasons_pie":{},
        "acm_closing_reasons":{},
        "daily_trend_creation":{}, "daily_trend_bd":{}, "daily_trend_closing":{},
        "other_request_types":{}, "scanned_rows": len(all_items),
        "fetch_complete": True, "stop_reason": "",
        "executive_insights": []
    }

    if from_dt and to_dt:
        cur = from_dt
        while cur < to_dt:
            ds = cur.strftime("%Y-%m-%d")
            stats["daily_trend_creation"][ds], stats["daily_trend_bd"][ds], stats["daily_trend_closing"][ds] = 0, 0, 0
            cur += timedelta(days=1)

    # acm_filter may be a single string (legacy callers, e.g. /api/compare which
    # always filters to exactly one ACM per group) or an iterable of ACM names
    # (the Analytics Report's multi-select ACM filter). Normalize to a set so
    # both call shapes work unchanged.
    if isinstance(acm_filter, str):
        acm_filter_set = {acm_filter.strip().lower()} if acm_filter and acm_filter.strip().lower() != "all" else {"all"}
    else:
        acm_filter_set = set(a.strip().lower() for a in (acm_filter or []) if a and a.strip())
        if not acm_filter_set: acm_filter_set = {"all"}
    region_filter_c  = region_filter.strip().lower() if region_filter else "all"
    type_filter_c    = type_filter.strip().lower() if type_filter else "all"
    allowed_acms_set = set([a.lower() for a in allowed_acms]) if allowed_acms else {"all"}
    allowed_regs_set = set([r.lower() for r in allowed_regs]) if allowed_regs else {"all"}

    with ThreadPoolExecutor(max_workers=10) as executor:
        normalized_maps = list(executor.map(_build_field_map_safe, all_items))

    for fm in normalized_maps:
        record_dt = fm["date"]
        if from_dt or to_dt:
            if not record_dt: continue
            if from_dt and record_dt < from_dt: continue
            if to_dt   and record_dt >= to_dt:  continue

        region = fm["region"]
        if region in ("", "none"):
            if fm["acm_pk"] in PK_ACMS or fm["acm_fb"] in PK_ACMS: region = "pk"
            elif fm["acm_in"] in IN_ACMS or fm["acm_fb"] in IN_ACMS: region = "in"

        if region_filter_c != "all" and region != region_filter_c: continue
        if "all" not in allowed_regs_set and region not in allowed_regs_set: continue

        acm = fm["acm_in"] if region == "in" else fm["acm_pk"]
        if not acm: acm = fm["acm_fb"]

        if "all" not in allowed_acms_set and acm.lower().strip() not in allowed_acms_set: continue
        if "all" not in acm_filter_set and acm.lower().strip() not in acm_filter_set: continue

        req_type, status, agency_type, closing_reason, other_app = fm["req_type"], fm["status"], fm["a_type"], fm["cl_rsn"], fm["o_app"]

        if type_filter_c != "all" and type_filter_c != agency_type: continue

        is_done     = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        is_bd_kpi      = "bd creation" in req_type
        is_closing_kpi = "closing agency" in req_type
        is_creation_kpi= any(p in req_type for p in ["agency creation","applied already","follow-up"])

        agency_type_title = agency_type.title() if agency_type else "Unknown"
        date_str = record_dt.strftime("%Y-%m-%d") if record_dt else None

        if is_done and date_str:
            if is_creation_kpi and date_str in stats["daily_trend_creation"]: stats["daily_trend_creation"][date_str] += 1
            if is_bd_kpi and date_str in stats["daily_trend_bd"]: stats["daily_trend_bd"][date_str] += 1
            if is_closing_kpi and date_str in stats["daily_trend_closing"]: stats["daily_trend_closing"][date_str] += 1

        if is_closing_kpi:
            stats["kpis"]["closings"] += 1
            if is_done: stats["closing_status"]["Done"] += 1
            elif is_rejected: stats["closing_status"]["Rejected"] += 1
            else: stats["closing_status"]["Under Investigation"] += 1
            if closing_reason:
                cr_title = closing_reason.title()
                stats["closing_reasons_pie"][cr_title] = stats["closing_reasons_pie"].get(cr_title,0)+1
                if acm:
                    ca = acm.title()
                    if ca not in stats["acm_closing_reasons"]: stats["acm_closing_reasons"][ca] = {"User Request":0,"Duplicated Hosting":0}
                    if "user" in closing_reason: stats["acm_closing_reasons"][ca]["User Request"] += 1
                    elif "dup" in closing_reason: stats["acm_closing_reasons"][ca]["Duplicated Hosting"] += 1
        elif is_bd_kpi:
            stats["kpis"]["bds"] += 1
            if is_done: stats["bd_status"]["Done"] += 1
            elif is_rejected: stats["bd_status"]["Rejected"] += 1
            else: stats["bd_status"]["Under Investigation"] += 1
        elif is_creation_kpi:
            stats["kpis"]["creations"] += 1
            if is_done: stats["creation_status"]["Done"] += 1
            elif is_rejected: stats["creation_status"]["Rejected"] += 1
            else: stats["creation_status"]["Under Investigation"] += 1
            if is_done and acm:
                ca = acm.title()
                stats["acm_performance"][ca] = stats["acm_performance"].get(ca,0)+1
            if is_done and other_app:
                oa = other_app.title()
                stats["other_apps"][oa] = stats["other_apps"].get(oa,0)+1
            if agency_type_title != "Unknown":
                stats["agency_types"][agency_type_title] = stats["agency_types"].get(agency_type_title,0)+1
            for ct in fm["cr_ways"]:
                if ct: stats["creation_types"][ct.title()] = stats["creation_types"].get(ct.title(),0)+1
            if is_rejected:
                for rr in fm["rj_rsns"]:
                    if rr: stats["reject_reasons"][rr.title()] = stats["reject_reasons"].get(rr.title(),0)+1
        elif req_type:
            stats["other_request_types"][req_type.title()] = stats["other_request_types"].get(req_type.title(),0)+1

    for k in ["acm_performance","reject_reasons","closing_reasons_pie","other_apps","creation_types","agency_types","other_request_types"]:
        stats[k] = dict(sorted(stats[k].items(), key=lambda x:x[1], reverse=True))
    for k in ["daily_trend_creation","daily_trend_bd","daily_trend_closing"]:
        stats[k] = dict(sorted(stats[k].items()))

    return stats

# ════════════════════════════════════════════════════════════════════
# BACKGROUND SNAPSHOT MANAGER
# ════════════════════════════════════════════════════════════════════
BACKGROUND_SYNC_INTERVAL = 180   
BACKGROUND_SYNC_MAX_AGE  = 600   

_bg_sync = {
    "requests_items": [], "requests_keys": set(),
    "updated_at": 0, "fetch_complete": True, "stop_reason": "",
    "syncing": False,
}
_bg_sync_lock = threading.Lock()
_bg_thread_started = False
_bg_thread_lock = threading.Lock()

REDIS_KEY_REQUESTS_SNAPSHOT = "xena:snapshot:requests_table"

BACKGROUND_SYNC_FETCH_BUDGET_SECONDS = 50

def _background_sync_requests_table():
    with _bg_sync_lock:
        if _bg_sync["syncing"]: return
        _bg_sync["syncing"] = True
    try:
        # BUG FIX: this used to call fetch_feishu_records with no from_dt AND no time
        # budget, which means "walk the entire table's history, every single cycle,
        # with no way to stop early" (the early-stop heuristic only kicks in when
        # from_dt is set). /api/sync/refresh calls this synchronously and blocks on the
        # result, so an unbounded fetch here was very likely why the Redis snapshot
        # was going stale/never populating -- the cron call itself was timing out on
        # Vercel before it could finish and write anything to Redis, which in turn is
        # why /api/my-requests kept falling through to the slow, also-timing-out cold
        # path. Bounding this means the cron job reliably finishes and writes a
        # (possibly partial, newest-first) snapshot every cycle instead of sometimes
        # writing nothing at all.
        items, keys, complete, reason = fetch_feishu_records(REQUESTS_TABLE_ID, max_seconds=BACKGROUND_SYNC_FETCH_BUDGET_SECONDS)
        now = time.time()
        with _bg_sync_lock:
            _bg_sync["requests_items"]  = items
            _bg_sync["requests_keys"]   = keys
            _bg_sync["updated_at"]      = now
            _bg_sync["fetch_complete"]  = complete
            _bg_sync["stop_reason"]     = reason
        if REDIS_ENABLED and items:
            redis_set_json(REDIS_KEY_REQUESTS_SNAPSHOT, {
                "items": items, "keys": sorted(list(keys)), "updated_at": now,
                "fetch_complete": complete, "stop_reason": reason,
            }, ttl=BACKGROUND_SYNC_MAX_AGE + 300)
    except Exception as e:
        logger.error("background_sync_failed", table="grand_table", error=str(e))
    finally:
        with _bg_sync_lock:
            _bg_sync["syncing"] = False

def _background_sync_loop():
    while True:
        _background_sync_requests_table()
        time.sleep(BACKGROUND_SYNC_INTERVAL)

def ensure_background_sync_started():
    global _bg_thread_started
    with _bg_thread_lock:
        if not _bg_thread_started:
            threading.Thread(target=_background_sync_loop, daemon=True).start()
            _bg_thread_started = True

GET_SNAPSHOT_COLD_FETCH_BUDGET_SECONDS = 20

def get_requests_table_snapshot(from_dt=None):
    local_data = load_local_json("requests.json")
    local_age = local_json_age_seconds("requests.json")
    local_is_fresh = local_data and (local_age is None or local_age < LOCAL_SNAPSHOT_MAX_AGE_SECONDS)

    if local_is_fresh:
        master_keys = set()
        if isinstance(local_data, list) and len(local_data) > 0:
            master_keys.update(local_data[0].get("fields", {}).keys())
        return local_data, master_keys, True, "", True

    ensure_background_sync_started()
    with _bg_sync_lock:
        items, keys, updated_at = _bg_sync["requests_items"], _bg_sync["requests_keys"], _bg_sync["updated_at"]
        complete, reason = _bg_sync["fetch_complete"], _bg_sync["stop_reason"]

    if items and (time.time() - updated_at) < BACKGROUND_SYNC_MAX_AGE:
        return items, keys, complete, reason, True

    if REDIS_ENABLED:
        cached = redis_get_json(REDIS_KEY_REQUESTS_SNAPSHOT)
        if cached and cached.get("items") and (time.time() - cached.get("updated_at", 0)) < BACKGROUND_SYNC_MAX_AGE:
            r_items, r_keys = cached["items"], set(cached.get("keys", []))
            with _bg_sync_lock:
                _bg_sync["requests_items"] = r_items
                _bg_sync["requests_keys"]  = r_keys
                _bg_sync["updated_at"]     = cached.get("updated_at", time.time())
                _bg_sync["fetch_complete"] = cached.get("fetch_complete", True)
                _bg_sync["stop_reason"]    = cached.get("stop_reason", "")
            threading.Thread(target=_background_sync_requests_table, daemon=True).start()
            return r_items, r_keys, cached.get("fetch_complete", True), cached.get("stop_reason", ""), True

    # BUG FIX ("Server timeout: Data too large or loading"): this is the true cold path
    # (no warm in-process cache, no fresh Redis snapshot) -- give it a hard time budget
    # so it always returns *something* as valid JSON well before Vercel's own function
    # timeout can kill it and hand the frontend an unparseable error page. Since Feishu
    # returns newest-first (Numbering DESC), a partial fetch under this budget still
    # covers recent lookback windows like "my requests, last 15 days" correctly -- it
    # just may not reach every record if the table is huge and cold-starting from zero.
    # If you're on a Vercel plan with a longer configurable function duration, raise
    # GET_SNAPSHOT_COLD_FETCH_BUDGET_SECONDS accordingly.
    live_items, live_keys, live_complete, live_reason = fetch_requests_sharded(from_dt=from_dt, max_seconds=GET_SNAPSHOT_COLD_FETCH_BUDGET_SECONDS)
    if live_items:
        return live_items, live_keys, live_complete, live_reason, False

    # Nothing fresher was reachable (live Feishu fetch itself came back empty/failed,
    # e.g. a transient outage). Fall back to the stale build snapshot rather than
    # showing an empty dashboard -- stale-but-present beats nothing -- but say so
    # honestly via stop_reason so the frontend can surface a staleness warning
    # instead of silently presenting old data as current.
    if local_data:
        age_note = f" (build snapshot is {int(local_age/3600) if local_age else '?'}h old)"
        return local_data, set(local_data[0].get("fields", {}).keys()) if local_data else set(), False, f"stale_snapshot{age_note}", True

    return live_items, live_keys, live_complete, live_reason, False

REDIS_KEY_POINTS_SNAPSHOT = "xena:snapshot:points_table"
_bg_points_sync = {"items": [], "updated_at": 0, "fetch_complete": True, "stop_reason": "", "syncing": False}
_bg_points_lock = threading.Lock()
_bg_points_thread_started = False
_bg_points_thread_lock = threading.Lock()

def _background_sync_points_table():
    with _bg_points_lock:
        if _bg_points_sync["syncing"]: return
        _bg_points_sync["syncing"] = True
    try:
        items, _keys, complete, reason = fetch_feishu_records(POINTS_TABLE_ID)
        now = time.time()
        with _bg_points_lock:
            _bg_points_sync["items"]          = items
            _bg_points_sync["updated_at"]     = now
            _bg_points_sync["fetch_complete"] = complete
            _bg_points_sync["stop_reason"]    = reason
        if REDIS_ENABLED and items:
            redis_set_json(REDIS_KEY_POINTS_SNAPSHOT, {
                "items": items, "updated_at": now, "fetch_complete": complete, "stop_reason": reason,
            }, ttl=BACKGROUND_SYNC_MAX_AGE + 300)
    except Exception as e:
        logger.error("background_sync_failed", table="points_table", error=str(e))
    finally:
        with _bg_points_lock:
            _bg_points_sync["syncing"] = False

def _background_sync_points_loop():
    while True:
        _background_sync_points_table()
        time.sleep(BACKGROUND_SYNC_INTERVAL)

def ensure_points_sync_started():
    global _bg_points_thread_started
    with _bg_points_thread_lock:
        if not _bg_points_thread_started:
            threading.Thread(target=_background_sync_points_loop, daemon=True).start()
            _bg_points_thread_started = True

def kick_table_resync():
    """Phase 10.29 (Ahmed): write-through freshness for the warm snapshots.
    A portal write (charge / correct-charge) lands in Feishu instantly, but
    the background loop only re-reads the tables every BACKGROUND_SYNC_INTERVAL
    (180s) -- and the Feishu webhook that would have made it instant can't
    reach the deployment through the GFW. So every successful portal write
    kicks an immediate background re-sync of BOTH tables: the Live Formula
    chips and the Target Board reflect the new numbers within seconds, still
    with zero extra Feishu calls on the read path."""
    try:
        threading.Thread(target=_background_sync_points_table, daemon=True).start()
        threading.Thread(target=_background_sync_requests_table, daemon=True).start()
    except Exception:
        pass

def get_points_snapshot_from_public_file():
    """Target Board (Agency Exchange) data source: ONLY the public build snapshot
    (public/data/points.json), exactly like Search Records' preferred source.
    Never touches Feishu, never touches the background sync / Redis chain that
    get_points_table_snapshot() falls through to -- if the file is missing or
    stale we say so honestly instead of silently fetching live."""
    data = load_local_json("points.json")
    if not data:
        return None, False, "public/data/points.json not found on this deployment"
    age = local_json_age_seconds("points.json")
    note = ""
    if age is not None and age > LOCAL_SNAPSHOT_MAX_AGE_SECONDS:
        note = f"stale_snapshot ({int(age/3600)}h old)"
    return data, True, note

def get_points_table_snapshot():
    local_data = load_local_json("points.json")
    local_age = local_json_age_seconds("points.json")
    local_is_fresh = local_data and (local_age is None or local_age < LOCAL_SNAPSHOT_MAX_AGE_SECONDS)

    if local_is_fresh:
        return local_data, True, "", True

    ensure_points_sync_started()
    with _bg_points_lock:
        items, updated_at = _bg_points_sync["items"], _bg_points_sync["updated_at"]
        complete, reason = _bg_points_sync["fetch_complete"], _bg_points_sync["stop_reason"]

    if items and (time.time() - updated_at) < BACKGROUND_SYNC_MAX_AGE:
        return items, complete, reason, True

    if REDIS_ENABLED:
        cached = redis_get_json(REDIS_KEY_POINTS_SNAPSHOT)
        if cached and cached.get("items") and (time.time() - cached.get("updated_at", 0)) < BACKGROUND_SYNC_MAX_AGE:
            with _bg_points_lock:
                _bg_points_sync["items"]          = cached["items"]
                _bg_points_sync["updated_at"]     = cached.get("updated_at", time.time())
                _bg_points_sync["fetch_complete"] = cached.get("fetch_complete", True)
                _bg_points_sync["stop_reason"]    = cached.get("stop_reason", "")
            threading.Thread(target=_background_sync_points_table, daemon=True).start()
            return cached["items"], cached.get("fetch_complete", True), cached.get("stop_reason", ""), True

    live_items, _live_keys, live_complete, live_reason = fetch_feishu_records(POINTS_TABLE_ID)
    if live_items:
        return live_items, live_complete, live_reason, False

    # Same stale-but-present fallback as get_requests_table_snapshot(): only reached
    # if live Feishu is also unreachable right now.
    if local_data:
        age_note = f" (build snapshot is {int(local_age/3600) if local_age else '?'}h old)"
        return local_data, False, f"stale_snapshot{age_note}", True

    return live_items, live_complete, live_reason, False


app = Flask(__name__)

@app.before_request
def _presence_before_request():
    """Phase 10.27 (Ahmed): piggyback presence on traffic that ALREADY
    happens -- zero extra requests. Identity comes from the same
    headers/params the endpoints themselves read. The X-Preview-As target is
    deliberately NOT stamped (previewing a member must not mark THEM online)."""
    try:
        if not request.path.startswith("/api/"):
            return
        email = (request.headers.get("X-User-Email", "") or request.args.get("email", "")
                 or request.form.get("email", "") or "")
        name = (request.headers.get("X-User-Name", "") or request.args.get("user", "")
                or request.form.get("user", "") or "")
        if not email and request.is_json:
            body = request.get_json(silent=True) or {}
            email = str(body.get("email", "") or "")
            name = name or str(body.get("user", "") or "")
        # Key by email when we have it; some endpoints only carry the caller's
        # NAME (`user` param) -- the admin/users merge checks both keys.
        presence_touch(sanitize_text(email, 200) or sanitize_text(name, 120),
                       sanitize_text(name, 120))
    except Exception:
        pass

@app.route('/api/webhook/feishu', methods=['GET', 'POST'])
def feishu_webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
        if "challenge" in data:
            return jsonify({"challenge": data["challenge"]})
            
        header = data.get("header", {})
        event = data.get("event", {})
        
        if header.get("event_type") in ["bitable.application.record.created_v1", "bitable.application.record.updated_v1"]:
            if event.get("table_id") == REQUESTS_TABLE_ID:
                threading.Thread(target=_background_sync_requests_table, daemon=True).start()
            elif event.get("table_id") == POINTS_TABLE_ID:
                threading.Thread(target=_background_sync_points_table, daemon=True).start()
                
        return jsonify({"code": 0, "msg": "success"}), 200
    except Exception as e:
        logger.error("webhook_parsing_error", error=str(e))
        return jsonify({"code": 0, "msg": "success"}), 200

@app.route('/', methods=['GET'])
def home():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return send_file(os.path.join(root_dir, 'index.html'))

@app.route('/api/samurai-video', methods=['GET'])
def samurai_video():
    # Serves the cinematic intro video (sam.mp4). Deploy sam.mp4 at the project root,
    # alongside index.html (same root_dir home() already uses above) -- no separate
    # /public static folder or vercel.json change needed, this mirrors the exact
    # pattern already used for index.html itself.
    # conditional=True enables HTTP range-request support, which browsers rely on for
    # smooth video buffering/seeking -- without it, the whole file must download
    # before playback can start.
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    video_path = os.path.join(root_dir, 'sam.mp4')
    if not os.path.exists(video_path):
        return jsonify({"error": "sam.mp4 not found at project root -- deploy it alongside index.html"}), 404
    response = send_file(video_path, mimetype='video/mp4', conditional=True)
    response.headers['Cache-Control'] = 'public, max-age=604800, immutable'
    return response

@app.route('/api/samurai-image', methods=['GET'])
def samurai_image():
    # Serves the intro's still image (samurai silhouetted against the moon).
    # Two variants now exist -- a portrait composition (full standing figure,
    # shot for tall/mobile screens) and a landscape composition (shot for wide
    # desktop screens) -- so each device gets an image natively shaped for its
    # viewport instead of one image being cropped or letterboxed to fit both.
    # ?variant=portrait|landscape selects which; the frontend (playSamuraiIntro
    # in index.html) picks based on window.innerWidth before requesting.
    # Deploy BOTH samurai_moon_portrait.png and samurai_moon_landscape.png at
    # the project root, alongside index.html and sam.mp4.
    #
    # CACHING: response is Cache-Control: immutable, max-age=7 days (below).
    # That's safe ONLY because the frontend appends a ?v= version param that gets
    # bumped whenever the underlying PNG file changes (see SAMURAI_ASSET_VERSION
    # in index.html) -- the ?v= query param makes it a *new* URL every time, so
    # the old cached response is never reused. If a future edit here drops the
    # version param, browsers/Vercel's edge cache will keep serving whatever PNG
    # they cached first for up to a week, even after a new file is deployed.
    variant = request.args.get('variant', 'portrait')
    filename = 'samurai_moon_landscape.png' if variant == 'landscape' else 'samurai_moon_portrait.png'
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    image_path = os.path.join(root_dir, filename)
    if not os.path.exists(image_path):
        return jsonify({"error": f"{filename} not found at project root -- deploy it alongside index.html"}), 404
    response = send_file(image_path, mimetype='image/png', conditional=True)
    response.headers['Cache-Control'] = 'public, max-age=604800, immutable'
    return response

@app.route('/api/login', methods=['GET'])
def login():
    if MOCK_MODE: return redirect(f"/?user=Test%20User&email=test@example.com&uat=mock_token_123&avatar=https://ui-avatars.com/api/?name=Test+User")
    safe_redirect = urllib.parse.quote(REDIRECT_URI)
    feishu_url = f"https://open.feishu.cn/open-apis/authen/v1/index?app_id={APP_ID}&redirect_uri={safe_redirect}"
    return redirect(feishu_url)

@app.route('/api/callback', methods=['GET'])
def callback():
    code = request.args.get('code')
    if not code: return redirect("/?auth_error=" + urllib.parse.quote("Authorization failed: no code returned.", safe=''))

    try:
        token_resp = http_requests.post(
            "https://open.feishu.cn/open-apis/authen/v1/access_token",
            headers={"Content-Type": "application/json"},
            json={"app_id": APP_ID, "app_secret": APP_SECRET, "grant_type": "authorization_code", "code": code},
            timeout=15
        ).json()

        uat = (token_resp.get("data") or {}).get("access_token") or token_resp.get("access_token")

        if not uat:
            err = token_resp.get("msg") or token_resp.get("error_description") or "Token exchange failed"
            return redirect("/?auth_error=" + urllib.parse.quote(f"Login failed: {err}", safe=''))

        info_resp = http_requests.get("https://open.feishu.cn/open-apis/authen/v1/user_info", headers={"Authorization": f"Bearer {uat}"}, timeout=15).json()
        user_data = info_resp.get("data", {})
        
        lark_name  = user_data.get("name", "Unknown User")
        lark_email = user_data.get("email") or user_data.get("enterprise_email") or ""
        avatar_url = user_data.get("avatar_72") or user_data.get("avatar_url") or ""
        lark_open_id = user_data.get("open_id") or ""

        refresh_token = (token_resp.get("data") or {}).get("refresh_token") or token_resp.get("refresh_token")
        if refresh_token and lark_open_id and REDIS_ENABLED:
            # Phase 10.14: 24h (was 4h) -- see _refresh_user_token for why.
            redis_cmd("SET", f"xena:refresh_token:{lark_open_id}", refresh_token, "EX", 24 * 3600)
        if lark_open_id and lark_email and REDIS_ENABLED:
            # Phase 10.44: open_id->email binding, used by /api/auth/refresh-session
            # to verify the caller before handing back a fresh uat (open_id alone
            # is not a secret). Renewed on every silent refresh alongside the
            # refresh-token chain, so it lives exactly as long as the session can.
            redis_cmd("SET", f"xena:uid_email:{lark_open_id}", lark_email, "EX", 24 * 3600)
        if lark_open_id and lark_name and REDIS_ENABLED:
            try:
                # Phase 11.8: every login refreshes the resolver's name/avatar
                # cache for THIS user — so a comment author who never appears
                # in any sheet person field (never applied, never assigned)
                # still resolves, straight from their own login. Zero extra
                # Feishu calls: user_info was already fetched above.
                redis_set_json(f"xena:uname:{lark_open_id}",
                               {"name": lark_name, "avatar": avatar_url or ""},
                               ttl=HARVEST_NAME_TTL)
            except Exception:
                pass
        uat_expires_in = (token_resp.get("data") or {}).get("expires_in") or token_resp.get("expires_in") or 7200
        if lark_open_id and REDIS_ENABLED:
            redis_cmd("SET", f"xena:uat:{lark_open_id}", uat, "EX", max(int(uat_expires_in) - 300, 60))

        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
        audit.log(lark_name, "LOGIN", lark_email, ip=ip)
        
        return redirect(f"/?user={urllib.parse.quote(lark_name, safe='')}&email={urllib.parse.quote(lark_email, safe='')}&uat={urllib.parse.quote(uat, safe='')}&avatar={urllib.parse.quote(avatar_url, safe='')}&open_id={urllib.parse.quote(lark_open_id, safe='')}")

    except Exception as exc:
        return redirect("/?auth_error=" + urllib.parse.quote(f"Login error: {str(exc)[:120]}", safe=''))

@app.route('/api/app-version', methods=['GET'])
def app_version():
    """No auth required -- just a version fingerprint so the frontend can
    detect a new deploy shipping mid-session and force a fresh login rather
    than keep running stale code (or, worse, against server logic that's
    already changed)."""
    return jsonify({"version": APP_VERSION})

@app.route('/api/auth/check-session', methods=['GET'])
def check_session():
    """Cheap, side-effect-free session probe. Reuses the exact same
    server-side check submit_request() already does right before filing a
    ticket (_get_cached_uat / _refresh_user_token against the agent's real
    open_id) -- but callable up front, so the frontend can catch an already-
    dead session the moment a form tab is opened instead of only after the
    agent has filled the whole thing in and clicked Submit. No Feishu write
    of any kind happens here, just a token cache read (and, if that's cold,
    a refresh-token exchange) -- safe to call on every tab open."""
    user = sanitize_text(request.args.get('user', ''))
    open_id = sanitize_text(request.args.get('open_id', ''))
    if not user:
        return jsonify({"error": "Unauthorized"}), 403

    # Same guard as submit_request(): only actually verifiable when Redis is
    # enabled, we're not in mock mode, and this session has an open_id on
    # file. Anything else (legacy session, Redis off, mock mode) has nothing
    # server-side to check against, so there's no false "expired" here --
    # the client's own 4h/2h timers remain the only signal for those cases.
    if REDIS_ENABLED and not MOCK_MODE and open_id:
        uat = _get_cached_uat(open_id) or _refresh_user_token(open_id)
        if not uat:
            if not redis_healthy():
                # Phase 10.44: Redis itself is unreachable right now, so an
                # empty cache PROVES NOTHING about the session. Fail open
                # instead of force-logging the agent over an infrastructure
                # blip -- submits still get the real check when Redis recovers.
                logger.warn("check_session_redis_down_fail_open", user=user)
                return jsonify({"valid": True, "degraded": True})
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
            audit.log(user, "SESSION_EXPIRED_ON_OPEN", "Detected opening New Request", ip=ip, severity="Warning")
            return jsonify({"error": "Your session has expired. Please log in again — anything you already filled in has been saved and will be restored automatically.", "code": "SESSION_EXPIRED"}), 401

    return jsonify({"valid": True})

@app.route('/api/auth/refresh-session', methods=['GET'])
def refresh_session():
    """Silent session renewal (Phase 10.44). The browser's own uat copy dies
    with Feishu's expires_in (~2-3h) even though the server-side refresh chain
    lives 24h+ -- so the frontend calls this every ~30 min (and before every
    submit/save) to swap in the server's freshest uat without any visible
    login. Cost: one Redis read when the cache is warm (zero Feishu calls),
    one Feishu refresh exchange when it's cold (~every 2h of activity).
    Returns the fresh uat so the browser's copy stops going stale mid-form.
    Security: hands the token back ONLY when the caller proves the email bound
    to this open_id at login (open_id alone is not a secret). 401 with
    SESSION_EXPIRED only when the chain is verifiably dead -- a Redis outage
    answers degraded/success instead of killing the session."""
    if MOCK_MODE:
        return jsonify({"success": True, "uat": "mock_token_123", "expires_in": 7200})
    user = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    open_id = sanitize_text(request.args.get('open_id', ''))
    if not user:
        return jsonify({"error": "Unauthorized"}), 403
    if REDIS_ENABLED and open_id:
        bound_email = redis_cmd("GET", f"xena:uid_email:{open_id}")
        if not bound_email:
            # No binding on file. Two very different cases: (a) a session that
            # predates this field but whose refresh chain is still alive --
            # must NOT be logged out over a missing migration artifact, so it
            # gets a degraded pass (no token handed out; the binding is
            # created at the next real login); (b) genuinely nothing on file
            # -- a verifiably dead chain.
            if not redis_healthy():
                return jsonify({"success": True, "degraded": True})
            if redis_cmd("GET", f"xena:refresh_token:{open_id}") or redis_cmd("GET", f"xena:uat:{open_id}"):
                return jsonify({"success": True, "degraded": True})
            return jsonify({"error": "Your session has expired. Please log in again — anything you already filled in has been saved and will be restored automatically.", "code": "SESSION_EXPIRED"}), 401
        if not email or bound_email.strip().lower() != email.strip().lower():
            # A live binding that the caller can't match = not the owner's
            # browser -- refuse WITHOUT the SESSION_EXPIRED code so the
            # frontend shows a plain error instead of nuking the session.
            return jsonify({"error": "Session identity mismatch."}), 403
        uat = _get_cached_uat(open_id) or _refresh_user_token(open_id)
        if not uat:
            if not redis_healthy():
                logger.warn("refresh_session_redis_down_fail_open", user=user)
                return jsonify({"success": True, "degraded": True})
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
            audit.log(user, "SESSION_EXPIRED_ON_RENEW", "Silent renewal found a dead chain", ip=ip, severity="Warning")
            return jsonify({"error": "Your session has expired. Please log in again — anything you already filled in has been saved and will be restored automatically.", "code": "SESSION_EXPIRED"}), 401
        ttl = redis_cmd("TTL", f"xena:uat:{open_id}")
        return jsonify({"success": True, "uat": uat,
                        "expires_in": ttl if isinstance(ttl, int) and ttl > 0 else 6900})
    # Nothing to verify server-side (Redis disabled / legacy session without
    # open_id) -- keep the session running on the client-side timers alone.
    return jsonify({"success": True, "degraded": True})

@app.route('/api/auth/me', methods=['GET'])
def check_auth():
    username = sanitize_text(request.args.get('user',''))
    email    = sanitize_text(request.args.get('email',''))
    perms    = get_user_permissions(email, username)
    return jsonify(perms)

@app.route('/api/search', methods=['GET', 'POST'])
@rate_limit(*RATE_LIMIT_SEARCH)
def search():
    req_data = request.json if request.method == 'POST' else request.args
    
    code    = sanitize_agency_code(req_data.get('code',''))
    user    = sanitize_text(req_data.get('user',''))
    email   = sanitize_text(req_data.get('email',''))
    qtype   = req_data.get('type','points')
    
    if qtype not in ('points','target'): qtype = 'points'
    if not code: return jsonify({"found":False,"error":"Invalid or missing agency code."}), 400

    perms = get_user_permissions(email, user)
    
    if not perms.get("is_super_admin") and not any(qtype in m for m in perms.get("modules", [])):
        return jsonify({"found": False, "error": f"Access Denied: You do not have permission to view {qtype.title()}."}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get(qtype,["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("query",["all"])

    data = fetch_agency_data(code, qtype, allowed_acms, allowed_regs)
    
    if data.get("found"):
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
        audit.log(user, "AGENCY_SEARCH", f"Code: {code} | Type: {qtype}", ip=ip, severity="Info")
        return jsonify(data)
    else:
        return jsonify(data), 404

@app.route('/api/admin/users', methods=['GET','POST','DELETE'])
def manage_users():
    # BUG FIX: see the matching comment in audit_logs() -- this must check by email
    # AND name, exactly like /api/auth/me does, or admins whose Feishu "Person" text
    # doesn't match their Lark name get an Admin nav group they can't actually use.
    admin_name  = sanitize_text(request.headers.get('X-User-Name','')).lower()
    admin_email = sanitize_text(request.headers.get('X-User-Email','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions(admin_email, admin_name)
        if perms.get("is_super_admin"): is_authorized = True
    if not is_authorized:
        audit.log(admin_name, "UNAUTHORIZED_ADMIN_ACCESS", "admin_panel", ip=request.headers.get("X-Forwarded-For",""), severity="Critical")
        return jsonify({"error":"Unauthorized"}), 403

    if MOCK_MODE: return jsonify([{"id":"mock","email":"test@example.com","modules":"admin, target, points","acms_raw":"all","regions_raw":"all"}])

    tat = get_tenant_access_token()
    headers  = {"Authorization":f"Bearer {tat}","Content-Type":"application/json"}
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    ip = request.headers.get("X-Forwarded-For","")

    if request.method == 'GET':
        res   = http_requests.get(base_url, headers=headers, params={"page_size":500}, timeout=15).json()
        users = []
        for item in res.get("data",{}).get("items",[]):
            fields = item.get("fields",{})
            person_text = extract_field_text(fields.get("Person",""))
            display_email = extract_field_text(fields.get("Email","")) or person_text
            users.append({"id":item.get("record_id"),"email":display_email,
                          # Person rides the same records fetch (zero extra API calls)
                          # so the frontend can show real Lark names on agent cards.
                          "person":person_text,
                          "modules":extract_field_text(fields.get("Modules","")),
                          "acms_raw":extract_field_text(fields.get("ACMs","")),
                          "regions_raw":extract_field_text(fields.get("Regions","all")),
                          # Role presets are stored as regular rows in this same table,
                          # keyed by a "__ROLE__::<name>" email sentinel, so named roles
                          # (CS, ACM, etc.) persist centrally for all admins without
                          # needing a new Feishu table. The frontend splits these out
                          # from real agent rows using this flag.
                          "is_role": display_email.startswith("__ROLE__::")})
        # Phase 10.27 (Ahmed): presence rides INSIDE this same response (zero
        # extra requests) -- one Redis HGETALL for everyone, keyed by the
        # lowercased email/name the before_request hook stamps.
        pmap = presence_map()
        for u in users:
            p = pmap.get((u["email"] or "").lower()) or pmap.get((u.get("person") or "").lower())
            u["last_seen"] = (p or {}).get("ts")
        # Phase 12: ride the per-member access toggle state along (ONE HGETALL
        # for the whole list) so the admin UI can render each row's switch.
        # Absent from the hash = enabled.
        tmap = _access_toggles_map()
        for u in users:
            ident  = (u["email"] or "").lower()
            pident = (u.get("person") or "").lower()
            u["enabled"] = tmap.get(ident, tmap.get(pident, True))
        return jsonify(users)

    elif request.method == 'POST':
        data = request.json or {}
        email_to_check = sanitize_text(data.get("email",""))
        if not email_to_check:
            # Defense-in-depth: a blank identity should never be persisted as an
            # agent/role row (it renders as an unlabeled "ghost" entry in Manage
            # Agents that can't be edited by identity, only removed). Reject it
            # outright instead of silently creating one.
            return jsonify({"success": False, "error": "Email/identity is required and cannot be empty."}), 400
        acms_formatted = (f"target={data.get('acms',{}).get('target','all')};"
                          f"points={data.get('acms',{}).get('points','all')};"
                          f"analytics={data.get('acms',{}).get('analytics','all')};"
                          f"query={data.get('acms',{}).get('query','all')};"
                          f"exchange={data.get('acms',{}).get('exchange','all')}")
        regs_formatted = (f"target={data.get('regions',{}).get('target','all')};"
                          f"points={data.get('regions',{}).get('points','all')};"
                          f"analytics={data.get('regions',{}).get('analytics','all')};"
                          f"query={data.get('regions',{}).get('query','all')};"
                          f"exchange={data.get('regions',{}).get('exchange','all')}")
                          
        payload_fields = {"Email":email_to_check,"Modules":data.get("modules",""),
                          "ACMs":acms_formatted,"Regions":regs_formatted}
        
        payload = {"fields": payload_fields}
        existing_record_id = None
        res_all = http_requests.get(base_url, headers=headers, params={"page_size": 500}, timeout=15).json()
        
        for item in res_all.get("data", {}).get("items", []):
            db_email = extract_field_text(item.get("fields", {}).get("Email", "")).lower().strip()
            db_person = extract_field_text(item.get("fields", {}).get("Person", "")).lower().strip()
            target_check = email_to_check.lower().strip()
            
            if target_check and (target_check == db_email or target_check == db_person):
                existing_record_id = item["record_id"]
                break

        if existing_record_id: res = http_requests.put(f"{base_url}/{existing_record_id}", headers=headers, json=payload, timeout=15).json()
        else: res = http_requests.post(base_url, headers=headers, json=payload, timeout=15).json()

        if res.get("code") != 0: return jsonify({"success":False,"error":res.get("msg","Unknown error")}), 500
            
        audit.log(admin_name, "UPDATE_USER" if existing_record_id else "ADD_USER", email_to_check, ip=ip, severity="Info")
        cache_invalidate() # FIX: Clear the entire permission cache immediately on updates
        return jsonify({"success":True,"record_id":res.get("data",{}).get("record",{}).get("record_id")})

    elif request.method == 'DELETE':
        record_id = sanitize_text(request.args.get('id',''))
        res = http_requests.delete(f"{base_url}/{record_id}", headers=headers, timeout=15).json()
        if res.get("code") != 0: return jsonify({"success":False,"error":res.get("msg","Delete failed")}), 500
        
        cache_invalidate() # FIX: Clear the permission cache immediately when deleting an agent
        
        audit.log(admin_name, "DELETE_USER", record_id, ip=ip, severity="Warning")
        return jsonify({"success":True})

def _require_admin_identity():
    """Shared admin gate for the small Phase-12 admin endpoints (same rules
    as /api/admin/users: hardcoded ADMIN_USERS by name, or is_super_admin
    permissions). Returns (admin_name, admin_email, error_response|None)."""
    admin_name  = sanitize_text(request.headers.get('X-User-Name','')).lower()
    admin_email = sanitize_text(request.headers.get('X-User-Email','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions(admin_email, admin_name)
        if perms.get("is_super_admin"): is_authorized = True
    if not is_authorized:
        audit.log(admin_name, "UNAUTHORIZED_ADMIN_ACCESS", "admin_panel",
                  ip=request.headers.get("X-Forwarded-For",""), severity="Critical")
        return admin_name, admin_email, (jsonify({"error": "Unauthorized"}), 403)
    return admin_name, admin_email, None

@app.route('/api/admin/member-toggle', methods=['POST'])
@rate_limit(20, 300)
def admin_member_toggle():
    """Phase 12: THE one toggle per member — covers login access + comment
    notifications + every module (Ahmed: "One toggle per member covering
    everything: login access + notifications"). Body {email, enabled}.
    Writes the Redis toggle hash; _get_user_permissions_raw reads it on every
    request, so the flip takes effect immediately (no cache bust, no
    re-login needed for the admin). The target's NEXT request is denied /
    allowed. Toggling the hardcoded super-admins is accepted but harmless —
    their failsafe path never checks the toggle."""
    admin_name, admin_email, err = _require_admin_identity()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    target = sanitize_text(data.get("email", "")).strip().lower()
    if not target:
        return jsonify({"success": False, "error": "email is required"}), 400
    enabled = bool(data.get("enabled", True))
    try:
        redis_cmd("HSET", ACCESS_TOGGLES_KEY, target, "1" if enabled else "0")
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 502
    audit.log(admin_name, "MEMBER_TOGGLE_ON" if enabled else "MEMBER_TOGGLE_OFF",
              target, ip=request.headers.get("X-Forwarded-For",""),
              severity="Warning" if not enabled else "Info")
    return jsonify({"success": True, "email": target, "enabled": enabled})

@app.route('/api/admin/harvest-names', methods=['POST'])
@rate_limit(30, 300)   # the admin UI loops ~4-page calls until the pass completes
def admin_harvest_names():
    """Phase 12 (Ahmed: "find sol that the name that is not in the sheet not
    appare and appare like user ... or you can make it"): admin-triggered bulk
    walk of the Grand Table harvesting every person envelope into the
    name/avatar cache — the scope-free directory that resolves teammates who
    appear in the sheet but show as 'user-xxxx' in comments, WITHOUT the
    contact-visibility admin access (Route A). Body {pages: N} (cap 20/call,
    500 records/page); the frontend loops until done:true. Shares the
    incremental sweep's cursor, so the background sweep continues from here."""
    admin_name, admin_email, err = _require_admin_identity()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    try:
        pages = int(data.get("pages") or 10)
    except Exception:
        pages = 10
    pages = max(1, min(20, pages))
    result = _harvest_bulk(pages)
    audit.log(admin_name, "HARVEST_NAMES", f"pages={result.get('pages')} harvested={result.get('harvested')} done={result.get('done')}",
              ip=request.headers.get("X-Forwarded-For",""), severity="Info")
    if result.get("error") and not result.get("pages"):
        return jsonify({"success": False, **result}), 502
    return jsonify({"success": True, **result})

@app.route('/api/access-request', methods=['POST'])
@rate_limit(5, 300)
def request_access():
    """Logged-in-but-no-permissions users land on the Access Denied screen and can
    click 'Request Access' from there. We record the request as a Critical audit
    entry (action=ACCESS_REQUEST) rather than a new Feishu table, so it shows up
    immediately in the existing Audit Log / Access Requests admin views without any
    base-schema changes. See /api/admin/access-requests for how these are read back
    and matched against later ADD_USER/UPDATE_USER/ACCESS_REQUEST_RESOLVED entries
    to determine which requests are still pending."""
    data = request.json or {}
    name = sanitize_text(data.get("name", ""), 100)
    email = sanitize_text(data.get("email", ""), 150)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if not name and not email:
        return jsonify({"success": False, "error": "Missing name/email."}), 400
    audit.log(name or email, "ACCESS_REQUEST", email or "(no email)", details=f"Requested by {name}", ip=ip, severity="Critical", blocking=True)
    return jsonify({"success": True})

def _get_recent_audit_logs_cached():
    """Shared by every admin-alert endpoint below. The expensive part of this --
    a live Feishu fetch of up to 200 audit records, plus parsing them -- used to
    run TWICE per poll cycle (once for access-requests, once for sheet-perm
    alerts), independently, from every open admin tab, every 15 seconds. On a
    Vercel Hobby plan that adds up fast: each poll is 2 function invocations
    doing real work, which at 15s intervals is ~345k invocations/month from a
    single tab left open, against a 1M/month budget shared with the whole app.
    This cuts that back down: one fetch, cached in Redis for CACHE_TTL seconds,
    shared across BOTH alert types AND across every admin tab polling around
    the same time, not just one process's local memory."""
    CACHE_TTL = 20
    if REDIS_ENABLED:
        cached = redis_get_json("xena:admin_alerts_logs_cache")
        if cached is not None:
            return cached

    logs = audit.get_recent(500)
    if AUDIT_TABLE_ID and not MOCK_MODE:
        try:
            tat = get_tenant_access_token()
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{AUDIT_TABLE_ID}/records/search?automatic_fields=true"
            headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
            payload = {"page_size": 200, "sort": [{"field_name": "Timestamp", "desc": True}]}
            res = http_requests.post(url, headers=headers, json=payload, timeout=10).json()
            if res.get("code") == 0:
                remote_logs = []
                for item in res.get("data", {}).get("items", []):
                    f = item.get("fields", {})
                    ts_val = f.get("Timestamp")
                    dt_str = datetime.fromtimestamp(ts_val/1000.0).isoformat() if isinstance(ts_val,(int,float)) else str(ts_val)
                    remote_logs.append({
                        "ts": dt_str, "actor": extract_field_text(f.get("Agent","")),
                        "action": extract_field_text(f.get("Action","")),
                        "target": extract_field_text(f.get("Target","")),
                    })
                logs = remote_logs + logs
        except Exception:
            pass

    if REDIS_ENABLED:
        try:
            redis_set_json("xena:admin_alerts_logs_cache", logs, ttl=CACHE_TTL)
        except Exception:
            pass
    return logs

def _derive_access_requests(logs):
    pending = {}
    resolved_after = {}
    for entry in sorted(logs, key=lambda x: x.get("ts","")):
        action = (entry.get("action") or "").upper()
        target = (entry.get("target") or "")
        email_part = target.split("|")[0].strip().lower() if target else ""
        if action == "ACCESS_REQUEST":
            pending[email_part or entry.get("actor","")] = entry
        elif action in ("ADD_USER", "UPDATE_USER", "ACCESS_REQUEST_RESOLVED"):
            key = email_part or entry.get("actor","").lower()
            resolved_after[key] = entry.get("ts","")
    result = []
    for key, entry in pending.items():
        resolved_ts = resolved_after.get(key)
        if resolved_ts and resolved_ts >= entry.get("ts",""):
            continue
        result.append(entry)
    result.sort(key=lambda x: x.get("ts",""), reverse=True)
    return result

def _derive_sheet_permission_alerts(logs):
    pending = {}
    resolved_after = {}
    for entry in sorted(logs, key=lambda x: x.get("ts","")):
        action = (entry.get("action") or "").upper()
        actor_key = (entry.get("actor") or "").lower()
        target = (entry.get("target") or "")
        if action == "SHEET_PERMISSION_MISSING":
            pending[actor_key] = entry
        elif action == "SHEET_PERMISSION_RESOLVED":
            resolved_after[actor_key] = entry.get("ts","")
        elif action == "SUBMIT_NEW_REQUEST" and ("created_via: user_token" in target):
            resolved_after[actor_key] = entry.get("ts","")
    result = []
    for key, entry in pending.items():
        resolved_ts = resolved_after.get(key)
        if resolved_ts and resolved_ts >= entry.get("ts",""):
            continue
        result.append(entry)
    result.sort(key=lambda x: x.get("ts",""), reverse=True)
    return result

def _require_admin():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    admin_email = sanitize_text(request.headers.get('X-User-Email','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions(admin_email, admin_name)
        is_authorized = bool(perms.get("is_super_admin"))
    return is_authorized, admin_name, admin_email

def _preview_access_granted(perms):
    """Phase 10.13: 'Preview agent view' is its own Access Management toggle
    (module key 'preview_access'); super admins / admin-module holders always
    have it. Same convention as every other module check in this file."""
    if perms.get("is_super_admin"): return True
    return any(m in ("preview_access", "admin") for m in perms.get("modules", []))

@app.route('/api/admin/preview-targets', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def preview_targets():
    """
    Phase 10.13 (Ahmed): the member list for "Preview agent view" under Access
    Management. Returns every real agent row (role presets excluded) with
    their module string so the preview holder can pick whose view to simulate.
    """
    user  = sanitize_text(request.headers.get('X-User-Name','') or request.args.get('user',''))
    email = sanitize_text(request.headers.get('X-User-Email','') or request.args.get('email',''))
    perms = get_user_permissions(email, user)
    if not _preview_access_granted(perms):
        return jsonify({"success": False, "error": "Access denied: Preview access required."}), 403
    if MOCK_MODE:
        return jsonify({"success": True, "agents": [
            {"email": "agent.one@example.com", "modules": ["query", "query_requests"]},
            {"email": "agent.two@example.com", "modules": ["tickets", "tickets_live_queue"]},
            {"email": "agent.three@example.com", "modules": ["submit", "submit_new_request", "submit_my_requests"]},
        ]})
    tat = get_tenant_access_token()
    headers  = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    try:
        res = http_requests.get(base_url, headers=headers, params={"page_size": 500}, timeout=15).json()
        agents = []
        for item in res.get("data", {}).get("items", []):
            fields = item.get("fields", {})
            display_email = extract_field_text(fields.get("Email", "")) or extract_field_text(fields.get("Person", ""))
            if not display_email or display_email.startswith("__ROLE__::"):
                continue
            # Phase 10.13.1: modules go out as a real LIST (the access table
            # stores them as one comma-joined string; the picker chips need
            # an array -- a raw string broke the page with ".filter is not a
            # function").
            modules_raw = extract_field_text(fields.get("Modules", "")) or ""
            agents.append({"email": display_email,
                           "modules": [m.strip() for m in modules_raw.split(",") if m.strip()]})
        agents.sort(key=lambda a: a["email"].lower())
        return jsonify({"success": True, "agents": agents})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/admin/preview-access', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def preview_access_view():
    """
    Phase 10.13 (Ahmed): resolves the TARGET member's permissions so the
    portal can re-render exactly what they would see -- the Lark Base
    "Previewing as" idea. View-only by construction: the viewer keeps their
    own session/identity; the frontend only swaps the module list used for
    rendering and blocks every mutating action while preview is active.
    """
    user   = sanitize_text(request.args.get('user',''))
    email  = sanitize_text(request.args.get('email',''))
    target = sanitize_text(request.args.get('target',''))
    if not target:
        return jsonify({"success": False, "error": "Missing target"}), 400
    perms = get_user_permissions(email, user)
    if not _preview_access_granted(perms):
        return jsonify({"success": False, "error": "Access denied: Preview access required."}), 403
    audit.log(user, "PREVIEW_AGENT_VIEW", target, ip=request.headers.get("X-Forwarded-For", request.remote_addr or ""), severity="Info")
    target_perms = get_user_permissions(target, target)
    return jsonify({"success": True, "target": target, "permissions": {
        "modules":        target_perms.get("modules", []),
        "is_super_admin": bool(target_perms.get("is_super_admin")),
        "permissions":    target_perms.get("permissions", {}),
    }})

@app.route('/api/admin/alerts-summary', methods=['GET'])
def admin_alerts_summary():
    """ONE call for both alert types, used by the recurring 15s poll and the
    admin-requests tab -- this is the one to hit repeatedly, not the two
    endpoints below (kept for compatibility/direct linking, but each of those
    still shares the same cached log fetch via _get_recent_audit_logs_cached)."""
    is_authorized, _, _ = _require_admin()
    if not is_authorized:
        return jsonify({"error": "Unauthorized"}), 403
    logs = _get_recent_audit_logs_cached()
    return jsonify({
        "access_requests": _derive_access_requests(logs),
        "sheet_permission_alerts": _derive_sheet_permission_alerts(logs),
        # Phase 10.27 (Ahmed): presence map (email/name -> {name, ts}) rides
        # this existing call too, so an open Manage Agents page keeps its
        # online/last-active pills fresh with zero extra requests.
        "presence": presence_map(),
    })

@app.route('/api/admin/access-requests', methods=['GET'])
def list_access_requests():
    """Returns pending access requests, derived from the audit log: any
    ACCESS_REQUEST entry that isn't followed by a later ADD_USER / UPDATE_USER /
    ACCESS_REQUEST_RESOLVED entry for the same requester is still pending."""
    is_authorized, _, _ = _require_admin()
    if not is_authorized:
        return jsonify({"error": "Unauthorized"}), 403
    return jsonify(_derive_access_requests(_get_recent_audit_logs_cached()))

@app.route('/api/admin/sheet-permission-alerts', methods=['GET'])
def list_sheet_permission_alerts():
    """Same idea as /api/admin/access-requests, same derivation approach (read
    the audit log, no new table): flags agents whose ticket had to be filed
    under the app identity because their own Lark account isn't a collaborator
    with edit rights on the Bitable. A SHEET_PERMISSION_MISSING entry counts as
    resolved once a LATER entry exists for the same agent that's either an
    admin's manual SHEET_PERMISSION_RESOLVED, or that agent's own successful
    SUBMIT_NEW_REQUEST under their real token (created_via: user_token /
    user_token_refreshed) -- i.e. it self-heals the moment their permissions
    actually get fixed and they submit again, no admin action required."""
    is_authorized, _, _ = _require_admin()
    if not is_authorized:
        return jsonify({"error": "Unauthorized"}), 403
    return jsonify(_derive_sheet_permission_alerts(_get_recent_audit_logs_cached()))

@app.route('/api/admin/sheet-permission-alerts/resolve', methods=['POST'])
def resolve_sheet_permission_alert():
    """Lets an admin manually dismiss an alert (e.g. after fixing the agent's
    Bitable collaborator access outside of a resubmission) instead of waiting
    for it to self-heal on their next submit."""
    is_authorized, admin_name, admin_email = _require_admin()
    if not is_authorized:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json or {}
    target_user = sanitize_text(data.get("user", ""), 100)
    if not target_user:
        return jsonify({"success": False, "error": "Missing user."}), 400
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    audit.log(target_user, "SHEET_PERMISSION_RESOLVED", f"Dismissed by {admin_name or admin_email}", ip=ip, severity="Info")
    return jsonify({"success": True})

@app.route('/api/admin/audit-logs', methods=['GET'])
def audit_logs():
    # BUG FIX: this used to authorize by X-User-Name ONLY (get_user_permissions("",
    # admin_name)). Any admin whose Feishu "Person" field text doesn't exactly match
    # their Lark display name (but whose Email does match) would see the Admin nav
    # group in the sidebar -- because /api/auth/me resolves permissions by email OR
    # name -- yet get a 403 the moment they actually opened Audit Log or Manage
    # Agents, since this endpoint (and /api/admin/users) only ever checked name.
    # Now both identifiers are sent and checked, matching /api/auth/me's own logic.
    admin_name  = sanitize_text(request.headers.get('X-User-Name','')).lower()
    admin_email = sanitize_text(request.headers.get('X-User-Email','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions(admin_email, admin_name)
        if not perms.get("is_super_admin"): return jsonify({"error":"Unauthorized"}), 403

    limit    = min(int(request.args.get('limit','200')), 1000)
    q_actor  = sanitize_text(request.args.get('actor','')).lower()
    q_action = sanitize_text(request.args.get('action','')).lower()
    q_search = sanitize_text(request.args.get('search','')).lower()  # free-text over target/action/actor
    q_sev    = sanitize_text(request.args.get('severity','')).lower()
    q_from   = sanitize_text(request.args.get('from',''))  # ISO date, inclusive
    q_to     = sanitize_text(request.args.get('to',''))    # ISO date, inclusive

    if MOCK_MODE:
        logs = audit.get_recent(limit)
    else:
        tat = get_tenant_access_token()
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{AUDIT_TABLE_ID}/records/search?automatic_fields=true"
        headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
        payload = {
            "page_size": min(limit, 500),
            "sort": [{"field_name": "Timestamp", "desc": True}],
        }
        try:
            res = http_requests.post(url, headers=headers, json=payload, timeout=10).json()
            if res.get("code") != 0: raise Exception(res.get("msg", "Feishu API Error"))

            logs = []
            for item in res.get("data", {}).get("items", []):
                f = item.get("fields", {})
                ts_val = f.get("Timestamp")
                if isinstance(ts_val, (int, float)): dt_str = datetime.fromtimestamp(ts_val/1000.0).isoformat()
                else: dt_str = str(ts_val)

                logs.append({
                    "ts": dt_str,
                    "actor": extract_field_text(f.get("Agent", "")),
                    "action": extract_field_text(f.get("Action", "")),
                    "target": extract_field_text(f.get("Target", "")),
                    "ip": extract_field_text(f.get("IP Address", "")),
                    "severity": extract_field_text(f.get("Severity", "Info"))
                })
            logs.sort(key=lambda x: x["ts"], reverse=True)
        except Exception as e:
            logger.error("audit_log_fetch_failed", error=str(e))
            logs = audit.get_recent(limit)

    def _matches(l):
        if q_actor  and q_actor  not in (l.get("actor","") or "").lower(): return False
        if q_action and q_action not in (l.get("action","") or "").lower(): return False
        if q_sev    and q_sev    != (l.get("severity","") or "").lower(): return False
        if q_search:
            hay = " ".join([l.get("actor",""), l.get("action",""), l.get("target",""), l.get("ip","")]).lower()
            if q_search not in hay: return False
        ts = (l.get("ts") or "")[:10]  # YYYY-MM-DD
        if q_from and ts and ts < q_from: return False
        if q_to   and ts and ts > q_to:   return False
        return True

    filtered = [l for l in logs if _matches(l)]
    return jsonify(filtered[:limit])

def _feishu_get(url, headers=None, params=None, timeout=20, tries=2, stream=False):
    """GET against open.feishu.cn with one automatic retry on transient
    network failures (ReadTimeout / ConnectionError). The Vercel -> Feishu-CN
    cross-border link regularly spikes past 10s; a single 10s attempt turned
    those spikes into hard user-facing failures ("Failed to open ticket ...
    Read timed out"), so the ceiling is now 20s with a second attempt before
    the error is allowed to surface. Only network-layer exceptions retry --
    HTTP responses (even 4xx/5xx) come back to the caller unchanged."""
    last = None
    for attempt in range(tries):
        try:
            return feishu_session.get(url, headers=headers, params=params,
                                      timeout=timeout, stream=stream)
        except Exception as e:
            last = e
            logger.warning("feishu_get_retry", url=url[:90], attempt=attempt + 1, error=str(e))
            if attempt + 1 < tries:
                time.sleep(0.8 * (attempt + 1))
    raise last

@app.route('/api/requests/single', methods=['GET'])
def get_single_request():
    record_id = sanitize_text(request.args.get('record_id',''))
    if not record_id:
        return jsonify({"success": False, "error": "Missing record_id"}), 400

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}"
    try:
        resp = _feishu_get(url, headers={"Authorization": f"Bearer {tat}"}, timeout=20, tries=2)
        data = resp.json()
        if data.get("code") == 0:
            # Phase 10.47: teach the name resolver the people on this ticket
            # (person fields carry real display names) so comments/chips stop
            # falling back to masked "user-xxxx" without any contact scope.
            _harvest_person_names((data["data"]["record"] or {}).get("fields"))
            return jsonify({"success": True, "record": data["data"]["record"]})
        return jsonify({"success": False, "error": data.get("msg")}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/requests/pictures', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def get_request_pictures():
    """
    Phase 10.13 (Ahmed): backs the Universal Query card's "View Pictures"
    button. Lazily fetches ONLY this record's attachment fields (Evidence
    Screen / Evidence Screen 2 / NID & Otherapp Screen / NIDs) -- nothing is
    fetched until the button is clicked, and the button itself only exists
    for holders of the "Audition access" toggle in Access Management (module
    key 'audition_access'), enforced here server-side too.
    """
    user      = sanitize_text(request.args.get('user',''))
    email     = sanitize_text(request.args.get('email',''))
    record_id = sanitize_text(request.args.get('record_id',''))
    if not record_id:
        return jsonify({"success": False, "error": "Missing record_id"}), 400
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any(m in ("audition_access", "admin") for m in perms.get("modules", [])):
        return jsonify({"success": False, "error": "Access denied: Audition access required."}), 403
    if MOCK_MODE:
        return jsonify({"success": True, "groups": [
            {"field": "Evidence Screen", "pics": [{"name": "evidence-1.png", "url": "/api/attachments/mock_tok_1?field_id=fldhBymdRS"}]},
        ]})
    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}"
    try:
        resp = _feishu_get(url, headers={"Authorization": f"Bearer {tat}"}, timeout=20, tries=2)
        data = resp.json()
        if data.get("code") != 0:
            return jsonify({"success": False, "error": data.get("msg")}), 400
        fields = data["data"]["record"].get("fields", {})
        groups = []
        for att_field, fid in ATTACHMENT_FIELD_IDS.items():
            pics = extract_attachments(fields.get(att_field), fid)
            if pics:
                groups.append({"field": att_field, "pics": pics})
        return jsonify({"success": True, "groups": groups})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/members/search', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def search_members():
    """
    Backs the "Done by" / "Mentioned Person" user picker.
    Uses Feishu's native user search API with User Access Token (UAT) to instantly 
    search all allowed sub-departments without triggering 40004 or 99991663 errors.

    ENDPOINT FIX: this used to call `contact/v3/users/search`, which needs its own
    contact-book read scopes (e.g. contact:user.base:readonly / visible-scope
    permissions) -- NOT the `contact:user:search` scope. That's why, once
    `contact:user:search` got approved in the Feishu Developer Console, requests
    here stopped erroring (no more 99991672) but silently started returning zero
    results instead: the endpoint actually being called was never covered by the
    scope that was granted. `contact:user:search` gates a different API -- Feishu's
    "Search users" endpoint at `/open-apis/search/v1/user` (GET, query param
    `query`, response shape `data.users[]` with name/open_id/user_id/avatar) -- so
    this now calls that endpoint instead.
    """
    query = sanitize_text(request.args.get('q', '')).strip()
    uat = request.args.get('uat', '')
    
    # The frontend only calls this when typing, but guard against empty queries
    if not query:
        return jsonify({"success": True, "results": [], "count": 0})
        
    if not uat:
        return jsonify({"success": False, "error": "Missing User Access Token. Please refresh the page."}), 400

    # MUST use user_access_token (UAT) -- Feishu's "Search users" API is
    # user-identity-only and will reject a tenant_access_token outright.
    url = "https://open.feishu.cn/open-apis/search/v1/user"
    headers = {
        "Authorization": f"Bearer {uat}",
        "Content-Type": "application/json; charset=utf-8"
    }
    params = {
        "query": query,
        "page_size": 50
    }

    try:
        resp = http_requests.get(url, headers=headers, params=params, timeout=10)
        data = resp.json()

        if data.get("code") != 0:
            # A friendlier message specifically for the missing-scope case (99991672) --
            # this doesn't grant the scope, that has to be done in the Feishu Developer
            # Console (Permissions -> add contact:user:search -> publish a new version
            # -> users must log out/back in for a token carrying the new scope). This
            # just makes it obvious in the UI what's actually missing if it recurs,
            # instead of a bare numeric error code.
            if data.get("code") == 99991672:
                return jsonify({
                    "success": False,
                    "error": "Missing Feishu permission 'contact:user:search' (Search user's information in the address book). Add it in the Feishu Developer Console under Permissions, publish a new app version, then log out and back in."
                }), 400
            return jsonify({
                "success": False, 
                "error": f"{data.get('code')}: {data.get('msg')}"
            }), 400

        members = []
        for u in (data.get("data", {}) or {}).get("users", []):
            open_id = u.get("open_id")
            name = u.get("name")
            if not open_id or not name:
                continue

            members.append({
                "open_id": open_id,
                "name": name,
                "avatar_url": (u.get("avatar") or {}).get("avatar_72", "")
            })

        return jsonify({"success": True, "results": members, "count": len(members)})

    except Exception as e:
        logger.error("member_search_failed", error=str(e))
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/ocr/lark', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def lark_ocr_extract():
    """
    Secondary/fallback OCR for the image lightbox: Feishu/Lark's optical_char_recognition
    API (https://open.feishu.cn/open-apis/optical_char_recognition/v1/image/basic_recognize).

    NOTE on why this is additive, not a replacement for the existing RapidOCR/Tesseract
    overlay: this Lark endpoint just returns a flat `text_list` of recognized strings
    with no bounding boxes/coordinates, so it can't drive the pixel-aligned selectable
    text layer drawn on top of the image. It powers a separate "Extract Text" button
    that dumps a plain copyable text block below the image -- useful when the on-image
    selection is fuzzy/wrong, but not a drop-in swap for it.
    """
    data = request.get_json(silent=True) or {}
    user = sanitize_text(data.get('user', ''))
    image_b64 = data.get('image', '')

    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    if not image_b64:
        return jsonify({"success": False, "error": "Missing image data"}), 400

    # Frontend sends a raw base64 string, but strip a data: URL prefix defensively
    # in case that ever changes.
    if image_b64.startswith('data:'):
        image_b64 = image_b64.split(',', 1)[-1]

    # Feishu's hard limit is 5MB of image bytes; base64 inflates that by ~4/3, so
    # reject clearly-oversized payloads before spending a request on them.
    if len(image_b64) > 7_000_000:
        return jsonify({"success": False, "error": "Image is too large for OCR (5MB limit). Try zooming out or downloading it instead."}), 400

    try:
        tat = get_tenant_access_token()
        url = "https://open.feishu.cn/open-apis/optical_char_recognition/v1/image/basic_recognize"
        headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json; charset=utf-8"}
        resp = http_requests.post(url, headers=headers, json={"image": image_b64}, timeout=20)
        result = resp.json()

        if result.get("code") != 0:
            return jsonify({"success": False, "error": f"{result.get('code')}: {result.get('msg')}"}), 400

        text_list = ((result.get("data") or {}).get("text_list")) or []
        return jsonify({"success": True, "text_list": text_list, "text": "\n".join(text_list)})

    except Exception as e:
        logger.error("lark_ocr_failed", error=str(e))
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/requests/update', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def update_request():
    """
    Ticket workspace "Save & Resolve" / "Run Duplicate Check" patch endpoint --
    see twSaveWorkspace() / twRunVerify() in the frontend, which POST
    multipart/form-data with `user`, `record_id`, a JSON-encoded `fields` blob,
    and any newly attached files keyed by their field name.

    BUG FIX (missing route): this endpoint's own @app.route/def line had been lost
    in an earlier edit, leaving everything below as dead, unreachable code sitting
    inside search_members() -- every "Save & Resolve" call was hitting a plain 404
    instead of ever running any of this handling. Restored here.
    """
    user = sanitize_text(request.form.get('user', ''))
    record_id = sanitize_text(request.form.get('record_id', ''))

    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    if not record_id:
        return jsonify({"success": False, "error": "Missing record_id"}), 400

    try:
        fields = json.loads(request.form.get('fields', '{}'))
    except Exception as e:
        return jsonify({"success": False, "error": f"Invalid fields JSON: {str(e)}"}), 400

    fields = {k: v for k, v in fields.items() if k not in EXCLUDED_UPDATE_FIELDS}

    tat = get_tenant_access_token()
    
    # Safely upload newly selected files
    #
    # BUG FIX (new attachment uploads were deleting the old ones): this used to do
    # `fields[field_name] = tokens`, sending ONLY the newly uploaded file(s). Feishu's
    # Attachment fields have full-replace write semantics -- whatever list you send
    # becomes the entire cell content -- so any attachment already on the record that
    # wasn't in that list got silently wiped the moment a new one was added. The
    # frontend now also sends the record's *existing* attachment tokens for this field
    # inside the `fields` JSON payload (see createTwControl/twSaveWorkspace in
    # index.html), so here we merge the newly uploaded tokens onto whatever existing
    # ones were sent instead of overwriting -- new images land side by side with the
    # old ones, matching how the sheet is expected to behave.
    # Phase 10.27 (speed, Ahmed): same parallel-upload treatment submit got in
    # 10.24 -- each file used to wait its own sequential cross-border round-trip
    # on a FRESH connection (raw http_requests, no keep-alive). All attachments
    # across all fields now upload in PARALLEL (max 4 workers) over the shared
    # keep-alive feishu_session. Same number of Feishu calls / quota; pool.map
    # preserves job order, so each field's token list keeps the agent's attach
    # order.
    # Phase 10.29 (failure contract, Ahmed): a failed file used to be logged
    # and SKIPPED while the save proceeded -- the ticket then looked saved but
    # was silently missing an attachment. Now every file gets ONE automatic
    # retry (a second attempt rides the same keep-alive connection, so it
    # costs ~nothing on a healthy link), and if any file still fails the WHOLE
    # save is aborted with 502 BEFORE the record PUT: nothing is written, the
    # frontend names the failed file(s), and the agent just hits save again.
    # All-or-nothing accuracy, parallel speed kept.
    update_upload_jobs = []  # (field_name, filename, mimetype, file_bytes)
    for field_name in request.files:
        if field_name in EXCLUDED_UPDATE_FIELDS: continue
        for f in request.files.getlist(field_name):
            if not f.filename: continue
            file_bytes = f.read()
            if not file_bytes: continue
            update_upload_jobs.append((field_name, f.filename, f.mimetype, file_bytes))

    def _update_upload_one(job):
        field_name, fname, mtype, file_bytes = job
        form_data = {'file_name': fname, 'parent_type': 'bitable_file', 'parent_node': BASE_ID, 'size': str(len(file_bytes))}
        files = {'file': (fname, file_bytes, mtype)}
        last_msg = "unknown upload error"
        for attempt in (1, 2):  # one automatic retry per file (Phase 10.29)
            try:
                up_res = feishu_session.post("https://open.feishu.cn/open-apis/drive/v1/medias/upload_all", headers={"Authorization": f"Bearer {tat}"}, data=form_data, files=files, timeout=30).json()
                if up_res.get("code") == 0:
                    return (field_name, up_res["data"]["file_token"])
                last_msg = f"upload code {up_res.get('code')}: {up_res.get('msg')}"
            except Exception as e:
                last_msg = str(e)
                files = {'file': (fname, file_bytes, mtype)}  # rebuild for retry
        raise Exception(last_msg)

    if update_upload_jobs:
        update_tokens = {}  # field_name -> [tokens in attach order]
        failed_uploads = []  # (field_name, filename, error)
        with ThreadPoolExecutor(max_workers=min(4, len(update_upload_jobs))) as pool:
            # futures with per-job error capture so every file gets its
            # attempts even when a sibling file fails.
            futures = [pool.submit(_update_upload_one, j) for j in update_upload_jobs]
            for j, fut in zip(update_upload_jobs, futures):
                try:
                    field_name, tok = fut.result()
                    update_tokens.setdefault(field_name, []).append({"file_token": tok})
                except Exception as e:
                    logger.error("file_upload_failed", field=j[0], filename=j[1], error=str(e))
                    failed_uploads.append((j[0], j[1], str(e)))
        if failed_uploads:
            names = ", ".join(f"{fn} ({fld})" for fld, fn, _ in failed_uploads)
            return jsonify({"success": False,
                            "error": f"{len(failed_uploads)} file(s) failed to upload: {names}. Nothing was saved -- please hit Save again.",
                            "failed_files": [{"field": fld, "filename": fn, "error": err} for fld, fn, err in failed_uploads]}), 502
        for field_name, tokens in update_tokens.items():
            if tokens:
                existing = fields.get(field_name)
                existing_tokens = []
                if isinstance(existing, list):
                    existing_tokens = [{"file_token": x["file_token"]} for x in existing
                                        if isinstance(x, dict) and x.get("file_token")]
                fields[field_name] = existing_tokens + tokens

    # Drop read-only fields so Feishu doesn't reject the save attempt
    actual_fields = get_table_schema(REQUESTS_TABLE_ID, tat, BASE_ID)
    if actual_fields:
        fields = {k: v for k, v in fields.items() if k in actual_fields and k not in EXCLUDED_UPDATE_FIELDS}

    # BUG FIX (NumberFieldConvFail): drop/convert blank or non-numeric values for
    # Number-type fields before they ever reach Feishu -- see coerce_number_fields().
    field_types = get_field_ui_types(REQUESTS_TABLE_ID, tat, BASE_ID)
    if not field_types:
        # Cache came back empty (cold start / stale Redis entry) -- force a fresh
        # fetch rather than silently skipping every type-based safety check below.
        with _field_types_cache["lock"]:
            _field_types_cache["data"].pop(REQUESTS_TABLE_ID, None)
        if REDIS_ENABLED:
            redis_cmd("DEL", f"xena:fieldtypes:{REQUESTS_TABLE_ID}")
        field_types = get_field_ui_types(REQUESTS_TABLE_ID, tat, BASE_ID)
    if field_types:
        fields = coerce_number_fields(fields, field_types)
        # BUG FIX: never write to Formula/Lookup/AutoNumber/CreatedTime/etc fields,
        # even if one somehow made it this far (e.g. a future frontend regression like
        # the "Assigned Member" one) -- see strip_readonly_fields().
        fields = strip_readonly_fields(fields, field_types)
        # BUG FIX (AttachFieldConvFail): drop any Attachment-typed field that isn't a
        # valid list of file tokens (e.g. a stale echoed-back value) -- see
        # strip_invalid_attachment_fields().
        fields = strip_invalid_attachment_fields(fields, field_types)
        # DEFENSIVE (UserFieldConvFail): same protection as submit_request() -- if any
        # field reaching here is actually a Feishu User/Person column and its value
        # isn't already a well-formed [{"id": open_id}] list (e.g. a stale echoed-back
        # display name), drop it instead of letting it take down the whole ticket save.
        # audit_on_bad_submitted_by=False here since "Submitted By" isn't editable from
        # this workspace -- that specific alert only matters on the create path.
        fields = strip_invalid_user_fields(fields, field_types, actor=user, audit_on_bad_submitted_by=False)

    # DEFENSIVE MERGE (Ahmed, 2026-10-15 -- ticket #30796 lost 2 images): a save
    # that adds an image must NEVER wipe the images already on the record. The
    # merge above relies on the browser sending the existing tokens inside the
    # `fields` JSON -- but a stale cached frontend (or any client) that sends
    # only the new file would still replace the whole cell, because Feishu
    # Attachment writes are full-replace. So whenever an outgoing value is a
    # list of file tokens (only Attachment fields ever look like that), fetch
    # the record ONCE and union the tokens currently on the sheet into the
    # write: sheet order first, then any new tokens, deduped by file_token.
    # Attachments are append-only in this workspace (there is no removal UI),
    # so the union can never fight a legitimate removal. Cost: one extra read,
    # and only on saves that actually touch attachments. If that read fails we
    # abort BEFORE the PUT (all-or-nothing, same contract as Phase 10.29) --
    # a blind write is exactly how the images were lost.
    def _is_token_list(v):
        return isinstance(v, list) and len(v) > 0 and all(
            isinstance(x, dict) and x.get("file_token") for x in v)
    if any(_is_token_list(v) for v in fields.values()):
        try:
            cur_resp = feishu_session.get(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}",
                headers={"Authorization": f"Bearer {tat}"}, timeout=15)
            cur_data = cur_resp.json()
            if cur_data.get("code") != 0:
                raise Exception(cur_data.get("msg") or "record fetch failed")
            cur_fields = ((cur_data.get("data") or {}).get("record") or {}).get("fields") or {}
            for k in list(fields.keys()):
                if not _is_token_list(fields[k]):
                    continue
                on_sheet = cur_fields.get(k)
                sheet_tokens = [{"file_token": x["file_token"]} for x in on_sheet
                                if isinstance(x, dict) and x.get("file_token")] \
                               if isinstance(on_sheet, list) else []
                seen = {t["file_token"] for t in sheet_tokens}
                fields[k] = sheet_tokens + [t for t in fields[k] if t["file_token"] not in seen]
        except Exception as e:
            logger.error("attachment_defensive_merge_failed", record_id=record_id, error=str(e))
            return jsonify({"success": False,
                            "error": f"Could not verify the existing attachments before saving: {e}. Nothing was saved -- please hit Save again."}), 502

    # Phase 10.12 (fast save): the frontend now diffs against the opened record
    # and sends only fields that actually changed (attachment tokens only when
    # new files were picked). When nothing changed the payload arrives empty --
    # skip the Feishu PUT entirely (saves the write AND the Base recalculation
    # it triggers, which was most of the save time) and tell the client.
    if not fields:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
        audit.log(user, "UPDATE_TICKET", f"Record: {record_id} | No changes", ip=ip, severity="Info")
        return jsonify({"success": True, "unchanged": True})

    # Updating with TAT avoids all permission constraints for agents editing tickets
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    logger.info("update_fields_payload", record_id=record_id, fields=fields)
    try:
        resp = feishu_session.put(url, headers=headers, json={"fields": fields}, timeout=15)
        data = resp.json()
        logger.info("update_feishu_response", record_id=record_id, response=data)
        if data.get("code") == 0:
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
            audit.log(user, "UPDATE_TICKET", f"Record: {record_id}", ip=ip, severity="Info")
            # Phase 10.29: write-through freshness -- saves can change status /
            # claim fields that feed the Live Formula ledger.
            kick_table_resync()
            return jsonify({"success": True})
        return jsonify({"success": False, "error": data.get("msg")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# ════════════════════════════════════════════════════════════════════
# AGENCY POINT CHARGE (portal-side, instant)
# ════════════════════════════════════════════════════════════════════
# Faithful Python port of the Base workflow's "Agency point calculator" JS
# handler -- same allocator math, same receipt formats, same tracker formats --
# so the portal can charge an Agency Point Request in ONE request instead of
# flipping "Order Action" and polling for the (slow) Base automation. The old
# flow also had a real failure mode: the workflow reads Quantities Input from
# the RECORD, so charging before saving meant it approved nothing. Here the
# order details travel in the request body, exactly as the agent sees them.

def _jsnum(x):
    """Format a number the way JS template literals print it (200 -> '200',
    49.0 -> '49', 0.5 -> '0.5') so receipts read identically to the sheet's."""
    return f"{float(x):g}"

def compute_agency_point_charge(privileges, qty_raw, target_raw, used_points, balance, month_text, now=None):
    month_names = ["January", "February", "March", "April", "May", "June", "July",
                   "August", "September", "October", "November", "December"]
    now = now or cairo_now()
    submitted_month_name = month_names[now.month - 1]

    privs = [str(p).strip() for p in (privileges or []) if str(p).strip()]
    qtys = [int(x) for x in re.findall(r"\d+", str(qty_raw or ""))]
    target = str(target_raw or "").lower()
    try: used = float(used_points or 0)
    except (TypeError, ValueError): used = 0.0
    try: bal = float(balance or 0)
    except (TypeError, ValueError): bal = 0.0

    if not privs:
        return {"error": "No items selected."}

    # === month tracker reader (skips decorative lines, same as the JS) ===
    stored_month = ""
    monthly = {}
    for line in str(month_text or "").split("\n"):
        if "━" in line or "┈" in line:
            continue
        m = re.search(r"Month:\s*([A-Za-z0-9]+)", line, re.I)
        if m:
            stored_month = m.group(1).strip()
        elif ":" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                name = re.sub(r"[🔹📋🌟【】]", "", parts[0]).strip()
                try:
                    monthly[name] = int(parts[1].strip())
                except ValueError:
                    pass

    same_month = stored_month.lower() == submitted_month_name.lower()
    if not same_month:
        try:
            same_month = int(stored_month) == now.month
        except (TypeError, ValueError):
            same_month = False
    if not same_month:
        monthly = {}

    # === the smart allocator ===
    remaining = bal
    total_cost = 0
    receipt_lines = []
    current_order = {}
    for i, p in enumerate(privs):
        item = p.lower()
        q = qtys[i] if i < len(qtys) else 0
        if q <= 0:
            continue
        rule = next((r for r in POINT_PRICE_DB if r["key"] in item), None)
        if not rule:
            continue
        price = rule["other"] if "other" in target else rule["same"]
        display = " ".join((w[:1].upper() + w[1:]) if w else w for w in rule["key"].split(" "))
        past_usage = monthly.get(display, 0)

        limit_allowed = rule["limit"] if rule.get("type") == "order" else rule["limit"] - past_usage
        if limit_allowed < 0:
            limit_allowed = 0
        desired = min(q, limit_allowed)
        affordable = int(remaining // price) if price > 0 else 0
        approved = min(desired, affordable)

        if approved == q:
            receipt_lines.append(f"✅ FULLY APPROVED: {approved}x {display} @ {price} pts = {approved * price}")
        elif approved > 0:
            if q > limit_allowed:
                reason = f"Max {rule['limit']} allowed per request." if rule.get("type") == "order" else f"Limit maxes out at {limit_allowed} more."
            else:
                reason = f"Not enough points for {desired}."
            receipt_lines.append(f"⚠️ PARTIALLY APPROVED: {approved}x {display} @ {price} pts = {approved * price}\n    └─ (Rejected {q - approved}x: {reason})")
        else:
            if limit_allowed == 0:
                reason = f"Max {rule['limit']} allowed per request." if rule.get("type") == "order" else f"Monthly limit ({rule['limit']}) already reached."
            else:
                reason = f"Insufficient points ({_jsnum(remaining)} left)."
            receipt_lines.append(f"❌ FULLY REJECTED: 0x {display}\n    └─ (Reason: {reason})")

        if approved > 0:
            total_cost += approved * price
            remaining -= approved * price
            current_order[display] = current_order.get(display, 0) + approved
            monthly[display] = past_usage + approved

    monthly_str = "\n ┈┈┈ \n".join(f"🔹 {k}: {v}" for k, v in monthly.items())
    new_monthly_tracker = f"🌟 【 MONTH: {submitted_month_name.upper()} 】 🌟\n━━━━━\n" + monthly_str
    if current_order:
        current_tracker = "Items approved in this order:\n" + "\n".join(f"🔹 {k}: {v}" for k, v in current_order.items())
    else:
        current_tracker = "No items approved in this order."
    base_receipt = "\n".join(receipt_lines) + f"\n\n💰 TOTAL DEDUCTED: {_jsnum(total_cost)} pts\n💳 REMAINING BALANCE: {_jsnum(remaining)} pts\n"
    monthly_summary = "\n🌟 **CURRENT MONTH USAGE** 🌟\n━━━━━\n" + monthly_str
    confirm_receipt = "📝 **CONFIRMED RECEIPT** 📝\n\n" + base_receipt + monthly_summary

    return {
        "total_cost": total_cost, "remaining": remaining,
        "new_used_points": used + total_cost,
        "new_monthly_tracker": new_monthly_tracker,
        "current_tracker": current_tracker,
        "current_order": current_order,
        "receipt": confirm_receipt,
    }

def merge_usage_trackers(existing_tracker, new_order):
    """Phase 10.10 (charge-more): merge this charge's approved items into the
    ticket's existing Latest Usage Tracker, so topping up a request
    (1 Trend + 1 Traffic, then +1 Trend) leaves the ticket showing the
    CUMULATIVE order (2 Trend + 1 Traffic). Existing entries keep their
    original order; brand-new items append at the end."""
    merged = {}
    for line in str(existing_tracker or "").split("\n"):
        line = line.strip()
        if not line or ":" not in line or "━" in line or "┈" in line:
            continue
        name_part, _, qty_part = line.partition(":")
        name = re.sub(r"[🔹📋🌟【】]", "", name_part).strip()
        if not name or "items approved" in name.lower() or "no items" in name.lower():
            continue
        try:
            merged[name] = merged.get(name, 0) + int(qty_part.strip())
        except ValueError:
            pass
    for name, qty in (new_order or {}).items():
        merged[name] = merged.get(name, 0) + int(qty)
    if not merged:
        return "No items approved in this order."
    return "Items approved in this order:\n" + "\n".join(f"🔹 {k}: {v}" for k, v in merged.items())

def _resolve_agency_wallet(tfields, agency_code_hint, wallet_hint, fetch_wallet):
    """Phase 10.25 (Ahmed): the ticket is still authoritative for which wallet
    gets charged, BUT the agent can correct a wrong Agency Code right in the
    form (ticket filed as 327999, fixed to 32799) without a Save first. The
    on-screen wallet card resolves the TYPED code, while the charge used to
    key off the ticket's stale stored code -- so the wallet was visibly found
    on screen (5,179 pts, agency 32799) yet every charge answered "Agency
    327999 not found" forever. Now: when the ticket's stored code is blank or
    finds no wallet, and the typed hint does resolve one, the hinted wallet is
    charged and the caller heals the ticket's Agency Code in the same receipt
    write (zero extra Feishu calls -- the hinted wallet was already fetched in
    the parallel step). When BOTH codes resolve to wallets the ticket still
    wins -- that mismatch is a fat-finger hazard, not a correction.
    Returns (agency_record|None, agency_code, code_corrected)."""
    stored_code = extract_field_text(get_field_local(tfields, "Agency Code")).strip()
    agency_code = stored_code or agency_code_hint
    if not agency_code:
        return None, "", False
    if agency_code_hint == agency_code:
        # The parallel wallet fetch already searched this exact code -- its
        # result (found or None) IS the answer, no second search needed.
        return wallet_hint, agency_code, bool(not stored_code and agency_code)
    agency_rec = fetch_wallet(agency_code)
    if agency_rec:
        return agency_rec, agency_code, False
    # The stored code found no wallet -- fall back to the agent's typed
    # correction. wallet_hint already IS that hint's search result (the
    # parallel step searches the hint whenever one was sent), so a falsy
    # wallet_hint means the hint found nothing too -- no second search.
    if wallet_hint:
        return wallet_hint, agency_code_hint, True
    return None, agency_code, False

def _wallet_acm_for_ticket(af, tfields):
    """Phase 10.38 (Ahmed): "when do the recharge also please update the acm
    name in the same call it exist in the agency point table so update it in
    the request". The Agency Points (wallet) row is the ACM source of truth;
    the ticket's region-appropriate Acm Name field gets that value in the same
    receipt PUT (zero extra Feishu calls). Returns (ticket_field_name,
    acm_name) or (None, "") when the wallet carries no ACM -- nothing is
    written then. Region comes from the ticket first, then the wallet; if
    neither says PK/IN, the wallet's own filled column decides (PK wins ties,
    matching the audit-list read priority at the agency-list endpoint)."""
    acm_pk = extract_field_text(get_field_local(af, "Acm Name (PK)")).strip()
    acm_in = extract_field_text(get_field_local(af, "Acm Name (IN)")).strip()
    acm_alias = extract_field_text(get_field_local(af, "Acm", "Assigned Member")).strip()
    if not (acm_pk or acm_in or acm_alias):
        return None, ""
    region = (extract_field_text(get_field_local(tfields, "Region")) or
              extract_field_text(get_field_local(af, "Region", "Agency Region"))).strip().upper()
    if region == "IN":
        # Region known -> the region-matched column is the value source
        # (both columns filled: the IN wallet of an IN agency wins).
        return "Acm Name (IN)", (acm_in or acm_pk or acm_alias)
    if region == "PK":
        return "Acm Name (PK)", (acm_pk or acm_in or acm_alias)
    if acm_in and not acm_pk:
        return "Acm Name (IN)", acm_in
    return "Acm Name (PK)", (acm_pk or acm_in or acm_alias)

@app.route('/api/agency-points/charge', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def agency_points_charge():
    """Instant Agency Point charge for the ticket workspace's ⚡ Confirm & Charge
    button. Body (JSON): record_id, user, email, privileges[], quantities_input,
    target_type, agency_code (hint). Phase 10.10: the ticket + the agency wallet
    are fetched in PARALLEL (wallet searched by the agency_code hint the client
    already has on screen; the ticket stays authoritative -- on mismatch the
    wallet is re-searched with the ticket's code), and the button stays usable
    after a successful charge -- CHARGE-MORE: each call deducts only the
    quantities currently typed, appends an "ADDITIONAL CHARGE" receipt and
    merges the Latest Usage Tracker into a cumulative view (1 Trend + 1 Traffic
    charged, then 1 more Trend => tracker shows 2 Trend + 1 Traffic). A 120s
    Redis duplicate guard replays the result of an identical request
    (double-click / retry storms) instead of double-deducting. A zero-approval
    attempt writes nothing to the wallet and never clobbers an existing good
    receipt. NEVER writes "Order Action" -- that field is the Base workflow's
    trigger and writing 'Confirm & Charge' to it would double-charge via the
    automation."""
    payload = request.get_json(silent=True) or {}
    user = sanitize_text(payload.get('user', ''))
    email = sanitize_text(payload.get('email', ''))
    record_id = sanitize_text(payload.get('record_id', ''))
    agency_code_hint = sanitize_text(payload.get('agency_code', '')).strip()
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    if not record_id:
        return jsonify({"success": False, "error": "Missing record_id"}), 400
    if MOCK_MODE:
        return jsonify({"success": False, "error": "Mock mode — charge is disabled."}), 400

    # Order details come from the REQUEST BODY (what the agent sees in the form
    # right now), falling back to the record. This is THE fix for the old
    # "No items approved in this order" bug -- the sheet workflow could only
    # read the record, so charging before saving meant it saw an empty
    # Quantities Input and approved nothing.
    privileges = payload.get("privileges")
    qty_raw = payload.get("quantities_input") or ""
    target_raw = payload.get("target_type") or ""

    # Duplicate guard: an identical charge request within 120s replays the
    # cached result instead of deducting twice (double-click, retry storm).
    dup_key = f"xena:ap_charge:{record_id}"
    dup_sig = hashlib.sha1(json.dumps([record_id, privileges, qty_raw, target_raw], sort_keys=True, default=str).encode()).hexdigest()
    if REDIS_ENABLED:
        try:
            cached = redis_get_json(dup_key)
            if cached and cached.get("sig") == dup_sig and isinstance(cached.get("result"), dict):
                replay = dict(cached["result"])
                replay["duplicate"] = True
                return jsonify(replay)
        except Exception:
            pass

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    # 1) Fetch the ticket AND the agency wallet in parallel (the wallet search
    #    uses the agency_code hint from the client; verified against the ticket
    #    below).
    def _fetch_ticket():
        r = feishu_session.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}",
                               headers={"Authorization": f"Bearer {tat}"}, timeout=10).json()
        if r.get("code") != 0:
            raise RuntimeError(r.get("msg") or "Could not read the ticket.")
        return r["data"]["record"].get("fields", {})

    def _fetch_wallet(code):
        if not code:
            return None
        r = feishu_session.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search",
            headers=headers,
            json={"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "contains", "value": [code]}]}},
            timeout=30).json()
        if r.get("code") != 0:
            raise RuntimeError(r.get("msg") or "Agency Points lookup failed.")
        for it in r.get("data", {}).get("items", []):
            if extract_field_text(get_field_local(it.get("fields", {}), "Agency Code")).strip() == code:
                return it
        return None

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_ticket = pool.submit(_fetch_ticket)
            fut_wallet = pool.submit(_fetch_wallet, agency_code_hint)
            tfields = fut_ticket.result()
            wallet_hint = fut_wallet.result()
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

    existing_receipt = extract_field_text(get_field_local(tfields, "Transaction Receipt"))
    existing_tracker = extract_field_text(get_field_local(tfields, "Latest Usage Tracker"))

    # Phase 10.25: wallet resolution via _resolve_agency_wallet -- the ticket
    # stays authoritative, but a code the agent just corrected in the form
    # (stored code finds no wallet, typed code does) is honored, and the
    # ticket's stale code is healed in the receipt write below.
    try:
        agency_rec, agency_code, code_corrected = _resolve_agency_wallet(
            tfields, agency_code_hint, wallet_hint, _fetch_wallet)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400
    if not agency_code:
        return jsonify({"success": False, "error": "This ticket has no Agency Code."}), 400
    if not agency_rec:
        return jsonify({"success": False, "error": f"Agency {agency_code} not found in the Agency Points table."}), 404

    # Body values win; fall back to the record for anything the client omitted.
    if not isinstance(privileges, list) or not privileges:
        pv = get_field_local(tfields, "Agency Point Privilege")
        privileges = pv if isinstance(pv, list) else ([pv] if pv else [])
    if not qty_raw:
        qty_raw = extract_field_text(get_field_local(tfields, "Quantities Input"))
    if not target_raw:
        target_raw = extract_field_text(get_field_local(tfields, "Target Type"))

    af = agency_rec.get("fields", {})
    used_pts = parse_float_safe(extract_field_text(get_field_local(af, "Used Points", "Used")))
    balance = parse_float_safe(extract_field_text(get_field_local(af, "Point Balance", "Balance")))
    if balance == 0:
        total_pts = parse_float_safe(extract_field_text(get_field_local(af, "# Total Points", "Total Points", "Total")))
        if total_pts > 0:
            balance = total_pts - used_pts
    month_text = extract_field_text(get_field_local(af, "Monthly Usage Tracker"))

    # 3) The allocator -- identical math to the sheet's JS handler.
    charge = compute_agency_point_charge(privileges, qty_raw, target_raw, used_pts, balance, month_text)
    if charge.get("error"):
        return jsonify({"success": False, "error": charge["error"]}), 400

    total_cost = charge["total_cost"]

    # 4) Write the agency wallet FIRST (Used Points + Monthly Usage Tracker) --
    # exactly the two fields the sheet workflow's "Update Agency Points table"
    # step writes. If this fails, nothing was deducted and the ticket is
    # untouched, so the agent can simply retry. Only runs when this charge
    # actually approves something -- a zero-approval attempt changes nothing.
    # NOTE: "Used Points" is a TEXT field in the Agency Points table (the sheet
    # workflow also writes it as text) -- sending a number fails with
    # TextFieldConvFail, so it goes out as a JS-style numeric string.
    if total_cost > 0:
        try:
            wr = feishu_session.put(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/{agency_rec['record_id']}",
                headers=headers,
                json={"fields": {"Used Points": _jsnum(charge["new_used_points"]), "Monthly Usage Tracker": charge["new_monthly_tracker"]}},
                timeout=15).json()
            if wr.get("code") != 0:
                return jsonify({"success": False, "error": f"Wallet update failed (nothing deducted): {wr.get('msg')}"}), 400
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 5) Charge-more bookkeeping: the ticket's tracker becomes CUMULATIVE
    #    (existing approved items + this charge's items), and every charge
    #    appends its own receipt after an "ADDITIONAL CHARGE" divider, so the
    #    full deduction history stays visible on the ticket.
    if total_cost > 0 and existing_tracker and "No items approved" not in existing_tracker:
        cumulative_tracker = merge_usage_trackers(existing_tracker, charge["current_order"])
    elif total_cost > 0:
        cumulative_tracker = charge["current_tracker"]
    else:
        cumulative_tracker = existing_tracker or charge["current_tracker"]

    if total_cost > 0 and existing_receipt and "CONFIRMED RECEIPT" in existing_receipt.upper():
        full_receipt = existing_receipt.rstrip() + "\n\n➕ ── ADDITIONAL CHARGE ── ➕\n\n" + charge["receipt"]
    elif total_cost == 0 and existing_receipt:
        full_receipt = existing_receipt
    else:
        full_receipt = charge["receipt"]

    # 6) Write the receipt back onto the ticket -- only when there is something
    # new to say: a real charge, or the very first (possibly zero-approval)
    # receipt. A failed zero attempt never clobbers an existing good receipt.
    # If THIS write fails the points are already deducted, so we still return
    # the receipt to the caller instead of pretending the charge never happened.
    write_ticket = total_cost > 0 or not existing_receipt
    ticket_write_failed = False
    acm_field, acm_name = None, ""  # Phase 10.38: wallet ACM ride-along (set inside the write)
    if write_ticket:
        try:
            ticket_fields = {"Transaction Receipt": full_receipt,
                             "Latest Usage Tracker": cumulative_tracker,
                             "Quantities Input": ""}
            if code_corrected:
                # Heal the ticket's stale/blank Agency Code with the code the
                # agent actually charged -- rides the same write, zero extra
                # Feishu calls (Phase 10.25).
                ticket_fields["Agency Code"] = agency_code
            # Phase 10.38 (Ahmed): the wallet's ACM lands on the ticket's
            # Acm Name (PK)/(IN) field in the SAME write -- "update the acm
            # name in the same call it exist in the agency point table".
            acm_field, acm_name = _wallet_acm_for_ticket(af, tfields)
            if acm_field:
                ticket_fields[acm_field] = acm_name
            tw = feishu_session.put(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}",
                headers=headers,
                json={"fields": ticket_fields},
                timeout=15).json()
            if tw.get("code") != 0:
                ticket_write_failed = True
                logger.error("ap_charge_ticket_write_failed", record_id=record_id, response=tw)
        except Exception as e:
            ticket_write_failed = True
            logger.error("ap_charge_ticket_write_error", record_id=record_id, error=str(e))

    audit.log(user, "AP_CHARGE", f"Record: {record_id} | Agency: {agency_code} | Deducted: {total_cost} | Balance after: {charge['remaining']}" + (" | ticket Agency Code corrected" if code_corrected else "") + (f" | ticket ACM synced: {acm_name}" if acm_field else ""), ip=ip, severity="Info")
    result = {"success": True, "duplicate": False,
              "deducted": total_cost, "balance_after": charge["remaining"],
              "receipt": charge["receipt"], "full_receipt": full_receipt,
              "tracker": cumulative_tracker,
              "agency_code": agency_code, "agency_code_corrected": code_corrected,
              "ticket_write_failed": ticket_write_failed}
    if acm_field:
        # Lets the open workspace sync its ACM control + ltActiveTicket
        # without a re-fetch (Phase 10.38).
        result["acm_field"] = acm_field
        result["acm_name"] = acm_name
    if REDIS_ENABLED:
        try:
            redis_set_json(dup_key, {"sig": dup_sig, "result": result}, ttl=120)
        except Exception:
            pass
    # Phase 10.29: write-through freshness — the charge just changed the
    # wallet table, so refresh the warm snapshots now instead of waiting up
    # to 3 min for the background loop.
    kick_table_resync()
    return jsonify(result)

# ════════════════════════════════════════════════════════════════════
# AGENCY POINT CHARGE CORRECTION (Phase 10.12 — edit the last charge)
# ════════════════════════════════════════════════════════════════════
# The agent confirmed a charge with the wrong quantities (wanted 2 Trend +
# 1 Traffic, typed 1 + 2). This endpoint reverses the LAST charge on the
# ticket — points back into the wallet, its items subtracted from both the
# ticket's Latest Usage Tracker and the wallet's Monthly Usage Tracker — then
# runs the freshly typed quantities through the SAME allocator as a normal
# charge, so monthly limits and the (restored) balance are enforced again.

AP_DIVIDER = "➕ ── ADDITIONAL CHARGE ── ➕"
_AP_LINE_RE = re.compile(r"(?:✅ FULLY APPROVED|⚠️ PARTIALLY APPROVED):\s*(\d+)x\s+(.+?)\s+@\s*[\d.]+\s*pts\s*=\s*[\d.]+")
_AP_TOTAL_RE = re.compile(r"TOTAL DEDUCTED:\s*([\d.]+)\s*pts")

def parse_last_charge_segment(receipt):
    """Split a possibly multi-charge receipt on the ADDITIONAL CHARGE divider
    and parse the LAST segment's approved lines + total deduction, so exactly
    that charge can be reversed. Returns None when there is no parseable
    confirmed deduction to reverse."""
    text = str(receipt or "")
    if "CONFIRMED RECEIPT" not in text.upper():
        return None
    parts = text.split(AP_DIVIDER)
    head = AP_DIVIDER.join(parts[:-1]).rstrip() if len(parts) > 1 else ""
    segment = parts[-1].strip()
    order = {}
    for m in _AP_LINE_RE.finditer(segment):
        qty = int(m.group(1))
        if qty <= 0:
            continue
        name = m.group(2).strip()
        order[name] = order.get(name, 0) + qty
    tm = _AP_TOTAL_RE.search(segment)
    deducted = float(tm.group(1)) if tm else 0.0
    if not order or deducted <= 0:
        return None
    return {"head": head, "order": order, "deducted": deducted, "segment": segment}

def _subtract_order_from_lines(text, order):
    """Parse '🔹 Name: qty' tracker lines and subtract the given order's
    quantities (clamped at 0; emptied entries drop out). Original line order
    is preserved; decorative (━/┈) and header lines are ignored."""
    items = {}
    for line in str(text or "").split("\n"):
        line = line.strip()
        if not line or ":" not in line or "━" in line or "┈" in line:
            continue
        name_part, _, qty_part = line.partition(":")
        name = re.sub(r"[🔹📋🌟【】]", "", name_part).strip()
        if not name or "items approved" in name.lower() or "no items" in name.lower() or "month" in name.lower():
            continue
        try:
            items[name] = int(qty_part.strip())
        except ValueError:
            pass
    for name, qty in (order or {}).items():
        if name in items:
            items[name] = max(0, items[name] - int(qty))
    return {k: v for k, v in items.items() if v > 0}

def subtract_from_ticket_tracker(tracker_text, order):
    items = _subtract_order_from_lines(tracker_text, order)
    if not items:
        return "No items approved in this order."
    return "Items approved in this order:\n" + "\n".join(f"🔹 {k}: {v}" for k, v in items.items())

def reverse_month_tracker(month_text, order, now=None):
    """Subtract a reversed charge from the wallet's Monthly Usage Tracker,
    keeping the decorative month header. When the stored tracker belongs to a
    different month the allocator resets it anyway, so it is returned as-is."""
    month_names = ["January", "February", "March", "April", "May", "June", "July",
                   "August", "September", "October", "November", "December"]
    now = now or cairo_now()
    text = str(month_text or "")
    stored = ""
    for line in text.split("\n"):
        m = re.search(r"Month:\s*([A-Za-z0-9]+)", line, re.I)
        if m:
            stored = m.group(1).strip()
            break
    month_name = month_names[now.month - 1]
    same = stored.lower() == month_name.lower()
    if not same:
        try:
            same = int(stored) == now.month
        except (TypeError, ValueError):
            same = False
    if not same:
        return text
    items = _subtract_order_from_lines(text, order)
    monthly_str = "\n ┈┈┈ \n".join(f"🔹 {k}: {v}" for k, v in items.items())
    return f"🌟 【 MONTH: {month_name.upper()} 】 🌟\n━━━━━\n" + monthly_str

@app.route('/api/agency-points/correct-charge', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def agency_points_correct_charge():
    """Edit-last-charge for the ticket workspace. Body (JSON): same shape as
    /api/agency-points/charge. The wallet write ALWAYS happens (the reversal
    must persist even when the corrected order approves nothing); the receipt
    keeps every earlier charge (the head before the last ADDITIONAL CHARGE
    segment) and replaces only the last one. The 120s duplicate guard from the
    original charge is invalidated so the correction is never replay-blocked.
    NEVER writes "Order Action" (the Base workflow trigger)."""
    payload = request.get_json(silent=True) or {}
    user = sanitize_text(payload.get('user', ''))
    record_id = sanitize_text(payload.get('record_id', ''))
    agency_code_hint = sanitize_text(payload.get('agency_code', '')).strip()
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    if not record_id:
        return jsonify({"success": False, "error": "Missing record_id"}), 400
    if MOCK_MODE:
        return jsonify({"success": False, "error": "Mock mode — charge is disabled."}), 400

    privileges = payload.get("privileges")
    qty_raw = payload.get("quantities_input") or ""
    target_raw = payload.get("target_type") or ""

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    def _fetch_ticket():
        r = feishu_session.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}",
                               headers={"Authorization": f"Bearer {tat}"}, timeout=10).json()
        if r.get("code") != 0:
            raise RuntimeError(r.get("msg") or "Could not read the ticket.")
        return r["data"]["record"].get("fields", {})

    def _fetch_wallet(code):
        if not code:
            return None
        r = feishu_session.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search",
            headers=headers,
            json={"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "contains", "value": [code]}]}},
            timeout=30).json()
        if r.get("code") != 0:
            raise RuntimeError(r.get("msg") or "Agency Points lookup failed.")
        for it in r.get("data", {}).get("items", []):
            if extract_field_text(get_field_local(it.get("fields", {}), "Agency Code")).strip() == code:
                return it
        return None

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_ticket = pool.submit(_fetch_ticket)
            fut_wallet = pool.submit(_fetch_wallet, agency_code_hint)
            tfields = fut_ticket.result()
            wallet_hint = fut_wallet.result()
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

    existing_receipt = extract_field_text(get_field_local(tfields, "Transaction Receipt"))
    existing_tracker = extract_field_text(get_field_local(tfields, "Latest Usage Tracker"))

    seg = parse_last_charge_segment(existing_receipt)
    if not seg:
        return jsonify({"success": False, "error": "No completed charge found to edit."}), 400

    # Phase 10.25: same corrected-code resolution as the charge endpoint.
    try:
        agency_rec, agency_code, code_corrected = _resolve_agency_wallet(
            tfields, agency_code_hint, wallet_hint, _fetch_wallet)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400
    if not agency_code:
        return jsonify({"success": False, "error": "This ticket has no Agency Code."}), 400
    if not agency_rec:
        return jsonify({"success": False, "error": f"Agency {agency_code} not found in the Agency Points table."}), 404

    if not isinstance(privileges, list) or not privileges:
        pv = get_field_local(tfields, "Agency Point Privilege")
        privileges = pv if isinstance(pv, list) else ([pv] if pv else [])
    if not qty_raw:
        qty_raw = extract_field_text(get_field_local(tfields, "Quantities Input"))
    if not target_raw:
        target_raw = extract_field_text(get_field_local(tfields, "Target Type"))

    af = agency_rec.get("fields", {})
    used_pts = parse_float_safe(extract_field_text(get_field_local(af, "Used Points", "Used")))
    balance = parse_float_safe(extract_field_text(get_field_local(af, "Point Balance", "Balance")))
    if balance == 0:
        total_pts = parse_float_safe(extract_field_text(get_field_local(af, "# Total Points", "Total Points", "Total")))
        if total_pts > 0:
            balance = total_pts - used_pts
    month_text = extract_field_text(get_field_local(af, "Monthly Usage Tracker"))

    # 1) Reverse the last charge: its points go back to the balance, its items
    #    come out of the wallet's month tracker.
    reversed_used = max(0.0, used_pts - seg["deducted"])
    reversed_balance = balance + seg["deducted"]
    reversed_month = reverse_month_tracker(month_text, seg["order"])

    # 2) Re-run the allocator on the reversed state — limits and the restored
    #    balance are enforced exactly like a fresh charge.
    charge = compute_agency_point_charge(privileges, qty_raw, target_raw, reversed_used, reversed_balance, reversed_month)
    if charge.get("error"):
        return jsonify({"success": False, "error": charge["error"]}), 400

    # 3) ONE wallet write, always: even a corrected order that approves
    #    nothing must still persist the reversal (used -= old deduction).
    try:
        wr = feishu_session.put(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/{agency_rec['record_id']}",
            headers=headers,
            json={"fields": {"Used Points": _jsnum(charge["new_used_points"]), "Monthly Usage Tracker": charge["new_monthly_tracker"]}},
            timeout=15).json()
        if wr.get("code") != 0:
            return jsonify({"success": False, "error": f"Wallet update failed (nothing changed): {wr.get('msg')}"}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    # 4) Ticket bookkeeping: tracker = old tracker minus the reversed items,
    #    plus the corrected order's items; receipt = everything before the
    #    last charge + the new corrected receipt.
    reversed_ticket_tracker = subtract_from_ticket_tracker(existing_tracker, seg["order"])
    if charge["total_cost"] > 0:
        if reversed_ticket_tracker and "No items approved" not in reversed_ticket_tracker:
            cumulative_tracker = merge_usage_trackers(reversed_ticket_tracker, charge["current_order"])
        else:
            cumulative_tracker = charge["current_tracker"]
    else:
        cumulative_tracker = reversed_ticket_tracker

    if seg["head"]:
        full_receipt = seg["head"] + "\n\n" + AP_DIVIDER + "\n\n" + charge["receipt"]
    else:
        full_receipt = charge["receipt"]

    # If THIS write fails the wallet is already corrected, so the result is
    # still returned (flagged) rather than pretending nothing happened.
    ticket_write_failed = False
    # Phase 10.38: same wallet-ACM ride-along as the charge endpoint.
    acm_field, acm_name = _wallet_acm_for_ticket(af, tfields)
    try:
        ticket_fields = {"Transaction Receipt": full_receipt,
                         "Latest Usage Tracker": cumulative_tracker,
                         "Quantities Input": ""}
        if code_corrected:
            ticket_fields["Agency Code"] = agency_code  # Phase 10.25 heal, same write
        if acm_field:
            ticket_fields[acm_field] = acm_name  # Phase 10.38 ACM sync, same write
        tw = feishu_session.put(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}",
            headers=headers,
            json={"fields": ticket_fields},
            timeout=15).json()
        if tw.get("code") != 0:
            ticket_write_failed = True
            logger.error("ap_correct_ticket_write_failed", record_id=record_id, response=tw)
    except Exception as e:
        ticket_write_failed = True
        logger.error("ap_correct_ticket_write_error", record_id=record_id, error=str(e))

    # The correction changed the wallet state — any cached duplicate of the
    # original charge is stale and must not be replayed.
    if REDIS_ENABLED:
        try:
            redis_cmd("DEL", f"xena:ap_charge:{record_id}")
        except Exception:
            pass

    audit.log(user, "AP_CORRECT", f"Record: {record_id} | Agency: {agency_code} | Reversed: {seg['deducted']} | Deducted: {charge['total_cost']} | Balance after: {charge['remaining']}" + (" | ticket Agency Code corrected" if code_corrected else "") + (f" | ticket ACM synced: {acm_name}" if acm_field else ""), ip=ip, severity="Info")
    # Phase 10.29: write-through freshness — correction changed the wallet.
    kick_table_resync()
    result = {"success": True, "duplicate": False, "corrected": True,
              "reversed": seg["deducted"],
              "deducted": charge["total_cost"], "balance_after": charge["remaining"],
              "receipt": charge["receipt"], "full_receipt": full_receipt,
              "tracker": cumulative_tracker,
              "agency_code": agency_code, "agency_code_corrected": code_corrected,
              "ticket_write_failed": ticket_write_failed}
    if acm_field:
        result["acm_field"] = acm_field  # Phase 10.38: workspace syncs its ACM control
        result["acm_name"] = acm_name
    return jsonify(result)

@app.route('/api/requests/submit', methods=['POST'])
def submit_request():
    user = sanitize_text(request.form.get('user', ''))
    email = sanitize_text(request.form.get('email', ''))
    uat = request.form.get('uat', '')
    submitter_open_id = sanitize_text(request.form.get('open_id', ''))
    
    if not user:
        return jsonify({"error": "Unauthorized"}), 403

    try:
        payload_str = request.form.get('payload', '{}')
        user_fields = json.loads(payload_str)
    except Exception as e:
        return jsonify({"error": f"Invalid payload JSON: {str(e)}"}), 400

    req_type = user_fields.get("Request Type")
    if not req_type:
        return jsonify({"error": "Request Type is required."}), 400

    # AUDIT WORKSTATION FILINGS: the Audit tab always sends Create Way starting
    # with "Audit" ("Audit (with all info)") -- that's the only place in the app
    # that sets it (Create New Request never sends Create Way at all), so this
    # flag identifies a workstation filing precisely.
    create_way = str(user_fields.get("Create Way") or "").strip().lower()
    is_audit_filing = create_way.startswith("audit")
    if is_audit_filing:
        # Access Management grant: the Audit tab is behind its own module key
        # ("submit_audit", granted via the "Audit Workstation" toggle in Manage
        # Agents). Master "submit" / admins keep access automatically, same
        # convention as every other module check in this file.
        perms = get_user_permissions(email, user)
        if not perms.get("is_super_admin") and not any(m in ("submit", "submit_audit", "admin") for m in perms.get("modules", [])):
            return jsonify({"error": "Access denied: you don't have Audit Workstation permission. Ask an admin to grant it in Access Management."}), 403

    tat = get_tenant_access_token()
    # SESSION ENFORCEMENT: tickets must be filed under the real agent's identity,
    # never quietly under the app's. Where the server CAN verify a token's real
    # state (Redis enabled, real open_id on file), trust ONLY that -- not the uat
    # the browser sent in the form. The server cache is stored with an EX
    # matching Feishu's own expires_in (see _refresh_user_token), so an empty
    # cache result IS the authoritative "this token is dead" signal; the
    # browser's copy can't be trusted for that, it's just old localStorage that
    # doesn't know its own token died -- exactly how a stale request used to
    # slip through to Feishu and get caught only by guessing at its ever-
    # changing rejection codes (99991663/99991668/99991677 and counting).
    # Where server-side verification isn't possible at all (mock mode, Redis
    # disabled, or a legacy session with no open_id on file), there's nothing to
    # check against, so this falls back to trusting the browser-sent uat as-is,
    # same as the original design.
    if REDIS_ENABLED and not MOCK_MODE and submitter_open_id:
        uat = _get_cached_uat(submitter_open_id) or _refresh_user_token(submitter_open_id)
        if not uat and not redis_healthy():
            # Phase 10.44: Redis is unreachable, so the empty cache above
            # proves NOTHING -- fail open to the browser-sent uat (the
            # pre-10.14 behavior) rather than force a logout over an
            # infrastructure blip. A genuinely dead token is still caught by
            # Feishu itself in the create call below (99991677 path).
            logger.warn("submit_redis_down_fail_open", user=user)
            uat = request.form.get('uat', '')
    if not uat:
        audit.log(user, "SESSION_EXPIRED_ON_SUBMIT", f"Type: {req_type}", ip=request.headers.get("X-Forwarded-For", request.remote_addr or ""), severity="Warning")
        return jsonify({"error": "Your session has expired. Please log in again, then resubmit this request — everything you filled in has been saved and will be restored automatically.", "code": "SESSION_EXPIRED"}), 401
    api_token = uat

    final_fields = {}

    for key, val in user_fields.items():
        if key not in EXCLUDED_SUBMIT_FIELDS:
            if val not in (None, "", []):
                final_fields[key] = val

    # Phase 10.24 (speed, Ahmed): attachment files used to upload ONE BY ONE --
    # each a full cross-border round-trip Vercel -> Feishu on a FRESH connection
    # (raw http_requests, no keep-alive), so a 4-file submit waited 4 sequential
    # uploads before the record was even created. The same uploads now run in
    # PARALLEL (max 4 workers) over the shared keep-alive feishu_session -- the
    # same pattern the charge endpoint already uses for its parallel reads.
    # Same number of Feishu calls, same quota cost, same uat->tat fallback,
    # same failure contract (any failed file aborts the whole submit BEFORE
    # anything is created); only the wall-clock wait drops -- biggest for
    # multi-file tickets. pool.map preserves job order, so each field's
    # file_token list keeps the exact order the agent attached them in.
    upload_jobs = []  # (field_name, filename, mimetype, file_bytes)
    for field_name in request.files:
        if field_name in EXCLUDED_SUBMIT_FIELDS:
            continue
        for f in request.files.getlist(field_name):
            if not f.filename:
                continue
            file_bytes = f.read()
            if not file_bytes:
                continue
            upload_jobs.append((field_name, f.filename, f.mimetype, file_bytes))

    def _upload_one(job):
        field_name, fname, mtype, file_bytes = job
        form_data = {
            'file_name': fname,
            'parent_type': 'bitable_file',
            'parent_node': BASE_ID,
            'size': str(len(file_bytes))
        }
        files = {'file': (fname, file_bytes, mtype)}
        up_res = feishu_session.post(
            "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all",
            headers={"Authorization": f"Bearer {api_token}"}, data=form_data, files=files, timeout=30
        ).json()
        if up_res.get("code") != 0 and api_token != tat:
            up_res = feishu_session.post(
                "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all",
                headers={"Authorization": f"Bearer {tat}"}, data=form_data, files=files, timeout=30
            ).json()
        if up_res.get("code") != 0:
            raise Exception(f"Failed to upload {fname}. Error: Upload API failed: {up_res.get('msg')}")
        return (field_name, up_res["data"]["file_token"])

    if upload_jobs:
        try:
            with ThreadPoolExecutor(max_workers=min(4, len(upload_jobs))) as pool:
                uploaded = list(pool.map(_upload_one, upload_jobs))
        except Exception as e:
            logger.error("file_upload_failed", error=str(e))
            return jsonify({"error": str(e)}), 502
        for field_name, tok in uploaded:
            final_fields.setdefault(field_name, []).append({"file_token": tok})

    # AUDIT WORKSTATION FILINGS land already-decided: the auditor made the call
    # (Done / Rejected / Under Investigation) while filing, so the ticket should
    # never enter the live queue as Pending. It is written Closed from the start,
    # and Assigned Member is set to the very person who filed it (their Feishu
    # open_id, which the frontend always sends) -- so "who handled this audit"
    # and "who submitted it" are the same person by construction. User-type
    # fields need [{"id": open_id}] and that shape passes
    # strip_invalid_user_fields() below untouched; if the session has no open_id
    # on file (legacy/mock), the assignment is simply skipped rather than
    # blocking the filing.
    if is_audit_filing:
        final_fields["Request Status"] = "Closed"
        if submitter_open_id:
            final_fields["Assigned Member"] = [{"id": submitter_open_id}]
    else:
        final_fields["Request Status"] = "Pending"
    # Per request: "Submitted By" is intentionally NOT written on Create New Request
    # anymore -- ignored entirely. Real submitter attribution relies on Feishu's own
    # "Created By"/"Respondents" system column (stamped by whichever token performs
    # the create call below), plus the audit log entry at the end of this function,
    # which always records the true submitter regardless of which token creates the row.

    actual_fields = get_table_schema(REQUESTS_TABLE_ID, tat, BASE_ID)
    if actual_fields:
        dropped = [k for k in final_fields if k not in actual_fields]
        if dropped:
            logger.warn("submit_dropped_unknown_fields", fields=dropped)
        final_fields = {k: v for k, v in final_fields.items() if k in actual_fields}

    # BUG FIX (NumberFieldConvFail): same coercion as update_request -- protects new
    # submissions too, e.g. a Number field filled in with stray whitespace/commas.
    field_types = get_field_ui_types(REQUESTS_TABLE_ID, tat, BASE_ID)
    if field_types:
        final_fields = coerce_number_fields(final_fields, field_types)
        # BUG FIX: never write to Formula/Lookup/AutoNumber/CreatedTime/etc fields.
        final_fields = strip_readonly_fields(final_fields, field_types)
        # BUG FIX (UserFieldConvFail on Create New Request): if any field reaching
        # here is actually a Feishu User/Person column rather than Text, drop it
        # instead of letting it take down the whole submission -- see
        # strip_invalid_user_fields() for the full story.
        final_fields = strip_invalid_user_fields(final_fields, field_types, actor=user)

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records"
    success = False
    created_via = "user_token"
    # Feishu error codes that specifically mean "this token is dead" (expired/
    # invalid) -- these are the ONLY case that forces a re-login. Anything else
    # (a field this token isn't scoped to write, a transient rejection, etc.)
    # falls back to the tenant token exactly like before, so a ticket is never
    # blocked for a reason that logging in again wouldn't actually fix.
    # 99991663 = invalid tenant_access_token, 99991668 = invalid user_access_token,
    # 99991677 = user_access_token expired (confirmed from production logs --
    # this one was missing before and is the code actually being hit).
    TOKEN_AUTH_ERROR_CODES = (99991663, 99991668, 99991677)
    def _is_token_dead(d):
        if d.get("code") in TOKEN_AUTH_ERROR_CODES:
            return True
        # Fallback for any Feishu auth-error code not in the list above --
        # match on the message text so an unseen variant doesn't slip through
        # as a silent tenant-token fallback the way 99991677 just did.
        msg = str(d.get("msg", "")).lower()
        return ("token" in msg) and ("expir" in msg or "invalid" in msg)

    try:
        # SINGLE-CALL CREATE (User Context): create the row in ONE request, under the
        # agent's own identity first and foremost, with every field the user filled
        # in already attached (final_fields, including "Request Status") -- not a
        # blank row created first and patched with the real data afterward. Feishu's
        # own "Created By"/"Respondents" system columns get stamped with the real
        # submitter AND the data lands at the same time, so there's nothing to roll
        # back if it fails -- a failed call here just never creates a row at all.
        create_headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
        # Phase 10.24: ride the shared keep-alive session (same single Feishu
        # call, same quota -- just no fresh TCP+TLS handshake per create).
        resp = feishu_session.post(url, headers=create_headers, json={"fields": final_fields}, timeout=15)
        data = resp.json()

        if data.get("code") != 0:
            logger.warn("submit_primary_create_rejected", user=user,
                        feishu_code=data.get("code"), feishu_msg=data.get("msg"))

            if _is_token_dead(data):
                # The token itself is dead (expired/invalid) -- retry once with a
                # freshly-refreshed uat first...
                fresh_uat = _refresh_user_token(submitter_open_id) if submitter_open_id else None
                if fresh_uat:
                    logger.info("submit_retry_with_refreshed_uat", user=user)
                    create_headers["Authorization"] = f"Bearer {fresh_uat}"
                    resp = feishu_session.post(url, headers=create_headers, json={"fields": final_fields}, timeout=15)
                    data = resp.json()
                    if data.get("code") == 0:
                        created_via = "user_token_refreshed"
                    else:
                        logger.warn("submit_refreshed_uat_still_rejected",
                                    user=user, feishu_code=data.get("code"), feishu_msg=data.get("msg"))

                # ...and if the refresh itself was impossible or still rejected,
                # this really is an expired session -- force a fresh login instead
                # of quietly filing the ticket under the app identity.
                if data.get("code") != 0:
                    audit.log(user, "SESSION_EXPIRED_ON_SUBMIT", f"Type: {req_type} | feishu_code: {data.get('code')}", ip=request.headers.get("X-Forwarded-For", request.remote_addr or ""), severity="Warning")
                    return jsonify({"error": "Your session has expired. Please log in again, then resubmit this request — everything you filled in has been saved and will be restored automatically.", "code": "SESSION_EXPIRED"}), 401
            else:
                # NOT a dead-token error -- e.g. this agent's Lark account isn't a
                # collaborator with edit rights on the Bitable itself, or some other
                # rejection logging in again won't fix. Fall back to the tenant
                # token as a last resort so the ticket is never lost -- same
                # behavior as before this change, just no longer triggered by
                # simple expiry (that's handled above instead).
                # NOTE: Feishu's own "Created By" column will still show the app
                # identity, not the real submitter -- that's a platform-level
                # automatic field driven by whichever token performed the create
                # call, and nothing we send can override it. So instead, add a
                # short attribution line onto the END of the agent's own "Applier
                # Note" text -- never replacing or removing what they wrote, just
                # appended below it -- naming the real submitter (their Lark name,
                # the one WE already know from their website login, not from
                # Feishu's token) so a reviewer opening the record still sees who
                # actually filed it. Also try SUBMITTED_BY_FIELD_NAME as a bonus,
                # in case that dedicated column ever gets added to the table --
                # it silently no-ops today if it doesn't exist yet.
                rejection_code, rejection_msg = data.get("code"), data.get("msg")
                fallback_fields = dict(final_fields)
                attribution_line = f"🔒 Submitted via website by {user} (no sheet edit access)"
                existing_note = str(fallback_fields.get("Applier Note") or "").strip()
                fallback_fields["Applier Note"] = f"{existing_note}\n\n{attribution_line}" if existing_note else attribution_line
                if actual_fields and SUBMITTED_BY_FIELD_NAME in actual_fields:
                    fallback_fields[SUBMITTED_BY_FIELD_NAME] = user
                create_headers["Authorization"] = f"Bearer {tat}"
                resp = feishu_session.post(url, headers=create_headers, json={"fields": fallback_fields}, timeout=15)
                data = resp.json()
                created_via = "tenant_token_fallback"
                # Flag this for admins the same way an Access Request shows up --
                # a derived, no-extra-table read of the audit log (see
                # /api/admin/sheet-permission-alerts) rather than inventing new
                # storage. Auto-resolves the moment this agent successfully
                # submits again under their own token (see that endpoint), or an
                # admin can dismiss it manually once they've fixed the Bitable
                # collaborator permissions.
                audit.log(user, "SHEET_PERMISSION_MISSING",
                          f"Type: {req_type} | feishu_code: {rejection_code} | feishu_msg: {rejection_msg}",
                          ip=request.headers.get("X-Forwarded-For", request.remote_addr or ""), severity="Critical")


        if data.get("code") == 0:
            success = True
        else:
            raise Exception(f"Creation failed: {data.get('msg')}")

    except Exception as e:
        logger.error("feishu_submit_failed", error=str(e), req_type=req_type)
        return jsonify({"error": f"Failed to create record: {str(e)}"}), 502

    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    audit.log(user, "SUBMIT_NEW_REQUEST", f"Type: {req_type} | Code: {final_fields.get('Agency Code', 'N/A')} | created_via: {created_via}", ip=ip, severity="Info")

    # record_id is returned so the frontend can do a deliberate delayed
    # follow-up PATCH via /api/requests/update -- see the Audit tab's
    # submitAuditForm(), which deliberately submits everything except
    # "Mentioned Group" up front, then sets that one field ~3s later so the
    # @mention notification fires once the ticket's other data has settled.
    new_record_id = (data.get("data") or {}).get("record", {}).get("record_id")
    # Phase 10.29: write-through freshness -- a new ticket can carry a claim,
    # so the Live Formula ledger should see it within seconds, not after the
    # next 3-min background cycle.
    kick_table_resync()
    return jsonify({"success": True, "message": f"Successfully submitted {req_type}!", "created_via": created_via, "record_id": new_record_id})

@app.route('/api/points/records', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_records():
    start = time.time()
    user   = sanitize_text(request.args.get('user',''))
    email  = sanitize_text(request.args.get('email',''))
    perms  = get_user_permissions(email, user)
    ip     = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    if not perms.get("is_super_admin") and not any("points" in m for m in perms.get("modules",[])):
        return jsonify({"error":"Access denied"}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("points",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("points",["all"])

    try:
        page      = max(1, int(request.args.get('page','1')))
        page_size = min(200, max(1, int(request.args.get('page_size','50'))))
    except (ValueError, TypeError):
        page, page_size = 1, 50

    search       = sanitize_text(request.args.get('search',''), 100).lower()
    f_agency_code= sanitize_text(request.args.get('agency_id', request.args.get('agency_code',''))).lower()
    f_region     = sanitize_text(request.args.get('region','')).lower()
    f_acm        = sanitize_text(request.args.get('acm','')).lower()
    sort_by      = sanitize_text(request.args.get('sort_by','point_balance'))
    sort_dir     = 'desc' if request.args.get('sort_dir','desc').lower() != 'asc' else 'asc'

    served_from_cache = False
    if MOCK_MODE:
        all_items = MockFeishuDB.generate_agency("All") * 10
        fetch_complete, stop_reason = True, ""
    else:
        all_items, fetch_complete, stop_reason, served_from_cache = get_points_table_snapshot()

    if not fetch_complete and not all_items: return jsonify({"error": f"Feishu sync failed: {stop_reason}"}), 502

    filtered = []
    for item in all_items:
        f = item.get("fields", {})
        agency_code = extract_field_text(get_field_local(f, "Agency Code")).strip()
        acm         = extract_field_text(get_field_local(f, "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member")).strip()
        region      = 'PK' if acm.lower() in PK_ACMS else ('IN' if acm.lower() in IN_ACMS else '')

        if "all" not in allowed_acms and acm.lower() not in [a.lower() for a in allowed_acms]: continue
        if "all" not in allowed_regs and region.lower() not in [r.lower() for r in allowed_regs]: continue

        if search and search not in (agency_code + acm).lower(): continue
        if f_agency_code and f_agency_code not in agency_code.lower(): continue
        if f_region      and f_region      not in region.lower():   continue
        if f_acm         and f_acm         not in acm.lower():      continue

        base_pts   = parse_float_safe(extract_field_text(get_field_local(f, "Base Points")))
        bonus_pts  = parse_float_safe(extract_field_text(get_field_local(f, "Bonus Points")))
        total_pts  = parse_float_safe(extract_field_text(get_field_local(f, "Total Points", "# Total Points")))
        used_pts   = parse_float_safe(extract_field_text(get_field_local(f, "Used Points")))
        balance    = parse_float_safe(extract_field_text(get_field_local(f, "Point Balance")))
        monthly_target, ytd_target = parse_monthly_target_field(get_field_local(f, "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check"))

        if balance == 0 and total_pts > 0: balance = total_pts - used_pts

        health = 100
        if total_pts > 0:
            utilization = used_pts / total_pts
            if utilization > 0.90: health = 40
            elif utilization > 0.70: health = 70
            else: health = 95
        else: health = 0

        filtered.append({
            "agency_id": agency_code, "acm": acm, "region": region,
            "agency_name": extract_field_text(get_field_local(f, "Agency Name", "Name")),
            "base_points": base_pts, "bonus_points": bonus_pts,
            "total_points": total_pts, "used_points": used_pts,
            "point_balance": balance, "health_score": health, "ytd_target": ytd_target, "monthly_target": monthly_target,
        })

    sort_fields = {
        "agency_id": "agency_id", "acm": "acm", "region": "region",
        "base_points": "base_points", "bonus_points": "bonus_points",
        "total_points": "total_points", "used_points": "used_points",
        "point_balance": "point_balance", "health_score": "health_score",
    }
    sf = sort_fields.get(sort_by, "point_balance")
    reverse = (sort_dir == 'desc')

    try: filtered.sort(key=lambda x: (x[sf] is None, x[sf], x["agency_id"]), reverse=reverse)
    except TypeError: filtered.sort(key=lambda x: (str(x.get(sf,"")), x["agency_id"]), reverse=reverse)

    total_count = len(filtered)
    total_pts_sum = sum(r["total_points"] for r in filtered)
    used_pts_sum  = sum(r["used_points"] for r in filtered)
    balance_sum   = sum(r["point_balance"] for r in filtered)
    ytd_target_sum = sum(r["ytd_target"] for r in filtered)

    is_export = request.args.get('export', 'false').lower() == 'true'
    if is_export:
        if not perms.get("is_super_admin") and not any("export" in m for m in perms.get("modules",[])):
            audit.log(user, "UNAUTHORIZED_EXPORT", "Point Records", ip=ip, severity="Critical")
            return jsonify({"error":"Export access denied."}), 403
        
        audit.log(user, "EXPORT_DATA", f"Point Records ({total_count} rows)", ip=ip, severity="Info")
        return jsonify({
            "records": filtered[:5000], "total": total_count, "page": 1, "page_size": total_count,
            "total_pages": 1,
            "totals": {"total_points": total_pts_sum, "used_points": used_pts_sum, "point_balance": balance_sum, "ytd_target": ytd_target_sum},
            "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason),
            "served_from_background_cache": served_from_cache,
            "duration_ms": int((time.time() - start) * 1000), "raw_rows_fetched": len(all_items)
        })

    slice_start, slice_end = (page - 1) * page_size, (page - 1) * page_size + page_size
    page_records = filtered[slice_start:slice_end]

    return jsonify({
        "records": page_records, "total": total_count, "page": page, "page_size": page_size,
        "total_pages": max(1, -(-total_count // page_size)),
        "totals": {"total_points": total_pts_sum, "used_points": used_pts_sum, "point_balance": balance_sum, "ytd_target": ytd_target_sum},
        "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason),
        "served_from_background_cache": served_from_cache,
        "duration_ms": int((time.time() - start) * 1000), "raw_rows_fetched": len(all_items)
    })

@app.route('/api/audit/log-action', methods=['POST'])
def client_audit_log_action():
    data = request.json or {}
    user = sanitize_text(data.get('user', ''))
    email = sanitize_text(data.get('email', ''))
    action = sanitize_text(data.get('action', ''))
    target = sanitize_text(data.get('target', ''))
    severity = sanitize_text(data.get('severity', 'Info'))
    
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not perms.get("modules"): return jsonify({"error":"Unauthorized"}), 403
        
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    audit.log(user, action, target, ip=ip, severity=severity)
    return jsonify({"success": True})

@app.route('/api/sync/refresh', methods=['POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def sync_refresh():
    # Two ways in: the external cron-job.org caller presents a bearer token
    # (no Feishu session to check), everything else (a manual "sync now" from
    # an admin tool, curl testing, etc.) still goes through the normal
    # admin-user permission check exactly as before. Either is sufficient.
    user  = sanitize_text(request.args.get('user', request.headers.get('X-User-Name','')))
    email = sanitize_text(request.args.get('email',''))
    via_cron = is_cron_authorized(request)
    if not via_cron:
        perms = get_user_permissions(email, user)
        if not perms.get("is_super_admin") and not perms.get("modules"):
            return jsonify({"error": "Access denied"}), 403

    # ?table=requests / ?table=points refreshes just that one table (so
    # cron-job.org can hit each on its own lightweight schedule); omitting it
    # (or ?table=all) keeps the original behavior of refreshing both in one
    # call, e.g. for a manual admin-triggered sync.
    table = sanitize_text(request.args.get('table', 'all')).strip().lower()
    if table not in ('all', 'requests', 'points'):
        return jsonify({"error": "Invalid table. Use 'requests', 'points', or omit for both."}), 400

    cache_invalidate()
    jobs = []
    if table in ('all', 'requests'): jobs.append(_background_sync_requests_table)
    if table in ('all', 'points'):   jobs.append(_background_sync_points_table)
    with ThreadPoolExecutor(max_workers=max(len(jobs), 1)) as executor:
        futures = [executor.submit(job) for job in jobs]
        for f in futures: f.result()

    # Piggyback the Agency Exchange's daily points snapshot on the existing cron
    # cadence, so Movers history accrues reliably even if nobody opens the board
    # that day. Cheap no-op if today's snapshot was already taken. Only runs
    # when this call actually touched the points table (or refreshed both).
    if table in ('all', 'points'):
        try:
            with _bg_points_lock:
                pts_items_for_snapshot = list(_bg_points_sync["items"])
            if pts_items_for_snapshot:
                append_exchange_daily_snapshot(pts_items_for_snapshot)
        except Exception as e:
            logger.warn("exchange_snapshot_from_cron_failed", error=str(e))

    resp = {"success": True, "redis_enabled": REDIS_ENABLED, "table": table}
    if table in ('all', 'requests'):
        with _bg_sync_lock:
            resp["requests_table"] = {
                "record_count": len(_bg_sync["requests_items"]),
                "last_refreshed_at": _bg_sync["updated_at"],
                "fetch_complete": _bg_sync["fetch_complete"],
            }
    if table in ('all', 'points'):
        with _bg_points_lock:
            resp["points_table"] = {
                "record_count": len(_bg_points_sync["items"]),
                "last_refreshed_at": _bg_points_sync["updated_at"],
                "fetch_complete": _bg_points_sync["fetch_complete"],
            }

    audit_actor = "cron-job.org" if via_cron else (user or "unknown")
    audit.log(audit_actor, "MANUAL_SYNC_REFRESH", table, ip=request.headers.get("X-Forwarded-For",""), severity="Info")
    return jsonify(resp)

@app.route('/api/points/search', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_search():
    if request.args.get('q') and not request.args.get('search'):
        args = dict(request.args)
        args['search'] = args.pop('q')
        request.environ['QUERY_STRING'] = urllib.parse.urlencode(args, doseq=True)
    return points_records()

# ─── /api/query shared helpers (Phase 10.7) ───
# The combo-search logic used to live inline inside query_records(); it is now
# shared with /api/query/batch so BOTH endpoints run byte-for-byte the same
# search per field (same aliases, same operator order, same first-working-combo
# rule, same result shaping). Any future fix here automatically applies to both.
def _query_search_url_and_headers(tat):
    return (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true",
        {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"},
    )

def _query_combos_for_field(field, value):
    """Every (alias, operator, value) combination tried for one field, in the
    exact order the first-working-combo rule expects (aliases outer, contains/is/
    = inner; '=' only for all-digit values)."""
    combos = []
    for alias in QUERY_FIELD_ALIASES[field]:
        for op in ["contains", "is", "="]:
            if op == "=" and not value.isdigit(): continue
            val_array = (int(value),) if op == "=" else (value,)
            combos.append((alias, op, val_array))
    return combos

def _query_try_combo(search_url, headers, combo, projection=QUERY_RECORDS_FIELDS):
    """Fire one combo against Feishu's /records/search. Retries once without the
    field projection when the base rejects it (1254045), exactly like before."""
    alias, op, val_array = combo
    payload = {"page_size": 500, "filter": {"conjunction": "and", "conditions": [{"field_name": alias, "operator": op, "value": val_array}]}}
    if projection: payload["field_names"] = projection
    try:
        resp = feishu_session.post(search_url, headers=headers, json=payload, timeout=10)
        data = resp.json()
        if data.get("code") == 1254045 and projection:
            return _query_try_combo(search_url, headers, combo, projection=None)
        if data.get("code") == 0:
            return {"combo": combo, "ok": True, "items": data.get("data", {}).get("items", [])}
        elif data.get("code") not in (1254011, 1254402, 1254010):
            return {"combo": combo, "ok": False, "error": data.get("msg"), "fatal": data.get("code") == 99991663}
        return {"combo": combo, "ok": False, "error": None}
    except Exception as e:
        return {"combo": combo, "ok": False, "error": str(e)}

def _query_pick_first_ok(combos, results_by_combo):
    """The first-working-combo rule: walk combos in order, take the items of the
    first one that succeeded; remember the last error message otherwise."""
    all_items, fetch_complete, stop_reason, success = [], False, "", False
    for combo in combos:
        res = results_by_combo.get(combo)
        if res and res.get("ok"):
            return res["items"], True, "", True
        if res and res.get("error"):
            stop_reason = res["error"]
    return all_items, fetch_complete, stop_reason, success

# ─── Phase 10.14 (Ahmed): ONE Feishu search per radar scan ───
# /api/query/batch used to fan every field's alias×operator combos out as
# separate /records/search calls (up to ~18 Feishu API hits and 3 audit
# lines for a single radar scan -- "it still send as 3 request and recive
# as 3 request and this cost alot"). When every searched value is
# all-digits (the radar's User ID / Otherapp ID / NID always are), the exact
# same search runs as ONE /records/search call whose filter ORs each field's
# working alias; the hits are then sorted back into their per-field slots
# server-side. Feishu evaluates the very same (alias, operator, value)
# conditions the combo probe would have picked, and digits have no case, so
# the local contains/is/= re-check reproduces Feishu's own match decision
# exactly -- results are identical, just one request. Anything unusual (a
# non-digit value, a renamed column that breaks the OR filter) falls back
# to the legacy per-field combo probe unchanged, and that probe re-teaches
# the alias cache which column name currently works.
_COMBO_MAP_REDIS_KEY = "xena:query_combo_map"
_combo_map_cache = {"data": None, "ts": 0, "lock": threading.Lock()}

def _query_combo_map_get():
    """field -> [alias, operator] that last worked, cached in Redis (24h) so
    every warm instance can skip the probe entirely."""
    with _combo_map_cache["lock"]:
        if _combo_map_cache["data"] is not None and time.time() - _combo_map_cache["ts"] < 24 * 3600:
            return dict(_combo_map_cache["data"])
    data = redis_get_json(_COMBO_MAP_REDIS_KEY) if REDIS_ENABLED else None
    if isinstance(data, dict):
        with _combo_map_cache["lock"]:
            _combo_map_cache["data"] = data
            _combo_map_cache["ts"] = time.time()
        return dict(data)
    return {}

def _query_combo_map_set(mapping):
    with _combo_map_cache["lock"]:
        _combo_map_cache["data"] = dict(mapping)
        _combo_map_cache["ts"] = time.time()
    if REDIS_ENABLED:
        redis_set_json(_COMBO_MAP_REDIS_KEY, mapping, ttl=24 * 3600)

def _query_combo_map_drop():
    with _combo_map_cache["lock"]:
        _combo_map_cache["data"] = None
        _combo_map_cache["ts"] = 0
    if REDIS_ENABLED:
        redis_cmd("DEL", _COMBO_MAP_REDIS_KEY)

def _query_local_match(fields, field, op, value):
    """Reproduces Feishu's own decision for ONE (field, operator, value)
    condition, used to sort OR-search hits back into per-field slots. Only
    ever called with all-digit values (the OR path is gated on that), where
    contains/is/= have exact, case-free local equivalents. The field's text
    is extracted through the same alias chain the combo search filters on."""
    txt = ""
    for alias in QUERY_FIELD_ALIASES[field]:
        txt = extract_field_text(get_field_local(fields, alias))
        if txt:
            break
    if not txt:
        return False
    if op == "contains":
        return value in txt
    if op == "is":
        return txt == value
    if op == "=":
        return txt.isdigit() and int(txt) == int(value)
    return False

def _query_try_or_search(search_url, headers, conditions, projection):
    """Fires ONE /records/search whose filter ORs every field's condition,
    following pagination (up to 3 pages = 1,500 records, matching the old
    500-per-field ceiling). Same retry-without-projection and same
    error-shape conventions as _query_try_combo."""
    items, page_token, complete = [], None, True
    for _ in range(3):
        payload = {"page_size": 500,
                   "filter": {"conjunction": "or", "conditions": conditions}}
        if projection: payload["field_names"] = projection
        if page_token: payload["page_token"] = page_token
        try:
            resp = feishu_session.post(search_url, headers=headers, json=payload, timeout=15)
            data = resp.json()
        except Exception as e:
            return {"ok": False, "error": str(e)}
        if data.get("code") == 1254045 and projection:
            return _query_try_or_search(search_url, headers, conditions, projection=None)
        if data.get("code") != 0:
            if data.get("code") in (1254011, 1254402, 1254010):
                # bad filter / unknown column -- silent, caller falls back
                # to the per-field probe which re-learns the working alias.
                return {"ok": False, "error": None}
            return {"ok": False, "error": data.get("msg"), "fatal": data.get("code") == 99991663}
        block = data.get("data", {}) or {}
        items.extend(block.get("items", []) or [])
        page_token = block.get("page_token")
        if not page_token or not block.get("has_more"):
            break
    else:
        complete = False  # page cap hit -- same honesty as fetch_complete elsewhere
    # One record can satisfy several OR conditions; Feishu returns it once,
    # but dedupe defensively by record_id.
    seen, uniq = set(), []
    for it in items:
        rid = it.get("record_id")
        if rid and rid in seen: continue
        if rid: seen.add(rid)
        uniq.append(it)
    return {"ok": True, "items": uniq, "complete": complete}

def _query_format_results(all_items, allowed_acms_set, allowed_regs_set):
    """Shared raw-item -> response-row shaping (region/ACM inference, permission
    filtering, date formatting, newest-first sort). Used by both /api/query and
    /api/query/batch so their rows are always identical for the same items."""
    results = []
    for item in all_items:
        fields = item.get("fields", {})

        region = clean(get_field_local(fields, "Region", "Agency Region"))
        acm_pk = clean(get_field_local(fields, "Acm Name (PK)"))
        acm_in = clean(get_field_local(fields, "Acm Name (IN)"))
        acm_fb = clean(get_field_local(fields, "Acm", "Assigned Member"))
        if region in ("", "none"):
            if acm_pk in PK_ACMS or acm_fb in PK_ACMS: region = "pk"
            elif acm_in in IN_ACMS or acm_fb in IN_ACMS: region = "in"
        acm = (acm_in if region == "in" else acm_pk) or acm_fb

        if "all" not in allowed_acms_set and acm.lower().strip() not in allowed_acms_set: continue
        if "all" not in allowed_regs_set and region not in allowed_regs_set: continue

        submitted_raw = get_field_local(fields, "Submitted on Copy", "Submitted on", "Created Time")
        submitted_dt  = parse_feishu_date(submitted_raw)

        results.append({
            # Phase 10.13: record_id rides along (it is already on every search
            # item -- zero extra calls) so the Universal Query card's
            # Audition-access buttons (View Pictures / Open in Ticket Workspace)
            # can act on the exact record.
            "record_id":        item.get("record_id"),
            "numbering":        extract_field_text(get_field_local(fields, "Numbering")),
            "request_type":     extract_field_text(get_field_local(fields, "Request Type", "Type")),
            "submitted_on":     submitted_dt.strftime("%Y-%m-%d") if submitted_dt else extract_field_text(submitted_raw),
            "respondents":      extract_field_text(get_field_local(fields, "Respondents", "Created By")),
            "user_id":          extract_field_text(get_field_local(fields, "User ID")),
            "agency_code":      extract_field_text(get_field_local(fields, "Agency Code")),
            "agency_type":      extract_field_text(get_field_local(fields, "Agency Type", "Type of Agency")),
            "otherapp_id":      extract_field_text(get_field_local(fields, "Otherapp ID", "Otherapp Name", "Other App Name")),
            "acm":              acm.title() if acm else "",
            "region":           region.upper() if region else "",
            "bd_code":          extract_field_text(get_field_local(fields, "Bd Code", "BD Code")),
            "status":           extract_field_text(get_field_local(fields, "Status", "Request Status")),
            "reject_reason":    extract_field_text(get_field_local(fields, "Reject Reason", "Rejection Reason")),
            "audition_note":    extract_field_text(get_field_local(fields, "Audition note", "Audition Note")),
            "duplicated_check": extract_field_text(get_field_local(fields, "Duplicated Check")),
            "closing_reason":   extract_field_text(get_field_local(fields, "Closing Reason", "Closing Agencies Reason")),
            "_sort_ts": submitted_dt.timestamp() if submitted_dt else 0,
        })

    results.sort(key=lambda r: r["_sort_ts"], reverse=True)
    for r in results: r.pop("_sort_ts", None)
    return results

@app.route('/api/query/batch', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def query_records_batch():
    """
    Phase 10.7 (Ahmed): the duplicate radar used to fire 3 separate /api/query
    calls per scan (one per identity field) -- 3 Vercel invocations, 3 chances
    of a cold start, 3 permission checks, per ticket open. This endpoint runs
    the EXACT same per-field logic (shared helpers above: same aliases, same
    operators, same first-working-combo rule, same row shaping) for every
    requested field inside ONE invocation, sharing the tenant token and the
    connection pool. Results are identical to N separate calls -- only cheaper
    and faster. Body: {"user","email","queries":[{"field","value"}, ...]};
    response: {"responses":[ ...one /api-query-shaped body per query, in the
    same order... ]}. Order is preserved because the frontend merges by index.
    """
    data  = request.get_json(silent=True) or {}
    user  = sanitize_text(data.get('user', ''))
    email = sanitize_text(data.get('email', ''))
    raw_queries = data.get('queries')
    ip    = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    if not isinstance(raw_queries, list) or not raw_queries:
        return jsonify({"error": "Missing queries list."}), 400

    # Validate up front; bad entries become error slots so the response list
    # ALWAYS mirrors the request list 1:1 (the frontend merges by index).
    normalized = []
    for q in raw_queries[:5]:  # sanity bound -- the radar never asks for more than 3
        if not isinstance(q, dict):
            normalized.append(('', '', 'Invalid query entry.'))
            continue
        field = sanitize_text(str(q.get('field', ''))).strip().lower()
        value = sanitize_text(str(q.get('value', '')), 200).strip()
        if field not in QUERY_FIELD_ALIASES:
            normalized.append((field, value, 'Invalid search field.'))
        elif not value:
            normalized.append((field, value, 'Please enter a value to search.'))
        else:
            normalized.append((field, value, None))

    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("query" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("query",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("query",["all"])
    allowed_acms_set = set(a.lower() for a in allowed_acms) if allowed_acms else {"all"}
    allowed_regs_set = set(r.lower() for r in allowed_regs) if allowed_regs else {"all"}

    # Phase 10.14 (Ahmed): ONE audit line per scan, not one per field -- the
    # whole radar scan is a single search now ("need all the duplicated radar
    # request to be send and recive as one request"). The per-field values
    # stay fully visible inside that one line, so the trail loses nothing.
    audit_fields = ",".join(f"{f}={v}" for f, v, err in normalized if not err)
    if audit_fields:
        audit.log(user, "QUERY_SEARCH", f"batch:{audit_fields}", ip=ip, severity="Info")

    if MOCK_MODE:
        responses = []
        for field, value, err in normalized:
            if err:
                responses.append({"error": err, "field": field, "value": value})
                continue
            results = _query_format_results(MockFeishuDB.generate_requests(10), allowed_acms_set, allowed_regs_set)
            responses.append({"results": results, "count": len(results), "field": field, "value": value,
                              "fetch_complete": True, "stop_reason": "", "served_from_background_cache": False})
        return jsonify({"responses": responses})

    tat = get_tenant_access_token()
    search_url, headers = _query_search_url_and_headers(tat)

    jobs = []       # (response_index, field, value) for valid entries
    for idx, (field, value, err) in enumerate(normalized):
        if not err:
            jobs.append((idx, field, value))

    responses = [None] * len(normalized)
    for idx, (field, value, err) in enumerate(normalized):
        if err:
            responses[idx] = {"error": err, "field": field, "value": value}

    # ── Phase 10.14 fast path: ONE OR-search for the whole scan ──
    # Gated on every value being all-digits (always true for the radar's
    # User ID / Otherapp ID / NID) so the server-side slot classification is
    # an exact reproduction of Feishu's own per-condition decision. Any
    # non-digit value takes the legacy probe below instead -- zero accuracy
    # risk either way.
    if jobs and all(value.isdigit() for _, _, value in jobs):
        combo_map = _query_combo_map_get()
        conditions, chosen = [], {}
        for _, field, value in jobs:
            alias, op = (combo_map.get(field) or [QUERY_FIELD_ALIASES[field][0], "contains"])
            chosen[field] = (alias, op)
            conditions.append({"field_name": alias, "operator": op,
                               "value": [int(value)] if op == "=" else [value]})
        # The projection must cover every alias we filter on, or the local
        # slot classification would read an empty field.
        projection = list(QUERY_RECORDS_FIELDS)
        for alias, _op in chosen.values():
            if alias not in projection:
                projection.append(alias)
        or_res = _query_try_or_search(search_url, headers, conditions, projection)
        if or_res.get("ok"):
            for idx, field, value in jobs:
                alias, op = chosen[field]
                slot_items = [it for it in or_res["items"]
                              if _query_local_match(it.get("fields", {}) or {}, field, op, value)]
                results = _query_format_results(slot_items, allowed_acms_set, allowed_regs_set)
                responses[idx] = {"results": results, "count": len(results), "field": field, "value": value,
                                  "fetch_complete": or_res["complete"],
                                  "stop_reason": ("" if or_res["complete"] else "page_cap_reached"),
                                  "served_from_background_cache": False}
            if not combo_map:
                # First good scan on this instance -- remember the aliases
                # that worked so later scans skip straight to them.
                _query_combo_map_set({f: [a, o] for f, (a, o) in chosen.items()})
            return jsonify({"responses": responses})
        # The OR filter was rejected (a column got renamed / schema changed):
        # drop the stale alias map and fall through to the legacy probe,
        # which re-teaches the cache below from its per-field winners.
        if combo_map:
            _query_combo_map_drop()

    # ── Legacy per-field combo probe ──
    # The exact pre-10.14 behavior, kept as the automatic fallback (non-digit
    # values, OR filter rejected) and as the teacher for the alias cache:
    # each field's first working combo is what the OR path reuses next scan.
    # Fire every field's combos on ONE shared pool -- the same combos 3
    # separate calls would have run, picked apart per field afterwards. The
    # worker ceiling stays at 9 (the per-request ceiling /api/query always
    # had), so this is actually GENTLER on Feishu's QPS than 3 parallel calls.
    all_combos = []
    legacy_jobs = []  # (response_index, combos)
    for idx, field, value in jobs:
        combos = _query_combos_for_field(field, value)
        legacy_jobs.append((idx, combos))
        all_combos.extend(combos)

    combo_results = {}
    if all_combos:
        with ThreadPoolExecutor(max_workers=min(9, len(all_combos))) as executor:
            for res in executor.map(lambda c: _query_try_combo(search_url, headers, c), all_combos):
                combo_results[res["combo"]] = res

    learned = {}
    for idx, combos in legacy_jobs:
        field, value, _ = normalized[idx]
        results_by_combo = {c: combo_results.get(c) for c in combos}
        all_items, fetch_complete, stop_reason, success = _query_pick_first_ok(combos, results_by_combo)
        if not success:
            responses[idx] = {"error": f"Data fetch failed: Feishu API Error: {stop_reason or 'Invalid Filter.'}",
                              "field": field, "value": value}
            continue
        winner = next((c for c in combos if (results_by_combo.get(c) or {}).get("ok")), None)
        if winner:
            learned[field] = [winner[0], winner[1]]
        results = _query_format_results(all_items, allowed_acms_set, allowed_regs_set)
        responses[idx] = {"results": results, "count": len(results), "field": field, "value": value,
                          "fetch_complete": fetch_complete,
                          "stop_reason": ("" if fetch_complete else stop_reason),
                          "served_from_background_cache": False}

    if learned:
        merged = _query_combo_map_get()
        merged.update(learned)
        _query_combo_map_set(merged)

    return jsonify({"responses": responses})

@app.route('/api/query', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def query_records():
    user  = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    field = sanitize_text(request.args.get('field','')).strip().lower()
    value = sanitize_text(request.args.get('value',''), 200).strip()
    ip    = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    if field not in QUERY_FIELD_ALIASES: return jsonify({"error": "Invalid search field."}), 400
    if not value: return jsonify({"error": "Please enter a value to search."}), 400

    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("query" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("query",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("query",["all"])
    allowed_acms_set = set(a.lower() for a in allowed_acms) if allowed_acms else {"all"}
    allowed_regs_set = set(r.lower() for r in allowed_regs) if allowed_regs else {"all"}

    audit.log(user, "QUERY_SEARCH", f"{field}={value}", ip=ip, severity="Info")

    if MOCK_MODE:
        all_items = MockFeishuDB.generate_requests(10)
        fetch_complete, stop_reason, success = True, "", True
    else:
        tat = get_tenant_access_token()
        search_url, headers = _query_search_url_and_headers(tat)
        # Phase 10.7: the combo search + first-working-combo pick now run
        # through the shared helpers (/api/query/batch uses them too), so both
        # endpoints can never drift apart.
        combos = _query_combos_for_field(field, value)

        # Phase 10.34 (speed, zero cost): projection=None -- Feishu's
        # records/search costs per CALL, not per field, and it already
        # downloads every field of each hit. Full fields ride each result row
        # so the frontend seeds the ticket workspace on open: zero
        # /api/requests/single calls (and the old top-3 warm prefetches are
        # gone -- FEWER calls than before). /api/query/batch keeps its slim
        # projection on purpose (radar rows never open a workspace).
        results_by_combo = {}
        with ThreadPoolExecutor(max_workers=min(9, len(combos) or 1)) as executor:
            for res in executor.map(lambda c: _query_try_combo(search_url, headers, c, projection=None), combos):
                results_by_combo[res["combo"]] = res

        all_items, fetch_complete, stop_reason, success = _query_pick_first_ok(combos, results_by_combo)

        if not success: return jsonify({"error": f"Data fetch failed: Feishu API Error: {stop_reason or 'Invalid Filter.'}"}), 502

    results = _query_format_results(all_items, allowed_acms_set, allowed_regs_set)

    # Phase 10.34: attach full fields per row (keyed by record_id) AFTER the
    # shared formatter ran -- the formatter itself stays slim for /api/query/batch.
    _ff_by_id = {it.get("record_id"): it.get("fields", {}) for it in all_items if it.get("record_id")}
    for r in results:
        r["full_fields"] = _ff_by_id.get(r.get("record_id"))

    return jsonify({
        "results": results, "count": len(results), "field": field, "value": value,
        "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason),
        "served_from_background_cache": False
    })

@app.route('/api/my-requests', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def my_requests():
    user = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    open_id = sanitize_text(request.args.get('open_id', ''))
    perms = get_user_permissions(email, user)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    
    if not perms.get("is_super_admin") and not any("submit_my_requests" in m or "submit" in m for m in perms.get("modules",[])):
        return jsonify({"error":"Access denied"}), 403
    
    # Preset values are now the number of FULL PRIOR days beyond today, not a total
    # window size -- "Today" is 0, "Last 3 days" is 3 (meaning today + the 3 calendar
    # days before it = 4 days shown total: e.g. if today is Saturday, that's Sat, Fri,
    # Thu, Wed). Every preset always includes today. Accepts the old 'today' string
    # too, as an alias for 0, for any stale cached frontend still sending it.
    days_param = sanitize_text(request.args.get('days', '0'))
    prior_days = 0 if days_param == 'today' else max(0, min(_safe_int(days_param, 0), 90))

    # Per-user timezone: the browser sends its own UTC offset (minutes east of UTC)
    # so Cairo / Pakistan / India / USA / Turkey viewers each see THEIR own "today".
    # Falls back to Cairo (+180) for any stale client that doesn't send it.
    tz_offset_min = max(-720, min(840, _safe_int(request.args.get('tz_offset', ''), 180)))
    lookback_days = prior_days  # kept in the response payload for the frontend label

    # FIX: this is now a fully live, on-demand fetch -- no Redis/background snapshot
    # involved at all, and the frontend only calls this endpoint when the person clicks
    # "Search" (not automatically when the page opens), so a live round-trip to Feishu
    # here is expected and acceptable rather than something to hide behind a cache.
    #
    # We use the /records/search POST endpoint (not the classic GET .../records?filter=
    # formula endpoint) because its structured filter conditions are far more reliable
    # against this base than the CurrentValue.[...] formula string was. Max 500
    # records/page on this endpoint, paginated via page_token.
    tat = get_tenant_access_token()
    session = http_requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"

    # BUG FIX ("Vercel Runtime Timeout Error: Task timed out after 60 seconds" /
    # /api/my-requests spins forever, nothing shows): every fetch stage below shares
    # one wall-clock deadline. Each stage stops fetching more pages once it's spent,
    # and later stages are skipped entirely if there's no time left, so the endpoint
    # always returns *something* (partial results + fetch_complete:false) comfortably
    # before the platform timeout instead of never returning at all. Defined before
    # the fast path now, because the fast path is paginated too (see below).
    MY_REQUESTS_TIME_BUDGET_SECONDS = 45  # leaves ~15s headroom under Vercel's 60s cap
    _deadline = time.time() + MY_REQUESTS_TIME_BUDGET_SECONDS

    def _run_search(payload, deadline):
        """Runs the live paginated /records/search fetch for a given payload. Returns
        (items, complete, reason). Stops early (complete=False) once `deadline` is hit,
        even mid-pagination, rather than continuing to burn the request's time budget."""
        items, page_token, complete, reason = [], None, True, ""
        for _ in range(50):  # 50 * 100 = 5,000 records hard ceiling as a sanity bound
            if time.time() >= deadline:
                complete, reason = False, "time_budget_exceeded"
                break
            body = dict(payload)
            if page_token:
                body["page_token"] = page_token
            try:
                remaining = max(3, min(15, deadline - time.time()))
                resp = session.post(search_url, json=body, timeout=remaining)
                data = resp.json()
                if data.get("code") != 0:
                    complete, reason = False, f"{data.get('code')}: {data.get('msg')}"
                    break
                block = data.get("data", {})
                items.extend(block.get("items", []))
                page_token = block.get("page_token")
                if not page_token or not block.get("has_more", False):
                    break
            except Exception as e:
                complete, reason = False, str(e)
                break
        return items, complete, reason

    # SPEED FIX: mirror /api/query -- try one targeted server-side filter on the
    # reliable submitter field FIRST, exactly like the fast Query page does for
    # Agency Code / User ID / etc. This returns only this person's own records
    # straight from Feishu (no full-table scan, no local matching), so it's just as
    # fast as Query. Requires SUBMITTED_BY_FIELD_NAME to be a real column in the base
    # (see the comment near its definition) when no open_id is available -- if it
    # isn't yet, Feishu returns FieldNameNotFound (1254045) and that's surfaced
    # directly to the caller as fetch_complete:false, not silently retried.
    #
    # BUG FIX (Today/Last-N-days showing wrong or missing records, e.g. only
    # yesterday's records appearing): the previous 1254018 "wrong ExactDate array
    # shape" error came from sending the ms boundary as a JSON *number*. Feishu's own
    # filter docs give the value as a two-element array whose second element is the
    # ms timestamp as a STRING -- e.g. ["ExactDate", "1740441600000"] -- not an int.
    # That's the actual shape mismatch; the filter itself is otherwise exactly the
    # right tool for this. "Submitted on" was also unreliable (per earlier
    # comments/testing) -- "Submitted on Copy" is the field this app already writes
    # a real epoch-ms DateTime value to on submission (see the mock-write path
    # above), so that's the one to filter on here, matching what every other read
    # path in this file already prefers via get_field_local(..., "Submitted on
    # Copy", "Submitted on", "Created Time").
    #
    # Bounds: isGreater is a strict '>', so the lower bound is nudged 1ms earlier
    # than from_dt's true epoch to make it effectively inclusive of records stamped
    # exactly at the start of the window. isLess against the *next* midnight gives a
    # clean, inclusive "through end of today" upper bound without needing to guess
    # at "now" down to the millisecond.
    _local_from_ms, to_ms_exclusive = local_day_window_ms(prior_days, tz_offset_min)
    from_ms = _local_from_ms - 1

    # BUG FIX (Today showing yesterday, "Last N days" missing today AND the oldest
    # day -- both edges of the window wrong): the ExactDate isGreater/isLess filter
    # below was trusted to do the date-boundary math itself against a JSON-string ms
    # value. In practice Feishu evaluates ExactDate boundaries against "Submitted on
    # Copy" using the BASE's own configured timezone (Beijing, UTC+8), not as a
    # literal timezone-agnostic epoch-ms comparison -- so a window computed for the
    # viewer's local midnight (e.g. Cairo, UTC+3) gets silently shifted by the ~5-8
    # hour gap between the two timezones, which is exactly consistent with "today's
    # records missing, window shifted a day early." Rather than depend on Feishu's
    # internal ExactDate semantics at all, the identity filter (Respondents == this
    # person, already narrow) is now the ONLY server-side filter; the date window is
    # applied afterward in Python using true epoch-ms extracted from each record via
    # _extract_epoch_ms(), compared directly against [from_ms, to_ms_exclusive) --
    # so the boundary math only ever happens in code we control.
    # SPEED FIX (prolific respondents make this endpoint slow): the identity-only
    # filter above fetches EVERY record that respondent has ever submitted, then
    # filters dates in Python -- fine for light users, painfully slow for someone
    # with hundreds/thousands of historical requests. We still can't trust Feishu's
    # ExactDate boundary math directly (see the big comment above: it evaluates
    # against the BASE's configured Beijing/UTC+8 timezone, not the viewer's), so we
    # don't use it as the final word. Instead we send Feishu a deliberately widened
    # ("padded") version of the window -- 2 full days earlier and later than what we
    # actually want -- purely to narrow the candidate set server-side. That padding
    # comfortably covers the ~5-8h Beijing-vs-viewer offset in either direction with
    # margin to spare. The exact, correct boundary is still enforced afterward by
    # _in_local_window() below using true epoch-ms in Python, so accuracy is
    # unaffected regardless of whether the viewer is in Cairo, Pakistan, or the US --
    # this padding only ever makes the Feishu-side fetch smaller, never wrong.
    DATE_FILTER_PAD_MS = 2 * 24 * 60 * 60 * 1000
    padded_from_ms = from_ms - DATE_FILTER_PAD_MS
    padded_to_ms_exclusive = to_ms_exclusive + DATE_FILTER_PAD_MS

    fast_items, fast_ok, fast_complete, fast_reason = [], False, True, ""
    try:
        identity_condition = (
            {"field_name": "Respondents", "operator": "is", "value": [open_id]}
            if open_id else
            {"field_name": "Respondents", "operator": "is", "value": ["ou_no_open_id_legacy_session"]}
        )
        date_from_condition = {"field_name": "Submitted on Copy", "operator": "isGreater", "value": ["ExactDate", str(padded_from_ms)]}
        date_to_condition = {"field_name": "Submitted on Copy", "operator": "isLess", "value": ["ExactDate", str(padded_to_ms_exclusive)]}

        fast_payload = {
            "page_size": 500,
            "sort": [{"field_name": "Numbering", "desc": True}],
            "filter": {
               "conjunction": "and",
               "conditions": [identity_condition, date_from_condition, date_to_condition]
            },
         }   
        fast_items, fast_complete, fast_reason = _run_search(fast_payload, _deadline)
        # If Feishu rejects the combined filter for a real reason (not just the old
        # 1254018 shape issue -- that's fixed by the string-typed ms value above),
        # fast_reason carries "<code>: <msg>" through to stop_reason rather than
        # silently swallowing it. _in_local_window() below still re-validates
        # everything Feishu does return, so a bad date filter can only ever make
        # results incomplete/slower to discover, never silently wrong.
        fast_ok = True
    except Exception as e:
        fast_ok = False
        fast_reason = str(e)
        logger.warn("my_requests_fast_path_failed", error=str(e))
        fast_items, fast_complete = [], False

    # Local, timezone-exact post-filter -- see the big comment above for why this
    # replaced the Feishu-side ExactDate conditions.
    def _in_local_window(it):
        ms = _extract_epoch_ms(get_field_local(it.get("fields", {}), "Submitted on Copy", "Submitted on", "Created Time"))
        return bool(ms) and from_ms <= ms < to_ms_exclusive
    all_items = [it for it in fast_items if _in_local_window(it)]
    fetch_complete, stop_reason = fast_complete, fast_reason

    # BUG FIX (duplicate cards): the same record could legitimately come back more
    # than once across pages (e.g. a page re-requested after a retry), and nothing
    # downstream ever deduplicated by record_id -- which is exactly the "one
    # request repeated so many times" symptom. Dedup by record_id up front.
    seen_ids, deduped_items = set(), []
    for it in all_items:
        rid = it.get("record_id")
        if rid and rid in seen_ids:
            continue
        if rid:
            seen_ids.add(rid)
        deduped_items.append(it)
    all_items = deduped_items

    results = []
    
    for item in all_items:
        fields = item.get("fields", {})
        
        raw_date = get_field_local(fields, "Submitted on Copy", "Submitted on", "Created Time")
        # Format in the VIEWER'S local timezone (not hardcoded Cairo +3h).
        _ms = _extract_epoch_ms(raw_date)
        if _ms:
            _dt_local = datetime.fromtimestamp(_ms / 1000.0, tz=timezone.utc) + timedelta(minutes=tz_offset_min)
            dt_str = _dt_local.strftime("%Y-%m-%d %H:%M")
            _sort_ts = _ms
        else:
            dt_str = extract_field_text(raw_date)
            _sort_ts = 0

        region = clean(get_field_local(fields, "Region", "Agency Region"))
        acm_pk = clean(get_field_local(fields, "Acm Name (PK)"))
        acm_in = clean(get_field_local(fields, "Acm Name (IN)"))
        acm_fb = clean(get_field_local(fields, "Acm", "Assigned Member"))
        if region in ("", "none"):
            if acm_pk in PK_ACMS or acm_fb in PK_ACMS: region = "pk"
            elif acm_in in IN_ACMS or acm_fb in IN_ACMS: region = "in"
        acm = (acm_in if region == "in" else acm_pk) or acm_fb
        
        respondents = extract_field_text(get_field_local(fields, "Respondents", "Created By"))
        
        results.append({
            "record_id": item.get("record_id"),
            "numbering": extract_field_text(get_field_local(fields, "Numbering")),
            "request_type": extract_field_text(get_field_local(fields, "Request Type", "Type")),
            "submitted_on": dt_str,
            "respondents": respondents,
            "user_id": extract_field_text(get_field_local(fields, "User ID")),
            "agency_code": extract_field_text(get_field_local(fields, "Agency Code")),
            "agency_type": extract_field_text(get_field_local(fields, "Agency Type", "Type of Agency")),
            "otherapp_id": extract_field_text(get_field_local(fields, "Otherapp ID", "Otherapp Name", "Other App Name")),
            "acm": acm.title() if acm else "",
            "region": region.upper() if region else "",
            "bd_code": extract_field_text(get_field_local(fields, "Bd Code", "BD Code")),
            "status": extract_field_text(get_field_local(fields, "Status", "Request Status")),
            "reject_reason": extract_field_text(get_field_local(fields, "Reject Reason", "Rejection Reason")),
            "audition_note": extract_field_text(get_field_local(fields, "Audition note", "Audition Note")),
            "closing_reason": extract_field_text(get_field_local(fields, "Closing Reason", "Closing Agencies Reason")),
            "_sort_ts": _sort_ts,
        })
    
    results.sort(key=lambda r: r["_sort_ts"], reverse=True)
    for r in results: r.pop("_sort_ts", None)
    
    audit.log(user, "MY_REQUESTS_VIEW", f"Fetched {len(results)} records", ip=ip, severity="Info")
    
    return jsonify({
        "results": results,
        "count": len(results),
        "fetch_complete": fetch_complete,
        "stop_reason": ("" if fetch_complete else stop_reason),
        "served_from_background_cache": False,  # always a live Feishu fetch now
        "lookback_days": lookback_days,
        "fast_path_used": fast_ok,  # false only if the single combined search itself threw
    })

@app.route('/api/live-queue', methods=['GET'])
def live_queue():
    user = sanitize_text(request.args.get('user',''))
    
    if not user or MOCK_MODE:
        return jsonify({"success": True, "tickets": []})
        
    tat = get_tenant_access_token()
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    payload = {
        "page_size": 50,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": "Request Status", 
                    "operator": "contains",
                    "value": ["In Progress"] 
                }
            ]
        },
        "sort": [{"field_name": "Numbering", "desc": True}]
    }
    
    try:
        resp = http_requests.post(search_url, headers=headers, json=payload, timeout=5)
        data = resp.json()
        
        if data.get("code") == 1254045:
            payload["filter"]["conditions"][0]["field_name"] = "Status"
            resp = http_requests.post(search_url, headers=headers, json=payload, timeout=5)
            data = resp.json()
            
        if data.get("code") == 0:
            items = data.get("data", {}).get("items", [])
            tickets = []
            user_clean = user.strip().lower()
            for item in items:
                fields = item.get("fields", {})
                assigned_member = extract_field_text(get_field_local(fields, "Assigned Member")).lower()
                if user_clean in assigned_member:
                    tickets.append({
                        "record_id": item.get("record_id"),
                        "numbering": extract_field_text(get_field_local(fields, "Numbering")),
                        "agency_code": extract_field_text(get_field_local(fields, "Agency Code")),
                        "request_type": extract_field_text(get_field_local(fields, "Request Type", "Type")),
                        "user_id": extract_field_text(get_field_local(fields, "User ID")),
                        "assigned_member": extract_field_text(get_field_local(fields, "Assigned Member")),
                    })
            return jsonify({"success": True, "tickets": tickets})
            
        return jsonify({"success": False, "error": data.get("msg")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/new-request/agency-info', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def new_request_agency_info():
    """Lightweight, on-demand lookup for the Create New Request form: given an
    agency code (and optionally a target privilege), returns the agency's
    current point balance and -- for a given privilege -- this month's
    Claimed/Used/Remaining, computed with the exact same logic as the Agency
    Target Table so the numbers a submitter sees here always match what shows
    up there. Built from the same warm, cache-first snapshots the rest of the
    portal already reads (no live per-agency Feishu call), and is only ever
    called when a submitter is actively typing in the form, not on a timer."""
    user = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("submit" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    code = sanitize_agency_code(request.args.get('agency_code', ''))
    privilege = sanitize_text(request.args.get('privilege', ''), 60).strip()
    if not code:
        return jsonify({"error": "agency_code is required."}), 400

    if MOCK_MODE:
        points_items = MockFeishuDB.generate_agency(code)
        requests_items = MockFeishuDB.generate_requests(60) if hasattr(MockFeishuDB, "generate_requests") else []
    else:
        points_items, _pc, _pr, _pcache = get_points_table_snapshot()
        requests_items, _rk, _rc, _rr, _rcache = get_requests_table_snapshot() if privilege else ([], None, None, None, None)

    match = None
    for item in points_items:
        f = item.get("fields", {})
        if extract_field_text(get_field_local(f, "Agency Code")).strip() == str(code).strip():
            match = f
            break
    if not match:
        return jsonify({"found": False})

    total_pts = parse_float_safe(extract_field_text(get_field_local(match, "Total Points", "# Total Points")))
    used_pts = parse_float_safe(extract_field_text(get_field_local(match, "Used Points")))
    balance = parse_float_safe(extract_field_text(get_field_local(match, "Point Balance")))
    if balance == 0 and total_pts > 0:
        balance = total_pts - used_pts

    result = {
        "found": True, "agency_code": code,
        "agency_name": extract_field_text(get_field_local(match, "Agency Name", "Name")) or code,
        "point_balance": balance,
    }

    if privilege:
        priv_type = classify_privilege_type(privilege)
        if priv_type:
            ledger_rows = compute_agency_privilege_ledger(points_items, requests_items, "all", "all", ["all"], ["all"])
            agency_row = next((r for r in ledger_rows if r["agency_code"] == code), None)
            priv_row = next((p for p in (agency_row["privileges"] if agency_row else []) if p["privilege"] == priv_type), None)
            if priv_row:
                result["target"] = priv_row

    return jsonify(result)

@app.route('/api/agency-list', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def agency_list():
    user = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    perms = get_user_permissions(email, user)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    
    if not perms.get("is_super_admin") and not any("query_agency_list" in m or "query" in m for m in perms.get("modules",[])):
        return jsonify({"error":"Access denied"}), 403
    
    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("query",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("query",["all"])
    allowed_acms_set = set(a.lower() for a in allowed_acms) if allowed_acms else {"all"}
    allowed_regs_set = set(r.lower() for r in allowed_regs) if allowed_regs else {"all"}
    
    f_region = sanitize_text(request.args.get('region','')).lower()
    f_agency_code = sanitize_text(request.args.get('agency_code','')).lower()
    f_agency_name = sanitize_text(request.args.get('agency_name','')).lower()
    f_acm = sanitize_text(request.args.get('acm','')).lower()
    f_agency_type = sanitize_text(request.args.get('agency_type','')).lower()
    f_bd_code = sanitize_text(request.args.get('bd_code','')).lower()
    f_parent_agency = sanitize_text(request.args.get('parent_agency','')).lower()
    f_from_str = sanitize_text(request.args.get('from',''))
    f_to_str = sanitize_text(request.args.get('to',''))
    f_from_dt = parse_feishu_date(f_from_str) if f_from_str else None
    f_to_dt = parse_feishu_date(f_to_str) if f_to_str else None
    # Same viewer-local-timezone handling as /api/my-requests, so "Create Time"
    # shows in the person's own timezone rather than a hardcoded one.
    tz_offset_min = max(-720, min(840, _safe_int(request.args.get('tz_offset', ''), 180)))
    
    if MOCK_MODE:
        all_items = MockFeishuDB.generate_requests(300)
        fetch_complete, stop_reason = True, ""
    else:
        all_items, master_keys, fetch_complete, stop_reason, _ = get_requests_table_snapshot(from_dt=None)
    
    results = []
    target_types = ["agency creation", "agency applied already by acm or bd link ( follow-up )", "applied already", "follow-up"]
    
    for item in all_items:
        fields = item.get("fields", {})
        req_type = clean(get_field_local(fields, "Request Type", "Type"))
        
        if not any(t in req_type for t in target_types):
            continue
        
        # BUG FIX (every row showed "00:00" regardless of actual submit time): the
        # field priority here was never the problem -- "Submitted on Copy" already
        # carries a real timestamp, same as it does for My Recent Requests, which
        # displays real hours from this exact field. The bug was the FUNCTION:
        # parse_feishu_date() deliberately zeroes out the time (it's meant for
        # day-level bucketing elsewhere in this file), so using it here for display
        # was always going to show midnight no matter which field fed it. Switched
        # to the same _extract_epoch_ms() + viewer-timezone conversion that
        # /api/my-requests already uses successfully, instead of reinventing it.
        raw_date = get_field_local(fields, "Submitted on Copy", "Submitted on", "Created Time")
        dt = parse_feishu_date(raw_date)  # kept for the day-level date-range filter below
        _ms = _extract_epoch_ms(raw_date)
        if _ms:
            _dt_local = datetime.fromtimestamp(_ms / 1000.0, tz=timezone.utc) + timedelta(minutes=tz_offset_min)
            create_time_str = _dt_local.strftime("%Y-%m-%d %H:%M")
        else:
            create_time_str = extract_field_text(raw_date)
        
        region = clean(get_field_local(fields, "Region", "Agency Region"))
        acm_pk = clean(get_field_local(fields, "Acm Name (PK)"))
        acm_in = clean(get_field_local(fields, "Acm Name (IN)"))
        acm_fb = clean(get_field_local(fields, "Acm", "Assigned Member"))
        if region in ("", "none"):
            if acm_pk in PK_ACMS or acm_fb in PK_ACMS: region = "pk"
            elif acm_in in IN_ACMS or acm_fb in IN_ACMS: region = "in"
        acm = (acm_in if region == "in" else acm_pk) or acm_fb
        
        if "all" not in allowed_acms_set and acm.lower().strip() not in allowed_acms_set: 
            continue
        if "all" not in allowed_regs_set and region not in allowed_regs_set: 
            continue
        
        agency_code = extract_field_text(get_field_local(fields, "Agency Code"))
        # Was falling back to a generic "Name" field, which in this table
        # duplicates the ACM's own name -- every row showed the ACM name
        # twice (once as "Agency Name", once as "ACM Name"). Only ever read
        # the actual "Agency Name" column now.
        agency_name = extract_field_text(get_field_local(fields, "Agency Name"))
        bd_code = extract_field_text(get_field_local(fields, "Bd Code", "BD Code"))
        agency_type = extract_field_text(get_field_local(fields, "Agency Type", "Type of Agency"))
        # Same field priority as the column itself -- see the comment on the
        # results dict below for why "Parent Sub Agency" comes first.
        parent_agency = extract_field_text(get_field_local(fields, "Parent Sub Agency", "Parent-Sub Agency", "Parent/Sub Agency", "Parent Agency", "Parent-Agency ID"))
        
        if f_region and f_region not in region: continue
        if f_agency_code and f_agency_code not in agency_code.lower(): continue
        if f_agency_name and f_agency_name not in agency_name.lower(): continue
        if f_acm and f_acm not in acm.lower(): continue
        if f_agency_type and f_agency_type not in agency_type.lower(): continue
        if f_bd_code and f_bd_code not in bd_code.lower(): continue
        if f_parent_agency and f_parent_agency not in parent_agency.lower(): continue
        if f_from_dt and dt and dt < f_from_dt: continue
        if f_to_dt and dt and dt > f_to_dt.replace(hour=23, minute=59, second=59): continue
        
        manager_raw = extract_field_text(get_field_local(fields, "User ID", "Agency Manager ID", "Manager ID"))
        manager_name = extract_field_text(get_field_local(fields, "Applier real name", "Manager Name", "Agency Manager Name"))
        manager_display = manager_raw + (f" ({manager_name})" if manager_name else "")
        
        results.append({
            "record_id": item.get("record_id", ""),
            "region": region.upper() if region else "",
            "agency_code": agency_code,
            "agency_name": agency_name,
            "agency_manager": manager_display,
            "create_time": create_time_str,
            "agency_type": agency_type,
            "bd_code": bd_code,
            # "Parent Agency" column shows the "Parent Sub Agency" field per
            # request, not the old "Parent Agency"/"Parent-Agency ID" fields --
            # those are kept as trailing fallbacks only in case "Parent Sub
            # Agency" isn't the exact field name in every record.
            "parent_agency": parent_agency,
            "acm_name": acm.title() if acm else "",
            "status": extract_field_text(get_field_local(fields, "Status", "Request Status")),
            "_sort_ts": _ms / 1000.0 if _ms else 0,
        })
    
    results.sort(key=lambda r: r["_sort_ts"], reverse=True)
    for r in results: r.pop("_sort_ts", None)
    
    audit.log(user, "AGENCY_LIST_VIEW", f"Fetched {len(results)} records", ip=ip, severity="Info")
    
    return jsonify({
        "results": results,
        "count": len(results),
        "fetch_complete": fetch_complete,
        "stop_reason": ("" if fetch_complete else stop_reason),
    })

@app.route('/api/analytics', methods=['GET', 'POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def analytics():
    start = time.time()
    body = request.json if request.method == 'POST' else request.args

    user   = sanitize_text(body.get('user',''))
    email  = sanitize_text(body.get('email',''))
    uat    = sanitize_text(body.get('uat',''), max_length=512)
    region = sanitize_text(body.get('region','PK')).strip()
    acm    = sanitize_text(body.get('acm','All')).strip()
    atype  = sanitize_text(body.get('type','All')).strip()
    from_s = sanitize_text(body.get('from',''))
    to_s   = sanitize_text(body.get('to',''))
    cmp_from = sanitize_text(body.get('compare_from',''))
    cmp_to   = sanitize_text(body.get('compare_to',''))
    ip       = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    audit.log(user, "GENERATE_ANALYTICS", f"R:{region}|ACM:{acm}", ip=ip, severity="Info")

    from_dt, to_dt = None, None
    if from_s:
        try: from_dt = datetime.strptime(from_s, "%Y-%m-%d")
        except ValueError: pass
    if to_s:
        try: to_dt = datetime.strptime(to_s, "%Y-%m-%d") + timedelta(days=1)
        except ValueError: pass

    perms = get_user_permissions(email, user)
    # Namespace-safe check (matches frontend hasNs): raw substring matching leaked
    # analytics endpoints to target_analytics holders ("analytics" in "target_analytics").
    if not perms.get("is_super_admin") and not any(m == "analytics" or m.startswith("analytics_") for m in perms.get("modules",[])):
        return jsonify({"error":"Access denied"}), 403

    region_filter = region.lower() if region.lower() != "all" else "all"
    # acm may now be a comma-separated list from the multi-select ACM filter
    # (e.g. "sara,ahmed"), plain "All", or a single name -- all three shapes
    # are handled the same way here, and run_analytics() does the actual
    # membership check against the resulting set.
    acm_parts = [a.strip().lower() for a in acm.split(",") if a.strip()]
    acm_filter = "all" if (not acm_parts or "all" in acm_parts or "all acms" in acm_parts) else set(acm_parts)
    type_filter   = atype.lower() if atype.lower() not in ("all","all types") else "all"

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("analytics",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("analytics",["all"])

    oldest_dt = from_dt
    newest_dt = to_dt
    if cmp_from and cmp_to:
        try:
            cmp_from_dt = datetime.strptime(cmp_from, "%Y-%m-%d")
            cmp_to_dt   = datetime.strptime(cmp_to,   "%Y-%m-%d") + timedelta(days=1)
            if cmp_from_dt and (not oldest_dt or cmp_from_dt < oldest_dt): oldest_dt = cmp_from_dt
            if cmp_to_dt and newest_dt and cmp_to_dt > newest_dt: newest_dt = cmp_to_dt
        except ValueError: pass

    all_items, master_keys, fetch_complete, stop_reason, from_bg_cache = get_requests_table_snapshot(from_dt=oldest_dt)

    stats = run_analytics(all_items, from_dt, to_dt, region_filter, acm_filter, type_filter, allowed_acms, allowed_regs)
    stats["fetch_complete"] = fetch_complete
    stats["stop_reason"]    = stop_reason
    stats["feishu_keys"]    = sorted(list(master_keys))
    stats["served_from_background_cache"] = from_bg_cache

    cmp_stats = None
    if cmp_from and cmp_to:
        try:
            cmp_stats = run_analytics(all_items, cmp_from_dt, cmp_to_dt, region_filter, acm_filter, type_filter, allowed_acms, allowed_regs)
            stats["comparison"] = {
                "from": cmp_from, "to": cmp_to, "kpis": cmp_stats["kpis"],
                "creation_status": cmp_stats["creation_status"], "bd_status": cmp_stats["bd_status"],
                "closing_status": cmp_stats["closing_status"], "acm_performance": cmp_stats["acm_performance"],
                "daily_trend_creation": cmp_stats["daily_trend_creation"], "daily_trend_bd": cmp_stats["daily_trend_bd"],
                "daily_trend_closing": cmp_stats["daily_trend_closing"],
            }
        except Exception as e: stats["comparison_error"] = str(e)

    stats["executive_insights"] = generate_executive_insights(stats, cmp_stats)

    duration_ms = int((time.time() - start) * 1000)
    logger.info("analytics_complete", region=region_filter, acm=acm_filter, rows=stats["scanned_rows"], duration_ms=duration_ms)
    stats["duration_ms"] = duration_ms
    stats["cache_hit"]   = False  

    return jsonify(stats)

@app.route('/api/compare', methods=['GET', 'POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def compare():
    start = time.time()
    body = request.json if request.method == 'POST' else request.args

    user   = sanitize_text(body.get('user',''))
    email  = sanitize_text(body.get('email',''))
    mode   = sanitize_text(body.get('mode','acm')).strip().lower()
    region = sanitize_text(body.get('region','All')).strip()
    rtype  = sanitize_text(body.get('type','All')).strip()
    ip     = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    perms = get_user_permissions(email, user)
    # Namespace-safe check (matches frontend hasNs): raw substring matching leaked
    # analytics endpoints to target_analytics holders ("analytics" in "target_analytics").
    if not perms.get("is_super_admin") and not any(m == "analytics" or m.startswith("analytics_") for m in perms.get("modules",[])):
        return jsonify({"error":"Access denied"}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("analytics",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("analytics",["all"])
    region_filter = region.lower() if region.lower() != "all" else "all"
    type_filter   = rtype.lower() if rtype.lower() not in ("all","all types") else "all"

    def parse_d(s, end=False):
        if not s: return None
        try:
            d = datetime.strptime(s, "%Y-%m-%d")
            return d + timedelta(days=1) if end else d
        except ValueError:
            return None

    groups_spec = []  

    if mode == "period":
        acm = sanitize_text(body.get('acm','All')).strip()
        acm_filter = acm.lower() if acm.lower() not in ("all","all acms") else "all"
        try:
            periods = body.get('periods')
            periods = json.loads(periods) if isinstance(periods, str) else (periods or [])
        except Exception:
            periods = []
        if not periods or len(periods) < 2:
            return jsonify({"error": "Provide at least 2 periods to compare."}), 400
        if len(periods) > 4:
            return jsonify({"error": "Compare up to 4 periods at once."}), 400
        for i, p in enumerate(periods):
            label = sanitize_text(p.get('label') or f"Period {i+1}")
            groups_spec.append((label, parse_d(p.get('from')), parse_d(p.get('to'), end=True), acm_filter))
    else:
        mode = "acm"
        from_dt, to_dt = parse_d(body.get('from')), parse_d(body.get('to'), end=True)
        acms_raw = body.get('acms')
        if isinstance(acms_raw, str):
            acms = [a.strip() for a in acms_raw.split(",") if a.strip()]
        else:
            acms = [a.strip() for a in (acms_raw or []) if a and a.strip()]
        if len(acms) < 2:
            return jsonify({"error": "Provide at least 2 ACMs to compare."}), 400
        if len(acms) > 4:
            return jsonify({"error": "Compare up to 4 ACMs at once."}), 400
        for acm in acms:
            groups_spec.append((acm.title(), from_dt, to_dt, acm.lower()))

    oldest_dt = min([g[1] for g in groups_spec if g[1] is not None], default=None)
    newest_dt = max([g[2] for g in groups_spec if g[2] is not None], default=None)

    all_items, master_keys, fetch_complete, stop_reason, from_bg_cache = get_requests_table_snapshot(from_dt=oldest_dt)

    # Executive YTD analytics (Target whole check) only make sense when comparing
    # ACMs -- the Points table is a live snapshot with no per-period history, so a
    # "Compare Periods" run would show the exact same target/realized numbers in
    # every period column, which is misleading. We only compute it for ACM mode.
    points_snapshot = None
    if mode == "acm":
        if MOCK_MODE:
            points_snapshot = MockFeishuDB.generate_agency("All") * 10
        else:
            points_snapshot, _pts_complete, _pts_reason, _pts_cache = get_points_table_snapshot()

    groups = []
    for label, from_dt, to_dt, acm_filter in groups_spec:
        raw = run_analytics(all_items, from_dt, to_dt, region_filter, acm_filter, type_filter, allowed_acms, allowed_regs)
        shaped = _shape_compare_group(label, raw)
        if points_snapshot is not None:
            exec_metrics = compute_executive_metrics(points_snapshot, acm_filter, region_filter, allowed_acms, allowed_regs)
            shaped["ytd_metrics"]  = exec_metrics["ytd_metrics"]
            shaped["diagnostics"]  = exec_metrics["diagnostics"]
            shaped["is_estimated_trend"] = exec_metrics["is_estimated_trend"]

            # Radar dims: target_fulfillment / point_efficiency / account_health come
            # straight from the points snapshot; creation_momentum / closing_resilience
            # come from the requests-table activity for the SAME group so all 5 sit on
            # one consistent chart.
            shaped["radar_dimensions"] = {
                "target_fulfillment": exec_metrics["radar_raw"]["target_fulfillment"],
                "point_efficiency": exec_metrics["radar_raw"]["point_efficiency"],
                "account_health": exec_metrics["radar_raw"]["account_health"],
                "creation_momentum": raw["kpis"]["creations"],   # normalized to 0-100 across groups below
                "closing_resilience": shaped["closing_efficiency_pct"],
            }

            # Approximate cumulative pacing trend: distribute the group's current
            # total realized points across elapsed months, weighted by that month's
            # share of creation activity (see compute_executive_metrics docstring for
            # why this is an estimate rather than a true point ledger).
            monthly_creations = defaultdict(float)
            for d_str, count in raw["daily_trend_creation"].items():
                try:
                    m_key = datetime.strptime(d_str, "%Y-%m-%d").strftime("%Y-%m")
                except ValueError:
                    continue
                monthly_creations[m_key] += count
            months_sorted = sorted(monthly_creations.keys())
            total_creation_weight = sum(monthly_creations.values()) or 1
            realized = exec_metrics["ytd_metrics"]["realized_points"]
            target   = exec_metrics["ytd_metrics"]["ytd_target"]
            cum_actual = 0.0
            trend = []
            for idx, m_key in enumerate(months_sorted, start=1):
                cum_actual += (monthly_creations[m_key] / total_creation_weight) * realized
                target_pace = round((idx / 12) * 100, 1)
                actual_pace = round((cum_actual / target * 100) if target > 0 else 0, 1)
                trend.append({"month": datetime.strptime(m_key, "%Y-%m").strftime("%b"), "target_pace": target_pace, "actual_pace": actual_pace})
            shaped["cumulative_pacing_trend"] = trend
        else:
            shaped["ytd_metrics"] = None
            shaped["ytd_metrics_note"] = "YTD target analytics are only available when comparing ACMs (the Points table has no per-period history)."
        groups.append(shaped)

    # Normalize creation_momentum to a 0-100 scale relative to the highest creation
    # count among the compared groups, so the radar chart's 5th axis is on the same
    # visual scale as the percentage-based axes.
    if points_snapshot is not None and groups:
        max_creations = max((g["radar_dimensions"]["creation_momentum"] for g in groups), default=0) or 1
        for g in groups:
            g["radar_dimensions"]["creation_momentum"] = round(g["radar_dimensions"]["creation_momentum"] / max_creations * 100, 1)

    audit.log(user, "COMPARE_RUN", f"mode:{mode}|groups:{len(groups)}", ip=ip, severity="Info")
    duration_ms = int((time.time() - start) * 1000)
    return jsonify({
        "mode": mode, "groups": groups,
        "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason),
        "served_from_background_cache": from_bg_cache, "duration_ms": duration_ms,
    })

def _shape_compare_group(label, raw):
    return {
        "label": label,
        "kpis": raw["kpis"],
        "closing_efficiency_pct": round((raw["kpis"]["closings"] / raw["kpis"]["creations"] * 100) if raw["kpis"]["creations"] > 0 else 0, 1),
        "status_mix": {
            "Done": raw["creation_status"]["Done"],
            "Rejected": raw["creation_status"]["Rejected"],
            "Under Investigation": raw["creation_status"]["Under Investigation"]
        },
        "daily_trend": [{"date": d, "creations": raw["daily_trend_creation"].get(d, 0)} for d in sorted(raw["daily_trend_creation"].keys())]
    }

def compute_executive_metrics(points_items, acm_filter, region_filter, allowed_acms, allowed_regs):
    """Aggregates the live Agency Points snapshot (points.json / Feishu tbl6LYUxGi8tlkJH)
    into the executive YTD analytics used by the Advanced Compare Engine's Tier-1 KPI
    cards, Tier-2 charts, and Tier-3 diagnostics table.

    IMPORTANT LIMITATION: the Points table is a live current-state snapshot -- it has
    no per-month history of when points were earned. So `cumulative_pacing_trend` is
    an approximation: it distributes the group's current total realized points across
    the months elapsed so far this year, proportionally to how request *creation*
    activity was distributed (from the Requests table), rather than from real
    month-by-month point ledger entries. This is clearly labeled as an estimate in the
    payload (`is_estimated_trend: true`) so the frontend can caption it accordingly.
    """
    matched = []
    for item in points_items:
        f = item.get("fields", {})
        agency_code = extract_field_text(get_field_local(f, "Agency Code")).strip()
        agency_name = extract_field_text(get_field_local(f, "Agency Name", "Name")).strip()
        acm    = extract_field_text(get_field_local(f, "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member")).strip()
        region = 'PK' if acm.lower() in PK_ACMS else ('IN' if acm.lower() in IN_ACMS else '')

        if "all" not in allowed_acms and acm.lower() not in [a.lower() for a in allowed_acms]: continue
        if "all" not in allowed_regs and region.lower() not in [r.lower() for r in allowed_regs]: continue
        if acm_filter != "all" and acm.lower() != acm_filter: continue
        if region_filter != "all" and region.lower() != region_filter: continue

        total_pts  = parse_float_safe(extract_field_text(get_field_local(f, "Total Points", "# Total Points")))
        used_pts   = parse_float_safe(extract_field_text(get_field_local(f, "Used Points")))
        balance    = parse_float_safe(extract_field_text(get_field_local(f, "Point Balance")))
        _monthly, ytd_target = parse_monthly_target_field(get_field_local(f, "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check"))
        if balance == 0 and total_pts > 0: balance = total_pts - used_pts

        health = 95
        if total_pts > 0:
            utilization = used_pts / total_pts
            if utilization > 0.90: health = 40
            elif utilization > 0.70: health = 70
        elif total_pts == 0:
            health = 0

        realization_pct = round((total_pts / ytd_target * 100) if ytd_target > 0 else 0, 1)
        variance = total_pts - ytd_target
        if ytd_target <= 0: pacing_status = "No Target"
        elif realization_pct >= 100: pacing_status = "On Track"
        elif realization_pct >= 80: pacing_status = "Needs Attention"
        else: pacing_status = "Lagging"

        matched.append({
            "agency_code": agency_code, "agency_name": agency_name or agency_code, "acm": acm,
            "ytd_target": ytd_target, "total_points": total_pts, "used_points": used_pts,
            "point_balance": balance, "health_score": health,
            "realization_pct": realization_pct, "pacing_status": pacing_status, "variance": variance,
        })

    ytd_target_sum  = sum(a["ytd_target"] for a in matched)
    total_points_sum = sum(a["total_points"] for a in matched)
    used_points_sum  = sum(a["used_points"] for a in matched)
    balance_sum       = sum(a["point_balance"] for a in matched)
    avg_health        = round(sum(a["health_score"] for a in matched) / len(matched), 1) if matched else 0

    now = cairo_now()
    day_of_year = now.timetuple().tm_yday
    days_in_year = 366 if (now.year % 4 == 0 and (now.year % 100 != 0 or now.year % 400 == 0)) else 365
    ytd_expected_pct = round(day_of_year / days_in_year * 100, 2)

    fulfillment_pct = round((total_points_sum / ytd_target_sum * 100) if ytd_target_sum > 0 else 0, 1)
    pacing_index    = round((fulfillment_pct / ytd_expected_pct * 100) if ytd_expected_pct > 0 else 0, 1)
    projected_eoy   = round((total_points_sum / day_of_year) * days_in_year, 0) if day_of_year > 0 else 0
    gap_variance    = round(total_points_sum - ytd_target_sum, 0)

    point_efficiency = round((used_points_sum / total_points_sum * 100) if total_points_sum > 0 else 0, 1)

    # Diagnostics table rows, worst-variance-first so the biggest gaps surface first.
    diagnostics = sorted(matched, key=lambda a: a["variance"])[:200]

    return {
        "ytd_metrics": {
            "ytd_target": ytd_target_sum, "realized_points": total_points_sum,
            "used_points": used_points_sum, "point_balance": balance_sum,
            "fulfillment_rate_pct": fulfillment_pct, "pacing_index": pacing_index,
            "projected_eoy": projected_eoy, "gap_variance": gap_variance,
            "ytd_expected_pct": ytd_expected_pct, "agency_count": len(matched),
        },
        "radar_raw": {
            "target_fulfillment": min(100, fulfillment_pct),
            "point_efficiency": min(100, point_efficiency),
            "account_health": avg_health,
        },
        "diagnostics": diagnostics,
        "is_estimated_trend": True,
    }

# ════════════════════════════════════════════════════════════════════
# AGENCY EXCHANGE ("Target Board") — stock-exchange-style board for ACMs: which
# agencies are pacing well against their fixed YTD target, which are lagging.
# Reads ONLY from the public build snapshot (get_points_snapshot_from_public_file,
# defined just above get_points_table_snapshot) -- the same file Search Records
# prefers -- so opening this page never triggers a live Feishu call. No
# Requests-table reads either way. Deliberately has nothing to do with the
# Points/Rewards system (Point Balance, Used Points, allocator) beyond
# displaying it for context -- this is pure target-realization analytics.
# ════════════════════════════════════════════════════════════════════

def compute_exchange_board_rows(points_items, acm_filter, region_filter, allowed_acms, allowed_regs):
    """One row per agency: target realization, pace status, health, and a
    "will they miss their implied full-year target at this rate" flag.
    `realization_pct` is already pace-correct at month granularity --
    parse_monthly_target_field() sums the target only through the current elapsed
    month, so 100% here genuinely means "on pace today", not "on pace by Dec 31".
    All monetary-looking fields here are in raw POINTS -- multiply by 100,000 to
    get XCoins. Conversion happens at display time in the frontend, not here."""
    now = cairo_now()
    elapsed_months = now.month
    year_end = now.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=0)
    days_left_in_year = max(1, (year_end.date() - now.date()).days)
    rows = []
    for item in points_items:
        f = item.get("fields", {})
        agency_code = extract_field_text(get_field_local(f, "Agency Code")).strip()
        if not agency_code:
            continue
        agency_name = extract_field_text(get_field_local(f, "Agency Name", "Name")).strip()
        acm = extract_field_text(get_field_local(f, "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member")).strip()
        region = 'PK' if acm.lower() in PK_ACMS else ('IN' if acm.lower() in IN_ACMS else clean(get_field_local(f, "Region")).upper())

        if "all" not in allowed_acms and acm.lower() not in [a.lower() for a in allowed_acms]: continue
        if "all" not in allowed_regs and region.lower() not in [r.lower() for r in allowed_regs]: continue
        if acm_filter != "all" and acm.lower() != acm_filter: continue
        if region_filter != "all" and region.lower() != region_filter: continue

        total_pts = parse_float_safe(extract_field_text(get_field_local(f, "Total Points", "# Total Points")))
        used_pts  = parse_float_safe(extract_field_text(get_field_local(f, "Used Points")))
        balance   = parse_float_safe(extract_field_text(get_field_local(f, "Point Balance")))
        if balance == 0 and total_pts > 0: balance = total_pts - used_pts
        monthly, ytd_target = parse_monthly_target_field(get_field_local(f, "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check"))

        health_score = 95
        if total_pts > 0:
            utilization = used_pts / total_pts
            if utilization > 0.90: health_score = 40
            elif utilization > 0.70: health_score = 70
        elif total_pts == 0:
            health_score = 0

        realization_pct = round((total_pts / ytd_target * 100) if ytd_target > 0 else 0, 1)
        variance = round(total_pts - ytd_target, 0)

        if ytd_target <= 0:            pacing_status = "No Target"
        elif realization_pct >= 110:   pacing_status = "Ahead"
        elif realization_pct >= 95:    pacing_status = "On Track"
        elif realization_pct >= 80:    pacing_status = "Needs Attention"
        else:                          pacing_status = "Lagging"

        # Simple pace-based projection: NOT a claim about the actual full-year
        # target (future months may not have a target value yet) -- purely "at the
        # rate this agency is accumulating points, where do they land by Dec 31".
        projected_eoy_points = round((total_pts / elapsed_months) * 12, 0) if elapsed_months > 0 else total_pts

        # This calendar month's target slot, if it has one -- lets the board show
        # "what's expected of them right now" without needing any activity data.
        current_month_target = monthly[elapsed_months - 1]["value"] if elapsed_months <= 12 else None
        months_with_target = sum(1 for m in monthly if m["status"] == "value")

        # "Downgrade risk" -- honest linear projection, same idea as compute_executive_metrics
        # elsewhere in this file: extend this agency's average monthly target rate out
        # to a full 12 months and see if their current pace would land short of it.
        # Only computed when we actually have >=1 elapsed month with a real target value;
        # otherwise there's nothing to project against.
        elapsed_value_months = sum(1 for m in monthly[:elapsed_months] if m["status"] == "value")
        implied_full_year_target = round((ytd_target / elapsed_value_months) * 12, 0) if elapsed_value_months > 0 else 0
        will_miss_target = bool(implied_full_year_target > 0 and projected_eoy_points < implied_full_year_target)
        gap_to_full_year = max(0.0, implied_full_year_target - total_pts) if implied_full_year_target > 0 else 0.0
        required_daily_rate = round(gap_to_full_year / days_left_in_year, 2) if gap_to_full_year > 0 else 0.0

        rows.append({
            "agency_code": agency_code, "agency_name": agency_name or agency_code,
            "acm": acm.title(), "region": region.upper(),
            "total_points": total_pts, "used_points": used_pts, "point_balance": balance,
            "ytd_target": ytd_target, "realization_pct": realization_pct, "variance": variance,
            "pacing_status": pacing_status, "health_score": health_score,
            "projected_eoy_points": projected_eoy_points,
            "current_month_target": current_month_target, "months_with_target": months_with_target,
            "implied_full_year_target": implied_full_year_target, "will_miss_target": will_miss_target,
            "gap_to_full_year": round(gap_to_full_year, 0), "required_daily_rate": required_daily_rate,
        })
    return rows

def _exchange_row_matches_status(row, status):
    status = (status or "").lower()
    if status in ("", "all"):        return True
    if status == "at_risk":          return row["pacing_status"] in ("Needs Attention", "Lagging")
    if status == "ahead":            return row["pacing_status"] == "Ahead"
    if status == "on_track":         return row["pacing_status"] == "On Track"
    if status == "lagging":          return row["pacing_status"] == "Lagging"
    if status == "needs_attention":  return row["pacing_status"] == "Needs Attention"
    if status == "no_target":        return row["pacing_status"] == "No Target"
    if status == "downgrade_risk":   return row["will_miss_target"]
    return True

def build_exchange_kpi_strip(rows):
    n = len(rows)
    with_target = [r for r in rows if r["ytd_target"] > 0]
    avg_realization = round(sum(r["realization_pct"] for r in with_target) / len(with_target), 1) if with_target else 0
    return {
        "agency_count": n,
        "avg_realization_pct": avg_realization,
        "total_target": round(sum(r["ytd_target"] for r in rows), 0),
        "total_points": round(sum(r["total_points"] for r in rows), 0),
        "total_balance": round(sum(r["point_balance"] for r in rows), 0),
        "count_ahead":         sum(1 for r in rows if r["pacing_status"] == "Ahead"),
        "count_on_track":      sum(1 for r in rows if r["pacing_status"] == "On Track"),
        "count_needs_attention": sum(1 for r in rows if r["pacing_status"] == "Needs Attention"),
        "count_lagging":       sum(1 for r in rows if r["pacing_status"] == "Lagging"),
        "count_no_target":     sum(1 for r in rows if r["pacing_status"] == "No Target"),
        "count_at_risk":       sum(1 for r in rows if r["pacing_status"] in ("Needs Attention", "Lagging")),
        "count_will_miss_target": sum(1 for r in rows if r["will_miss_target"]),
    }

def build_exchange_region_rollup(rows):
    out = {}
    for reg in ("PK", "IN"):
        sub = [r for r in rows if r["region"] == reg]
        wt = [r for r in sub if r["ytd_target"] > 0]
        out[reg] = {
            "agency_count": len(sub),
            "avg_realization_pct": round(sum(r["realization_pct"] for r in wt) / len(wt), 1) if wt else 0,
            "count_at_risk": sum(1 for r in sub if r["pacing_status"] in ("Needs Attention", "Lagging")),
            "total_points": round(sum(r["total_points"] for r in sub), 0),
            "total_target": round(sum(r["ytd_target"] for r in sub), 0),
        }
    return out

def build_exchange_needs_push(rows, limit=8):
    """Worst-first list of agencies below pace, with the raw gap to their
    pace-adjusted target and the daily rate (in points -- frontend converts to
    XCoins) they'd need to sustain to still land on their implied full-year
    target by Dec 31. Pure arithmetic over fields already on each row."""
    candidates = [r for r in rows if r["pacing_status"] in ("Lagging", "Needs Attention") and r["ytd_target"] > 0]
    candidates.sort(key=lambda r: r["variance"])
    out = []
    for r in candidates[:limit]:
        out.append({
            "agency_code": r["agency_code"], "agency_name": r["agency_name"],
            "acm": r["acm"], "region": r["region"], "realization_pct": r["realization_pct"],
            "gap_now": abs(round(r["variance"], 0)), "required_daily_rate": r["required_daily_rate"],
            "will_miss_target": r["will_miss_target"],
        })
    return out

def build_exchange_sparklines(rows, max_days=7):
    """Cheap 'weekly trend' per agency for the board's mini sparklines. Reuses the
    same Redis daily-snapshot history the Movers panel already maintains -- reads
    at most `max_days` extra keys total (not per agency), and returns {} entirely
    if Redis isn't configured or there isn't history yet. No Feishu, no
    Requests-table, nothing beyond a handful of small Redis GETs."""
    if not REDIS_ENABLED:
        return {}
    idx = redis_get_json(REDIS_KEY_EXCHANGE_HISTORY_INDEX) or []
    if len(idx) < 2:
        return {}
    recent_dates = idx[-max_days:]
    series = {}
    for d in recent_dates:
        snap = redis_get_json(f"{REDIS_KEY_EXCHANGE_HISTORY_PREFIX}{d}") or {}
        for code, val in snap.items():
            series.setdefault(code, []).append(val)
    return series

def generate_exchange_insights(rows, region_rollup, movers):
    """Plain-English rules engine over numbers already on the board -- no ML, no
    invented signals, no Requests-table reads. Reads straight off realization_pct /
    variance / pacing_status (Points-table-derived) plus the Redis-backed Movers
    diff (also Points-table-derived, no extra cost)."""
    insights = []
    with_target = [r for r in rows if r["ytd_target"] > 0]

    lagging = sorted([r for r in with_target if r["pacing_status"] in ("Lagging", "Needs Attention")], key=lambda r: r["variance"])
    if lagging:
        top = lagging[0]
        gap_pct = round(100 - top["realization_pct"], 1)
        insights.append({"type": "lagging", "severity": "warning",
            "text": f"{top['agency_name']} is {gap_pct}% behind pace-adjusted target — the largest gap on the board."})
        if len(lagging) > 1:
            names = ", ".join(r["agency_name"] for r in lagging[1:3])
            insights.append({"type": "lagging_more", "severity": "warning",
                "text": f"Also trailing pace: {names}."})

    ahead = [r for r in with_target if r["realization_pct"] >= 100]
    if ahead:
        best = max(ahead, key=lambda r: r["realization_pct"])
        insights.append({"type": "ahead", "severity": "success",
            "text": f"{best['agency_name']} is at {best['realization_pct']}% of pace-adjusted target — leading the board."})

    no_target = [r for r in rows if r["pacing_status"] == "No Target"]
    if no_target:
        insights.append({"type": "no_target", "severity": "info",
            "text": f"{len(no_target)} agenc{'y has' if len(no_target)==1 else 'ies have'} no target set yet, so they're excluded from realization averages."})

    at_risk_downgrade = [r for r in with_target if r["will_miss_target"]]
    if at_risk_downgrade:
        names = ", ".join(r["agency_name"] for r in at_risk_downgrade[:3])
        extra = f" and {len(at_risk_downgrade)-3} more" if len(at_risk_downgrade) > 3 else ""
        insights.append({"type": "downgrade_risk", "severity": "warning",
            "text": f"{len(at_risk_downgrade)} agenc{'y is' if len(at_risk_downgrade)==1 else 'ies are'} projected to miss their implied full-year target at the current rate: {names}{extra}."})

    if movers and movers.get("available"):
        if movers.get("decliners"):
            d = movers["decliners"][0]
            insights.append({"type": "decline", "severity": "warning",
                "text": f"{d['agency_name']} dropped {abs(round(d['delta'])):,.0f} points since {movers['since_date']} — worth a check-in."})
        if movers.get("gainers"):
            g = movers["gainers"][0]
            insights.append({"type": "gain", "severity": "success",
                "text": f"{g['agency_name']} gained {round(g['delta']):,.0f} points since {movers['since_date']} — biggest mover this week."})

    pk, ind = region_rollup.get("PK", {}), region_rollup.get("IN", {})
    if pk.get("agency_count") and ind.get("agency_count"):
        pk_avg, in_avg = pk["avg_realization_pct"], ind["avg_realization_pct"]
        if abs(pk_avg - in_avg) >= 5:
            leader, laggard, lead_val, lag_val = ("PK", "IN", pk_avg, in_avg) if pk_avg > in_avg else ("IN", "PK", in_avg, pk_avg)
            insights.append({"type": "region", "severity": "info",
                "text": f"Region {leader} is pacing ahead of {laggard} this period ({lead_val}% vs {lag_val}% avg realization)."})

    if not insights:
        insights.append({"type": "stable", "severity": "info", "text": "No major flags right now — the board looks stable."})
    return insights

def append_exchange_daily_snapshot(points_items):
    """Appends one compact {agency_code: total_points} row to Redis, once per
    calendar day (Cairo time), so the Movers panel has real history to diff
    against instead of guessing. No-ops if Redis isn't configured or if today's
    snapshot was already taken (checked via a single cheap GET)."""
    if not REDIS_ENABLED:
        return
    today_str = cairo_now().strftime("%Y-%m-%d")
    last = redis_cmd("GET", REDIS_KEY_EXCHANGE_LAST_SNAPSHOT)
    if last == today_str:
        return
    snap = {}
    for item in points_items:
        f = item.get("fields", {})
        code = extract_field_text(get_field_local(f, "Agency Code")).strip()
        if not code: continue
        snap[code] = parse_float_safe(extract_field_text(get_field_local(f, "Total Points", "# Total Points")))
    if not snap:
        return
    if redis_set_json(f"{REDIS_KEY_EXCHANGE_HISTORY_PREFIX}{today_str}", snap, ttl=EXCHANGE_HISTORY_MAX_DAYS * 86400):
        idx = redis_get_json(REDIS_KEY_EXCHANGE_HISTORY_INDEX) or []
        idx = sorted(set(idx + [today_str]))[-EXCHANGE_HISTORY_MAX_DAYS:]
        redis_set_json(REDIS_KEY_EXCHANGE_HISTORY_INDEX, idx, ttl=EXCHANGE_HISTORY_MAX_DAYS * 86400)
        redis_cmd("SET", REDIS_KEY_EXCHANGE_LAST_SNAPSHOT, today_str)

def compute_exchange_movers(rows, lookback_days=7):
    """Diffs today's board against the oldest snapshot within lookback_days to
    find real gainers/decliners. Honestly reports when there isn't enough history
    yet instead of fabricating trend data."""
    if not REDIS_ENABLED:
        return {"available": False, "reason": "History tracking requires Redis, which isn't configured on this deployment."}
    idx = redis_get_json(REDIS_KEY_EXCHANGE_HISTORY_INDEX) or []
    if len(idx) < 2:
        return {"available": False, "reason": "Building history — check back in a day or two for movers."}

    cutoff_str = (cairo_now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    candidates = [d for d in idx if d >= cutoff_str]
    old_date = min(candidates) if candidates else min(idx)
    if old_date == max(idx):
        return {"available": False, "reason": "Building history — check back in a day or two for movers."}

    old_snap = redis_get_json(f"{REDIS_KEY_EXCHANGE_HISTORY_PREFIX}{old_date}") or {}
    movers = []
    for row in rows:
        old_val = old_snap.get(row["agency_code"])
        if old_val is None:
            continue
        delta = row["total_points"] - old_val
        pct = round((delta / old_val * 100), 1) if old_val > 0 else (100.0 if delta > 0 else 0.0)
        if delta == 0:
            continue
        movers.append({"agency_code": row["agency_code"], "agency_name": row["agency_name"],
                        "acm": row["acm"], "delta": round(delta, 0), "pct_change": pct,
                        "total_points": row["total_points"]})

    gainers   = sorted([m for m in movers if m["delta"] > 0], key=lambda m: -m["delta"])[:5]
    decliners = sorted([m for m in movers if m["delta"] < 0], key=lambda m: m["delta"])[:5]
    return {"available": True, "since_date": old_date, "gainers": gainers, "decliners": decliners}

def _watchlist_key(user):
    return f"{REDIS_KEY_WATCHLIST_PREFIX}{sanitize_text(user).strip().lower()}"

_watchlist_memory_fallback = {}

def get_exchange_watchlist(user):
    if not user: return []
    key = _watchlist_key(user)
    if REDIS_ENABLED:
        return redis_get_json(key) or []
    return _watchlist_memory_fallback.get(key, [])

def set_exchange_watchlist(user, codes):
    key = _watchlist_key(user)
    codes = list(dict.fromkeys([sanitize_agency_code(c) for c in codes if c]))[:150]
    if REDIS_ENABLED:
        redis_set_json(key, codes)
    else:
        _watchlist_memory_fallback[key] = codes
    return codes

@app.route('/api/exchange/board', methods=['GET', 'POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def exchange_board():
    start = time.time()
    body = request.json if request.method == 'POST' else request.args

    user   = sanitize_text(body.get('user', ''))
    email  = sanitize_text(body.get('email', ''))
    scope  = sanitize_text(body.get('scope', 'mine')).strip().lower()   # 'mine' | 'all'
    region = sanitize_text(body.get('region', 'All')).strip()
    status = sanitize_text(body.get('status', 'all')).strip().lower()
    search_q = sanitize_text(body.get('q', '')).strip().lower()
    sort_by  = sanitize_text(body.get('sort', 'variance')).strip().lower()
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    perms = get_user_permissions(email, user)
    # Namespace-safe check (matches frontend hasNs): raw substring matching leaked
    # these endpoints to target_analytics / unrelated-module holders.
    if not perms.get("is_super_admin") and not any(m == "analytics" or m.startswith("analytics_") or m == "exchange" or m.startswith("exchange_") for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    allowed_acms = perms.get("permissions", {}).get("acms", {}).get("analytics", ["all"])
    allowed_regs = perms.get("permissions", {}).get("regions", {}).get("analytics", ["all"])
    region_filter = region.lower() if region.lower() != "all" else "all"

    acm_filter = "all"
    if scope != "all" and not perms.get("is_super_admin"):
        acm_filter = user.strip().lower()

    if MOCK_MODE:
        points_items = []
        for i in range(30):
            rec = MockFeishuDB.generate_agency(str(40100 + i))[0]
            rec["fields"]["Target whole check"] = " | ".join(str(random.randint(200, 2000)) for _ in range(cairo_now().month)) + " | N/A" * (12 - cairo_now().month)
            points_items.append(rec)
        data_source, stop_reason = "mock", ""
    else:
        # Target Board reads ONLY from the public build file (same source Search
        # Records prefers) -- no live Feishu calls from this page, ever. If the
        # cron that regenerates it has stalled, we say so instead of silently
        # falling back to a live fetch.
        points_items, _complete, stop_reason = get_points_snapshot_from_public_file()
        if points_items is None:
            return jsonify({"error": stop_reason}), 503
        data_source = "public_file"

    rows = compute_exchange_board_rows(points_items, acm_filter, region_filter, allowed_acms, allowed_regs)

    if status != "all":
        rows = [r for r in rows if _exchange_row_matches_status(r, status)]
    if search_q:
        rows = [r for r in rows if search_q in r["agency_name"].lower() or search_q in r["agency_code"].lower() or search_q in r["acm"].lower()]

    sort_map = {
        "variance":    lambda r: r["variance"],
        "realization": lambda r: -r["realization_pct"],
        "balance":     lambda r: -r["point_balance"],
        "name":        lambda r: r["agency_name"].lower(),
    }
    rows.sort(key=sort_map.get(sort_by, sort_map["variance"]))

    kpi_strip = build_exchange_kpi_strip(rows)
    region_rollup = build_exchange_region_rollup(rows)
    needs_push = build_exchange_needs_push(rows)

    if not MOCK_MODE:
        try: append_exchange_daily_snapshot(points_items)
        except Exception as e: logger.warn("exchange_snapshot_failed", error=str(e))
    movers = compute_exchange_movers(rows)
    sparklines = build_exchange_sparklines(rows)
    for r in rows:
        r["trend_7d"] = sparklines.get(r["agency_code"], [])
    insights = generate_exchange_insights(rows, region_rollup, movers)
    watchlist = get_exchange_watchlist(user)

    audit.log(user, "EXCHANGE_BOARD_VIEW", f"scope:{scope}|rows:{len(rows)}", ip=ip, severity="Info")
    duration_ms = int((time.time() - start) * 1000)
    return jsonify({
        "rows": rows, "kpi_strip": kpi_strip, "region_rollup": region_rollup,
        "insights": insights, "movers": movers, "watchlist": watchlist, "needs_push": needs_push,
        "scope": scope, "is_super_admin": perms.get("is_super_admin", False),
        "generated_at": cairo_now().isoformat(), "duration_ms": duration_ms,
        "data_source": data_source, "stop_reason": stop_reason,
    })

@app.route('/api/exchange/agency/<code>', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def exchange_agency_detail(code):
    user  = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    perms = get_user_permissions(email, user)
    # Namespace-safe check (matches frontend hasNs): raw substring matching leaked
    # these endpoints to target_analytics / unrelated-module holders.
    if not perms.get("is_super_admin") and not any(m == "analytics" or m.startswith("analytics_") or m == "exchange" or m.startswith("exchange_") for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    allowed_acms = perms.get("permissions", {}).get("acms", {}).get("analytics", ["all"])
    allowed_regs = perms.get("permissions", {}).get("regions", {}).get("analytics", ["all"])
    code = sanitize_agency_code(code)

    if MOCK_MODE:
        points_items = MockFeishuDB.generate_agency(code)
    else:
        # Same file-only source as the board -- no live Feishu calls.
        points_items, _c, _r = get_points_snapshot_from_public_file()
        if points_items is None:
            return jsonify({"error": _r}), 503

    match = None
    for item in points_items:
        f = item.get("fields", {})
        if extract_field_text(get_field_local(f, "Agency Code")).strip() == str(code).strip():
            match = f
            break
    if not match:
        return jsonify({"error": f"Agency {code} not found in the current snapshot."}), 404

    acm = extract_field_text(get_field_local(match, "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member")).strip()
    region = 'PK' if acm.lower() in PK_ACMS else ('IN' if acm.lower() in IN_ACMS else clean(get_field_local(match, "Region")).upper())

    if allowed_acms and "all" not in allowed_acms and acm.lower() not in [a.lower() for a in allowed_acms]:
        return jsonify({"error": "Access denied for this ACM."}), 403
    if allowed_regs and "all" not in allowed_regs and region.lower() not in [r.lower() for r in allowed_regs]:
        return jsonify({"error": "Access denied for this region."}), 403

    total_pts = parse_float_safe(extract_field_text(get_field_local(match, "Total Points", "# Total Points")))
    used_pts  = parse_float_safe(extract_field_text(get_field_local(match, "Used Points")))
    balance   = parse_float_safe(extract_field_text(get_field_local(match, "Point Balance")))
    if balance == 0 and total_pts > 0: balance = total_pts - used_pts
    monthly_target, ytd_target = parse_monthly_target_field(get_field_local(match, "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check"))
    realization_pct = round((total_pts / ytd_target * 100) if ytd_target > 0 else 0, 1)

    elapsed_months = cairo_now().month
    projected_eoy_points = round((total_pts / elapsed_months) * 12, 0) if elapsed_months > 0 else total_pts
    current_month_target = monthly_target[elapsed_months - 1]["value"] if elapsed_months <= 12 else None

    health_score, health_status = 95, "Healthy"
    if total_pts > 0:
        util = used_pts / total_pts
        if util > 0.90: health_score, health_status = 40, "Critical"
        elif util > 0.70: health_score, health_status = 70, "At Risk"
    elif total_pts == 0:
        health_score, health_status = 0, "Inactive"

    audit.log(user, "EXCHANGE_AGENCY_VIEW", code, ip=request.headers.get("X-Forwarded-For", ""), severity="Info")
    return jsonify({
        "agency_code": code,
        "agency_name": extract_field_text(get_field_local(match, "Agency Name", "Name")) or code,
        "acm": acm.title(), "region": region.upper(),
        "total_points": total_pts, "used_points": used_pts, "point_balance": balance,
        "ytd_target": ytd_target, "monthly_target": monthly_target, "current_month_target": current_month_target,
        "realization_pct": realization_pct, "health_score": health_score, "health_status": health_status,
        "projected_eoy_points": projected_eoy_points,
    })

def privilege_total_earned_quota(privilege_name, base_points):
    """Reimplements the Feishu formula field's tier logic in Python, so the
    Agency Target Table can compute it over the already-cached bulk snapshot
    instead of depending on a specific pre-computed Feishu formula field name
    (which this codebase can't verify exists under a known field name) or,
    worse, a live per-agency lookup. Tiers per privilege type, based on the
    agency's Base Points."""
    p = (privilege_name or "").lower()
    bp = base_points or 0
    if "dynamic avatar" in p:
        return int(bp // 100)
    if "20 mic" in p:
        return 1 if bp >= 150 else 0
    if "30 mic" in p:
        if bp >= 1000: return 4
        if bp >= 500: return 2
        if bp >= 300: return 1
        return 0
    if "new user welcome room" in p:
        return 1 if bp >= 20 else 0
    if "game room" in p:
        return 1 if bp >= 20 else 0
    return 0

# Canonical privilege buckets the table always reports on, in display order --
# matches the tiers privilege_total_earned_quota() knows how to price. Any raw
# privilege string from the Requests table gets classified into one of these
# (or dropped, if it doesn't match anything the ledger prices).
PRIVILEGE_TYPE_ORDER = ["Dynamic Avatar", "30 Mic", "20 Mic", "New User Welcome Room", "Game Room"]

def classify_privilege_type(privilege_name):
    """Maps a raw privilege string from the Requests table (e.g. '30 MIC 15 days
    ( 150 - 175 for others )') onto one of PRIVILEGE_TYPE_ORDER, reusing the same
    substring rules privilege_total_earned_quota() prices by, so a claim always
    lands under the same bucket it was quoted a quota against."""
    p = (privilege_name or "").lower()
    if "dynamic avatar" in p: return "Dynamic Avatar"
    if "20 mic" in p: return "20 Mic"
    if "30 mic" in p: return "30 Mic"
    if "new user welcome room" in p or "welcome room" in p: return "New User Welcome Room"
    if "game room" in p: return "Game Room"
    return None

def compute_agency_privilege_ledger(points_items, requests_items, acm_filter, region_filter, allowed_acms, allowed_regs):
    """Per-agency, per-privilege ledger for the CURRENT calendar month: Claimed
    quota (from Base Points tiers), Used / Pending / Rejected (tallied from the
    Requests snapshot), and Remaining = Claimed - Used. Returns one row for
    EVERY agency in the Points snapshot -- not just agencies with a claim this
    month -- each carrying all five canonical privilege buckets (zeroed out if
    unused), so the table reads as a full roster rather than an activity feed.
    Built entirely from the two bulk snapshots the rest of the portal already
    keeps warm (Points table + Requests table via get_requests_table_snapshot(),
    the same cache-first source Compare/Analytics already read) -- no live
    per-agency Feishu calls, same cost model as those existing pages."""
    now = cairo_now()
    cur_month, cur_year = now.month, now.year

    base_points_map, agency_meta = {}, {}
    for item in points_items:
        f = item.get("fields", {})
        code = extract_field_text(get_field_local(f, "Agency Code")).strip()
        if not code:
            continue
        acm = extract_field_text(get_field_local(f, "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member")).strip()
        region = 'PK' if acm.lower() in PK_ACMS else ('IN' if acm.lower() in IN_ACMS else clean(get_field_local(f, "Region")).upper())
        if "all" not in allowed_acms and acm.lower() not in [a.lower() for a in allowed_acms]: continue
        if "all" not in allowed_regs and region.lower() not in [r.lower() for r in allowed_regs]: continue
        if acm_filter != "all" and acm.lower() != acm_filter: continue
        if region_filter != "all" and region.lower() != region_filter: continue
        base_points_map[code] = parse_float_safe(extract_field_text(get_field_local(f, "Base Points")))
        agency_meta[code] = {
            "agency_name": extract_field_text(get_field_local(f, "Agency Name", "Name")) or code,
            "acm": acm.title(), "region": region.upper(),
        }

    ledger = defaultdict(lambda: {"used": 0, "pending": 0, "rejected": 0})
    for item in requests_items:
        f = item.get("fields", {})
        req_type = extract_field_text(get_field_local(f, "Request Type")).strip().lower()
        if "target" not in req_type:
            continue
        code = extract_field_text(get_field_local(f, "Agency Code")).strip()
        if code not in base_points_map:
            continue
        d = parse_feishu_date(get_field_local(f, "Submitted on Copy", "Submitted on", "Created Time"))
        if not d or d.month != cur_month or d.year != cur_year:
            continue
        raw_privilege = extract_field_text(get_field_local(f, "Agency Point Privilege", "Privilege", "Agency Privilege")).strip()
        priv_type = classify_privilege_type(raw_privilege)
        if not priv_type:
            continue
        status = extract_field_text(get_field_local(f, "Status")).strip().lower()
        raw_counter = extract_field_text(get_field_local(f, "Counter", "Qty", "Quantities Input")).strip()
        qty = 1
        if raw_counter:
            m = re.search(r'\d+', raw_counter)
            if m: qty = int(m.group())

        key = (code, priv_type)
        if any(ok in status for ok in ("done", "complet", "approv", "confirm")):
            ledger[key]["used"] += qty
        elif any(rej in status for rej in ("reject", "fail", "decline")):
            ledger[key]["rejected"] += qty
        else:
            ledger[key]["pending"] += qty

    rows = []
    for code, meta in agency_meta.items():
        base_pts = base_points_map.get(code, 0)
        privileges = []
        for priv_type in PRIVILEGE_TYPE_ORDER:
            counts = ledger.get((code, priv_type), {"used": 0, "pending": 0, "rejected": 0})
            claimed = privilege_total_earned_quota(priv_type, base_pts)
            used = counts["used"]
            privileges.append({
                "privilege": priv_type, "claimed": claimed,
                "used": used, "remaining": max(0, claimed - used),
                "pending": counts["pending"], "rejected": counts["rejected"],
            })
        rows.append({
            "agency_code": code, "agency_name": meta["agency_name"],
            "acm": meta["acm"], "region": meta["region"],
            "base_points": base_pts,
            "privileges": privileges,
            "total_used": sum(p["used"] for p in privileges),
            "total_pending": sum(p["pending"] for p in privileges),
            "total_rejected": sum(p["rejected"] for p in privileges),
        })
    return rows

def compute_single_agency_target_ledger(points_items, requests_items, code):
    """Phase 10.27 (Ahmed): ONE agency's target-privilege ledger -- the exact
    same math as compute_agency_privilege_ledger (same month window, same
    tier quotas, same status buckets) but short-circuited to a single Agency
    Code, so the ticket workspace / New-Ticket draft can recompute the
    sheet's "Formula" field LIVE on every edit instead of waiting for a save
    + Feishu recalculation. Reads only the two warm bulk snapshots -- no live
    Feishu calls, same zero-cost model as Check Agency Target itself."""
    code = (code or "").strip()
    if not code:
        return None

    base_pts, meta = 0, None
    for item in points_items:
        f = item.get("fields", {})
        if extract_field_text(get_field_local(f, "Agency Code")).strip() != code:
            continue
        acm = extract_field_text(get_field_local(f, "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member")).strip()
        region = 'PK' if acm.lower() in PK_ACMS else ('IN' if acm.lower() in IN_ACMS else clean(get_field_local(f, "Region")).upper())
        base_pts = parse_float_safe(extract_field_text(get_field_local(f, "Base Points")))
        meta = {
            "agency_name": extract_field_text(get_field_local(f, "Agency Name", "Name")) or code,
            "acm": acm.title(), "region": region.upper(),
        }
        break
    if meta is None:
        return None

    now = cairo_now()
    cur_month, cur_year = now.month, now.year
    counts = defaultdict(lambda: {"used": 0, "pending": 0, "rejected": 0})
    for item in requests_items:
        f = item.get("fields", {})
        req_type = extract_field_text(get_field_local(f, "Request Type")).strip().lower()
        if "target" not in req_type:
            continue
        if extract_field_text(get_field_local(f, "Agency Code")).strip() != code:
            continue
        d = parse_feishu_date(get_field_local(f, "Submitted on Copy", "Submitted on", "Created Time"))
        if not d or d.month != cur_month or d.year != cur_year:
            continue
        priv_type = classify_privilege_type(extract_field_text(get_field_local(f, "Agency Point Privilege", "Privilege", "Agency Privilege")).strip())
        if not priv_type:
            continue
        status = extract_field_text(get_field_local(f, "Status")).strip().lower()
        raw_counter = extract_field_text(get_field_local(f, "Counter", "Qty", "Quantities Input")).strip()
        qty = 1
        if raw_counter:
            m = re.search(r'\d+', raw_counter)
            if m: qty = int(m.group())
        if any(ok in status for ok in ("done", "complet", "approv", "confirm")):
            counts[priv_type]["used"] += qty
        elif any(rej in status for rej in ("reject", "fail", "decline")):
            counts[priv_type]["rejected"] += qty
        else:
            counts[priv_type]["pending"] += qty

    privileges = []
    for priv_type in PRIVILEGE_TYPE_ORDER:
        c = counts.get(priv_type, {"used": 0, "pending": 0, "rejected": 0})
        claimed = privilege_total_earned_quota(priv_type, base_pts)
        privileges.append({
            "privilege": priv_type, "claimed": claimed,
            "used": c["used"], "remaining": max(0, claimed - c["used"]),
            "pending": c["pending"], "rejected": c["rejected"],
        })
    return {
        "agency_code": code, "agency_name": meta["agency_name"],
        "acm": meta["acm"], "region": meta["region"],
        "base_points": base_pts, "privileges": privileges,
    }

@app.route('/api/agency-target/formula', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def agency_target_formula():
    """Phase 10.27 (Ahmed): LIVE recompute of the sheet's target "Formula"
    for one Agency Code -- "i edit but it still show the wrong code and
    didn't update the FORMULA ... we can use the same logic of check agency
    target". Exactly that: the same ledger logic as the Check Agency Target
    page, over the same warm snapshots, scoped to one agency so it answers
    in milliseconds. The workspace calls this on open + on every Agency Code
    / Privilege edit (debounced), and the New-Ticket draft uses it to fill
    the Formula card before the record even exists."""
    user = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    perms = get_user_permissions(email, user)
    # A workspace tool, not a data page: any module holder (or super admin)
    # may recompute -- the payload is one agency's quota counts, the same
    # numbers the sheet formula already shows that agent on the ticket.
    if not perms.get("is_super_admin") and not perms.get("modules"):
        return jsonify({"error": "Access denied"}), 403

    code = sanitize_text(request.args.get('code', '')).strip()
    if not code:
        return jsonify({"success": False, "error": "Missing code"}), 400

    # Optional: the privilege picked in the workspace form. The frontend shows
    # the sheet's Formula chips for THAT privilege, so the ledger row the
    # agent is editing is returned as "selected" (matched via the same
    # classify_privilege_type used everywhere else).
    priv_raw = sanitize_text(request.args.get('privilege', '')).strip()
    priv_type = classify_privilege_type(priv_raw) if priv_raw else None

    def _with_selected(row_dict):
        sel = None
        if priv_type:
            sel = next((p for p in row_dict.get("privileges", [])
                        if p.get("privilege") == priv_type), None)
        return {**row_dict, "selected": sel}

    if MOCK_MODE:
        row = {"agency_code": code, "agency_name": f"Agency {code}", "acm": "Ehtisham",
               "region": "PK", "base_points": 350,
               "privileges": [{"privilege": p, "claimed": 1, "used": 0, "remaining": 1,
                               "pending": 0, "rejected": 0} for p in PRIVILEGE_TYPE_ORDER]}
        return jsonify({"success": True, "found": True, **_with_selected(row),
                        "generated_at": cairo_now().isoformat()})

    points_items, _pc, _pr, _pcache = get_points_table_snapshot()
    requests_items, _keys, _rc, _rr, _rcache = get_requests_table_snapshot()
    row = compute_single_agency_target_ledger(points_items, requests_items, code)
    if row is None:
        return jsonify({"success": True, "found": False, "agency_code": code,
                        "selected": None,
                        "generated_at": cairo_now().isoformat()})
    return jsonify({"success": True, "found": True, **_with_selected(row),
                    "generated_at": cairo_now().isoformat()})

# ─── Phase 10.30 (Ahmed): LIVE per-agency formula ────────────────────────
# "find way to be live not 3 min late bcs maybe same agency has request one
# min and get it and then they submit request again and i was need to edit
# the agency code so it will show me still 1 left while it already taken and
# the second one should be rejected ... maybe cost one request to be". His
# race is real: the snapshot loop refreshes every 3 min, so two tickets for
# the same agency inside one window can both look valid. The fix is NOT a
# faster loop -- it is a TARGETED live read: for ONE agency code the whole
# ledger needs just 2 Feishu calls (the wallet row + this agency's request
# rows, fired in parallel), which is ~1-2s and a tiny quota cost paid only
# when an agent is actually looking at that agency's Formula. The snapshot
# endpoint stays as the instant first paint; this endpoint is the
# authoritative second paint.

def _live_agency_points_row(code, headers):
    """Live wallet-row lookup for ONE agency -- same contains + exact-match
    pattern as the charge path's _fetch_wallet."""
    r = feishu_session.post(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search",
        headers=headers,
        json={"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "contains", "value": [code]}]}},
        timeout=30).json()
    if r.get("code") != 0:
        raise RuntimeError(r.get("msg") or "Agency Points lookup failed.")
    for it in r.get("data", {}).get("items", []):
        if extract_field_text(get_field_local(it.get("fields", {}), "Agency Code")).strip() == code:
            return it
    return None


def _live_agency_requests_rows(code, headers, cap=500):
    """Live Requests-table rows for ONE agency (paginated, capped)."""
    items, token = [], None
    while True:
        payload = {"page_size": 100,
                   "filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "contains", "value": [code]}]}}
        if token:
            payload["page_token"] = token
        r = feishu_session.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true",
            headers=headers, json=payload, timeout=30).json()
        if r.get("code") != 0:
            raise RuntimeError(r.get("msg") or "Requests lookup failed.")
        d = r.get("data", {})
        items.extend(d.get("items", []))
        if not d.get("has_more") or len(items) >= cap:
            break
        token = d.get("page_token")
    return items


@app.route('/api/agency-target/formula/live', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def agency_target_formula_live():
    """Phase 10.30 (Ahmed): the SAME single-agency ledger as
    /api/agency-target/formula but computed from a LIVE targeted read (2
    Feishu searches in parallel) instead of the warm snapshots -- answers
    "is there REALLY 1 left right now?" in ~1-2s. Costs quota only when an
    agent actually views/edits an agency's Formula card (or taps the badge),
    never on a timer."""
    user = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not perms.get("modules"):
        return jsonify({"error": "Access denied"}), 403

    code = sanitize_text(request.args.get('code', '')).strip()
    if not code:
        return jsonify({"success": False, "error": "Missing code"}), 400

    priv_raw = sanitize_text(request.args.get('privilege', '')).strip()
    priv_type = classify_privilege_type(priv_raw) if priv_raw else None

    def _with_selected(row_dict):
        sel = None
        if priv_type:
            sel = next((p for p in row_dict.get("privileges", [])
                        if p.get("privilege") == priv_type), None)
        return {**row_dict, "selected": sel}

    if MOCK_MODE:
        row = {"agency_code": code, "agency_name": f"Agency {code}", "acm": "Ehtisham",
               "region": "PK", "base_points": 350,
               "privileges": [{"privilege": p, "claimed": 1, "used": 0, "remaining": 1,
                               "pending": 0, "rejected": 0} for p in PRIVILEGE_TYPE_ORDER]}
        return jsonify({"success": True, "found": True, "live": True, **_with_selected(row),
                        "generated_at": cairo_now().isoformat()})

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_wallet = pool.submit(_live_agency_points_row, code, headers)
            fut_reqs = pool.submit(_live_agency_requests_rows, code, headers)
            wallet_row = fut_wallet.result()
            req_items = fut_reqs.result()
    except Exception as e:
        logger.error("formula_live_failed", code=code, error=str(e))
        return jsonify({"success": False, "error": str(e)}), 502

    row = compute_single_agency_target_ledger([wallet_row] if wallet_row else [],
                                              req_items, code)
    if row is None:
        return jsonify({"success": True, "found": False, "live": True,
                        "agency_code": code, "selected": None,
                        "generated_at": cairo_now().isoformat()})
    return jsonify({"success": True, "found": True, "live": True, **_with_selected(row),
                    "generated_at": cairo_now().isoformat()})

@app.route('/api/agency-target/formula/sync', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def agency_target_formula_sync():
    """Phase 10.32 (Ahmed, verbatim): "in the case the agency code changed
    need to go to update the tkt and wait the formula field updated and then
    return and show the data this when the tkt is already exist".

    For an EXISTING ticket whose Agency Code was edited on screen: write the
    new code to the ticket (PUT), then poll the record until Feishu's own
    sheet "Formula" field (live-recalculated) reports the new code, and hand
    that formula string back. Zero requests happen unless the code actually
    changed or the agent taps the badge -- the fast path on ticket open still
    trusts the formula that arrived with the ticket.
    """
    body = request.get_json(silent=True) or {}
    user = sanitize_text(body.get('user', ''))
    email = sanitize_text(body.get('email', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not perms.get("modules"):
        return jsonify({"error": "Access denied"}), 403

    record_id = sanitize_text(body.get('record_id', '')).strip()
    code = sanitize_text(body.get('code', '')).strip()
    if not record_id or not code:
        return jsonify({"success": False, "error": "Missing record_id or code"}), 400

    code_re = re.compile(r'(?:Agency Code|Code):\s*([^|]+)')

    def _formula_code(formula_str):
        m = code_re.search(formula_str or '')
        return m.group(1).strip() if m else ''

    if MOCK_MODE:
        return jsonify({"success": True, "formula":
                        (f"Agency Code: {code} | Privilege: Dynamic Avatar | Total Earned: 1 | "
                         f"Claimed: 0 | Remaining: 1 | Pending: 0 | Rejected: 0"),
                        "updated": True, "stale": False, "waited_ms": 0})

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    rec_url = (f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}"
               f"/tables/{REQUESTS_TABLE_ID}/records/{record_id}")

    def _get_fields():
        resp = _feishu_get(rec_url, headers={"Authorization": f"Bearer {tat}"}, timeout=12, tries=2)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(data.get("msg") or "record fetch failed")
        return (data.get("data", {}).get("record", {}) or {}).get("fields", {}) or {}

    started = time.time()
    try:
        fields = _get_fields()
    except Exception as e:
        logger.error("formula_sync_fetch_failed", record_id=record_id, error=str(e))
        return jsonify({"success": False, "error": str(e)}), 502

    baseline_formula = extract_field_text(get_field_local(fields, "Formula")).strip()
    stored_code = extract_field_text(get_field_local(fields, "Agency Code")).strip()

    updated = False
    if stored_code != code:
        try:
            tw = feishu_session.put(rec_url, headers=headers,
                                    json={"fields": {"Agency Code": code}},
                                    timeout=15).json()
            if tw.get("code") != 0:
                logger.error("formula_sync_put_failed", record_id=record_id, response=tw)
                return jsonify({"success": False,
                                "error": tw.get("msg") or "ticket code update failed"}), 502
            updated = True
        except Exception as e:
            logger.error("formula_sync_put_error", record_id=record_id, error=str(e))
            return jsonify({"success": False, "error": str(e)}), 502

    # Poll until the sheet's own Formula field catches up with the code we
    # wrote (or confirms the already-stored code). ~14s ceiling, 1.2s cadence.
    deadline = started + 14.0
    formula = baseline_formula
    stale = True
    while True:
        if _formula_code(formula) == code and (not updated or formula != baseline_formula):
            stale = False
            break
        if time.time() >= deadline:
            break
        time.sleep(1.2)
        try:
            fields = _get_fields()
            formula = extract_field_text(get_field_local(fields, "Formula")).strip()
        except Exception as e:
            logger.error("formula_sync_poll_failed", record_id=record_id, error=str(e))
            break

    waited_ms = int((time.time() - started) * 1000)
    if stale:
        # Sheet formula still shows the old/blank code after the wait -- hand
        # back what we have and let the frontend offer a tap-to-retry.
        return jsonify({"success": True, "formula": formula, "updated": updated,
                        "stale": True, "waited_ms": waited_ms})
    return jsonify({"success": True, "formula": formula, "updated": updated,
                    "stale": False, "waited_ms": waited_ms})

@app.route('/api/agency-target-table', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def agency_target_table():
    """Search-Records-style table: per-agency, per-privilege target claim ledger
    (Total Earned / Claimed / Remaining / Pending / Rejected) for the current
    month. Built entirely from the two bulk snapshots already kept warm by the
    background cron (Points table + Requests table) -- the exact same
    cache-first sources Search Records and Compare already read. No live
    per-agency Feishu calls."""
    user = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    perms = get_user_permissions(email, user)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    # Namespace-safe check (matches frontend hasNs): raw substring matching leaked
    # this endpoint to target_analytics / unrelated-module holders.
    if not perms.get("is_super_admin") and not any(m == "analytics" or m.startswith("analytics_") or m == "points" or m.startswith("points_") for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    allowed_acms = perms.get("permissions", {}).get("acms", {}).get("points", perms.get("permissions", {}).get("acms", {}).get("analytics", ["all"]))
    allowed_regs = perms.get("permissions", {}).get("regions", {}).get("points", perms.get("permissions", {}).get("regions", {}).get("analytics", ["all"]))
    region_filter = sanitize_text(request.args.get('region', 'all')).strip().lower() or "all"
    search_q = sanitize_text(request.args.get('q', '')).strip().lower()

    try:
        page = max(1, int(request.args.get('page', '1')))
        page_size = min(200, max(1, int(request.args.get('page_size', '50'))))
    except (ValueError, TypeError):
        page, page_size = 1, 50

    acm_filter = "all"
    if sanitize_text(request.args.get('scope', 'all')).lower() != "all" and not perms.get("is_super_admin"):
        acm_filter = user.strip().lower()

    if MOCK_MODE:
        points_items = []
        for i in range(30):
            rec = MockFeishuDB.generate_agency(str(40200 + i))[0]
            points_items.append(rec)
        requests_items = MockFeishuDB.generate_requests(120) if hasattr(MockFeishuDB, "generate_requests") else []
    else:
        points_items, _pts_complete, _pts_reason, _pts_cache = get_points_table_snapshot()
        requests_items, _keys, _req_complete, _req_reason, _req_cache = get_requests_table_snapshot()

    rows = compute_agency_privilege_ledger(points_items, requests_items, acm_filter, region_filter, allowed_acms, allowed_regs)
    if search_q:
        rows = [r for r in rows if search_q in r["agency_name"].lower() or search_q in r["agency_code"].lower() or search_q in r["acm"].lower()]
    
    rows.sort(key=lambda r: (-r.get("base_points", 0), r["agency_name"].lower()))

    total_count = len(rows)

    is_export = request.args.get('export', 'false').lower() == 'true'
    if is_export:
        if not perms.get("is_super_admin") and not any("export" in m for m in perms.get("modules", [])):
            audit.log(user, "UNAUTHORIZED_EXPORT", "Agency Target Table", ip=ip, severity="Critical")
            return jsonify({"error": "Export access denied."}), 403
        audit.log(user, "EXPORT_DATA", f"Agency Target Table ({total_count} rows)", ip=ip, severity="Info")
        return jsonify({
            "rows": rows[:5000], "total_count": total_count, "page": 1, "page_size": total_count,
            "total_pages": 1, "privilege_types": PRIVILEGE_TYPE_ORDER,
            "generated_at": cairo_now().isoformat(),
        })

    start = (page - 1) * page_size
    page_rows = rows[start:start + page_size]

    audit.log(user, "AGENCY_TARGET_TABLE_VIEW", f"rows:{total_count}|page:{page}", ip=ip, severity="Info")
    return jsonify({
        "rows": page_rows, "total_count": total_count, "page": page, "page_size": page_size,
        "total_pages": max(1, (total_count + page_size - 1) // page_size),
        "privilege_types": PRIVILEGE_TYPE_ORDER,
        "generated_at": cairo_now().isoformat(),
    })

@app.route('/api/exchange/watchlist', methods=['GET'])
def get_exchange_watchlist_route():
    user = sanitize_text(request.args.get('user', ''))
    if not user:
        return jsonify({"error": "Missing user"}), 400
    return jsonify({"watchlist": get_exchange_watchlist(user)})

@app.route('/api/exchange/watchlist/toggle', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def toggle_exchange_watchlist_route():
    body = request.json or {}
    user = sanitize_text(body.get('user', ''))
    code = sanitize_agency_code(body.get('agency_code', ''))
    if not user or not code:
        return jsonify({"error": "Missing user or agency_code"}), 400
    current = get_exchange_watchlist(user)
    if code in current:
        current.remove(code)
        action = "removed"
    else:
        current.append(code)
        action = "added"
    current = set_exchange_watchlist(user, current)
    audit.log(user, "EXCHANGE_WATCHLIST_TOGGLE", f"{action}:{code}", ip=request.headers.get("X-Forwarded-For", ""), severity="Info")
    return jsonify({"watchlist": current, "action": action})

@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized: return jsonify({"error":"Unauthorized"}), 403
    cache_invalidate()
    audit.log(admin_name, "CACHE_CLEARED", "all", ip=request.headers.get("X-Forwarded-For",""), severity="Warning")
    return jsonify({"success":True,"message":"Cache cleared."})

@app.route('/api/debug/data-status', methods=['GET'])
def debug_data_status():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions("", admin_name)
        if not perms.get("is_super_admin"):
            return jsonify({"error": "Unauthorized"}), 403

    load_local_json("requests.json")
    load_local_json("points.json")

    return jsonify({
        "data_status": _data_status,
        "cached_in_ram": list(_local_json_cache.keys()),
        "candidate_paths_requests": _candidate_data_paths("requests.json"),
        "candidate_paths_points": _candidate_data_paths("points.json"),
        "build_meta_generated_at": _read_build_meta_generated_at(),
        "build_meta_candidate_paths": _candidate_data_paths("_build_meta.json"),
        "self_base_url_env": SELF_BASE_URL or None,
    })

TICKET_FIELD_KEY_MAP = {
    "Region": "region", "User ID": "user_id", "Agency Code": "agency_code",
    "Agency Name": "agency_name", "Applier real name": "applier_real_name",
    "Otherapp Name": "otherapp_name", "Otherapp ID": "otherapp_id", "Country": "country",
    "Whatsapp Number": "whatsapp_number", "NID Number": "nid_number", "Agency Type": "agency_type",
    "Bd Code": "bd_code", "Acm Name (PK)": "acm", "Acm Name (IN)": "acm",
    # Phase 10.45 (Ahmed 2026-10-14): new sheet fields -- carried in the
    # ticket payload so feed cards / detail views can show them.
    "Border Type": "border_type", "Reason": "reason", "Possible BD ID": "possible_bd_id",
    "Closing Reason": "closing_reason", "New ID": "new_id", "Old ID": "old_id",
    "Reporter ID": "reporter_id", "BD Nickname": "bd_nickname", "Email Adress": "email_address",
    "BD Hunted Agency Code": "bd_hunted_agency_code", "Target": "target", "Privilege": "privilege",
    "Agency Point Privilege": "agency_point_privilege", "New agency name": "new_agency_name",
    "Current agency manger name (same in NID )": "current_agency_manager_name",
    "New agency owner ID": "new_agency_owner_id", "New agency owner Name": "new_agency_owner_name",
    "Agency to be merged and closed": "agency_to_be_merged_and_closed",
    "Type of Action Host sign": "type_of_action_host_sign", "Type of Action 2": "type_of_action_2",
    "New Short ID": "new_short_id", "Wealth Level": "wealth_level", "Vip Level": "vip_level",
    "Applier Note": "applier_note",
}
TICKET_ATTACHMENT_FIELDS = ["Evidence Screen", "Evidence Screen 2", "NID & Otherapp Screen", "New and old onwer National IDS (Both NID)"]

# Feishu field_ids for the Attachment columns on the Requests table (from the live
# Bitable schema). Required to build the "extra" bitablePerm parameter when the base
# has Advanced Permissions enabled — without it, /drive/v1/medias/{token}/download
# returns a permission error even with a valid tenant_access_token.
ATTACHMENT_FIELD_IDS = {
    "NID & Otherapp Screen": "fldWEHdmZn",
    "Evidence Screen": "fldhBymdRS",
    "Evidence Screen 2": "fldBQzos6f",
    "New and old onwer National IDS (Both NID)": "fldKf2AtEJ",
}

def build_ticket_payload(item):
    fields = item.get("fields", {})
    ticket = {"record_id": item.get("record_id")}
    for feishu_name, key in TICKET_FIELD_KEY_MAP.items():
        val = extract_field_text(fields.get(feishu_name))
        if val:
            ticket[key] = val
    for att_field in TICKET_ATTACHMENT_FIELDS:
        pics = extract_attachments(fields.get(att_field), ATTACHMENT_FIELD_IDS.get(att_field))
        if pics:
            ticket.setdefault("attachments", []).extend(pics)
    ticket["request_type"] = extract_field_text(get_field_local(fields, "Request Type", "Type"))
    ticket["assigned_member"] = extract_field_text(get_field_local(fields, "Assigned Member"))
    ticket["status"] = extract_field_text(get_field_local(fields, "Status"))
    ticket["reject_reason"] = extract_field_text(get_field_local(fields, "Reject Reason", "Rejection Reason"))
    ticket["audition_note"] = extract_field_text(get_field_local(fields, "Audition note", "Audition Note"))
    ticket["duplicated_check"] = extract_field_text(get_field_local(fields, "Duplicated Check"))
    return ticket

@app.route('/api/attachments/<file_token>', methods=['GET'])
def proxy_attachment(file_token):
    user = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    field_id = sanitize_text(request.args.get('field_id', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("modules") and not perms.get("is_super_admin"):
        return jsonify({"error": "Access denied"}), 403
    try:
        tat = get_tenant_access_token()
        url = f"https://open.feishu.cn/open-apis/drive/v1/medias/{file_token}/download"
        headers = {"Authorization": f"Bearer {tat}"}

        # ROOT CAUSE (Issue 8): with Advanced Permissions enabled on the base, Feishu's
        # media download endpoint requires an "extra" param identifying which Bitable
        # attachment cell the token belongs to, or it rejects the request. We build it
        # when a field_id was supplied by the caller.
        def _attempt(with_extra):
            params = {}
            if with_extra and field_id:
                params["extra"] = json.dumps({
                    "bitablePerm": {
                        "tableId": REQUESTS_TABLE_ID,
                        "attachmentToken": file_token,
                        "fieldId": field_id,
                    }
                })
            return feishu_session.get(url, headers=headers, params=params, timeout=20, stream=True)

        resp = _attempt(with_extra=True)
        if resp.status_code != 200 and field_id:
            # Fall back without the extra param, in case advanced permissions aren't
            # actually enabled and the unnecessary param is what's causing the failure.
            resp = _attempt(with_extra=False)

        if resp.status_code != 200:
            logger.error("attachment_proxy_failed", file_token=file_token, field_id=field_id, status=resp.status_code, body=resp.text[:300])
            return jsonify({"error": "Could not fetch attachment"}), 502
        # SPEED FIX (Live Ticket Queue feels slow to open/re-open): a given file_token's
        # bytes never change once uploaded, but this endpoint had zero cache headers --
        # every single view of a ticket (and every re-render after Run Duplicate Check,
        # which reopens the whole workspace) re-fetched every attachment image from
        # Feishu from scratch, even for the exact same ticket seconds apart. Purely
        # additive: doesn't change what's returned or any existing behavior, just lets
        # the browser reuse what it already downloaded instead of asking again.
        response = Response(resp.content, content_type=resp.headers.get("Content-Type", "application/octet-stream"))
        response.headers["Cache-Control"] = "private, max-age=86400, immutable"
        return response
    except Exception as e:
        logger.error("attachment_proxy_failed", file_token=file_token, error=str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/tickets/under-investigation', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def list_under_investigation():
    """
    Phase 10.13 (Ahmed): the "Under Investigation" page under Ticket Queue.
    ONE targeted search, fired only when the agent clicks Search on that page
    (opening the page fetches nothing), returning just the basic columns so
    the list stays cheap. Each row carries record_id so its "Open" button can
    load the ticket straight into the Ticket Workspace.
    """
    user  = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("tickets" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403
    audit.log(user, "TICKETS_UNDER_INVESTIGATION", "search", ip=request.headers.get("X-Forwarded-For", request.remote_addr or ""), severity="Info")

    # Phase 10.34 (speed, zero cost): the field projection is GONE. Feishu's
    # records/search costs per CALL, not per field, and the search already
    # downloads every field of each matching record -- asking for fewer
    # fields bought nothing. Each row now rides its full fields along so the
    # frontend can seed the ticket workspace: opening ANY result needs zero
    # /api/requests/single calls (the old top-5 warm prefetches are gone too,
    # so this is FEWER calls than before, not more).
    if MOCK_MODE:
        items = MockFeishuDB.generate_requests(6)
    else:
        tat = get_tenant_access_token()
        search_url, headers = _query_search_url_and_headers(tat)
        payload = {
            "page_size": 500,
            "filter": {"conjunction": "and", "conditions": [
                {"field_name": "Status", "operator": "is", "value": ["Under Investigation"]}
            ]},
        }
        try:
            resp = feishu_session.post(search_url, headers=headers, json=payload, timeout=15)
            data = resp.json()
            if data.get("code") != 0:
                return jsonify({"error": f"Data fetch failed: Feishu API Error: {data.get('msg','Invalid Filter.')}"}), 502
            items = data.get("data", {}).get("items", [])
        except Exception as e:
            return jsonify({"error": f"Data fetch failed: {str(e)}"}), 502

    rows = []
    for item in items:
        fields = item.get("fields", {})
        submitted_raw = get_field_local(fields, "Submitted on Copy", "Submitted on", "Created Time")
        submitted_dt  = parse_feishu_date(submitted_raw)
        # Phase 10.13.1: ACM is one column in the grid -- the sheet splits it
        # into "Acm Name (IN)" / "Acm Name (PK)", so show whichever is filled.
        acm_in = extract_field_text(get_field_local(fields, "Acm Name (IN)"))
        acm_pk = extract_field_text(get_field_local(fields, "Acm Name (PK)"))
        rows.append({
            "record_id":    item.get("record_id"),
            "numbering":    extract_field_text(get_field_local(fields, "Numbering")),
            "submitted_on": submitted_dt.strftime("%Y-%m-%d") if submitted_dt else extract_field_text(submitted_raw),
            "request_type": extract_field_text(get_field_local(fields, "Request Type", "Type")),
            "respondents":  extract_field_text(get_field_local(fields, "Respondents", "Created By")),
            "mentioned_person": extract_field_text(get_field_local(fields, "Mentioned Person")),
            "type_of_action":   extract_field_text(get_field_local(fields, "Type of Action")),
            "region":       extract_field_text(get_field_local(fields, "Region", "Agency Region")),
            "acm":          acm_in or acm_pk,
            "user_id":      extract_field_text(get_field_local(fields, "User ID")),
            "agency_code":  extract_field_text(get_field_local(fields, "Agency Code")),
            "full_fields":  fields,  # Phase 10.34: seed the workspace on open -- zero extra calls
            "_sort_ts":     submitted_dt.timestamp() if submitted_dt else 0,
        })
    rows.sort(key=lambda r: r["_sort_ts"], reverse=True)
    for r in rows: r.pop("_sort_ts", None)
    return jsonify({"success": True, "results": rows, "count": len(rows)})

@app.route('/api/tickets/pull-assigned', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def pull_assigned_ticket():
    user = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    perms = get_user_permissions(email, user)
    # Phase 10.13: exact-key check. The new granular Ticket Queue sub-keys
    # (tickets_new / tickets_pending) contain the substring "tickets" but must
    # NOT implicitly grant live-queue pulling -- only the master 'tickets'
    # module or the Live Queue sub-key may pull, exactly as before.
    if not perms.get("is_super_admin") and not any(m in ("tickets", "tickets_live_queue") for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403
    if not user:
        return jsonify({"error": "Missing user"}), 400

    if MOCK_MODE:
        return jsonify({"tickets": []})

    tat = get_tenant_access_token()
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    def _search(status_field):
        payload = {
            "page_size": 100,
            "filter": {
                "conjunction": "and",
                "conditions": [
                    {"field_name": status_field, "operator": "contains", "value": ["In Progress"]},
                ],
            },
            "sort": [{"field_name": "Numbering", "desc": True}],
        }
        return session.post(search_url, json=payload, timeout=12)

    session = http_requests.Session()
    session.headers.update(headers)

    matches = []
    try:
        resp = _search("Request Status")
        data = resp.json()
        if data.get("code") == 1254045:
            resp = _search("Status")
            data = resp.json()
        if data.get("code") == 0:
            items = data.get("data", {}).get("items", [])
            user_clean = user.strip().lower()
            
            filtered_items = []
            for it in items:
                assigned_member = extract_field_text(it.get("fields", {}).get("Assigned Member", "")).lower()
                if user_clean in assigned_member:
                    filtered_items.append(it)
                    _harvest_person_names(it.get("fields", {}))  # Phase 10.47: free name-resolution seed
                    
            matches = []
            for it in filtered_items[:20]:
                t = build_ticket_payload(it)
                # Phase 10.22 (speed, Ahmed): the records/search above ALREADY
                # returned every field of this record -- ride the full record
                # along so the portal can seed its prefetch cache straight
                # from the pull. Opening a queued ticket then needs NO
                # /api/requests/single call at all: zero extra Feishu quota,
                # zero extra invocations -- just a bigger pull payload
                # (attachment cells are file tokens, not the files). Same
                # staleness as the old prefetch: fetched moments before the
                # click, and the workspace still re-fetches on save/verify.
                # Built for Ahmed's fresh-ticket workflow (ticket assigned ->
                # pulled -> clicked within seconds), where even a priority
                # prefetch had no time to finish before the click.
                t["full_fields"] = it.get("fields", {})
                matches.append(t)
        else:
            logger.error("pull_assigned_failed", error=data.get("msg"))
    except Exception as e:
        logger.error("pull_assigned_failed", error=str(e))

    return jsonify({"tickets": matches})

@app.route('/api/health', methods=['GET'])
def health():
    with _bg_sync_lock:
        bg_info = {
            "record_count": len(_bg_sync["requests_items"]),
            "age_seconds": (time.time() - _bg_sync["updated_at"]) if _bg_sync["updated_at"] else None,
            "syncing": _bg_sync["syncing"],
        }
    with _bg_points_lock:
        pts_bg_info = {
            "record_count": len(_bg_points_sync["items"]),
            "age_seconds": (time.time() - _bg_points_sync["updated_at"]) if _bg_points_sync["updated_at"] else None,
            "syncing": _bg_points_sync["syncing"],
        }
    return jsonify({
        "status": "ok", "ts": datetime.utcnow().isoformat(), "cairo_ts": cairo_now().isoformat(),
        "cache_entries": len(_cache), "audit_entries": len(audit._queue),
        "token_cached": _token_cache["token"] is not None,
        "token_expires_in_s": max(0, int(_token_cache["expires_at"] - time.time())),
        "background_sync": bg_info,
        "points_background_sync": pts_bg_info,
        "redis_enabled": REDIS_ENABLED,
        "mock_mode_active": MOCK_MODE,
    })

# ============================================================================
# Phase 10.43 (Ahmed, 2026-10-14): Grand Table record COMMENTS inside the
# portal -- a per-ticket comment thread (view + reply + @mention) plus a
# comment notifications feed, both synced 1:1 with the Base's native record
# comments (one thread in Feishu Base AND the portal; a mention posted from
# the portal triggers the normal Feishu notification, exactly like Farooq's
# comment in Ahmed's screenshot). Wire formats verified live against the
# Grand Table on 2026-10-14:
#   LIST: GET /open-apis/drive/v1/files/{BASE_ID}/comments?file_type=bitable
#         Oldest-first, page_size<=100, has_more/page_token. is_solved is
#         OMITTED (= all statuses; that is lark-cli's "--solved-status all"
#         wire shape). Every reply carries extra.notify_extra = {record,
#         table, view} and extra.image_list = [file_token, ...]. This base's
#         record_ids ARE the short rec+7 form (e.g. recW9LeWLX) that
#         notify_extra uses, so record filtering is a straight == match
#         (verified: record-list record_id_list vs comment anchors).
#   ADD:  POST /open-apis/drive/v1/files/{BASE_ID}/new_comments
#         {file_type:"bitable",
#          anchor:{block_id:<table>, base_record_id:<record>,
#                  base_view_id:<view>},
#          reply_elements:[{type:"text", text:...},
#                          {type:"mention_user", mention_user:"ou_..."}]}
#         The write uses the caller's UAT so authorship = the logged-in agent.
# ============================================================================
REQUESTS_DEFAULT_VIEW_ID = "vewpHhuE1X"  # "Results" grid view -- the anchor view the Base itself stamps on record comments
COMMENTS_SCAN_CACHE_KEY  = "xena:comments:scan:v1"
# Phase 11.9 (Ahmed: "check how long it take to load the comment ... i need it
# same exctly like the sheet fast smoth"): the 30s self-expiring TTL made nearly
# EVERY comment load a cold base-wide rescan (30 pages x cross-border ≈ 20-25s).
# Now the scan is PERSISTENT in Redis and freshness is event-driven instead:
# portal-made changes (new comment / reaction) are patched into the cached scan
# in place (instant, exact), sheet-side changes are picked up by a lock-deduped
# background revalidate the frontend triggers when a read reports stale.
COMMENTS_SCAN_KEEP_TTL   = 7 * 86400  # cache never really expires (safety net)
COMMENTS_SCAN_FRESH      = 45      # seconds; reads older than this are served
                                   # instantly AND flagged stale so the
                                   # frontend revalidates in the background
COMMENTS_SCAN_MAX_PAGES  = 30      # hard cap: 30 x 100 = 3000 comment threads
COMMENTS_SCAN_LOCK_KEY   = "xena:comments:scan:lock"  # one rescan at a time,
                                   # across all concurrent requests/instances
COMMENTS_SCAN_LOCK_MS    = 45000   # lock covers a worst-case full scan
# Phase 16 (Ahmed: "can you make it once every 30 min? + a refresh button"):
# automatic (stale-driven) scan-refresh — and with it the mirror sync pass and
# the name backfill, which both ride on the same background pass — runs at
# most once per SCAN_AUTO_MIN_GAP across ALL viewers. The bell panel's manual
# refresh button posts force:true to bypass the gap (the NX lock inside
# _scan_refresh still dedupes simultaneous clickers), and any successful pass
# (auto or forced) resets the clock.
SCAN_AUTO_MIN_GAP        = 1800    # 30 min between automatic refreshes
SCAN_AUTO_TS_KEY         = "xena:comments:scan_auto_ts"
COMMENTS_FEED_MAX        = 100
COMMENT_NAME_TTL         = 3600    # open_id -> display name from LIVE lookups, 1h
                                   # (Phase 11.4, Ahmed: "not cached" — masked ids are
                                   # never cached at all and retry live on every view;
                                   # resolved names/photos refresh within the hour)
# Phase 11.3: team-directory warm for comment/avatar resolution. The Access
# Control table lists every portal member's login EMAIL; Feishu's "Search
# users" API (contact:user:search — the scope this app HAS, UAT-only) turns
# an email into open_id + name + avatar. This is the sheet-parity trick with
# ZERO extra permissions: any teammate's open_id (comment author, @mention,
# reaction user, respondent) resolves to their real name + photo.
DIR_MEMBERS_KEY          = "xena:dir:members"   # cached access-table email list
DIR_MEMBERS_TTL          = 1800      # 30 min (Phase 11.4: was 6h — new members appear fast)
DIR_EMAIL_TTL            = 7 * 86400  # email -> open_id mapping is immutable, 7d
DIR_NEG_TTL              = 3600      # negative email-lookup cache, 1h — failed lookups retry soon
DIR_SEARCH_BUDGET        = 12        # max live user-searches per resolve call
HARVEST_NAME_TTL         = 7 * 86400  # sheet-sourced person names (person fields / sweep), 7d (Phase 12: was 24h — names vanished before the daily re-sweep could refresh them)
# Phase 11.4: incremental full-table person sweep — the scope-free way to
# resolve ANY teammate who ever touched a ticket, not just portal members.
SWEEP_KEY                = "xena:dir:sweep"   # {"page_token","done","done_ts","run_ts"}
SWEEP_PAGES_PER_RUN      = 3        # 3 x 500 records per run — bounded request-time cost
SWEEP_THROTTLE           = 30       # seconds between runs (first full pass ~6 min)
SWEEP_REARM_TTL          = 86400    # after a completed pass, wait 24h before re-sweeping
COMMENT_RECNUM_TTL       = 7 * 86400  # record_id -> Numbering is immutable, 7d

# ============================================================================
# Phase 15 — Portal Comments MIRROR (Ahmed: "this is miaoda answer for how it
# built instant live comment system — make it — live and fast and cheap").
#
# Miaoda's trick, adapted without losing the two-way sheet sync:
#   * READS  stop depending on the base-wide drive-comment scan and hit the
#     "Portal Comments" Bitable table instead — ONE indexed records/search
#     per view, exactly like Miaoda's comment table. Each row carries the
#     thread's FULLY FLATTENED payload (replies with names/avatars already
#     resolved + the ready-made feed row), so a read needs ZERO per-id name
#     lookups — the expensive part of every previous read. The Bitable API
#     is unmetered Feishu quota; per-view cost drops to ~1-3 Redis commands
#     (scan staleness flag + per-user read state), so a 5s bell / 3s open
#     thread poll stays comfortably inside the free-tier plan.
#   * WRITES still go to NATIVE Feishu record comments (UAT) — the sheet
#     stays the single source of truth, @mentions keep notifying in Lark,
#     comments typed in the Base keep flowing back. After a successful
#     native write the matching mirror row is refreshed immediately (1-2
#     extra calls per human-paced action, never per poll).
#   * SYNC   runs on the scan-refresh lock holder (the existing background
#     revalidate): it upserts ONLY threads whose update_time changed (or
#     whose payload still carries masked names), capped per pass. Sheet-
#     side changes therefore surface within the same ~45s window the scan
#     already had; portal-side changes are instant via the write patch.
#   * FAIL-OPEN: every mirror call is try/except — table error, empty
#     table, or MIRROR_ENABLE=False all fall back to the scan path below,
#     which is byte-for-byte the Phase 14 behavior.
# ============================================================================
MIRROR_TABLE_ID        = "tblUhfs2d43hxy3u"  # "Portal Comments" (created 2026-10-19)
MIRROR_ENABLE          = True
MIRROR_PAGE            = 200    # rows per records/search page (one call)
MIRROR_READ_PAGES_MAX  = 5      # sync pass reads at most 1000 newest rows
MIRROR_FEED_ROWS       = 200    # feed fast path: newest 200 thread rows, 1 call
MIRROR_STORE_MAX       = 200    # rows upserted per background sync pass
MIRROR_BATCH           = 100    # records per batch_create / batch_update call

def _comment_record_anchor(comment):
    """The {record, table, view} anchor of a comment thread (stamped on every
    reply by Feishu as extra.notify_extra)."""
    for r in ((comment.get("reply_list") or {}).get("replies") or []):
        ne = (r.get("extra") or {}).get("notify_extra") or {}
        if ne.get("record"):
            return ne
    return {}

def _reply_parts(reply):
    """Normalize a reply's content elements to a simple parts list the portal
    renders directly, plus a plain-text version for feed snippets."""
    parts, plain = [], []
    for el in (((reply.get("content") or {}).get("elements")) or []):
        t = el.get("type")
        if t == "text_run":
            txt = ((el.get("text_run") or {}).get("text")) or ""
            parts.append({"type": "text", "text": txt})
            plain.append(txt)
        elif t == "person":
            uid = (el.get("person") or {}).get("user_id") or ""
            parts.append({"type": "mention", "user_id": uid})
            plain.append("@")  # the name is filled in after user resolution
        elif t == "docs_link":
            url = (el.get("docs_link") or {}).get("url") or ""
            if url:
                parts.append({"type": "link", "url": url})
                plain.append(url)
    return parts, "".join(plain).strip()

def _reply_images(reply):
    imgs = (reply.get("extra") or {}).get("image_list") or []
    out = []
    for im in imgs:
        if isinstance(im, str) and im:
            out.append(im)
        elif isinstance(im, dict):
            tok = im.get("file_token") or im.get("token") or ""
            if tok:
                out.append(tok)
    return out

def _fetch_all_comments(token, force=False):
    """Read the shared base-wide comment scan. The scan is PERSISTENT in Redis
    (Phase 11.9): reads serve the cached copy regardless of age (the caller
    reports staleness via _scan_is_stale and the frontend revalidates in the
    background); only force=True — or an empty cache — paginates the base.
    Returns (payload, error) where payload = {"items": [...], "truncated": bool,
    "ts": epoch}."""
    cached = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
    if isinstance(cached, dict) and "items" in cached and not force:
        return cached, None
    items, page_token, pages = [], None, 0
    while pages < COMMENTS_SCAN_MAX_PAGES:
        # need_reaction=true attaches each reply's emoji reactions
        # ([{reaction_key, count, ahead_users:[open_id...]}]) so the portal
        # can show them on comments and surface them in the bell feed --
        # Ahmed: "when someone make reaction the notification appear in the
        # sheet but not in the website".
        params = {"file_type": "bitable", "page_size": 100, "need_reaction": "true"}
        if page_token:
            params["page_token"] = page_token
        try:
            resp = feishu_session.get(
                f"https://open.feishu.cn/open-apis/drive/v1/files/{BASE_ID}/comments",
                headers={"Authorization": f"Bearer {token}"}, params=params, timeout=15)
            data = resp.json()
        except Exception as e:
            return None, f"comment scan failed: {e}"
        if data.get("code") != 0:
            return None, f"{data.get('code')}: {data.get('msg')}"
        d = data.get("data") or {}
        items.extend(d.get("items") or [])
        pages += 1
        if not d.get("has_more"):
            page_token = None
            break
        page_token = d.get("page_token")
        if not page_token:
            break
    payload = {"items": items, "truncated": bool(page_token), "ts": int(time.time())}
    redis_set_json(COMMENTS_SCAN_CACHE_KEY, payload, ttl=COMMENTS_SCAN_KEEP_TTL)
    return payload, None

def _scan_is_stale(scan):
    """True when the cached scan is older than COMMENTS_SCAN_FRESH (or carries
    no timestamp — e.g. written before Phase 11.9)."""
    ts = int((scan or {}).get("ts") or 0) if isinstance(scan, dict) else 0
    return (time.time() - ts) > COMMENTS_SCAN_FRESH

# Phase 14: per-pass cap on how many distinct user ids one background
# backfill will try to resolve (the resolver's own throttles still apply on
# top — BITABLE_LOOKUP_MAX per call, 1h negative caches for misses).
# Phase 18: 200 -> 60. A 200-id force_live pass (contact batch + directory +
# bitable + per-id GET legs, cross-border) was the stage that blew the whole
# chain past Vercel's execution limit — the mirror sync behind it NEVER ran,
# which is why Ahmed's Portal Comments table sat at 1 row and his bell
# froze on that one thread. 60 ids/pass still drains the backlog in a few
# passes and leaves budget for the mirror stage that now runs FIRST.
BG_NAME_BACKFILL_MAX = 60

def _scan_backfill_names(payload, token):
    """Phase 14 (Ahmed: "find easy way to save all who made ever reply or
    have access on the sheet instead of missing names button"): the shared
    comment scan payload already carries EVERY open_id that ever appeared in
    a ticket comment — comment authors, every replier, every mentionee,
    every reaction. This runs ONLY on the scan-refresh lock holder (the
    background revalidate path — nobody is waiting on it) and hands the
    still-uncached ids to _resolve_comment_names(force_live=True): hits are
    saved to xena:uname:{uid} for 7d, misses land in the resolver's 1h
    negative caches so an unresolvable id costs at most one retry per hour,
    in the background. Interactive comment/feed loads stay cache-only and
    instant (Phase 13 contract) — this is what quietly fills the cache so
    the "Fix missing names" button becomes a rarely-needed fallback."""
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        return
    ids, seen = [], set()
    def _add(u):
        if u and u not in seen:
            seen.add(u)
            ids.append(u)
    for c in payload["items"]:
        if not isinstance(c, dict):
            continue
        _add(c.get("user_id"))
        for r in ((c.get("reply_list") or {}).get("replies") or []):
            if not isinstance(r, dict):
                continue
            _add(r.get("user_id"))
            for p in _reply_parts(r)[0]:
                if isinstance(p, dict) and p.get("type") == "mention":
                    _add(p.get("user_id"))
            for rx in _reply_reactions(r):
                for u in (rx.get("users") or []):
                    _add(u)
        for rx in (c.get("reaction_list") or []):
            for u in ((rx or {}).get("ahead_users") or []):
                _add(u)
    if not ids:
        return
    try:
        _resolve_comment_names(ids[:BG_NAME_BACKFILL_MAX], token, uat=None,
                                force_live=True)
    except Exception:
        pass

# Phase 17: background-stage error ledger. The mirror/backfill chain used to
# swallow every failure into a silent `except: pass` — which is exactly why
# Ahmed's production mirror sat EMPTY for a day with zero trace. Stages still
# never raise (a broken mirror must never break a comment read), but every
# failure is recorded here and surfaced by /api/comments/health.
_BG_ERRORS = []
def _bg_error(stage, err):
    try:
        _BG_ERRORS.append({"stage": stage, "error": str(err)[:300],
                           "ts": int(time.time())})
        del _BG_ERRORS[:-20]
        logger.warn("bg_stage_failed", stage=stage, error=str(err)[:300])
    except Exception:
        pass

# Phase 18: total time budget for one background chain pass. Vercel kills
# the function at its own limit no matter what — before this, the stages ran
# in an order (fetch -> sweep -> 200-id backfill -> mirror) that let the slow
# middle stages eat the whole budget, so the MIRROR stage (last) never ran
# in production: the Portal Comments table sat at 1 row and the bell froze
# on that thread. The chain now runs the mirror FIRST and skips whatever no
# longer fits the budget — a slow stage can never starve the ones behind it
# again, and skipped stages just run on the next pass.
CHAIN_BUDGET_S = 45

def _scan_chain(token):
    """The refresh-holder body: rescan + the background maintenance chain.
    Phase 12: the person-name sweep moved OUT of interactive comment loads
    (it added 6-10s to user-facing requests) — this background path is the
    right home: throttled, incremental, nobody staring at a spinner.
    Phase 14: _scan_backfill_names auto-saves every person the scan has ever
    seen so names fill up with no admin button pressed.
    Phase 15: _mirror_sync_pass keeps the Portal Comments table in sync
    (diff-based, unchanged threads cost zero upserts).
    Phase 18: stage order is now mirror -> backfill -> sweep with a shared
    CHAIN_BUDGET_S stop-line (see above)."""
    started = time.time()
    result = _fetch_all_comments(token, force=True)
    scan_payload = result[0] if isinstance(result, tuple) else None
    def _over_budget():
        return (time.time() - started) > CHAIN_BUDGET_S
    if not _over_budget():
        try:
            _mirror_sync_pass(scan_payload, token)
        except Exception as e:
            _bg_error("mirror", e)
    if not _over_budget():
        try:
            _scan_backfill_names(scan_payload, token)
        except Exception as e:
            _bg_error("backfill", e)
    if not _over_budget():
        try:
            _sweep_step(token)
        except Exception as e:
            _bg_error("sweep", e)
    return result

_SCAN_LOCAL_LOCK = threading.Lock()

def _scan_refresh_local(token):
    """Redis-free refresh (Phase 17): deduped per instance by a threading
    lock — the holder runs the FULL chain (without this the mirror sync and
    name backfill silently never happen when Redis is down, the root cause of
    Ahmed's empty Portal Comments table + masked names), waiters serve the
    fresh cache the holder leaves behind."""
    if _SCAN_LOCAL_LOCK.acquire(blocking=False):
        try:
            return _scan_chain(token)
        finally:
            _SCAN_LOCAL_LOCK.release()
    deadline = time.time() + 30
    while time.time() < deadline:
        time.sleep(1)
        fresh = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
        if isinstance(fresh, dict) and "items" in fresh:
            return fresh, None
    return _fetch_all_comments(token, force=True)

def _scan_refresh(token):
    """Refresh the shared scan, de-duplicated across concurrent requests and
    Vercel instances by a Redis lock: the lock holder rescans; everyone else
    waits (max ~25s) for the holder's fresh copy instead of all paginating the
    base at once. Returns (payload, error).
    Phase 17: with Redis disabled OR down (lock call skipped/failed), the
    per-instance local path runs instead — including the whole maintenance
    chain — instead of the old early return that skipped it, or the old 25s
    dead wait on a store that would never answer."""
    if not REDIS_ENABLED:
        return _scan_refresh_local(token)
    cached = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
    old_ts = int(cached.get("ts") or 0) if isinstance(cached, dict) else 0
    got = redis_cmd("SET", COMMENTS_SCAN_LOCK_KEY, str(time.time()),
                    "NX", "PX", COMMENTS_SCAN_LOCK_MS)
    if got:
        try:
            result = _scan_chain(token)
        finally:
            try:
                redis_cmd("DEL", COMMENTS_SCAN_LOCK_KEY)
            except Exception:
                pass
        return result
    if not redis_healthy():
        # The lock call was skipped/failed because the store is down — take
        # the local path rather than waiting 25s for a holder that can't land.
        return _scan_refresh_local(token)
    # Another instance/request is refreshing right now — wait for its result.
    deadline = time.time() + 25
    while time.time() < deadline:
        time.sleep(1)
        fresh = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
        if isinstance(fresh, dict) and "items" in fresh and int(fresh.get("ts") or 0) > old_ts:
            return fresh, None
    # The holder didn't land in time — serve whatever is cached rather than
    # stampeding the base; scan inline only when there is NOTHING cached.
    if isinstance(cached, dict) and "items" in cached:
        return cached, None
    return _fetch_all_comments(token, force=True)

def _scan_save(cached):
    """Write back a patched scan, preserving its original ts (a patch freshens
    one thread, not the whole base — the freshness window still governs the
    rest)."""
    try:
        redis_set_json(COMMENTS_SCAN_CACHE_KEY, cached, ttl=COMMENTS_SCAN_KEEP_TTL)
    except Exception:
        pass

def _scan_upsert_comment(record_id, created):
    """Patch a just-posted comment thread into the shared scan cache so the
    poster's reload (and everyone's next read) sees it WITHOUT a base-wide
    rescan. The new_comments response data IS the created comment object (same
    shape as a scan item) but carries no record anchor — stamped here from the
    request. Returns False when the payload doesn't look like a comment object
    (the caller then falls back to busting the cache)."""
    if not isinstance(created, dict) or not created.get("comment_id"):
        return False
    cached = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
    if not (isinstance(cached, dict) and isinstance(cached.get("items"), list)):
        return False
    anchor = {"record": record_id, "table": REQUESTS_TABLE_ID,
              "view": REQUESTS_DEFAULT_VIEW_ID}
    for r in ((created.get("reply_list") or {}).get("replies") or []):
        extra = r.get("extra")
        if not isinstance(extra, dict):
            extra = {}
            r["extra"] = extra
        ne = extra.get("notify_extra")
        if not isinstance(ne, dict):
            ne = {}
            extra["notify_extra"] = ne
        ne.update(anchor)
    items = cached["items"]
    items[:] = [c for c in items if c.get("comment_id") != created["comment_id"]]
    items.append(created)  # scan order is oldest-first; a new thread goes last
    _scan_save(cached)
    return True

def _scan_patch_reaction(reply_id, emoji, action, actor_open_id):
    """Apply a successful reaction to the shared scan cache in place — the
    Phase 11.9 replacement for busting the whole cache (which forced a 20s
    base-wide rescan after EVERY react, the 'react takes forever' Ahmed
    reported). Mirrors Feishu's idempotency: add is a no-op when the actor is
    already counted, delete is a no-op when they are not (actor unknown →
    count-only best effort; the next scheduled rescan reconciles)."""
    cached = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
    if not (isinstance(cached, dict) and isinstance(cached.get("items"), list)):
        return False
    hit = False
    for c in cached["items"]:
        for r in ((c.get("reply_list") or {}).get("replies") or []):
            if r.get("reply_id") != reply_id:
                continue
            hit = True
            rxs = r.get("reactions")
            if not isinstance(rxs, list):
                rxs = []
                r["reactions"] = rxs
            tgt = next((x for x in rxs if x.get("reaction_key") == emoji), None)
            if action == "add":
                if tgt is None:
                    tgt = {"reaction_key": emoji, "count": 0, "ahead_users": []}
                    rxs.append(tgt)
                users = tgt.get("ahead_users")
                if not isinstance(users, list):
                    users = []
                    tgt["ahead_users"] = users
                if actor_open_id and actor_open_id in users:
                    pass  # already counted — idempotent repeat
                else:
                    tgt["count"] = int(tgt.get("count") or 0) + 1
                    if actor_open_id:
                        users.append(actor_open_id)
            else:  # delete
                if tgt is not None:
                    users = tgt.get("ahead_users")
                    if not isinstance(users, list):
                        users = []
                    if actor_open_id and users and actor_open_id not in users:
                        pass  # not counted under this id — idempotent repeat
                    else:
                        tgt["count"] = max(0, int(tgt.get("count") or 0) - 1)
                        if actor_open_id:
                            tgt["ahead_users"] = [u for u in users if u != actor_open_id]
                    if int(tgt.get("count") or 0) <= 0:
                        rxs.remove(tgt)
    if hit:
        _scan_save(cached)
    return hit

# ============================================================================
# Phase 15 mirror helpers — all fail-open (MIRROR_ENABLE / try-expect guards
# at the call sites; nothing here may break a comment read or write).
# ============================================================================

def _mirror_cell_text(v):
    """Normalize a text field read back from the API: plain str, rich-text
    segment list, or number."""
    if isinstance(v, list):
        return "".join(str(x.get("text", x)) if isinstance(x, dict) else str(x) for x in v)
    if v in (None, False):
        return ""
    return str(v)

def _mirror_cell_num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0

def _mirror_search(body, token, pages=MIRROR_READ_PAGES_MAX):
    """records/search on the Portal Comments table. Returns (rows, error):
    rows = list of raw {record_id, fields} rows, newest-first when a sort is
    supplied in body; error = str when the table answered non-zero."""
    rows, page_token, n = [], None, 0
    while n < max(1, pages):
        b = dict(body or {})
        # Phase 17: page_size / page_token are QUERY params on records/search —
        # in the POST body Feishu silently ignores them (default 20-row pages,
        # pagination never advancing). Verified against the live table
        # 2026-10-19: body params don't error, they're just dropped.
        params = {"page_size": b.pop("page_size", None) or MIRROR_PAGE}
        if page_token:
            params["page_token"] = page_token
        try:
            resp = feishu_session.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{MIRROR_TABLE_ID}/records/search",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json; charset=utf-8"},
                params=params, json=b, timeout=15)
            data = resp.json()
        except Exception as e:
            return rows, f"mirror search failed: {e}"
        if data.get("code") != 0:
            return rows, f"mirror {data.get('code')}: {data.get('msg')}"
        d = data.get("data") or {}
        rows.extend(d.get("items") or [])
        n += 1
        if not d.get("has_more"):
            break
        page_token = d.get("page_token")
        if not page_token:
            break
    return rows, None

def _mirror_row_payloads(rows):
    """Extract the stored payload JSONs from raw mirror rows (sorted by the
    row's Update Time, newest-first). Rows with unreadable payloads are
    skipped; duplicate Comment IDs (a create/create race) collapse to the
    newest row."""
    out, seen = {}, {}
    for row in rows:
        f = row.get("fields") or {}
        cid = _mirror_cell_text(f.get("Comment ID"))
        try:
            payload = json.loads(_mirror_cell_text(f.get("Payload")) or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("comment_id", cid)
        payload["_update_time"] = _mirror_cell_num(f.get("Update Time"))
        if cid and cid in seen:      # dup rows: keep the newer Update Time
            if payload["_update_time"] < out[cid]["_update_time"]:
                continue
        if cid:
            seen[cid] = True
        out[cid or f"row{len(out)}"] = payload
    return list(out.values())

def _mirror_lookup_record(record_id, token):
    """Miaoda read #1: every comment thread of ONE ticket. Single indexed
    records/search filtered on Record ID. Returns the stored payload list
    (possibly empty), or None when the table errored (caller falls back)."""
    body = {"filter": {"conjunction": "and", "conditions": [
                {"field_name": "Record ID", "operator": "is", "value": [record_id]}]}}
    rows, err = _mirror_search(body, token, pages=2)
    if err:
        return None
    return _mirror_row_payloads(rows)

def _mirror_lookup_feed(token):
    """Miaoda read #2: the newest thread rows for the bell feed — one call,
    sorted by Update Time desc. Returns payloads (possibly empty) or None on
    table error."""
    body = {"sort": [{"field_name": "Update Time", "desc": True}],
            "page_size": min(MIRROR_FEED_ROWS, MIRROR_PAGE)}
    rows, err = _mirror_search(body, token, pages=1)
    if err:
        return None
    return _mirror_row_payloads(rows)

def _mirror_thread_payload(c, record_id, people, numbering):
    """The full mirror Payload for one scan thread: replies flattened EXACTLY
    like the GET /api/comments response but with names/avatars pre-resolved
    (Miaoda stores author_name on the record — so do we), plus the ready-made
    feed row for the bell. Reading it back later needs zero name lookups."""
    replies = (c.get("reply_list") or {}).get("replies") or []
    def _p(uid):
        e = people.get(uid) or {}
        return {"name": e.get("name") or (uid or ""), "avatar": e.get("avatar") or ""}
    out_replies, mention_ids, thread_reactions, img_count = [], [], [], 0
    for r in replies:
        parts, plain = _reply_parts(r)
        reactions = _reply_reactions(r)
        uid = r.get("user_id")
        item = {
            "comment_id": c.get("comment_id"),
            "reply_id": r.get("reply_id"),
            "user_id": uid,
            "user_name": _p(uid)["name"],
            "user_avatar": _p(uid)["avatar"],
            "create_time": r.get("create_time") or c.get("create_time"),
            "parts": parts,
            "text": plain,
            "images": _reply_images(r),
            "is_solved": bool(c.get("is_solved")),
            "reactions": reactions,
        }
        for p in item["parts"]:
            if p.get("type") == "mention":
                p["name"] = _p(p.get("user_id"))["name"]
                if p.get("user_id"):
                    mention_ids.append(p["user_id"])
        for rx in item["reactions"]:
            rx["names"] = [_p(u)["name"] for u in rx["users"]]
        out_replies.append(item)
        img_count += len(item["images"])
        _, r_plain = _reply_parts(r)
        for rx in reactions:
            for reactor in rx["users"]:
                thread_reactions.append({
                    "key": rx["key"], "reactor_id": reactor,
                    "reactor_name": _p(reactor)["name"],
                    "reactor_avatar": _p(reactor)["avatar"],
                    "on_user_id": uid, "on_name": _p(uid)["name"],
                    "on_text": (r_plain or "")[:120],
                })
    last = replies[-1] if replies else {}
    last_parts, last_plain = _reply_parts(last)
    # Phase 18: the bell line shows "@Name" like the Base notification panel
    # instead of a bare "@", and carries typed text_parts so the frontend can
    # render the mention as a highlighted chip.
    feed_text_parts, feed_text = [], []
    for p in last_parts:
        if p.get("type") == "mention":
            nm = _p(p.get("user_id"))["name"]
            feed_text_parts.append({"type": "mention", "user_id": p.get("user_id"), "name": nm})
            feed_text.append("@" + nm)
        elif p.get("type") == "link":
            feed_text_parts.append({"type": "text", "text": p.get("url") or ""})
            feed_text.append(p.get("url") or "")
        else:
            feed_text_parts.append({"type": "text", "text": p.get("text") or ""})
            feed_text.append(p.get("text") or "")
    feed_text = "".join(feed_text).strip()
    feed = {
        "comment_id": c.get("comment_id"),
        "last_reply_id": last.get("reply_id"),
        "record_id": record_id,
        "author_id": last.get("user_id"),
        "author_name": _p(last.get("user_id"))["name"],
        "author_avatar": _p(last.get("user_id"))["avatar"],
        "time": c.get("update_time") or last.get("create_time"),
        "text": (feed_text or last_plain or "")[:180],
        "text_parts": feed_text_parts,
        "image_count": img_count,
        "reply_count": len(replies),
        "mention_ids": sorted(set(mention_ids)),
        "is_solved": bool(c.get("is_solved")),
        "reactions": thread_reactions,
        "numbering": numbering,
    }
    return {"v": 2,
            "comment_id": c.get("comment_id"), "record_id": record_id,
            "update_time": int(c.get("update_time") or 0),
            "is_solved": bool(c.get("is_solved")),
            "user_id": c.get("user_id"),
            "replies": out_replies, "feed": feed}

def _mirror_row_fields(payload):
    """The Bitable field dict for one mirror row."""
    fd = payload.get("feed") or {}
    return {
        "Comment ID":  payload.get("comment_id") or "",
        "Record ID":   payload.get("record_id") or fd.get("record_id") or "",
        "Numbering":   fd.get("numbering") or "",
        "Update Time": int(payload.get("update_time") or 0),
        "Reply Count": int(fd.get("reply_count") or 0),
        "Solved":      bool(payload.get("is_solved")),
        "Payload":     json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
    }

def _mirror_upsert(payloads, token):
    """Upsert mirror rows by Comment ID: one targeted search to find existing
    rows, then batch_update them / batch_create the missing ones (chunks of
    MIRROR_BATCH). Never raises on API errors — returns (updated, created)."""
    if not payloads:
        return 0, 0
    cids = [p.get("comment_id") for p in payloads if p.get("comment_id")]
    have = {}
    if cids:
        for i in range(0, len(cids), MIRROR_PAGE):
            body = {"filter": {"conjunction": "and", "conditions": [
                        {"field_name": "Comment ID", "operator": "is", "value": cids[i:i + MIRROR_PAGE]}]}}
            rows, err = _mirror_search(body, token, pages=2)
            if err:
                return 0, 0
            for row in rows:
                cid = _mirror_cell_text((row.get("fields") or {}).get("Comment ID"))
                if cid:
                    have[cid] = row.get("record_id")
    updates, creates = [], []
    for p in payloads:
        cid = p.get("comment_id")
        f = _mirror_row_fields(p)
        if cid and cid in have:
            updates.append({"record_id": have[cid], "fields": f})
        else:
            creates.append({"fields": f})
    # Phase 17: the batch calls used to ignore even the RESPONSE code — a
    # permission/shape rejection looked exactly like success (another way the
    # production mirror stayed silently empty). Non-zero codes and request
    # failures now land in _BG_ERRORS / _MIRROR_STATS, and the returned counts
    # reflect what the table actually accepted.
    done_u, done_c = 0, 0
    for i in range(0, len(updates), MIRROR_BATCH):
        chunk = updates[i:i + MIRROR_BATCH]
        try:
            resp = feishu_session.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{MIRROR_TABLE_ID}/records/batch_update",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json; charset=utf-8"},
                json={"records": chunk}, timeout=20)
            data = resp.json()
            if data.get("code") == 0:
                done_u += len(chunk)
            else:
                _bg_error("mirror_upsert", f"batch_update {data.get('code')}: {data.get('msg')}")
        except Exception as e:
            _bg_error("mirror_upsert", f"batch_update failed: {e}")
    for i in range(0, len(creates), MIRROR_BATCH):
        chunk = creates[i:i + MIRROR_BATCH]
        try:
            resp = feishu_session.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{MIRROR_TABLE_ID}/records/batch_create",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json; charset=utf-8"},
                json={"records": chunk}, timeout=20)
            data = resp.json()
            if data.get("code") == 0:
                done_c += len(chunk)
            else:
                _bg_error("mirror_upsert", f"batch_create {data.get('code')}: {data.get('msg')}")
        except Exception as e:
            _bg_error("mirror_upsert", f"batch_create failed: {e}")
    return done_u, done_c

def _mirror_thread_ids(c):
    """Every open_id the payload builder will ask names for."""
    ids = [c.get("user_id")]
    for r in ((c.get("reply_list") or {}).get("replies") or []):
        ids.append(r.get("user_id"))
        for p in _reply_parts(r)[0]:
            if p.get("type") == "mention":
                ids.append(p.get("user_id"))
        for rx in _reply_reactions(r):
            ids.extend(rx["users"])
    return [i for i in ids if i]

def _mirror_scan_thread(scan, record_id=None, comment_id=None, reply_id=None):
    """Locate one thread in the shared scan cache by record, comment id or
    reply id. Returns the raw scan item or None."""
    if not (isinstance(scan, dict) and isinstance(scan.get("items"), list)):
        return None
    for c in scan["items"]:
        if record_id is not None:
            if _comment_record_anchor(c).get("record") != record_id:
                continue
        if comment_id is not None and c.get("comment_id") != comment_id:
            continue
        if reply_id is not None:
            if not any(r.get("reply_id") == reply_id
                       for r in ((c.get("reply_list") or {}).get("replies") or [])):
                continue
        return c
    return None

def _mirror_refresh_thread(token, record_id=None, comment_id=None, reply_id=None):
    """Rebuild + upsert ONE thread's mirror row right after a portal-side
    comment/reaction landed (the write patch that makes portal changes instant
    for every 3-5s poll). Names resolve cache-only — the author/reactor is
    the logged-in user and their name is warm."""
    if not MIRROR_ENABLE:
        return False
    try:
        scan = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
    except Exception:
        scan = None
    c = _mirror_scan_thread(scan, record_id=record_id,
                            comment_id=comment_id, reply_id=reply_id)
    if c is None:
        return False
    rid = _comment_record_anchor(c).get("record") or ""
    people = {}
    try:
        people = _resolve_comment_names(_mirror_thread_ids(c), token, uat=None)
    except Exception:
        people = {}
    numbering = None
    if rid:
        try:
            numbering = _resolve_record_numberings([rid], token).get(rid)
        except Exception:
            numbering = None
    payload = _mirror_thread_payload(c, rid, people, numbering)
    try:
        _mirror_upsert([payload], token)
    except Exception:
        pass
    return True

# Phase 17: last-pass stats, surfaced by /api/comments/health so an empty or
# stale mirror is explainable at a glance instead of invisible.
_MIRROR_STATS = {"last_run": 0, "candidates": 0, "updated": 0, "created": 0,
                 "last_error": ""}

def _mirror_sync_pass(scan, token):
    """Background sync of the mirror table — called ONLY on the scan-refresh
    lock holder, right after the Phase 14 name backfill (so the name cache is
    warm). Reads the newest mirror rows once, then upserts ONLY threads whose
    update_time moved (or whose payload still shows masked names), capped at
    MIRROR_STORE_MAX. Unchanged threads cost nothing."""
    if not MIRROR_ENABLE:
        return
    if not (isinstance(scan, dict) and isinstance(scan.get("items"), list)):
        return
    rows, err = _mirror_search(
        {"sort": [{"field_name": "Update Time", "desc": True}],
         "page_size": MIRROR_PAGE}, token, pages=MIRROR_READ_PAGES_MAX)
    if err:
        _MIRROR_STATS["last_error"] = err
        _bg_error("mirror_search", err)
        return
    have = {}
    for row in rows:
        f = row.get("fields") or {}
        cid = _mirror_cell_text(f.get("Comment ID"))
        if cid:
            have[cid] = (_mirror_cell_num(f.get("Update Time")),
                         _mirror_cell_text(f.get("Payload")),
                         row.get("record_id"))
    todo, todo_ids, rec_ids = [], [], set()
    for c in scan["items"]:
        anchor = _comment_record_anchor(c)
        if anchor.get("table") != REQUESTS_TABLE_ID or not anchor.get("record"):
            continue
        cid = c.get("comment_id")
        if not cid:
            continue
        uts = int(c.get("update_time") or 0)
        cur = have.get(cid)
        # Phase 18: '"v":2' — v1 payloads (bare "@" bell text, no text_parts)
        # re-sync once even when unchanged, so the Base-style mention-named
        # bell lines roll out without waiting for a thread update.
        if cur and cur[0] >= uts and '"user-' not in cur[1] and '"v":2' in cur[1]:
            continue  # unchanged AND fully named AND current version — free
        todo.append((c, anchor["record"]))
    if not todo:
        _MIRROR_STATS.update({"last_run": int(time.time()), "candidates": 0,
                              "updated": 0, "created": 0, "last_error": ""})
        return
    todo.sort(key=lambda x: int(x[0].get("update_time") or 0), reverse=True)
    todo = todo[:MIRROR_STORE_MAX]
    for c, rid in todo:
        todo_ids.extend(_mirror_thread_ids(c))
        rec_ids.add(rid)
    people = {}
    nums = {}
    try:
        people = _resolve_comment_names(todo_ids, token, uat=None)
    except Exception as e:
        _bg_error("mirror_names", e)
        people = {}
    try:
        nums = _resolve_record_numberings(list(rec_ids), token)
    except Exception as e:
        _bg_error("mirror_numbering", e)
        nums = {}
    payloads = [_mirror_thread_payload(c, rid, people, nums.get(rid)) for c, rid in todo]
    try:
        done_u, done_c = _mirror_upsert(payloads, token)
        _MIRROR_STATS.update({"last_run": int(time.time()), "candidates": len(todo),
                              "updated": done_u, "created": done_c, "last_error": ""})
    except Exception as e:
        _MIRROR_STATS["last_error"] = str(e)[:300]
        _bg_error("mirror", e)

def _scan_has_record(scan, record_id):
    """True when the shared scan holds at least one thread for this record —
    used to detect "mirror is empty because the sync hasn't reached it yet"
    and fall back to the scan path instead of serving an empty thread."""
    if not (isinstance(scan, dict) and isinstance(scan.get("items"), list)):
        return False
    return any(_comment_record_anchor(c).get("record") == record_id for c in scan["items"])

def _caller_open_id(email):
    """The logged-in caller's open_id from the email->open_id bridge cache
    (xena:uemail:*, written by person-field harvests / the directory warm).
    '' when unknown — purely cosmetic uses (cache patches), never auth."""
    em = (email or "").strip().lower()
    if not em:
        return ""
    try:
        v = redis_get_json(f"xena:uemail:{em}")
        return v if isinstance(v, str) else ""
    except Exception:
        return ""

def _harvest_person_names(obj):
    """Walk a record's fields (any nesting) and Redis-cache every person
    object {id: ou_..., name: ...} as xena:uname:{id}. Person field values
    from the Bitable API carry the real display name even though this app
    lacks contact-book READ scope (contact:user.base:readonly) -- so every
    ticket the portal touches teaches the comment/avatar resolver a few more
    teammates for free. Existing cached avatars are preserved."""
    # Returns how many person entries were NEWLY written this call (Phase 12:
    # the admin bulk-harvest endpoint reports this as progress). Existing
    # callers ignore the return value.
    written = 0
    try:
        if isinstance(obj, dict):
            oid, nm = obj.get("id"), obj.get("name")
            av = obj.get("avatar_url") or ""   # Phase 11.2: Bitable person
            # dicts carry avatar_url (+email/en_name) — verified live on the
            # Agent Roster. Cache it too so comment author / respondent /
            # presence-board photos resolve with ZERO contact scopes.
            if isinstance(oid, str) and oid.startswith("ou_") and isinstance(nm, str) and nm.strip():
                try:
                    ck = f"xena:uname:{oid}"
                    cached = redis_get_json(ck)
                    has_name = isinstance(cached, dict) and cached.get("name")
                    has_av = isinstance(cached, dict) and (cached.get("avatar") or "")
                    # Write only when it adds something: first sighting, or an
                    # avatar upgrade. Never downgrade a cached avatar to "".
                    if not has_name or (av and not has_av):
                        redis_set_json(ck, {"name": nm.strip(), "avatar": (has_av or av or "")},
                                       ttl=HARVEST_NAME_TTL)
                        written += 1
                    # Phase 11.3: email -> open_id bridge (person dicts carry
                    # email) so the directory warm can skip a live user-search
                    # for anyone already seen in a person cell.
                    em = (obj.get("email") or "").strip().lower()
                    if em:
                        ek = f"xena:uemail:{em}"
                        if not redis_get_json(ek):
                            redis_set_json(ek, oid, ttl=30 * 86400)
                except Exception:
                    pass
            for v in obj.values():
                written += _harvest_person_names(v)
        elif isinstance(obj, (list, tuple)):
            for v in obj:
                written += _harvest_person_names(v)
    except Exception:
        pass
    return written

# Phase 11.7 (Ahmed: "i has Respondents field that shows the submitters so
# please check can we give it try and if it not cost so many"): the Grand
# Table's person-ish fields whose inline dicts resolve an open_id WITHOUT
# any contact scope. "Respondents" IS the created_by field — the SUBMITTER
# (confirmed against the field list: fldAE3MeYy, type created_by). Verified
# live on the Grand Table 2026-10-16: the `contains` operator matches
# open_ids on BOTH user-type and created_by-type fields (Ahmed himself:
# 6,163 Assigned-Member records, 2,002 Respondents records).
BITABLE_PERSON_FIELDS  = ("Respondents", "Assigned Member", "Done by",
                          "Mentioned Person", "Lock Owner")
BITABLE_LOOKUP_MAX     = 8       # max targeted searches per resolve call
BITABLE_LOOKUP_NEG_TTL = 3600    # ids with no person-field hit retry hourly
AVATAR_LOOKUP_MAX      = 6       # max parallel avatar searches per resolve call (Phase 12)
AVATAR_LOOKUP_NEG_TTL  = 21600   # ids with no avatar-search hit retry after 6h

# Phase 12: emoji whitelist for /api/comments/react — the COMPLETE official
# Feishu drive v2 comment reaction_type enum (docs "Add/Cancel Emoji
# Response", open.larkoffice.com .../drive-v2/comment_reaction/update_reaction,
# fetched 2026-10-18). CASE-SENSITIVE: the tail entries are mixed-case on
# Feishu's side ("ThumbsDown", "Fire", "CheckMark"...), so validation maps any
# incoming casing to the canonical form below and sends THAT verbatim —
# before this, .upper() silently corrupted the mixed-case types ("i do
# something but appear another thing in the sheet").
COMMENT_REACT_EMOJIS = (
    "ANGRY", "APPLAUSE", "ATTENTION", "AWESOME", "BEAR", "BEER", "BETRAYED",
    "BIGKISS", "BLACKFACE", "BLUBBER", "BLUSH", "BOMB", "CAKE", "CHUCKLE",
    "CLAP", "CLEAVER", "COMFORT", "CRAZY", "CRY", "CUCUMBER", "DETERGENT",
    "DIZZY", "DONE", "DONNOTGO", "DROOL", "DROWSY", "DULL", "DULLSTARE",
    "EATING", "EMBARRASSED", "ENOUGH", "ERROR", "EYESCLOSED", "FACEPALM",
    "FINGERHEART", "FISTBUMP", "FOLLOWME", "FROWN", "GIFT", "GLANCE",
    "GOODJOB", "HAMMER", "HAUGHTY", "HEADSET", "HEART", "HEARTBROKEN",
    "HIGHFIVE", "HUG", "HUSKY", "INNOCENTSMILE", "JIAYI", "JOYFUL", "KISS",
    "LAUGH", "LIPS", "LOL", "LOOKDOWN", "LOVE", "MONEY", "MUSCLE", "NOSEPICK",
    "OBSESSED", "OK", "PARTY", "PETRIFIED", "POOP", "PRAISE", "PROUD", "PUKE",
    "RAINBOWPUKE", "ROSE", "SALUTE", "SCOWL", "SHAKE", "SHHH", "SHOCKED",
    "SHOWOFF", "SHY", "SICK", "SILENT", "SKULL", "SLAP", "SLEEP", "SLIGHT",
    "SMART", "SMILE", "SMIRK", "SMOOCH", "SMUG", "SOB", "SPEECHLESS",
    "SPITBLOOD", "STRIVE", "SWEAT", "TEARS", "TEASE", "TERROR", "THANKS",
    "THINKING", "THUMBSUP", "TOASTED", "TONGUE", "TRICK", "UPPERLEFT",
    "WAIL", "WAVE", "WELLDONE", "WHAT", "WHIMPER", "WINK", "WITTY", "WOW",
    "WRONGED", "XBLUSH", "YAWN", "YEAH", "FIREWORKS", "BULL", "CALF",
    "AWESOMEN", "2021", "CANDIEDHAWS", "REDPACKET", "FORTUNE", "LUCK",
    "FIRECRACKER", "Yes", "No", "Get", "LGTM", "Lemon", "EatingFood",
    "Hundred", "MinusOne", "ThumbsDown", "Fire", "OKR", "Drumstick",
    "BubbleTea", "Loudspeaker", "Pin", "Coffee", "Alarm", "Trophy", "Music",
    "Typing", "Pepper", "CheckMark", "CrossMark",
)
# case-insensitive lookup -> canonical (exact-case) reaction_type
COMMENT_REACT_CANON = {e.upper(): e for e in COMMENT_REACT_EMOJIS}

def _person_lookup_via_bitable(open_id, tat):
    """Phase 11.7: targeted, in-request open_id -> {name, avatar} resolution
    via the Grand Table's own person fields — the scope-free directory,
    instant instead of waiting for the background sweep cursor. ONE
    records/search with an or-filter across all five person-ish fields;
    person dicts ride inline (name + avatar_url + email), so a hit needs
    ZERO contact scopes and ignores 通讯录授权范围 entirely. Returns
    {"name","avatar"} or None. Cost: exactly one Bitable search per call."""
    if not open_id or not tat:
        return None
    try:
        resp = feishu_session.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search",
            headers={"Authorization": f"Bearer {tat}",
                     "Content-Type": "application/json; charset=utf-8"},
            json={"page_size": 3,
                  "field_names": list(BITABLE_PERSON_FIELDS),
                  "filter": {"conjunction": "or", "conditions": [
                      {"field_name": f, "operator": "contains", "value": [open_id]}
                      for f in BITABLE_PERSON_FIELDS]}},
            timeout=8)
        data = resp.json()
    except Exception:
        return None
    if data.get("code") != 0:
        return None
    for item in ((data.get("data") or {}).get("items") or []):
        fields = item.get("fields") or {}
        for fname in BITABLE_PERSON_FIELDS:
            values = fields.get(fname) or []
            if isinstance(values, dict):
                values = [values]
            for v in values:
                if (isinstance(v, dict) and v.get("id") == open_id
                        and (v.get("name") or "").strip()):
                    return {"name": v["name"].strip(),
                            "avatar": v.get("avatar_url") or ""}
    return None

def _directory_resolve_masked(masked, uat, found):
    """Resolve still-masked open_ids (name == "user-xxxx") via the team
    directory: the Access Control table holds every portal member's login
    EMAIL, and Feishu's "Search users" API (contact:user:search — the scope
    this app DOES have; UAT-only) maps an email to open_id + name + avatar.
    This is how the portal matches the Base UI's names/photos with ZERO
    extra permissions — verified live 2026-10-16 (query by email returns the
    exact member). Costs: 1 access-table read per 6h (Redis-cached), plus at
    most DIR_SEARCH_BUDGET live searches per call, each result cached 7d —
    so after the first warm a whole day of comment reads does ~zero
    searches. Entries resolved here land in the same xena:uname cache the
    harvest writes, so every resolver consumer (comments thread, feed,
    reactions, users/lookup, presence board) benefits at once."""
    if not masked or not uat:
        return
    try:
        members = redis_get_json(DIR_MEMBERS_KEY)
    except Exception:
        members = None
    if not isinstance(members, list):
        members = []
        try:
            resp = feishu_session.get(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records",
                headers={"Authorization": f"Bearer {get_tenant_access_token()}"},
                params={"page_size": 500}, timeout=15)
            data = resp.json()
            if data.get("code") == 0:
                for it in ((data.get("data") or {}).get("items") or []):
                    em = extract_field_text((it.get("fields") or {}).get("Email", "")).strip().lower()
                    if em:
                        members.append(em)
        except Exception:
            members = []
        try:
            redis_set_json(DIR_MEMBERS_KEY, members, ttl=DIR_MEMBERS_TTL)
        except Exception:
            pass
    if not members:
        return
    still = set(masked)
    searches = 0
    for em in members:
        if not still:
            break
        mk = f"xena:dirmail:{em}"
        try:
            m = redis_get_json(mk)
        except Exception:
            m = None
        if m is None:
            # Free bridge first: a person-field harvest may already have
            # mapped this email to an open_id (roster / ticket person cells).
            m = None
            try:
                bridged = redis_get_json(f"xena:uemail:{em}")
            except Exception:
                bridged = None
            if isinstance(bridged, str) and bridged.startswith("ou_"):
                cached = redis_get_json(f"xena:uname:{bridged}") or {}
                m = {"open_id": bridged, "name": (cached.get("name") or ""),
                     "avatar": (cached.get("avatar") or "")}
            elif searches < DIR_SEARCH_BUDGET:
                searches += 1
                try:
                    resp = http_requests.get(
                        "https://open.feishu.cn/open-apis/search/v1/user",
                        headers={"Authorization": f"Bearer {uat}"},
                        params={"query": em, "page_size": 5}, timeout=8)
                    sdata = resp.json()
                except Exception:
                    continue
                if sdata.get("code") != 0:
                    break  # UAT expired / scope gone -- further searches pointless
                users = (sdata.get("data") or {}).get("users") or []
                if users:
                    u0 = users[0]
                    m = {"open_id": u0.get("open_id") or "",
                         "name": u0.get("name") or "",
                         "avatar": (u0.get("avatar") or {}).get("avatar_72") or ""}
                else:
                    m = {"open_id": ""}  # negative-cache: no such member
            else:
                break  # search budget spent -- leave the rest masked
            try:
                # Positive mappings are immutable (7d); negative results use the
                # short DIR_NEG_TTL so a failed lookup retries within the hour
                # instead of sticking for a week (Phase 11.4).
                redis_set_json(mk, m or {"open_id": ""},
                               ttl=(DIR_EMAIL_TTL if (m or {}).get("open_id") else DIR_NEG_TTL))
            except Exception:
                pass
            if m and m.get("open_id") and m.get("name"):
                try:
                    redis_set_json(f"xena:uname:{m['open_id']}",
                                   {"name": m["name"], "avatar": m.get("avatar") or ""},
                                   ttl=COMMENT_NAME_TTL)
                except Exception:
                    pass
        oid = (m or {}).get("open_id") or ""
        if oid and oid in still and (m.get("name") or ""):
            found[oid] = {"name": m["name"], "avatar": m.get("avatar") or ""}
            still.discard(oid)

def _sweep_step(token):
    """One incremental slice of the full-table person sweep (Phase 11.4 —
    Ahmed: "i want it work for all people"). The Grand Table's own records
    are the widest directory this app can read with its APPROVED scopes:
    every person field (Assigned Member / Done by / Respondents / ...) plus
    the created_by / last_modified_by envelopes carries the real display
    name (+photo) of anyone who ever touched a ticket — and people who
    comment on tickets are exactly the people who work them. A full pass
    walks the whole table SWEEP_PAGES_PER_RUN pages at a time (cursor in
    Redis), so no single request pays more than a couple of seconds; once a
    pass completes it re-arms after SWEEP_REARM_TTL. Runs on the tenant
    token — no UAT needed. Returns True if this call actually swept pages."""
    now = time.time()
    try:
        st = redis_get_json(SWEEP_KEY)
    except Exception:
        st = None
    if not isinstance(st, dict):
        st = {}
    if st.get("done") and (now - float(st.get("done_ts") or 0)) < SWEEP_REARM_TTL:
        return False
    if (now - float(st.get("run_ts") or 0)) < SWEEP_THROTTLE:
        return False
    st["run_ts"] = now
    if st.get("done"):
        st, page_token = {"run_ts": now}, None   # re-armed: fresh pass
    else:
        page_token = st.get("page_token") or None
    try:
        redis_set_json(SWEEP_KEY, st, ttl=SWEEP_REARM_TTL + 3600)
    except Exception:
        pass
    tat = get_tenant_access_token() or token
    if not tat:
        return False
    pages = 0
    try:
        while pages < SWEEP_PAGES_PER_RUN:
            body = {"page_size": 500}
            if page_token:
                body["page_token"] = page_token
            resp = feishu_session.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true",
                headers={"Authorization": f"Bearer {tat}"}, json=body, timeout=20)
            data = resp.json()
            if data.get("code") != 0:
                break
            d = data.get("data") or {}
            for it in (d.get("items") or []):
                _harvest_person_names(it)  # whole item: fields + created_by/last_modified_by
            pages += 1
            if not d.get("has_more") or not d.get("page_token"):
                st = {"run_ts": st.get("run_ts"), "done": True, "done_ts": time.time()}
                page_token = None
                break
            page_token = d.get("page_token")
        if page_token:
            st = {"run_ts": st.get("run_ts"), "done": False, "page_token": page_token}
        try:
            redis_set_json(SWEEP_KEY, st, ttl=SWEEP_REARM_TTL + 3600)
        except Exception:
            pass
    except Exception:
        pass
    return pages > 0

def _harvest_bulk(pages_wanted, token=None):
    """Phase 12 (Ahmed: "please please find sol that the name that is not in
    the sheet not appare and appare like user ... or you can make it"): the
    admin-triggered BULK harvest. Same machinery as _sweep_step — walk the
    Grand Table feeding every person envelope (person fields + created_by /
    last_modified_by) into _harvest_person_names — but N pages in ONE call,
    no throttle, sharing the sweep cursor in SWEEP_KEY so the incremental
    sweep simply continues wherever this stopped. This is the scope-free
    directory: NO contact visibility / Route A needed for anyone who ever
    touched a ticket. Returns {pages, harvested, done, error?}."""
    try:
        st = redis_get_json(SWEEP_KEY)
    except Exception:
        st = None
    if not isinstance(st, dict):
        st = {}
    if st.get("done"):
        return {"pages": 0, "harvested": 0, "done": True}
    page_token = st.get("page_token") or None
    tat = get_tenant_access_token() or token
    if not tat:
        return {"pages": 0, "harvested": 0, "done": False, "error": "no tenant token"}
    pages, harvested, done = 0, 0, False
    try:
        while pages < max(1, int(pages_wanted or 1)):
            body = {"page_size": 500}
            if page_token:
                body["page_token"] = page_token
            resp = feishu_session.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true",
                headers={"Authorization": f"Bearer {tat}"}, json=body, timeout=20)
            data = resp.json()
            if data.get("code") != 0:
                return {"pages": pages, "harvested": harvested, "done": False,
                        "error": f"{data.get('code')}: {data.get('msg')}"}
            d = data.get("data") or {}
            for it in (d.get("items") or []):
                harvested += _harvest_person_names(it)
            pages += 1
            if not d.get("has_more") or not d.get("page_token"):
                done, page_token = True, None
                break
            page_token = d.get("page_token")
    except Exception as e:
        return {"pages": pages, "harvested": harvested, "done": False, "error": str(e)}
    st = {"run_ts": time.time(), "done": done}
    if done:
        st["done_ts"] = time.time()
    else:
        st["page_token"] = page_token
    try:
        redis_set_json(SWEEP_KEY, st, ttl=SWEEP_REARM_TTL + 3600)
    except Exception:
        pass
    return {"pages": pages, "harvested": harvested, "done": done}

# Phase 13 (Ahmed 2026-10-19 — "stop this stupid name check, it consumes the
# website"): all live name lookups are DISABLED by default. The resolver is
# cache-only: names come from the sheet harvest (Admin → Fix missing names)
# and the login self-cache; anything uncached shows the masked id instantly.
# Zero Feishu calls on any comment/feed/presence request. The legacy network
# legs stay below (COMMENT_NAME_LIVE_LOOKUP=True re-enables them) so the
# historical test suites keep exercising them.
COMMENT_NAME_LIVE_LOOKUP = False


def _resolve_comment_names(user_ids, token, uat=None, allow_sweep=False, force_live=False):
    """open_id -> {"name", "avatar"} -- real Lark display names AND profile
    photos. Caching (Phase 11.4): live lookups cached 1h, sheet-harvested
    names 24h, masked ids NEVER cached (they retry on every call).
    Resolution chain per chunk: TENANT token first
    (the app's own contact scope -- the caller's UAT usually lacks it, which
    is why comment authors used to fall back to masked "user-xxxx"), then the
    caller's UAT, then a masked short id so the UI never prints a raw open_id.
    Cache entries store name+avatar together; an entry with only a name is
    still valid (avatar may genuinely be empty).
    Phase 10.47: the contact-batch API needs contact:user.base:readonly,
    which this app does NOT have -- so both token legs usually fail and the
    masked fallback fires. Two scope-free sources now fill the gap:
    _harvest_person_names (names ride along in person fields of every record
    the portal touches) and, for the avatar, Feishu's "Search users" API
    (contact:user:search -- the scope the app DOES have), queried by the
    known display name and matched back strictly by open_id."""
    found, missing = {}, []
    for uid in dict.fromkeys(u for u in user_ids if u):
        ck = f"xena:uname:{uid}"
        try:
            cached = redis_get_json(ck)
        except Exception:
            cached = None
        if isinstance(cached, dict) and cached.get("name"):
            found[uid] = {"name": cached["name"], "avatar": cached.get("avatar") or ""}
        else:
            missing.append(uid)
    if not COMMENT_NAME_LIVE_LOOKUP and not force_live:
        # Phase 13: cache-only — masked seed, no contact batch, no directory
        # leg, no Bitable leg, no avatar pool. This is what makes every
        # comment / feed / presence request instant and free.
        # Phase 14 force_live: the BACKGROUND scan-refresh backfill
        # (_scan_backfill_names) re-enables the legs below for one capped
        # pass — user-facing requests never pass force_live, so they stay
        # instant; the legs only ever run out-of-band.
        for uid in missing:
            found[uid] = {"name": ("user-" + uid[-4:] if len(uid) >= 4 else uid), "avatar": ""}
        return found
    # Candidate tokens in priority order, de-duplicated, Nones dropped.
    # Phase 11.6: the caller's UAT is a FIRST-CLASS candidate here — it was
    # missing before, so a data-restricted/scope-dead TAT ended the batch leg
    # even when the UAT could see every user. The fall-through below now has
    # something real to fall through TO.
    tokens = []
    for t in (get_tenant_access_token(), token, uat):
        if t and t not in tokens:
            tokens.append(t)
    for i in range(0, len(missing), 50):
        chunk = missing[i:i + 50]
        chunk_resolved = 0
        for tk in tokens:
            try:
                params = [("user_ids", u) for u in chunk]
                params.append(("user_id_type", "open_id"))
                resp = feishu_session.get(
                    "https://open.feishu.cn/open-apis/contact/v3/users/batch",
                    headers={"Authorization": f"Bearer {tk}"}, params=params, timeout=12)
                data = resp.json()
            except Exception:
                continue
            if data.get("code") != 0:
                continue  # scope missing on this token -- try the next one
            for u in ((data.get("data") or {}).get("items") or []):
                oid, nm = u.get("open_id"), u.get("name")
                av = (u.get("avatar") or {}).get("avatar_72") or ""
                if oid and nm and oid not in found:
                    found[oid] = {"name": nm, "avatar": av}
                    redis_set_json(f"xena:uname:{oid}", {"name": nm, "avatar": av}, ttl=COMMENT_NAME_TTL)
                    chunk_resolved += 1
            # Phase 11.6 (Ahmed's scope-check: TAT returned code:0 but ZERO
            # users — the app's 通讯录授权范围 hides those employees): a code:0
            # with an empty/partial items list is NOT an answer. Only stop
            # trying tokens once THIS chunk is fully resolved; otherwise fall
            # through to the UAT, which sees everyone the caller can see.
            if chunk_resolved >= len(chunk):
                break
        # all tokens tried (or chunk resolved) -- whatever is still missing
        # falls through to the masked seed + directory/single-GET legs below
    for uid in missing:
        if uid not in found:
            found[uid] = {"name": ("user-" + uid[-4:] if len(uid) >= 4 else uid), "avatar": ""}
    # Phase 11.3: anyone still masked gets one shot through the team
    # directory (access-table emails -> search/v1/user) — the scope-free way
    # to match the Base UI's names/photos for EVERY teammate.
    masked_now = [uid for uid, e in found.items() if (e.get("name") or "").startswith("user-")]
    if masked_now and uat:
        try:
            _directory_resolve_masked(masked_now, uat, found)
        except Exception:
            pass
    # Phase 11.8: the two per-id legs below are PARALLEL (ThreadPoolExecutor)
    # and the scope-free Bitable leg now runs FIRST. The old sequential loops
    # cost up to ~16 cross-border round trips (8-20s from Vercel) whenever
    # several masked ids were pending — Ahmed's "comments load so so much"
    # and the slow Audition Status board both traced to this. A bounded pool
    # costs ~1 round trip per leg instead.

    # Phase 11.7 (moved ahead of the contact leg in 11.8): targeted Bitable
    # lookup — instant, in-request, needs ZERO contact scopes, so in tenants
    # where 通讯录授权范围 hides everyone (ours, today) this is the leg that
    # actually resolves. Cost: ONE records/search per masked id (cap
    # BITABLE_LOOKUP_MAX per call); a hit is cached 24h (HARVEST_NAME_TTL);
    # a miss is negative-cached 1h so a genuinely-absent id costs at most
    # one search per hour. Tenant token first (the app is a collaborator on
    # the base), UAT fallback.
    masked_now = [uid for uid, e in found.items() if (e.get("name") or "").startswith("user-")]
    if masked_now:
        bitable_tokens = []
        for t in (get_tenant_access_token(), uat):
            if t and t not in bitable_tokens:
                bitable_tokens.append(t)
        if bitable_tokens:
            todo = []
            for uid in masked_now:
                try:
                    if redis_get_json(f"xena:uname:neg:{uid}"):
                        continue
                except Exception:
                    pass
                todo.append(uid)
            todo = todo[:BITABLE_LOOKUP_MAX]
            def _bt_one(uid):
                for tk in bitable_tokens:
                    hit = _person_lookup_via_bitable(uid, tk)
                    if hit is not None:
                        return uid, hit
                return uid, None
            if todo:
                with ThreadPoolExecutor(max_workers=min(6, len(todo))) as pool:
                    for uid, hit in pool.map(_bt_one, todo):
                        if hit and hit.get("name"):
                            found[uid] = hit
                            try:
                                redis_set_json(f"xena:uname:{uid}",
                                               {"name": hit["name"], "avatar": hit.get("avatar") or ""},
                                               ttl=HARVEST_NAME_TTL)
                            except Exception:
                                pass
                        else:
                            try:
                                redis_set_json(f"xena:uname:neg:{uid}", {"ts": time.time()},
                                               ttl=BITABLE_LOOKUP_NEG_TTL)
                            except Exception:
                                pass
    # Per-id contact GET — the second live leg (see the batch leg's note
    # above): tenant token first (zero UAT dependency), then the caller's
    # UAT (carries the user scope after a fresh login). Universal when the
    # app's contact visibility allows it: resolves ANY open_id, portal
    # member or not. Parallel + capped at 8 ids; a miss is negative-cached
    # 1h (xena:uname:negc) so a data-restricted id stops costing round
    # trips on every single comment load.
    masked_now = [uid for uid, e in found.items() if (e.get("name") or "").startswith("user-")]
    if masked_now:
        contact_tokens = []
        for t in (get_tenant_access_token(), uat):
            if t and t not in contact_tokens:
                contact_tokens.append(t)
        if contact_tokens:
            todo = []
            for uid in masked_now:
                try:
                    if redis_get_json(f"xena:uname:negc:{uid}"):
                        continue
                except Exception:
                    pass
                todo.append(uid)
            todo = todo[:8]
            dead = set()
            def _cg_one(uid):
                for tk in contact_tokens:
                    if tk in dead:
                        continue
                    try:
                        resp = feishu_session.get(
                            f"https://open.feishu.cn/open-apis/contact/v3/users/{uid}",
                            headers={"Authorization": f"Bearer {tk}"},
                            params={"user_id_type": "open_id"}, timeout=8)
                        data = resp.json()
                    except Exception:
                        continue
                    if data.get("code") != 0:
                        dead.add(tk)  # scope missing on this token — stop its leg
                        continue
                    u = (data.get("data") or {}).get("user") or {}
                    if not (u.get("name") or ""):
                        # Phase 11.6: code:0 with an EMPTY user object usually
                        # means the app's 通讯录授权范围 hides this employee
                        # from the token — not that the user is gone. Try the
                        # next token before giving up.
                        continue
                    return uid, u
                return uid, None
            if todo:
                with ThreadPoolExecutor(max_workers=min(6, len(todo))) as pool:
                    for uid, u in pool.map(_cg_one, todo):
                        if u:
                            nm = u.get("name") or ""
                            av = (u.get("avatar") or {}).get("avatar_72") or ""
                            found[uid] = {"name": nm, "avatar": av}
                            try:
                                redis_set_json(f"xena:uname:{uid}", {"name": nm, "avatar": av},
                                               ttl=COMMENT_NAME_TTL)
                                em = (u.get("email") or u.get("enterprise_email") or "").strip().lower()
                                if em and not redis_get_json(f"xena:uemail:{em}"):
                                    redis_set_json(f"xena:uemail:{em}", uid, ttl=30 * 86400)
                            except Exception:
                                pass
                        else:
                            try:
                                redis_set_json(f"xena:uname:negc:{uid}", {"ts": time.time()},
                                               ttl=BITABLE_LOOKUP_NEG_TTL)
                            except Exception:
                                pass
    # Phase 11.4: still masked after the member directory? Sweep the Grand
    # Table itself (incremental, throttled) — anyone who ever touched a
    # ticket gets harvested straight from the sheet's own data, so coverage
    # extends to the WHOLE team, not just portal members. Masked ids are
    # never written to any cache: every comment load retries them live.
    # Phase 12: the sweep slice now runs ONLY when allow_sweep=True (the
    # background scan-refresh path). Interactive comment/feed loads just
    # re-check the caches — a 3-page cross-border sweep (6-10s) inside a
    # user-facing request was one of the "30s to appear" contributors.
    masked_now = [uid for uid, e in found.items() if (e.get("name") or "").startswith("user-")]
    if masked_now:
        swept = False
        if allow_sweep:
            try:
                swept = bool(_sweep_step(token))
            except Exception:
                swept = False
        if swept or not allow_sweep:
            for uid in masked_now:
                try:
                    cached = redis_get_json(f"xena:uname:{uid}")
                except Exception:
                    cached = None
                if isinstance(cached, dict) and cached.get("name"):
                    found[uid] = {"name": cached["name"], "avatar": cached.get("avatar") or ""}
    # Avatar backfill via Feishu's "Search users" API (contact:user:search --
    # the scope this app DOES have). Only for entries that already have a real
    # name (harvested from person fields / cached) but no photo yet; the hit
    # is matched back strictly by open_id, so same-name teammates can't cross.
    # Phase 12: this loop was UNBOUNDED and SEQUENTIAL with NO negative cache
    # — N named-but-photoless people cost N back-to-back cross-border searches
    # (~1-2s each) on EVERY comment/feed load. This was the main "30+ seconds
    # to appear" bug. Now: parallel pool, capped per call, and a miss is
    # negative-cached 6h so an unsearchable name stops costing round trips.
    if uat:
        av_todo = []
        for uid, entry in list(found.items()):
            nm = (entry.get("name") or "").strip()
            if entry.get("avatar") or not nm or nm.startswith("user-"):
                continue
            try:
                if redis_get_json(f"xena:uname:negav:{uid}"):
                    continue
            except Exception:
                pass
            av_todo.append((uid, nm))
        av_todo = av_todo[:AVATAR_LOOKUP_MAX]
        def _av_one(pair):
            uid, nm = pair
            try:
                resp = http_requests.get(
                    "https://open.feishu.cn/open-apis/search/v1/user",
                    headers={"Authorization": f"Bearer {uat}"},
                    params={"query": nm, "page_size": 10}, timeout=8)
                sdata = resp.json()
            except Exception:
                return uid, None
            if sdata.get("code") != 0:
                return uid, None
            for u in ((sdata.get("data") or {}).get("users") or []):
                if u.get("open_id") == uid:
                    av = (u.get("avatar") or {}).get("avatar_72") or ""
                    if av:
                        return uid, av
                    break
            return uid, None
        if av_todo:
            with ThreadPoolExecutor(max_workers=min(6, len(av_todo))) as pool:
                for uid, av in pool.map(_av_one, av_todo):
                    if av:
                        entry = found.get(uid)
                        if entry is not None:
                            entry["avatar"] = av
                        try:
                            redis_set_json(f"xena:uname:{uid}",
                                           {"name": (entry or {}).get("name") or "", "avatar": av},
                                           ttl=COMMENT_NAME_TTL)
                        except Exception:
                            pass
                    else:
                        try:
                            redis_set_json(f"xena:uname:negav:{uid}", {"ts": time.time()},
                                           ttl=AVATAR_LOOKUP_NEG_TTL)
                        except Exception:
                            pass
    return found

# Phase 18 (Ahmed: "the names not appare and the avatar ... even i refreshed
# sevral time and wait 2 hour"): interactive comment/feed loads are
# cache-only by design (Phase 13), and the background backfill that fills
# the cache can starve for days when the chain dies early or Redis is down —
# that's why 'user-xxxx' names never healed for him. The fix that works
# regardless of background health: heal the FEW masked names the caller is
# literally looking at, inline, right now — bounded to INLINE_RESOLVE_MAX
# ids per request so the instant-load contract survives (the resolver's own
# negative caches bound repeat probes to ~1/id/hour).
INLINE_RESOLVE_MAX = 6

def _resolve_inline_masked(ids, token, uat, people):
    """Patch `people` in place: any id still masked after the cache-only
    pass gets ONE bounded live-resolution attempt (≤ INLINE_RESOLVE_MAX ids,
    first-seen order = the newest/most-visible entries). Resolved entries
    land in the shared name cache, so the next view — and the background
    mirror sync — inherit them. Never raises; on any failure `people` is
    returned unchanged."""
    masked = [u for u in dict.fromkeys(ids)
              if u and (people.get(u, {}).get("name") or "").startswith("user-")]
    if not masked:
        return people
    try:
        live = _resolve_comment_names(masked[:INLINE_RESOLVE_MAX], token,
                                      uat=uat, force_live=True)
    except Exception:
        return people
    for u, e in (live or {}).items():
        if e and (e.get("name") or "") and not e["name"].startswith("user-"):
            people[u] = e
    return people

def _comments_caller():
    """Shared gate + identity for the comment endpoints. Returns
    (user, email, uat, token, error_response)."""
    user  = sanitize_text(request.args.get('user', '') or (request.get_json(silent=True) or {}).get('user', ''))
    email = sanitize_text(request.args.get('email', '') or (request.get_json(silent=True) or {}).get('email', ''))
    uat   = sanitize_text(request.args.get('uat', '') or (request.get_json(silent=True) or {}).get('uat', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("modules") and not perms.get("is_super_admin"):
        return user, email, uat, None, (jsonify({"success": False, "error": "Access denied"}), 403)
    # Phase 13 (Ahmed): comments are their own module now — admins grant the
    # 💬 Comments & Notifications toggle per member, exactly like 💾 Data
    # Export. Super-admins always pass.
    if not perms.get("is_super_admin") and "comments" not in perms.get("modules", []):
        return user, email, uat, None, (jsonify({"success": False,
            "error": "Comments access denied. Ask your admin to enable the 💬 Comments module for you."}), 403)
    # Reads can ride the tenant token (the app is a collaborator on the base);
    # the caller's UAT is preferred when present so read authorship checks and
    # any user-scoped comment permissions behave exactly like the Base UI.
    token = uat or get_tenant_access_token()
    return user, email, uat, token, None

def _reply_reactions(reply):
    """Normalize a reply's Feishu reaction list to
    [{key, count, users:[open_id...]}] -- always a list, never None."""
    out = []
    for r in (reply.get("reactions") or []):
        key = r.get("reaction_key")
        if not key:
            continue
        out.append({"key": key,
                    "count": int(r.get("count") or 0),
                    "users": [u for u in (r.get("ahead_users") or []) if u]})
    return out

@app.route('/api/comments', methods=['GET'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def list_ticket_comments():
    """The comment thread of ONE ticket record, oldest-first, each reply
    flattened to {comment_id, reply_id, user_id, user_name, user_avatar,
    create_time, parts, text, images, is_solved, reactions}."""
    record_id = sanitize_text(request.args.get('record_id', ''))
    if not record_id:
        return jsonify({"success": False, "error": "Missing record_id"}), 400
    user, email, uat, token, err = _comments_caller()
    if err:
        return err
    # Phase 15 (Miaoda mirror) fast path: ONE indexed records/search on the
    # Portal Comments table; names/avatars are already inside the row payload
    # (zero per-id lookups — the read that used to cost 10-40 Redis GETs now
    # costs one Feishu call + the scan staleness GET). Falls back to the scan
    # path when the table errors, or when it has no rows for this record but
    # the shared scan does (the background sync hasn't reached this thread
    # yet — e.g. right after deploy).
    if MIRROR_ENABLE:
        mirror = None
        try:
            mirror = _mirror_lookup_record(record_id, token)
        except Exception:
            mirror = None
        if mirror is not None:
            try:
                scan_c = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
            except Exception:
                scan_c = None
            if mirror or (isinstance(scan_c, dict) and not _scan_has_record(scan_c, record_id)):
                out = []
                for p in mirror:
                    out.extend(p.get("replies") or [])
                out.sort(key=lambda x: x.get("create_time") or 0)
                # Phase 18: masked names stored in the mirror payload heal
                # INLINE for the person looking at them (bounded) — no more
                # waiting days for a starving background backfill.
                m_ids = []
                for item in out:
                    if (item.get("user_name") or "").startswith("user-"):
                        m_ids.append(item.get("user_id"))
                    for ppart in (item.get("parts") or []):
                        if ppart.get("type") == "mention" and (ppart.get("name") or "").startswith("user-"):
                            m_ids.append(ppart.get("user_id"))
                if m_ids:
                    healed = _resolve_inline_masked(
                        m_ids, token, uat or None,
                        {u: {"name": "user-" + u[-4:], "avatar": ""} for u in m_ids if u})
                    for item in out:
                        h = healed.get(item.get("user_id")) or {}
                        if h.get("name") and not h["name"].startswith("user-"):
                            item["user_name"] = h["name"]
                            item["user_avatar"] = h.get("avatar") or item.get("user_avatar") or ""
                        for ppart in (item.get("parts") or []):
                            if ppart.get("type") == "mention":
                                h = healed.get(ppart.get("user_id")) or {}
                                if h.get("name") and not h["name"].startswith("user-"):
                                    ppart["name"] = h["name"]
                        for rx in (item.get("reactions") or []):
                            rx["names"] = [(healed.get(u) or {}).get("name") or n
                                           for u, n in zip(rx.get("users") or [], rx.get("names") or [])]
                return jsonify({"success": True, "comments": out, "count": len(out),
                                "truncated": bool((scan_c or {}).get("truncated")),
                                "stale": _scan_is_stale(scan_c),
                                "fast_path": True,
                                "me_open_id": _caller_open_id(email)})
    scan, scan_err = _fetch_all_comments(token)
    if scan is None and uat:
        # expired/insufficient UAT -- reads fall back to the tenant token
        scan, scan_err = _fetch_all_comments(get_tenant_access_token())
    if scan is None:
        status = 502 if "failed" in str(scan_err) else 400
        return jsonify({"success": False, "error": scan_err}), status
    out, ids = [], []
    for c in scan["items"]:
        anchor = _comment_record_anchor(c)
        if anchor.get("table") != REQUESTS_TABLE_ID or anchor.get("record") != record_id:
            continue
        for r in ((c.get("reply_list") or {}).get("replies") or []):
            parts, plain = _reply_parts(r)
            reactions = _reply_reactions(r)
            item = {
                "comment_id": c.get("comment_id"),
                "reply_id": r.get("reply_id"),
                "user_id": r.get("user_id"),
                "create_time": r.get("create_time") or c.get("create_time"),
                "parts": parts,
                "text": plain,
                "images": _reply_images(r),
                "is_solved": bool(c.get("is_solved")),
                "reactions": reactions,
            }
            out.append(item)
            ids.append(item["user_id"])
            ids.extend(p["user_id"] for p in parts if p["type"] == "mention")
            for rx in reactions:
                ids.extend(rx["users"])
    out.sort(key=lambda x: x.get("create_time") or 0)
    people = _resolve_comment_names(ids, token, uat=uat or None)
    # Phase 18: heal the masked names on THIS thread inline (bounded) — the
    # background backfill alone left them masked for days.
    _resolve_inline_masked(ids, token, uat or None, people)
    for item in out:
        item["user_name"] = people.get(item["user_id"], {}).get("name", item["user_id"])
        item["user_avatar"] = people.get(item["user_id"], {}).get("avatar", "")
        for p in item["parts"]:
            if p["type"] == "mention":
                p["name"] = people.get(p["user_id"], {}).get("name", p["user_id"])
        for rx in item["reactions"]:
            rx["names"] = [people.get(u, {}).get("name", u) for u in rx["users"]]
    return jsonify({"success": True, "comments": out, "count": len(out),
                    "truncated": bool(scan.get("truncated")),
                    "stale": _scan_is_stale(scan),
                    "me_open_id": _caller_open_id(email)})

def _build_reply_elements(text, mentions):
    """Splice @Name occurrences in the composer's plain text into Feishu
    reply_elements ({type:text} / {type:mention_user}). Longest names first so
    overlapping display names never half-match."""
    mentions = [m for m in (mentions or []) if m.get("name") and m.get("user_id")]
    mentions.sort(key=lambda m: len(m["name"]), reverse=True)
    elements, buf, i = [], [], 0
    while i < len(text):
        hit = None
        for m in mentions:
            token_str = "@" + m["name"]
            if text.startswith(token_str, i):
                hit = (token_str, m)
                break
        if hit:
            if buf:
                elements.append({"type": "text", "text": "".join(buf)})
                buf = []
            elements.append({"type": "mention_user", "mention_user": hit[1]["user_id"]})
            i += len(hit[0])
        else:
            buf.append(text[i])
            i += 1
    if buf:
        elements.append({"type": "text", "text": "".join(buf)})
    return [e for e in elements if e.get("type") != "text" or e.get("text", "").strip()]

@app.route('/api/comments', methods=['POST'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def add_ticket_comment():
    """Post a comment onto a ticket record AS the logged-in agent (UAT).
    Body: {record_id, text, mentions:[{name, user_id}], user, email, uat}.
    The comment lands in the Base itself -- visible in Feishu too, and every
    @mention triggers the normal Feishu notification."""
    body = request.get_json(silent=True) or {}
    record_id = sanitize_text(body.get('record_id', ''))
    text = str(body.get('text') or '').strip()
    if not record_id or not text:
        return jsonify({"success": False, "error": "Missing record_id or text"}), 400
    if len(text) > 3000:
        return jsonify({"success": False, "error": "Comment too long (max 3000 chars)"}), 400
    user, email, uat, token, err = _comments_caller()
    if err:
        return err
    if not uat:
        return jsonify({"success": False,
                        "error": "Missing User Access Token -- please log out and back in, then retry."}), 400
    elements = _build_reply_elements(text, body.get("mentions"))
    if not elements:
        return jsonify({"success": False, "error": "Empty comment"}), 400
    payload = {
        "file_type": "bitable",
        "anchor": {
            "block_id": REQUESTS_TABLE_ID,
            "base_record_id": record_id,
            "base_view_id": REQUESTS_DEFAULT_VIEW_ID,
        },
        "reply_elements": elements,
    }
    try:
        resp = feishu_session.post(
            f"https://open.feishu.cn/open-apis/drive/v1/files/{BASE_ID}/new_comments",
            headers={"Authorization": f"Bearer {uat}",
                     "Content-Type": "application/json; charset=utf-8"},
            json=payload, timeout=15)
        data = resp.json()
    except Exception as e:
        logger.error("comment_add_failed", record_id=record_id, error=str(e))
        return jsonify({"success": False, "error": str(e)}), 502
    if data.get("code") != 0:
        logger.error("comment_add_rejected", record_id=record_id, code=data.get("code"), msg=data.get("msg"))
        return jsonify({"success": False, "error": f"{data.get('code')}: {data.get('msg')}"}), 400
    # Phase 11.9: patch the new thread into the shared scan in place instead
    # of busting the whole cache (which forced the NEXT reader into a 20s
    # base-wide rescan). Fallback: bust, only if the payload shape surprises us.
    created_ok = _scan_upsert_comment(record_id, data.get("data") or {})
    if not created_ok:
        redis_cmd("DEL", COMMENTS_SCAN_CACHE_KEY)
    # Phase 15 (Miaoda mirror): refresh this thread's mirror row immediately —
    # one human-paced action, so 1-2 extra Bitable calls here are the price of
    # every viewer's NEXT 3-5s poll seeing the new comment instantly, with no
    # per-poll rescan and no per-id name lookups.
    if created_ok:
        try:
            _mirror_refresh_thread(token, comment_id=(data.get("data") or {}).get("comment_id"))
        except Exception:
            pass
    return jsonify({"success": True})

@app.route('/api/comments/scan-refresh', methods=['POST'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def comments_scan_refresh():
    """Frontend-triggered background revalidate (Phase 11.9 stale-while-
    revalidate): when a read answers from a scan older than COMMENTS_SCAN_FRESH
    it flags stale:true and the frontend calls this to bring the shared scan
    up to date, then silently re-pulls. Lock-deduped — concurrent callers all
    wait on the single in-flight rescan instead of stampeding the base.
    Phase 16: automatic (stale-driven) calls run at most once per
    SCAN_AUTO_MIN_GAP across all viewers; the bell panel's refresh button
    posts force:true to bypass that gap (still lock-deduped), and any
    successful pass resets the auto clock."""
    user, email, uat, token, err = _comments_caller()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    force = bool(body.get("force"))
    if not force:
        # Phase 17: via redis_get/set_json so the 30-min auto clock also runs
        # on the per-instance memory fallback when Redis is down — raw
        # redis_cmd GETs would read None forever and every stale-driven call
        # would trigger a full 15-20s base rescan.
        try:
            last_auto = float(redis_get_json(SCAN_AUTO_TS_KEY) or 0)
        except Exception:
            last_auto = 0
        if time.time() - last_auto < SCAN_AUTO_MIN_GAP:
            cached = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
            return jsonify({
                "success": True, "skipped": "auto_gap",
                "ts": int(cached.get("ts") or 0) if isinstance(cached, dict) else 0,
                "stale": _scan_is_stale(cached) if isinstance(cached, dict) else True})
    scan, scan_err = _scan_refresh(token)
    if scan is None and uat:
        scan, scan_err = _scan_refresh(get_tenant_access_token())
    if scan is None:
        return jsonify({"success": False, "error": scan_err}), 502
    try:
        redis_set_json(SCAN_AUTO_TS_KEY, time.time())
    except Exception:
        pass
    return jsonify({"success": True, "ts": int(scan.get("ts") or 0),
                    "stale": _scan_is_stale(scan)})

@app.route('/api/comments/health', methods=['GET'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def comments_health():
    """Phase 17 observability (Ahmed: "the table you made has nothing on it
    please check the mioade back end how it was work"). One admin-only GET
    that answers, with numbers, the questions that used to be invisible:
    is Redis reachable from this instance, is the shared scan cache warm,
    is the Portal Comments mirror table filling, and what did the background
    chain last fail on. Replaces the old silent try/excepts — every stage
    now records into _BG_ERRORS / _MIRROR_STATS / _redis_health."""
    user  = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and "admin" not in perms.get("modules", []):
        return jsonify({"success": False, "error": "Access denied"}), 403
    now = time.time()
    with _redis_health["lock"]:
        last_err, last_err_ts = _redis_health["last_error"], _redis_health["last_error_ts"]
    cached = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
    scan_items, scan_newest = 0, 0
    if isinstance(cached, dict):
        scan_items = len(cached.get("items") or [])
        # Phase 18: newest anchored thread in the scan — compared against the
        # mirror's newest row, this one pair of numbers explains a frozen
        # bell at a glance (mirror newest < scan newest = sync behind).
        for c in (cached.get("items") or []):
            a = _comment_record_anchor(c)
            if a.get("table") == REQUESTS_TABLE_ID and a.get("record"):
                uts = int(c.get("update_time") or 0)
                if uts > scan_newest:
                    scan_newest = uts
    mirror = {"enabled": MIRROR_ENABLE, "rows": None, "error": "",
              "stats": dict(_MIRROR_STATS), "newest_update_time": 0}
    if MIRROR_ENABLE:
        try:
            resp = feishu_session.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{MIRROR_TABLE_ID}/records/search",
                headers={"Authorization": f"Bearer {get_tenant_access_token()}",
                         "Content-Type": "application/json; charset=utf-8"},
                params={"page_size": 1},
                json={"sort": [{"field_name": "Update Time", "desc": True}]}, timeout=15)
            data = resp.json()
            if data.get("code") == 0:
                mirror["rows"] = (data.get("data") or {}).get("total")
                rows0 = (data.get("data") or {}).get("items") or []
                if rows0:
                    mirror["newest_update_time"] = int(_mirror_cell_num(
                        (rows0[0].get("fields") or {}).get("Update Time")))
            else:
                mirror["error"] = f"{data.get('code')}: {data.get('msg')}"
        except Exception as e:
            mirror["error"] = str(e)
    try:
        last_auto = float(redis_get_json(SCAN_AUTO_TS_KEY) or 0)
    except Exception:
        last_auto = 0
    return jsonify({
        "success": True,
        "redis": {"enabled": REDIS_ENABLED, "healthy": redis_healthy(),
                  "last_error": last_err,
                  "last_error_age_s": int(now - last_err_ts) if last_err_ts else None,
                  "mem_fallback_keys": len(_mem_store)},
        "scan_cache": {"present": isinstance(cached, dict), "items": scan_items,
                       "age_s": int(now - cached.get("ts", 0)) if isinstance(cached, dict) else None,
                       "newest_update_time": scan_newest},
        "mirror": mirror,
        "auto_refresh": {"min_gap_s": SCAN_AUTO_MIN_GAP,
                         "last_auto_age_s": int(now - last_auto) if last_auto else None},
        "bg_errors": list(_BG_ERRORS[-10:]),
    })

@app.route('/api/comments/react', methods=['POST'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def react_ticket_comment():
    """Add/remove an emoji reaction on a ticket comment (or one of its
    replies) AS the logged-in agent (UAT). Feishu drive v2 comment-reaction
    API with file_type=bitable — the SAME comment system the portal already
    reads and posts to, so the chip shows up in the Base UI too.
    Body: {reply_id, emoji, action:"add"|"delete", user, email, uat}.
    Toggle semantics live in the frontend; this endpoint is idempotent per
    (reply_id, emoji, user) on Feishu's side."""
    body = request.get_json(silent=True) or {}
    reply_id = sanitize_text(body.get('reply_id', ''))
    # Phase 12 (Ahmed: "still the reacts not accurate i do somthing but
    # appare another thing in the sheet"): the old code uppercased the emoji
    # key and sent THAT to Feishu — but the official enum's tail entries are
    # case-SENSITIVE mixed-case ("Yes", "Get", "OKR", "MinusOne", ...), so
    # .upper() silently turned them into a different/invalid reaction_type
    # and the sheet showed something else. Now we canonicalize ANY incoming
    # casing to the exact official spelling and send it verbatim.
    emoji    = COMMENT_REACT_CANON.get((body.get('emoji') or '').strip().upper())
    action   = (body.get('action') or 'add').strip().lower()
    if not reply_id or not emoji:
        return jsonify({"success": False, "error": "Missing reply_id or emoji"}), 400
    if emoji not in COMMENT_REACT_EMOJIS:
        return jsonify({"success": False,
                        "error": "Unsupported emoji reaction type."}), 400
    if action not in ("add", "delete"):
        return jsonify({"success": False, "error": "action must be add or delete"}), 400
    user, email, uat, token, err = _comments_caller()
    if err:
        return err
    if not uat:
        return jsonify({"success": False,
                        "error": "Missing User Access Token -- please log out and back in, then retry."}), 400
    try:
        resp = feishu_session.post(
            f"https://open.feishu.cn/open-apis/drive/v2/files/{BASE_ID}/comments/reaction?file_type=bitable",
            headers={"Authorization": f"Bearer {uat}",
                     "Content-Type": "application/json; charset=utf-8"},
            json={"action": action, "reply_id": reply_id, "reaction_type": emoji},
            timeout=10)
        data = resp.json()
    except Exception as e:
        logger.error("comment_react_failed", reply_id=reply_id, error=str(e))
        return jsonify({"success": False, "error": str(e)}), 502
    if data.get("code") != 0:
        logger.error("comment_react_rejected", reply_id=reply_id, code=data.get("code"), msg=data.get("msg"))
        return jsonify({"success": False, "error": f"{data.get('code')}: {data.get('msg')}"}), 400
    # Phase 11.9: patch the reaction into the shared scan in place — busting
    # the cache here made EVERY react trigger a 20s base-wide rescan on the
    # next read ("react take so much time to be done"). The reactor's own UI
    # is already updated optimistically; this keeps every other viewer
    # consistent within the same freshness window.
    actor = _caller_open_id(email)
    if not actor:
        cand = sanitize_text(body.get('open_id', '') or '')
        if cand.startswith("ou_"):
            actor = cand
    _scan_patch_reaction(reply_id, emoji, action, actor)
    # Phase 15 (Miaoda mirror): rewrite the owning thread's mirror row so the
    # reaction shows up in every viewer's next 3-5s poll instantly (their own
    # chip is already optimistic; this keeps the OTHER viewers consistent).
    try:
        _mirror_refresh_thread(token, reply_id=reply_id)
    except Exception:
        pass
    return jsonify({"success": True, "actor_open_id": actor})

def _resolve_record_numberings(record_ids, token):
    """record_id -> Numbering (immutable -> cached 7d). Unknown stays absent;
    the feed UI falls back to the short record id."""
    nums, missing = {}, []
    for rid in dict.fromkeys(r for r in record_ids if r):
        ck = f"xena:recnum:{rid}"
        try:
            cached = redis_get_json(ck)
        except Exception:
            cached = None
        if isinstance(cached, dict) and cached.get("numbering"):
            nums[rid] = cached["numbering"]
        else:
            missing.append(rid)
    for i in range(0, len(missing), 100):
        chunk = missing[i:i + 100]
        body = {"record_ids": chunk, "automatic_fields": False,
                "field_names": ["Numbering"]}
        try:
            resp = feishu_session.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/batch_get",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json; charset=utf-8"},
                json=body, timeout=20)
            data = resp.json()
            if data.get("code") != 0 and "field_names" in body:
                # older tenants may reject field_names on batch_get -- retry full-row
                body.pop("field_names")
                resp = feishu_session.post(
                    f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/batch_get",
                    headers={"Authorization": f"Bearer {token}",
                             "Content-Type": "application/json; charset=utf-8"},
                    json=body, timeout=20)
                data = resp.json()
        except Exception:
            break
        if data.get("code") != 0:
            break
        for rec in ((data.get("data") or {}).get("records") or []):
            rid = rec.get("record_id")
            num = (rec.get("fields") or {}).get("Numbering")
            if isinstance(num, list):  # formula/auto-number shapes
                num = "".join(str(x.get("text", x)) if isinstance(x, dict) else str(x) for x in num)
            if rid and num not in (None, ""):
                nums[rid] = str(num)
                redis_set_json(f"xena:recnum:{rid}", {"numbering": str(num)}, ttl=COMMENT_RECNUM_TTL)
    return nums

@app.route('/api/comments/feed', methods=['GET'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def comments_feed():
    """The notifications feed (Ahmed's screenshot): newest comment threads on
    Grand Table records first, each with the last reply's author/text/images,
    the ticket's Numbering, and a mentions_me flag for the Mentioned tab.
    Phase 12: unread state is SERVER-side (per-email seen-set + mark-all
    watermark in Redis, see _notif_load_seen/_notif_mark_read) — survives
    logout and reaction time-bumps, killing the phantom 99+."""
    my_open_id = sanitize_text(request.args.get('my_open_id', ''))
    user, email, uat, token, err = _comments_caller()
    if err:
        return err
    # Phase 15 (Miaoda mirror) fast path: the bell feed from the newest
    # Portal Comments rows — one sorted records/search, feed rows and
    # names/avatars pre-resolved inside the payload. Per-poll cost: 1 Feishu
    # call + this user's read-state (2 Redis GETs). Falls back to the scan
    # path when the table errors or the mirror hasn't been synced yet.
    if MIRROR_ENABLE:
        mirror = None
        try:
            mirror = _mirror_lookup_feed(token)
        except Exception:
            mirror = None
        if mirror is not None:
            try:
                scan_c = redis_get_json(COMMENTS_SCAN_CACHE_KEY)
            except Exception:
                scan_c = None
            # Phase 18 anti-freeze (Ahmed: "the new comments even i refreshed
            # sevral time and wait 2 hour ... not appared"): a NON-EMPTY but
            # STALE mirror must not hide newer threads — his production table
            # sat at 1 row (one portal write-patch) while the background sync
            # never landed, and the old "mirror non-empty => serve mirror"
            # rule froze the bell on that single thread for days. Now the
            # fast path serves only when the mirror's newest row is at least
            # as fresh as the scan's newest anchored thread; anything older
            # falls through to the scan path below.
            serve_mirror = False
            if mirror:
                serve_mirror = True
                if isinstance(scan_c, dict) and scan_c.get("items"):
                    m_new = max([int(p.get("update_time") or 0) for p in mirror] or [0])
                    s_new = 0
                    for c in scan_c["items"]:
                        a = _comment_record_anchor(c)
                        if a.get("table") != REQUESTS_TABLE_ID or not a.get("record"):
                            continue
                        uts = int(c.get("update_time") or 0)
                        if uts > s_new:
                            s_new = uts
                    if s_new and m_new < s_new:
                        serve_mirror = False
            elif isinstance(scan_c, dict) and not scan_c.get("items"):
                serve_mirror = True
            if serve_mirror:
                # Phase 18: heal masked author/reactor names inline (bounded)
                # so the bell stops showing 'user-xxxx' while the background
                # backfill is still catching up.
                f_ids = []
                for p in mirror[:COMMENTS_FEED_MAX]:
                    fd = (p.get("feed") or {})
                    if (fd.get("author_name") or "").startswith("user-"):
                        f_ids.append(fd.get("author_id"))
                    for rx in (fd.get("reactions") or []):
                        if (rx.get("reactor_name") or "").startswith("user-"):
                            f_ids.append(rx.get("reactor_id"))
                        if (rx.get("on_name") or "").startswith("user-"):
                            f_ids.append(rx.get("on_user_id"))
                healed = {}
                if f_ids:
                    healed = _resolve_inline_masked(
                        f_ids, token, uat or None,
                        {u: {"name": "user-" + u[-4:], "avatar": ""} for u in f_ids if u})
                def _hn(uid, cur):
                    h = healed.get(uid) or {}
                    return h.get("name") if h.get("name") and not h["name"].startswith("user-") else cur
                def _ha(uid, cur):
                    h = healed.get(uid) or {}
                    return h.get("avatar") or cur
                items = []
                for p in mirror[:COMMENTS_FEED_MAX]:
                    fd = (p.get("feed") or {})
                    if not fd.get("comment_id"):
                        continue
                    items.append({
                        "kind": "comment",
                        "comment_id": fd.get("comment_id"),
                        "last_reply_id": fd.get("last_reply_id"),
                        "record_id": fd.get("record_id"),
                        "author_id": fd.get("author_id"),
                        "author": _hn(fd.get("author_id"), fd.get("author_name") or fd.get("author_id") or ""),
                        "author_avatar": _ha(fd.get("author_id"), fd.get("author_avatar") or ""),
                        "time": fd.get("time"),
                        "text": fd.get("text") or "",
                        "text_parts": fd.get("text_parts") or [],
                        "image_count": fd.get("image_count") or 0,
                        "reply_count": fd.get("reply_count") or 0,
                        "mentions_me": bool(my_open_id) and my_open_id in (fd.get("mention_ids") or []),
                        "is_solved": bool(fd.get("is_solved")),
                        "reactions": [{"key": rx.get("key"), "reactor_id": rx.get("reactor_id"),
                                       "reactor_name": _hn(rx.get("reactor_id"), rx.get("reactor_name")),
                                       "reactor_avatar": _ha(rx.get("reactor_id"), rx.get("reactor_avatar") or ""),
                                       "on_user_id": rx.get("on_user_id"),
                                       "on_name": _hn(rx.get("on_user_id"), rx.get("on_name") or ""),
                                       "on_text": rx.get("on_text")}
                                      for rx in (fd.get("reactions") or [])],
                        "numbering": fd.get("numbering"),
                    })
                    for rx in (fd.get("reactions") or []):
                        if not rx.get("reactor_id"):
                            continue
                        items.append({
                            "kind": "reaction",
                            "comment_id": fd.get("comment_id"),
                            "record_id": fd.get("record_id"),
                            "author_id": rx.get("reactor_id"),
                            "author": _hn(rx.get("reactor_id"), rx.get("reactor_name") or rx.get("reactor_id")),
                            "author_avatar": _ha(rx.get("reactor_id"), rx.get("reactor_avatar") or ""),
                            "reaction_key": rx.get("key"),
                            "on_user_id": rx.get("on_user_id"),
                            "on_author": _hn(rx.get("on_user_id"), rx.get("on_name") or ""),
                            "time": fd.get("time"),
                            "text": rx.get("on_text") or "",
                            "image_count": 0,
                            "reply_count": fd.get("reply_count") or 0,
                            "mentions_me": bool(my_open_id) and rx.get("on_user_id") == my_open_id
                                           and rx.get("reactor_id") != my_open_id,
                            "is_solved": bool(fd.get("is_solved")),
                            "numbering": fd.get("numbering"),
                        })
                items.sort(key=lambda x: x.get("time") or 0, reverse=True)
                items = items[:COMMENTS_FEED_MAX]
                seen, wm = _notif_load_seen(email)
                unread_n = 0
                for it in items:
                    if it.get("kind") == "reaction":
                        it["uid"] = f"r:{it.get('comment_id')}:{it.get('author_id')}:{it.get('reaction_key')}"
                    else:
                        # Phase 18: keep last_reply_id on the item (the bell's
                        # quick-reaction button targets it) — only READ it for
                        # the uid instead of popping it away.
                        it["uid"] = f"c:{it.get('comment_id')}:{it.get('last_reply_id') or ''}"
                    mine = bool(my_open_id) and it.get("author_id") == my_open_id
                    it["unread"] = (not mine) and (it["uid"] not in seen) and (float(it.get("time") or 0) > wm)
                    if it["unread"]:
                        unread_n += 1
                return jsonify({"success": True, "items": items, "count": len(items),
                                "unread_count": unread_n,
                                "truncated": bool((scan_c or {}).get("truncated")),
                                "stale": _scan_is_stale(scan_c),
                                "fast_path": True})
    scan, scan_err = _fetch_all_comments(token)
    if scan is None and uat:
        # expired/insufficient UAT -- reads fall back to the tenant token
        scan, scan_err = _fetch_all_comments(get_tenant_access_token())
    if scan is None:
        status = 502 if "failed" in str(scan_err) else 400
        return jsonify({"success": False, "error": scan_err}), status
    items, ids, rec_ids = [], [], []
    for c in scan["items"]:
        anchor = _comment_record_anchor(c)
        if anchor.get("table") != REQUESTS_TABLE_ID or not anchor.get("record"):
            continue
        replies = ((c.get("reply_list") or {}).get("replies") or [])
        if not replies:
            continue
        last = replies[-1]
        parts, plain = _reply_parts(last)
        mentions_me = False
        if my_open_id:
            for r in replies:
                p_parts, _ = _reply_parts(r)
                if any(p["type"] == "mention" and p["user_id"] == my_open_id for p in p_parts):
                    mentions_me = True
                    break
        img_count = sum(len(_reply_images(r)) for r in replies)
        # Every reaction on any reply of this thread, flattened. Each carries
        # the reactor's open_id so the feed can say WHO reacted (Feishu gives
        # no per-reaction timestamp -- the thread's update_time, which bumps
        # when a reaction lands, is the closest honest clock).
        thread_reactions = []
        for r in replies:
            _, r_plain = _reply_parts(r)
            for rx in _reply_reactions(r):
                for reactor in rx["users"]:
                    thread_reactions.append({
                        "key": rx["key"],
                        "reactor_id": reactor,
                        "on_user_id": r.get("user_id"),
                        "on_text": (r_plain or "")[:120],
                    })
                    ids.append(reactor)
                    ids.append(r.get("user_id"))
        items.append({
            "kind": "comment",
            "comment_id": c.get("comment_id"),
            "last_reply_id": last.get("reply_id"),
            "record_id": anchor["record"],
            "author_id": last.get("user_id"),
            "time": c.get("update_time") or last.get("create_time"),
            "text": plain[:180],
            "_last_parts": parts,   # Phase 18: rebuilt into text/text_parts
                                    # with mention NAMES once people resolve
            "image_count": img_count,
            "reply_count": len(replies),
            "mentions_me": mentions_me,
            "is_solved": bool(c.get("is_solved")),
            "reactions": thread_reactions,
        })
        ids.append(last.get("user_id"))
        ids.append(c.get("user_id"))
        rec_ids.append(anchor["record"])
        # One EXTRA feed entry per reaction so a bare emoji on your comment
        # shows up in the bell exactly like it does inside the Base. The
        # entry's mentions_me flag means "they reacted to YOUR comment", so
        # it also lands in the Mentioned tab where Ahmed looks for it.
        bump_time = c.get("update_time") or last.get("create_time")
        for rx in thread_reactions:
            if not rx["reactor_id"]:
                continue
            items.append({
                "kind": "reaction",
                "comment_id": c.get("comment_id"),
                "record_id": anchor["record"],
                "author_id": rx["reactor_id"],   # the reactor
                "reaction_key": rx["key"],
                "on_user_id": rx["on_user_id"],
                "time": bump_time,
                "text": rx["on_text"],
                "image_count": 0,
                "reply_count": len(replies),
                "mentions_me": bool(my_open_id) and rx["on_user_id"] == my_open_id and rx["reactor_id"] != my_open_id,
                "is_solved": bool(c.get("is_solved")),
            })
    items.sort(key=lambda x: x.get("time") or 0, reverse=True)
    items = items[:COMMENTS_FEED_MAX]
    nums = _resolve_record_numberings([it["record_id"] for it in items], token)
    people = _resolve_comment_names(ids, token, uat=uat or None)
    # Phase 18: heal the bell's visible masked names inline (bounded) — the
    # same fix as the thread endpoint, so the notification panel stops
    # showing 'user-xxxx' while the background backfill catches up.
    _resolve_inline_masked(ids, token, uat or None, people)
    for it in items:
        it["numbering"] = nums.get(it["record_id"])
        it["author"] = people.get(it["author_id"], {}).get("name", it["author_id"])
        it["author_avatar"] = people.get(it["author_id"], {}).get("avatar", "")
        if it.get("kind") == "reaction":
            it["on_author"] = people.get(it.get("on_user_id"), {}).get("name", "")
        else:
            for rx in it.get("reactions") or []:
                rx["reactor_name"] = people.get(rx["reactor_id"], {}).get("name", rx["reactor_id"])
            # Phase 18: Base-style bell line — "@Name" instead of a bare "@",
            # plus typed text_parts so the frontend can highlight mentions.
            parts = it.pop("_last_parts", None) or []
            tp, tx = [], []
            for p in parts:
                if p.get("type") == "mention":
                    nm = people.get(p.get("user_id"), {}).get("name", p.get("user_id") or "")
                    tp.append({"type": "mention", "user_id": p.get("user_id"), "name": nm})
                    tx.append("@" + nm)
                elif p.get("type") == "link":
                    tp.append({"type": "text", "text": p.get("url") or ""})
                    tx.append(p.get("url") or "")
                else:
                    tp.append({"type": "text", "text": p.get("text") or ""})
                    tx.append(p.get("text") or "")
            tx = "".join(tx).strip()
            if tx:
                it["text"] = tx[:180]
            it["text_parts"] = tp
    for it in items:
        it.pop("_last_parts", None)  # reaction entries never carried parts
    # Phase 12 (Ahmed: "99+ notifications they are all old and i make them
    # readed but still after while show as unreaded"): read state moves from
    # client-side localStorage (time > lastSeen — reaction bumps re-flagged
    # OLD threads as new, and logging out wiped it entirely) to a SERVER-side
    # per-email seen-set + mark-all watermark in Redis. Every entry carries a
    # STABLE uid: a comment thread keys on its LAST reply (a genuinely new
    # reply = a new uid = unread again, exactly like the Base bell), a
    # reaction entry keys on (reactor, reaction key). unread = unseen AND
    # newer than the watermark AND not authored by me — my own comments and
    # my own reactions never notify me.
    seen, wm = _notif_load_seen(email)
    unread_n = 0
    for it in items:
        if it.get("kind") == "reaction":
            it["uid"] = f"r:{it.get('comment_id')}:{it.get('author_id')}:{it.get('reaction_key')}"
        else:
            # Phase 18: keep last_reply_id (bell quick-reaction target).
            it["uid"] = f"c:{it.get('comment_id')}:{it.get('last_reply_id') or ''}"
        mine = bool(my_open_id) and it.get("author_id") == my_open_id
        it["unread"] = (not mine) and (it["uid"] not in seen) and (float(it.get("time") or 0) > wm)
        if it["unread"]:
            unread_n += 1
    return jsonify({"success": True, "items": items, "count": len(items),
                    "unread_count": unread_n,
                    "truncated": bool(scan.get("truncated")),
                    "stale": _scan_is_stale(scan)})


NOTIF_SEEN_CAP = 3000          # per-email seen-uids kept (trimmed oldest-first)
NOTIF_SEEN_TTL = 30 * 86400    # read state lives 30 days

def _notif_load_seen(email):
    """(set_of_seen_uids, mark_all_watermark_ts) for the caller's email."""
    ek = (email or "").strip().lower()
    if not ek:
        return set(), 0.0
    try:
        lst = redis_get_json(f"xena:notif:seen:{ek}")
        wm  = redis_get_json(f"xena:notif:wm:{ek}")
    except Exception:
        return set(), 0.0
    return set(lst if isinstance(lst, list) else []), float(wm or 0)

def _notif_mark_read(email, uids=None, mark_all=False):
    """Persist read state server-side. uids are appended to the seen list
    (deduped, trimmed to the newest NOTIF_SEEN_CAP); mark_all moves the
    watermark to now so NOTHING currently in the feed can reappear unread —
    only genuinely new activity (a new reply = new uid, a new reaction =
    bumped time) can. Returns how many uids were newly added."""
    ek = (email or "").strip().lower()
    if not ek:
        return 0
    uids = [u for u in (uids or []) if isinstance(u, str) and u][:2000]
    added = 0
    try:
        if uids:
            lst = redis_get_json(f"xena:notif:seen:{ek}")
            if not isinstance(lst, list):
                lst = []
            have = set(lst)
            fresh = [u for u in uids if u not in have]
            added = len(fresh)
            lst = (lst + fresh)[-NOTIF_SEEN_CAP:]
            redis_set_json(f"xena:notif:seen:{ek}", lst, ttl=NOTIF_SEEN_TTL)
        if mark_all:
            redis_set_json(f"xena:notif:wm:{ek}", time.time(), ttl=NOTIF_SEEN_TTL)
    except Exception:
        pass
    return added

@app.route('/api/comments/feed/read', methods=['POST'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def comments_feed_read():
    """Phase 12: server-side read state for the bell. Body {uids:[...]} marks
    those entries read (sent when the user clicks one notification); {all:true}
    moves the watermark to now (the 'mark all read' button). Survives logout,
    device switches and reaction time-bumps — the phantom-99+ fix."""
    body = request.get_json(silent=True) or {}
    user, email, uat, token, err = _comments_caller()
    if err:
        return err
    uids = body.get("uids") if isinstance(body.get("uids"), list) else []
    mark_all = bool(body.get("all"))
    marked = _notif_mark_read(email, uids=uids, mark_all=mark_all)
    return jsonify({"success": True, "marked": marked, "all": mark_all})

@app.route('/api/comments/media/<file_token>', methods=['GET'])
@rate_limit(*RATE_LIMIT_COMMENTS)
def proxy_comment_media(file_token):
    """Streams an image attached to a comment (image_list token). TAT first
    (the app collaborates on the base), the caller's UAT as fallback -- same
    pattern as /api/attachments/<file_token> minus the bitable-cell extra."""
    user  = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("modules") and not perms.get("is_super_admin"):
        return jsonify({"error": "Access denied"}), 403
    url = f"https://open.feishu.cn/open-apis/drive/v1/medias/{file_token}/download"
    for token in [get_tenant_access_token(), sanitize_text(request.args.get('uat', ''))]:
        if not token:
            continue
        try:
            resp = feishu_session.get(url, headers={"Authorization": f"Bearer {token}"},
                                      timeout=20, stream=True)
        except Exception as e:
            logger.error("comment_media_failed", file_token=file_token, error=str(e))
            continue
        if resp.status_code == 200:
            response = Response(resp.content,
                                content_type=resp.headers.get("Content-Type", "application/octet-stream"))
            response.headers["Cache-Control"] = "private, max-age=86400, immutable"
            return response
        logger.error("comment_media_rejected", file_token=file_token, status=resp.status_code, body=resp.text[:200])
    return jsonify({"error": "Could not fetch comment image"}), 502

SCHEMA_SYNC_CACHE_KEY = "xena:schema:fields:v1"
SCHEMA_SYNC_TTL = 600  # the sheet's option lists change rarely; 10 min

@app.route('/api/schema/fields', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def schema_fields():
    """Live SingleSelect/MultiSelect option lists from the Grand Table's own
    field metadata, so the portal's dropdowns always match the sheet (Ahmed:
    "make the options field sync from the sheet to be sure it's updated").
    Returns {field_name: {ui_type, options:[...]}} for select-type fields
    only; Redis-cached SCHEMA_SYNC_TTL seconds; TAT-first with UAT fallback."""
    user  = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    uat   = sanitize_text(request.args.get('uat', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("modules") and not perms.get("is_super_admin"):
        return jsonify({"success": False, "error": "Access denied"}), 403
    cached = redis_get_json(SCHEMA_SYNC_CACHE_KEY)
    if isinstance(cached, dict) and cached.get("fields"):
        return jsonify({"success": True, "fields": cached["fields"], "cached": True})
    fields, page_token = {}, None
    for tk in [get_tenant_access_token(), uat]:
        if not tk:
            continue
        fields, page_token = {}, None
        ok = True
        for _ in range(20):
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            try:
                resp = _feishu_get(
                    f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/fields",
                    headers={"Authorization": f"Bearer {tk}"}, params=params, timeout=15, tries=2)
                data = resp.json()
            except Exception as e:
                logger.error("schema_fields_failed", error=str(e))
                ok = False
                break
            if data.get("code") != 0:
                logger.error("schema_fields_rejected", code=data.get("code"), msg=data.get("msg"))
                ok = False
                break
            d = data.get("data") or {}
            for f in (d.get("items") or []):
                name, ui = f.get("field_name"), f.get("ui_type")
                if ui not in ("SingleSelect", "MultiSelect"):
                    continue
                opts = [o.get("name") for o in ((f.get("property") or {}).get("options") or []) if o.get("name")]
                if name:
                    fields[name] = {"ui_type": ui, "options": opts}
            if not d.get("has_more"):
                page_token = None
                break
            page_token = d.get("page_token")
            if not page_token:
                break
        if ok and fields:
            break
    if not fields:
        return jsonify({"success": False, "error": "Could not read the sheet's field metadata"}), 502
    redis_set_json(SCHEMA_SYNC_CACHE_KEY, {"fields": fields}, ttl=SCHEMA_SYNC_TTL)
    return jsonify({"success": True, "fields": fields, "cached": False})

@app.route('/api/users/lookup', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def users_lookup():
    """open_id list -> {id: {name, avatar_url}} for the workspace's person
    chips (Respondents / Lock Owner / Done by / Mentioned Person ...), which
    arrive from the sheet with name+id but NO photo (Ahmed: "the pic is not
    showing in the website"). Shares the comment-name resolver (tenant token
    first, UAT fallback, 24h Redis cache) so repeated opens cost nothing."""
    user  = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    uat   = sanitize_text(request.args.get('uat', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("modules") and not perms.get("is_super_admin"):
        return jsonify({"success": False, "error": "Access denied"}), 403
    ids = [s for s in (sanitize_text(x) for x in request.args.get('ids', '').split(',')) if s][:100]
    if not ids:
        return jsonify({"success": True, "users": {}})
    people = _resolve_comment_names(ids, uat or get_tenant_access_token(), uat=uat or None)
    return jsonify({"success": True,
                    "users": {uid: {"name": p.get("name", ""), "avatar_url": p.get("avatar", "")}
                              for uid, p in people.items()}})

@app.route('/api/admin/scope-check', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def admin_scope_check():
    """Phase 11.5 (Ahmed: "i already has contact permissions so check if they
    will help you in the comments thing"): LIVE probe of every contact-permission
    leg the comment resolver can use, against one real open_id from the Agent
    Roster. Four legs are tested and reported individually:
      TAT batch   POST contact/v3/users/batch            (needs contact:contact.base:readonly as 应用身份)
      TAT single  GET  contact/v3/users/{open_id}        (same, tenant identity)
      UAT batch   POST contact/v3/users/batch as caller  (needs contact:user.base:readonly granted at LOGIN time)
      UAT single  GET  contact/v3/users/{open_id}        (same, user identity)
    The UAT legs only pass after the user has logged out + back in once, because
    a login token carries only the scopes that existed when it was issued."""
    user  = sanitize_text(request.args.get('user', ''))
    email = sanitize_text(request.args.get('email', ''))
    uat   = sanitize_text(request.args.get('uat', ''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and "admin" not in perms.get("modules", []):
        return jsonify({"success": False, "error": "Access denied"}), 403
    tat = get_tenant_access_token()
    try:
        items = _presence_fetch_roster(tat)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the Agent Roster: {e}"}), 502
    probe_oid, probe_nm = "", ""
    for it in items:
        _, oid, nm = _presence_row_identity(it)
        if oid:
            probe_oid, probe_nm = oid, nm
            break
    if not probe_oid:
        return jsonify({"success": False, "error": "No roster agent with an open_id to probe."}), 500

    def _probe_batch(tk):
        # Phase 11.6: the batch endpoint is GET with repeated user_ids params —
        # the earlier POST shape is what produced the "Extra data" JSON-parse
        # ghost error Ahmed saw in his first scope-check output.
        try:
            resp = feishu_session.get(
                "https://open.feishu.cn/open-apis/contact/v3/users/batch",
                headers={"Authorization": f"Bearer {tk}"},
                params=[("user_ids", probe_oid), ("user_id_type", "open_id")], timeout=10)
            data = resp.json()
        except Exception as e:
            return {"ok": False, "err": f"request failed: {e}"}
        if data.get("code") != 0:
            return {"ok": False, "err": f"code {data.get('code')}: {data.get('msg', '')}"}
        items = ((data.get("data") or {}).get("items")) or []
        if not items:
            return {"ok": True, "err": "code 0 but EMPTY — user outside the app's contact visibility (通讯录授权范围)",
                    "name": "", "avatar": False}
        u = items[0]
        return {"ok": True, "err": "", "name": u.get("name", ""),
                "avatar": bool((u.get("avatar") or {}).get("avatar_72"))}

    def _probe_single(tk):
        try:
            resp = feishu_session.get(
                f"https://open.feishu.cn/open-apis/contact/v3/users/{probe_oid}",
                headers={"Authorization": f"Bearer {tk}"},
                params={"user_id_type": "open_id"}, timeout=10)
            data = resp.json()
        except Exception as e:
            return {"ok": False, "err": f"request failed: {e}"}
        if data.get("code") != 0:
            return {"ok": False, "err": f"code {data.get('code')}: {data.get('msg', '')}"}
        u = (data.get("data") or {}).get("user") or {}
        if not (u.get("name") or ""):
            return {"ok": True, "err": "code 0 but EMPTY — user outside the app's contact visibility (通讯录授权范围)",
                    "name": "", "avatar": False}
        return {"ok": True, "err": "", "name": u.get("name", ""),
                "avatar": bool((u.get("avatar") or {}).get("avatar_72"))}

    legs = {
        "tat_batch":  _probe_batch(tat),
        "tat_single": _probe_single(tat),
        "uat_batch":  (_probe_batch(uat) if uat else {"ok": False, "err": "no login token on this request"}),
        "uat_single": (_probe_single(uat) if uat else {"ok": False, "err": "no login token on this request"}),
    }
    any_ok = any(l.get("ok") and l.get("name") for l in legs.values())
    # Phase 11.6: a leg that answers code 0 with an EMPTY user is NOT a scope
    # problem — the scope is live, the app's 通讯录授权范围 (developer console →
    # 安全设置/通讯录授权范围) hides that employee. Widening it to 全部员工 makes
    # the TAT legs resolve everyone with zero re-logins.
    empty_ok = any(l.get("ok") and not l.get("name") and "EMPTY" in l.get("err", "")
                   for l in legs.values())
    if any_ok:
        hint = "Contact legs are live — comment names/photos resolve for everyone."
    elif empty_ok:
        hint = ("The scope IS live (code 0) but the app cannot SEE this user — widen the app's "
                "contact visibility (开发者后台 → 安全设置/通讯录授权范围 → 全部员工), publish, then "
                "delete xena:tenant_access_token in Upstash. Meanwhile UAT legs carry the load "
                "after each member logs out/in once.")
    else:
        hint = ("All contact legs rejected: the scope exists in the console but is NOT on the "
                "tokens yet — every member must log out and back in once (UAT legs), and the "
                "tenant scope must be 已开通 as 应用身份 for the TAT legs.")
    return jsonify({"success": True, "probe": {"open_id": probe_oid, "roster_name": probe_nm},
                    "legs": legs, "resolver_can_resolve": any_ok, "hint": hint})

# ════════════════════════════════════════════════════════════════════
# Phase 11 (Ahmed): AGENT PRESENCE SYSTEM — "Audition Status Management"
# "the audition member to be able to change the status from online and
# offline and so on, show them how many today online offline break on task
# and so on, and this is reflect on the sheet on the Agent Roster table ...
# fast action reflected immediately in the sheet as this matter the part
# of the sec matter ... not consume so much from vercel quota".
#
# Architecture (the cheapest correct one — the sheet already owns the hard
# parts): the portal does EXACTLY ONE Feishu write per status change — a
# **batch_update** of the agent's Agent Roster row (Current Status +
# Status Change Time + Memory). Three ENABLED native Base workflows watch
# "Current Status" with trigger_control_list including "openAPIBatchUpdate"
# (a single-record PUT would NOT fire them — batch is mandatory):
#   • Auto-Log  (wkfCXTVstBdVo63z) → opens a Time Logs segment + stamps
#     Active Log ID; Auto-Closer then closes the previous segment.
#   • Smart Punch (wkfuNHH1KPZxLCVf) → maintains the Daily Logs shift row.
#   • Agent Pulls Ticket JS Engine (wkfjJD8vw76fDU51) → wakes the ticket
#     dispatcher the moment an agent goes Online ("the sec matter").
# So the portal never touches Time Logs / Daily Logs / the queue itself —
# one write, immediate sheet reflection, zero automation conflicts, minimal
# quota. Reads are coalesced behind a short in-process cache (TTL below) so
# N open dashboards cost ~1 Feishu read per TTL window, not N per refresh.
# "Memory" (the previous status) is maintained for parity with what the
# Loop Closer / dispatcher automations already do on that row.
# ════════════════════════════════════════════════════════════════════
PRESENCE_STATUSES = ["Online", "On Task", "Working Ticket", "Meeting", "Break", "Offline"]
PRESENCE_BOARD_TTL_S = 10   # read coalescing window (seconds)
_presence_board_cache = {"ts": 0.0, "data": None}
_presence_board_lock = threading.Lock()

def _presence_gate(email, user):
    """Module key 'audition_status' (granted in Manage Agents) or admin --
    same convention as every other module check in this file. Returns the
    perms dict when allowed, None otherwise."""
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any(m in ("audition_status", "admin") for m in perms.get("modules", [])):
        return None
    return perms

def _presence_fetch_roster(tat):
    """ONE Feishu call: the whole Agent Roster (a handful of rows)."""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ROSTER_TABLE_ID}/records"
    resp = feishu_session.get(url, headers={"Authorization": f"Bearer {tat}"},
                              params={"page_size": 100}, timeout=15)
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(data.get("msg") or "Agent Roster read failed")
    items = (data.get("data") or {}).get("items") or []
    # Phase 11.2: the roster's "Agent Name" person dicts carry name +
    # avatar_url for EVERY agent — harvest them into the name/avatar cache
    # so the presence board and the comment resolver get real photos for
    # the whole team for free (no contact scope needed).
    for _it in items:
        try:
            _harvest_person_names(_it.get("fields") or {})
        except Exception:
            pass
    return items

def _presence_row_identity(item):
    """record (Agent Roster) -> (record_id, open_id, name) from the Agent
    Name user field."""
    persons = (item.get("fields") or {}).get("Agent Name") or []
    oid, nm = "", ""
    if isinstance(persons, list) and persons and isinstance(persons[0], dict):
        oid = persons[0].get("id", "") or ""
        nm  = persons[0].get("name", "") or ""
    return item.get("record_id", ""), oid, nm

def _presence_build_board(items, uat=""):
    """Roster records -> shared (caller-independent) board payload."""
    # Phase 11.8: rows whose Agent Name person dict already carries BOTH name
    # and avatar use it DIRECTLY — zero resolver rounds; a board rebuild is
    # then exactly ONE roster read. Rows with a name but NO avatar go through
    # the resolver too (avatar fill only, dict name wins) — the resolver's
    # parallel legs + neg-caches keep that bounded. Only genuinely name-less
    # rows rely on the resolver for the name.
    people, need = {}, []
    for it in items:
        persons = (it.get("fields") or {}).get("Agent Name") or []
        _, oid, nm = _presence_row_identity(it)
        av = ""
        if isinstance(persons, list) and persons and isinstance(persons[0], dict):
            av = persons[0].get("avatar_url") or ""
        if oid and nm:
            people[oid] = {"name": nm, "avatar": av}
            if not av:
                need.append(oid)  # name known, avatar missing — resolver fills the avatar only
        elif oid:
            need.append(oid)
    if need:
        for oid, p in _resolve_comment_names(need, get_tenant_access_token(), uat=uat or None).items():
            if oid in people:
                if not people[oid].get("avatar") and p.get("avatar"):
                    people[oid]["avatar"] = p["avatar"]  # merge: sheet name wins, resolver avatar fills
            else:
                people[oid] = p
    agents, counts = [], {s: 0 for s in PRESENCE_STATUSES}
    for it in items:
        f = it.get("fields") or {}
        rid, oid, nm = _presence_row_identity(it)
        status = extract_field_text(f.get("Current Status", "")) or "Offline"
        since_ms = _extract_epoch_ms(f.get("Status Change Time"))
        p = people.get(oid, {})
        if p.get("name") and not str(p["name"]).startswith("user-"):
            nm = p["name"]
        agents.append({
            "record_id": rid, "open_id": oid, "name": nm,
            "avatar_url": p.get("avatar", "") or "",
            "status": status,
            "since_ms": since_ms,
            "memory": extract_field_text(f.get("Memory", "")) or "",
            "online_hours":  extract_field_text(f.get("Online Hours", ""))  or "00:00",
            "task_hours":    extract_field_text(f.get("Task Hours", ""))    or "00:00",
            "break_hours":   extract_field_text(f.get("Break Hours", ""))   or "00:00",
            "meeting_hours": extract_field_text(f.get("Meeting Hours", "")) or "00:00",
            "queue_status":  extract_field_text(f.get("Queue Status", ""))  or "",
            "active_tickets": _safe_int(extract_field_text(f.get("Active Tickets", "")), 0),
        })
        counts[status] = counts.get(status, 0) + 1
    # Online-ish statuses first, then by name -- stable, readable board.
    order = {s: i for i, s in enumerate(PRESENCE_STATUSES)}
    agents.sort(key=lambda a: (order.get(a["status"], 99), (a["name"] or "").lower()))
    return {"counts": counts, "agents": agents}

@app.route('/api/presence/board', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def presence_board():
    """Live team board for the Audition Status Management page: every roster
    agent's current status + since-when + this-month hours, plus per-status
    headcounts. Coalesced behind a 10s in-process cache (see header note)."""
    user    = sanitize_text(request.args.get('user', ''))
    email   = sanitize_text(request.args.get('email', ''))
    uat     = sanitize_text(request.args.get('uat', ''))
    open_id = sanitize_text(request.args.get('open_id', ''))
    if not _presence_gate(email, user):
        return jsonify({"success": False, "error": "Access denied: Audition Status module required."}), 403
    if MOCK_MODE:
        agents = [
            {"record_id": "recMockA", "open_id": "ou_mock_ahmed", "name": "Ahmed Samurai", "avatar_url": "",
             "status": "Online", "since_ms": int(time.time()*1000) - 5400000, "memory": "Offline",
             "online_hours": "12:34", "task_hours": "01:21", "break_hours": "00:42",
             "meeting_hours": "00:00", "queue_status": "⭐ NEXT", "active_tickets": 1},
            {"record_id": "recMockB", "open_id": "ou_mock_mano", "name": "Mano", "avatar_url": "",
             "status": "Break", "since_ms": int(time.time()*1000) - 900000, "memory": "Online",
             "online_hours": "09:10", "task_hours": "00:33", "break_hours": "01:05",
             "meeting_hours": "00:00", "queue_status": "💤 OFFLINE", "active_tickets": 0},
        ]
        counts = {s: 0 for s in PRESENCE_STATUSES}
        for a in agents: counts[a["status"]] += 1
        me_oid, me_nm = open_id.strip().lower(), (user or "").strip().lower()
        for a in agents:
            a["is_me"] = bool((me_oid and a["open_id"].lower() == me_oid) or (me_nm and a["name"].lower() == me_nm))
        now_ms = int(time.time() * 1000)
        return jsonify({"success": True, "server_time_ms": now_ms, "counts": counts,
                        "agents": agents, "statuses": PRESENCE_STATUSES, "cached": False})
    # Phase 11.5: ?fresh=1 bypasses the 10s in-process cache READ (the cache is
    # still rebuilt + rewritten). Needed because Vercel runs several instances:
    # presence_set clears the cache on the instance that handled the write, but
    # the follow-up refetch may land on a DIFFERENT instance still holding the
    # old board for up to 10s -> "the sheet updated but the website didn't".
    bypass_cache = (request.args.get('fresh', '') or '').strip() in ('1', 'true', 'yes')
    with _presence_board_lock:
        cached = _presence_board_cache.get("data")
        fresh = bool(cached) and (time.time() - _presence_board_cache["ts"] < PRESENCE_BOARD_TTL_S)
        board = cached if (fresh and not bypass_cache) else None
    if board is None:
        try:
            items = _presence_fetch_roster(get_tenant_access_token())
        except Exception as e:
            return jsonify({"success": False, "error": f"Could not read the Agent Roster: {e}"}), 502
        board = _presence_build_board(items, uat)
        with _presence_board_lock:
            _presence_board_cache["ts"] = time.time()
            _presence_board_cache["data"] = board
    # is_me is caller-specific -- layered per request on top of the shared board.
    me_oid, me_nm = open_id.strip().lower(), (user or "").strip().lower()
    agents = []
    for a in board["agents"]:
        aa = dict(a)
        aa["is_me"] = bool((me_oid and aa["open_id"].lower() == me_oid) or
                           (me_nm and aa["name"].lower() == me_nm))
        agents.append(aa)
    now_ms = int(time.time() * 1000)
    return jsonify({"success": True, "server_time_ms": now_ms, "counts": board["counts"],
                    "agents": agents, "statuses": PRESENCE_STATUSES, "cached": bool(board is cached and fresh)})

@app.route('/api/presence/set', methods=['POST'])
@rate_limit(*RATE_LIMIT_RECORDS)
def presence_set():
    """THE status write. One batch_update to the caller's Agent Roster row
    (Current Status + Status Change Time + Memory) -- batch, NOT single-record,
    because the native Auto-Log / Smart Punch / ticket-pull workflows only
    fire for 'openAPIBatchUpdate' (see the Phase 11 header note). Non-admins
    can only move their OWN row (matched by open_id, then by exact Lark name);
    admins may pass 'target' (record_id or open_id) to move anyone. Idempotent:
    setting the status the row already has costs ZERO Feishu writes."""
    body = request.get_json(silent=True) or {}
    user      = sanitize_text(body.get('user', ''))
    email     = sanitize_text(body.get('email', ''))
    open_id   = sanitize_text(body.get('open_id', ''))
    new_status = sanitize_text(body.get('status', ''))
    target    = sanitize_text(body.get('target', ''))
    if new_status not in PRESENCE_STATUSES:
        return jsonify({"success": False, "error": f"Invalid status. Must be one of: {', '.join(PRESENCE_STATUSES)}"}), 400
    perms = _presence_gate(email, user)
    if not perms:
        return jsonify({"success": False, "error": "Access denied: Audition Status module required."}), 403
    is_admin = bool(perms.get("is_super_admin") or "admin" in perms.get("modules", []))
    if target and not is_admin:
        return jsonify({"success": False, "error": "Only admins can set another member's status."}), 403
    if MOCK_MODE:
        return jsonify({"success": True, "status": new_status, "previous": "Offline",
                        "changed_at": int(time.time()*1000), "unchanged": False,
                        "agent": {"record_id": "recMockA", "name": user or "Mock Agent", "open_id": open_id}})
    try:
        items = _presence_fetch_roster(get_tenant_access_token())
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the Agent Roster: {e}"}), 502
    me_oid, me_nm = open_id.strip().lower(), (user or "").strip().lower()
    tgt_l = target.strip().lower()
    row = None
    for it in items:
        rid, oid, nm = _presence_row_identity(it)
        if is_admin and tgt_l:
            if rid.lower() == tgt_l or oid.lower() == tgt_l:
                row = it; break
        else:
            if (me_oid and oid.lower() == me_oid) or (me_nm and nm.lower() == me_nm):
                row = it; break
    if not row:
        return jsonify({"success": False,
                        "error": "You are not on the Agent Roster yet — ask an admin to add you there first."}), 404
    rid, oid, nm = _presence_row_identity(row)
    old_status = extract_field_text((row.get("fields") or {}).get("Current Status", "")) or ""
    if old_status == new_status:
        return jsonify({"success": True, "unchanged": True, "status": new_status, "previous": old_status,
                        "agent": {"record_id": rid, "name": nm, "open_id": oid}})
    now_ms = cairo_epoch_ms(cairo_now())
    # Phase 11.2: the OLD bitable v1 batch_update (PUT .../bitable/v1/...
    # /records/batch_update) is BROKEN on this tenant — verified live
    # 2026-10-16: it answers 99992402 "field validation failed" for ANY
    # field on ANY table (a same-value no-op write fails identically),
    # while single-record PUT and the v3 API both succeed. The v3 Base API
    # (POST /open-apis/base/v3/.../records/batch_update, update_records map)
    # works AND still fires the native workflows as "openAPIBatchUpdate"
    # (verified live: a v3 status flip opened a Time Logs segment via
    # Auto-Log wkfCXTVstBdVo63z within seconds). v3 CellValue shapes:
    # select = ["Option"] array, datetime = epoch-ms integer.
    url = f"https://open.feishu.cn/open-apis/base/v3/bases/{BASE_ID}/tables/{ROSTER_TABLE_ID}/records/batch_update"
    payload = {"update_records": {rid: {
        "Current Status": [new_status],
        "Status Change Time": now_ms,
        "Memory": [old_status or new_status]}}}
    try:
        resp = feishu_session.post(url, headers={"Authorization": f"Bearer {get_tenant_access_token()}",
                                                 "Content-Type": "application/json"},
                                   json=payload, timeout=15)
        data = resp.json()
    except Exception as e:
        return jsonify({"success": False, "error": f"Status write failed: {e}"}), 502
    if data.get("code") != 0:
        return jsonify({"success": False, "error": data.get("msg") or "Feishu rejected the status write"}), 502
    # The board cache must reflect the change on the very next read (this
    # instance at least) -- the actor's own UI refresh is the hot path.
    with _presence_board_lock:
        _presence_board_cache["ts"] = 0.0
        _presence_board_cache["data"] = None
    audit.log(user or email, "PRESENCE_SET", f"{nm}: {old_status or '—'} -> {new_status}",
              ip=request.headers.get("X-Forwarded-For", ""), severity="Info")
    return jsonify({"success": True, "unchanged": False, "status": new_status, "previous": old_status,
                    "changed_at": now_ms,
                    "agent": {"record_id": rid, "name": nm, "open_id": oid}})

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
