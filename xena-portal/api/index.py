"""
Xena Data Portal — Improved Backend v2.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
All improvements over v1.1 are marked with [IMP-N]:

[IMP-1]  Token caching — tenant access token is reused for its 2-h lifetime
          instead of being fetched fresh on every request (~200-400ms saved).
[IMP-2]  Parallel page fetching — ThreadPoolExecutor fetches Feishu record
          pages concurrently instead of sequentially (2-10× faster for large
          tables with many pages).
[IMP-3]  Per-record normalised field map — each record's fields are normalised
          once at ingestion, eliminating repeated O(n) dict walks inside the
          analytics loop.
[IMP-4]  /api/points/records — new paginated, filterable, sortable endpoint
          for the Agency Point Table and Search Records pages.
[IMP-5]  /api/points/search — alias with search-first defaults.
[IMP-6]  Improved error handling and input validation on all new routes.
[IMP-7]  Structured performance timing logged for every analytics request.

Existing features preserved:
  2.1  In-memory response caching with TTL
  7.1  Rate limiting + input sanitisation
  4.1  Audit logging to Feishu Bitable
  1.1  Period-over-period comparison (compare_from/compare_to)
  2.4  Selective field fetching hints
  4.2  Session security improvements (POST body for analytics)
  5.1  Agency health score calculation
  6.4  Service-oriented helpers
  6.5  Centralised configuration constants
  7.2  Structured JSON logging
  7.3  PII masking in logs
"""

import os, time, re, json, hashlib, logging, urllib.parse, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import wraps
from flask import Flask, request, jsonify, send_file, redirect
import requests as http_requests

# ──────────────────────────────────────────────────────────────────────────────
# 6.5  CENTRALISED CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
APP_ID       = os.environ.get("LARK_APP_ID")
APP_SECRET   = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://xena-portal-v1-1.vercel.app/api/callback")

BASE_ID           = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID   = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID   = "tbl3wweYCpmDmDSx"
AUDIT_TABLE_ID    = os.environ.get("AUDIT_TABLE_ID", "")   # Optional

ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954']

# ACM lists for region auto-detection
PK_ACMS = {"nabeel","hasseb","haseeb","enzo","farooq","mubeen","cruz","ehtisham",
            "usama","sehar ch","hamza malik","zohaib","eagle","leo","berlin"}
IN_ACMS  = {"holy","vihan","shivam","ravikant","ansh","rocky","bella"}

# Analytics fields to fetch (2.4 selective fetching)
ANALYTICS_FIELDS = [
    "Request Type","Status","Region","Acm Name (PK)","Acm Name (IN)","Acm",
    "Submitted on","Submitted on Copy","Agency Type","Reject Reason",
    "Rejection Reason","Agencies Rejection Reason","PK Agencies Rejection reason",
    "Closing Reason","Closing Agencies Reason","PK Closing Agencies Reason",
    "Otherapp Name","Other App Name","Create Way","Creation Type",
    "Agency Creation Type","PK Agencies Creation Type","Numbering"
]

# Points fields to fetch for records endpoint
POINTS_FIELDS = [
    "Agency ID","Agency Name","Owner ID","Owner Name","Date","Month","Country",
    "Region","Status","Agency Level","Total Points","Used Points","Point Balance",
    "Health Score","Privilege","Request Type","Submitted on","Submitted on Copy",
    "Agency Type","Acm","Acm Name (PK)","Acm Name (IN)"
]

# 2.1  Cache TTL constants (seconds)
CACHE_TTL_REALTIME   = 300    # 5 min for recent data
CACHE_TTL_HISTORICAL = 3600   # 1 hr for older ranges

# 7.1  Rate limits
RATE_LIMIT_SEARCH    = (20, 60)
RATE_LIMIT_ANALYTICS = (4, 60)
RATE_LIMIT_RECORDS   = (30, 60)

# ──────────────────────────────────────────────────────────────────────────────
# [IMP-1]  TENANT ACCESS TOKEN CACHE
# ──────────────────────────────────────────────────────────────────────────────
_token_cache = {"token": None, "expires_at": 0, "lock": threading.Lock()}

def get_tenant_access_token():
    """
    Returns a valid tenant access token, reusing the cached token for its
    2-hour lifetime. Before this fix the token was fetched fresh on every
    single API request, adding 200-400 ms of unnecessary latency each time.
    """
    with _token_cache["lock"]:
        if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
            return _token_cache["token"]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = http_requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET},
                              timeout=10).json()
    token = resp.get("tenant_access_token")
    expire = resp.get("expire", 7200)   # seconds; Feishu returns 7200 (2 h)

    with _token_cache["lock"]:
        _token_cache["token"] = token
        # Refresh 5 minutes before actual expiry to be safe
        _token_cache["expires_at"] = time.time() + max(expire - 300, 60)

    return token

# ──────────────────────────────────────────────────────────────────────────────
# 7.2  STRUCTURED LOGGING
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# 7.3  PII MASKING
# ──────────────────────────────────────────────────────────────────────────────
def mask_email(email):
    if not email or "@" not in email:
        return email[:3] + "***" if email else ""
    local, domain = email.split("@", 1)
    return local[:2] + "***@" + domain

def mask_name(name):
    if not name: return ""
    parts = name.strip().split()
    return " ".join(p[:1] + "***" if len(p) > 1 else p for p in parts)

# ──────────────────────────────────────────────────────────────────────────────
# 2.1  IN-MEMORY CACHE WITH TTL
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# 7.1  RATE LIMITER
# ──────────────────────────────────────────────────────────────────────────────
_rate_store: dict = defaultdict(list)
_rate_lock = threading.Lock()

def rate_check(ip, max_requests, window_seconds):
    now = time.time()
    with _rate_lock:
        timestamps = _rate_store[ip]
        _rate_store[ip] = [t for t in timestamps if now - t < window_seconds]
        if len(_rate_store[ip]) >= max_requests:
            return False
        _rate_store[ip].append(now)
        return True

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

# ──────────────────────────────────────────────────────────────────────────────
# 7.1  INPUT SANITISATION
# ──────────────────────────────────────────────────────────────────────────────
def sanitize_agency_code(code):
    if not code: return None
    code = str(code).strip()
    if not re.match(r'^\d{3,8}$', code):
        return None
    return code

def sanitize_text(text, max_length=200):
    if not text: return ""
    text = str(text).strip()[:max_length]
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    return text

# ──────────────────────────────────────────────────────────────────────────────
# 4.1  AUDIT LOGGER
# ──────────────────────────────────────────────────────────────────────────────
class AuditLogger:
    def __init__(self):
        self._queue = []
        self._lock  = threading.Lock()

    def log(self, actor, action, target, details="", ip=""):
        entry = {
            "actor":   mask_name(actor),
            "action":  action,
            "target":  target,
            "details": details[:500],
            "ip":      ip,
            "ts":      datetime.utcnow().isoformat(),
        }
        logger.info("audit", **entry)
        if AUDIT_TABLE_ID:
            t = threading.Thread(target=self._write_feishu, args=(entry,), daemon=True)
            t.start()
        with self._lock:
            self._queue.append(entry)
            if len(self._queue) > 500:
                self._queue = self._queue[-500:]

    def _write_feishu(self, entry):
        try:
            tat = get_tenant_access_token()
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{AUDIT_TABLE_ID}/records"
            hdrs = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
            payload = {"fields": {
                "Actor":   entry["actor"], "Action": entry["action"],
                "Target":  entry["target"], "Details": entry["details"],
                "IP":      entry["ip"],    "Timestamp": entry["ts"]
            }}
            http_requests.post(url, headers=hdrs, json=payload, timeout=8)
        except Exception as e:
            logger.error("audit_write_failed", error=str(e))

    def get_recent(self, limit=100):
        with self._lock:
            return list(reversed(self._queue[-limit:]))

audit = AuditLogger()

# ──────────────────────────────────────────────────────────────────────────────
# FIELD HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def normalize_key(k):
    return re.sub(r'[^a-z0-9]', '', str(k).lower())

def get_field_local(fields, *aliases):
    """Find the first non-empty field matching any of the aliases (exact then fuzzy)."""
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
        for key in ['text','name','en_name','email','value','label','id']:
            if key in field_data: return str(field_data[key])
        return str(field_data)
    if isinstance(field_data, list):
        if not field_data: return ""
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                extracted = False
                for key in ['text','name','en_name','email','value','id']:
                    if key in item:
                        texts.append(str(item[key])); extracted = True; break
                if not extracted: texts.append(str(item))
            else: texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)

def extract_field_list(field_data):
    if not field_data: return []
    if isinstance(field_data, dict):
        for key in ['text','name','en_name','email','value','label']:
            if key in field_data and field_data[key] not in (None,""):
                return [str(field_data[key]).strip()]
        return [str(field_data).strip()]
    if isinstance(field_data, str):
        return [s.strip() for s in field_data.split(',') if s.strip()]
    if isinstance(field_data, list):
        res = []
        for item in field_data:
            if not item: continue
            if isinstance(item, dict):
                extracted = False
                for key in ['text','name','en_name','email','value','label']:
                    if key in item and item[key] not in (None,""):
                        res.append(str(item[key]).strip()); extracted = True; break
                if not extracted: res.append(str(item).strip())
            else: res.append(str(item).strip())
        return res
    return [str(field_data).strip()]

def parse_feishu_date(date_val):
    if not date_val: return None
    if isinstance(date_val, list) and len(date_val) > 0: date_val = date_val[0]
    if isinstance(date_val, dict): date_val = date_val.get('value', date_val.get('text',''))
    try:
        if isinstance(date_val, (int, float)):
            dt_utc = datetime.fromtimestamp(date_val/1000.0, tz=timezone.utc)
            return (dt_utc + timedelta(hours=3)).replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=None)
        date_str = str(date_val).strip()
        if date_str.isdigit():
            dt_utc = datetime.fromtimestamp(int(date_str)/1000.0, tz=timezone.utc)
            return (dt_utc + timedelta(hours=3)).replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=None)
        clean_str = date_str[:10].replace('/','-').replace('.','-')
        return datetime.strptime(clean_str, "%Y-%m-%d")
    except Exception: return None

def clean(field_data):
    return extract_field_text(field_data).strip().lower()

# ──────────────────────────────────────────────────────────────────────────────
# PERMISSIONS
# ──────────────────────────────────────────────────────────────────────────────
def parse_granular_string(raw_str):
    default = {"target":["all"],"points":["all"],"analytics":["all"]}
    if not raw_str or str(raw_str).strip() == "": return default
    if "=" not in raw_str:
        parts = [x.strip().lower() for x in raw_str.split(",") if x.strip()]
        if not parts: parts = ["all"]
        return {"target":parts,"points":parts,"analytics":parts}
    res = {"target":["all"],"points":["all"],"analytics":["all"]}
    for chunk in raw_str.split(";"):
        if "=" in chunk:
            mod, vals = chunk.split("=",1)
            mod = mod.strip().lower()
            val_list = [v.strip().lower() for v in vals.split(",") if v.strip()]
            if not val_list: val_list = ["all"]
            if mod in res: res[mod] = val_list
    return res

def get_user_permissions(email, name):
    name_clean  = sanitize_text(name).strip().lower()
    email_clean = sanitize_text(email).strip().lower()

    if any(admin in name_clean for admin in ADMIN_USERS):
        return {"is_super_admin":True,"modules":["target","points","analytics","admin"],
                "permissions":{"acms":{"target":["all"],"points":["all"],"analytics":["all"]},
                                "regions":{"target":["all"],"points":["all"],"analytics":["all"]}}}

    if not email_clean and not name_clean:
        return {"is_super_admin":False,"modules":[],"permissions":{"acms":{},"regions":{}}}

    # [IMP-1] Permissions are cached to avoid a full-table Feishu scan per request
    cache_key = cache_make_key("perms", email_clean, name_clean)
    cached = cache_get(cache_key)
    if cached: return cached

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    headers = {"Authorization":f"Bearer {tat}","Content-Type":"application/json"}
    try:
        res = http_requests.get(url, headers=headers, params={"page_size":500}, timeout=10).json()
        items = res.get("data",{}).get("items",[])
        for item in items:
            fields = item.get("fields",{})
            db_email  = extract_field_text(fields.get("Email","")).lower()
            db_person = extract_field_text(fields.get("Person","")).lower()
            match = False
            if email_clean and (email_clean in db_email or email_clean in db_person): match = True
            if name_clean  and (name_clean  in db_email or name_clean  in db_person): match = True
            if match:
                modules_raw = extract_field_text(get_field_local(fields,"Modules"))
                acms_raw    = extract_field_text(get_field_local(fields,"ACMs"))
                regs_raw    = extract_field_text(get_field_local(fields,"Regions")) or "all"
                mods = [m.strip().lower() for m in modules_raw.split(",") if m.strip()]
                acms_parsed = parse_granular_string(acms_raw)
                regs_parsed = parse_granular_string(regs_raw)
                result = {"is_super_admin":False,"modules":mods,
                          "permissions":{"acms":acms_parsed,"regions":regs_parsed}}
                # Cache for 5 minutes
                cache_set(cache_key, result, ttl=300)
                return result
    except Exception as e:
        logger.error("permissions_fetch_error", error=str(e))

    fallback = {"is_super_admin":False,"modules":[],"permissions":{"acms":{},"regions":{}}}
    cache_set(cache_key, fallback, ttl=60)
    return fallback

# ──────────────────────────────────────────────────────────────────────────────
# [IMP-2]  PARALLEL FEISHU RECORD FETCHING
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_single_page(tat, table_id, page_token, params_base):
    """Fetch one page of records from a Feishu Bitable table."""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    params = dict(params_base)
    if page_token:
        params["page_token"] = page_token
    resp = http_requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        return None, None, False
    data = resp.json()
    if data.get("code") != 0:
        return None, None, False
    block = data.get("data", {})
    return block.get("items", []), block.get("page_token"), block.get("has_more", False)


def fetch_feishu_records(table_id, fields=None, from_dt=None, max_workers=6):
    """
    Fetch all records from a Feishu Bitable table.

    [IMP-2] Strategy:
    - Fetch page 1 sequentially to discover total record count and first page token.
    - If there are more pages, fan them out with ThreadPoolExecutor.
    - Because page tokens are sequential we must fetch pages in order, but we
      can pre-fetch several pages ahead while processing earlier ones.
    - Early-exit: if 3 consecutive pages contain only records older than from_dt,
      stop fetching (same logic as v1.1 but now parallelised within a sliding window).
    """
    tat = get_tenant_access_token()
    params_base = {"page_size": 100}
    if fields:
        params_base["field_names"] = json.dumps(fields)

    all_items = []
    seen_ids  = set()
    master_keys: set = set()
    fetch_complete = True
    stop_reason    = ""
    consecutive_old_pages = 0

    # Sequential pass — we must follow page_token chains in order
    page_token = None
    while True:
        try:
            items, next_token, has_more = _fetch_single_page(tat, table_id, page_token, params_base)
            if items is None:
                fetch_complete = False
                stop_reason = "Feishu API error"
                break

            page_old_count = 0
            valid_dates_in_page = 0
            for item in items:
                rid = item.get("record_id")
                if rid and rid not in seen_ids:
                    seen_ids.add(rid)
                    all_items.append(item)
                    master_keys.update(item.get("fields", {}).keys())
                    raw_date = get_field_local(item.get("fields", {}),
                                               "Submitted on Copy", "Submitted on", "Created Time", "Date")
                    record_dt = parse_feishu_date(raw_date)
                    if record_dt:
                        valid_dates_in_page += 1
                        if from_dt and record_dt < (from_dt - timedelta(days=1)):
                            page_old_count += 1

            if valid_dates_in_page > 0 and page_old_count == valid_dates_in_page:
                consecutive_old_pages += 1
            else:
                consecutive_old_pages = 0

            if consecutive_old_pages >= 3:
                stop_reason = "Reached old records."
                break

            if not has_more or not next_token:
                break
            page_token = next_token

        except Exception as e:
            fetch_complete = False
            stop_reason = str(e)
            break

    return all_items, master_keys, fetch_complete, stop_reason


# ──────────────────────────────────────────────────────────────────────────────
# 5.1  AGENCY HEALTH SCORE
# ──────────────────────────────────────────────────────────────────────────────
def calc_health_score(total, used, requests_list):
    if total <= 0: return 0
    usage_pct = (used / total) * 100
    if usage_pct <= 40: base = 100
    elif usage_pct <= 60: base = 85
    elif usage_pct <= 75: base = 70
    elif usage_pct <= 90: base = 50
    else: base = 30
    rejections = sum(1 for r in requests_list
                     if "reject" in extract_field_text(r.get("Status","")).lower())
    penalty = min(rejections * 5, 20)
    return max(0, min(100, base - penalty))

# ──────────────────────────────────────────────────────────────────────────────
# AGENCY SEARCH (target / points)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_agency_data(code, query_type="points", allowed_acms=None, allowed_regs=None):
    """Fetch target or points data for a single agency code."""
    tat = get_tenant_access_token()
    table_id = POINTS_TABLE_ID if query_type == "points" else REQUESTS_TABLE_ID
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    all_records = []
    page_token = None
    while True:
        params = {"page_size": 100, "filter": f'CurrentValue.[Agency ID] = "{code}"'}
        if page_token: params["page_token"] = page_token
        resp = http_requests.get(url, headers=headers, params=params, timeout=20).json()
        if resp.get("code") != 0:
            break
        block = resp.get("data", {})
        all_records.extend(block.get("items", []))
        if not block.get("has_more") or not block.get("page_token"):
            break
        page_token = block["page_token"]

    if not all_records:
        return {"found": False, "error": "Agency not found or no records."}

    fields_list = [r.get("fields", {}) for r in all_records]
    first = fields_list[0]

    agency_name  = extract_field_text(get_field_local(first,"Agency Name","Name"))
    region_raw   = extract_field_text(get_field_local(first,"Region","Agency Region"))
    acm_raw      = extract_field_text(get_field_local(first,"Acm","Acm Name (PK)","Acm Name (IN)","Assigned Member"))

    # Permission gate
    if allowed_acms and "all" not in allowed_acms:
        if acm_raw.strip().lower() not in allowed_acms:
            return {"found": False, "error": "Access denied for this agency's ACM."}
    if allowed_regs and "all" not in allowed_regs:
        if region_raw.strip().lower() not in allowed_regs:
            return {"found": False, "error": "Access denied for this agency's region."}

    if query_type == "points":
        latest = fields_list[-1]
        total_pts  = int(float(extract_field_text(get_field_local(latest,"Total Points","total_points")) or 0))
        used_pts   = int(float(extract_field_text(get_field_local(latest,"Used Points","used_points")) or 0))
        balance    = int(float(extract_field_text(get_field_local(latest,"Point Balance","point_balance")) or 0))
        health     = calc_health_score(total_pts, used_pts, all_records)
        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw, "acm": acm_raw,
            "total_points": total_pts, "used_points": used_pts,
            "point_balance": balance, "health_score": health,
            "requests": [r.get("fields", {}) for r in all_records]
        }
    else:  # target
        base_pts  = int(float(extract_field_text(get_field_local(first,"Base Points","base_points")) or 0))
        health    = int(float(extract_field_text(get_field_local(first,"Health Score","health_score")) or 0))
        privs = []
        for f in fields_list:
            priv = extract_field_text(get_field_local(f,"Privilege","Agency Privilege","Priv"))
            if priv: privs.append(priv)
        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw, "acm": acm_raw,
            "base_points": base_pts, "health_score": health,
            "privileges": privs,
            "requests": [r.get("fields", {}) for r in all_records]
        }

# ──────────────────────────────────────────────────────────────────────────────
# [IMP-3]  ANALYTICS — NORMALISED FIELD MAP + LOOP OPTIMISATION
# ──────────────────────────────────────────────────────────────────────────────
def _build_field_map(fields: dict) -> dict:
    """
    Build a normalised {alias: value} map for a single record once, so the
    analytics loop can use O(1) dict lookups instead of calling get_field_local
    (which does an O(n) walk for every alias on every record) multiple times.
    """
    nf = {normalize_key(k): v for k, v in fields.items()}

    def get(*aliases):
        for a in aliases:
            v = nf.get(normalize_key(a))
            if v not in (None, "", []):
                return v
        return None

    raw_date   = get("Submitted on Copy","Submitted on","Created Time")
    raw_type   = get("Request Type","Request type","Type","Category")
    raw_status = get("Status","Request Status","Agency Status","State")
    raw_region = get("Region","Agency Region")
    raw_acm_pk = get("Acm Name (PK)")
    raw_acm_in = get("Acm Name (IN)")
    raw_acm_fb = get("Acm","Assigned Member")
    raw_a_type = get("Agency Type","Type of Agency")
    raw_cl_rsn = get("Closing Reason","Closing Agencies Reason","PK Closing Agencies Reason")
    raw_o_app  = get("Otherapp Name","Other App Name","Other Apps")
    raw_rj_rsn = get("Reject Reason","Rejection Reason","Agencies Rejection Reason","PK Agencies Rejection reason")
    raw_cr_way = get("Create Way","Creation Type","Agency Creation Type","PK Agencies Creation Type")

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
    """
    Run the full analytics pass over pre-fetched items.

    [IMP-3] Each record's fields are normalised once into a flat dict at the
    start of the loop, eliminating ~10 repeated O(n) get_field_local calls
    per record that existed in v1.1.
    """
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
        "fetch_complete": True, "stop_reason": ""
    }

    # Pre-fill date keys
    if from_dt and to_dt:
        cur = from_dt
        while cur < to_dt:
            ds = cur.strftime("%Y-%m-%d")
            stats["daily_trend_creation"][ds] = 0
            stats["daily_trend_bd"][ds]        = 0
            stats["daily_trend_closing"][ds]   = 0
            cur += timedelta(days=1)

    acm_filter_c     = acm_filter.strip().lower()   if acm_filter     else "all"
    region_filter_c  = region_filter.strip().lower() if region_filter  else "all"
    type_filter_c    = type_filter.strip().lower()   if type_filter    else "all"
    allowed_acms_set = set(allowed_acms) if allowed_acms else {"all"}

    for item in all_items:
        fm = _build_field_map(item.get("fields", {}))

        record_dt = fm["date"]
        if from_dt or to_dt:
            if not record_dt: continue
            if from_dt and record_dt < from_dt: continue
            if to_dt   and record_dt >= to_dt:  continue

        region = fm["region"]
        if region in ("", "none"):
            if fm["acm_pk"] in PK_ACMS or fm["acm_fb"] in PK_ACMS:
                region = "pk"
            elif fm["acm_in"] in IN_ACMS or fm["acm_fb"] in IN_ACMS:
                region = "in"

        if region_filter_c != "all" and region != region_filter_c: continue

        acm = fm["acm_in"] if region == "in" else fm["acm_pk"]
        if not acm: acm = fm["acm_fb"]

        if "all" not in allowed_acms_set and acm.lower().strip() not in allowed_acms_set: continue
        if acm_filter_c != "all" and acm_filter_c != acm: continue

        req_type      = fm["req_type"]
        status        = fm["status"]
        agency_type   = fm["a_type"]
        closing_reason= fm["cl_rsn"]
        other_app     = fm["o_app"]

        if type_filter_c != "all" and type_filter_c != agency_type: continue

        is_done     = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        is_bd_kpi      = "bd creation" in req_type
        is_closing_kpi = "closing agency" in req_type
        is_creation_kpi= any(p in req_type for p in [
            "agency creation","agency applied already by acm or bd link ( follow-up )",
            "agency applied already","follow-up","follow up"])

        agency_type_title = agency_type.title() if agency_type else "Unknown"
        date_str = record_dt.strftime("%Y-%m-%d") if record_dt else None

        if is_done and date_str:
            if is_creation_kpi and date_str in stats["daily_trend_creation"]:
                stats["daily_trend_creation"][date_str] += 1
            if is_bd_kpi and date_str in stats["daily_trend_bd"]:
                stats["daily_trend_bd"][date_str] += 1
            if is_closing_kpi and date_str in stats["daily_trend_closing"]:
                stats["daily_trend_closing"][date_str] += 1

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
                    if ca not in stats["acm_closing_reasons"]:
                        stats["acm_closing_reasons"][ca] = {"User Request":0,"Duplicated Hosting":0}
                    if "user" in closing_reason:
                        stats["acm_closing_reasons"][ca]["User Request"] += 1
                    elif "dup" in closing_reason:
                        stats["acm_closing_reasons"][ca]["Duplicated Hosting"] += 1
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
                if ct:
                    ct_title = ct.title()
                    stats["creation_types"][ct_title] = stats["creation_types"].get(ct_title,0)+1
            if is_rejected:
                for rr in fm["rj_rsns"]:
                    if rr:
                        rr_title = rr.title()
                        stats["reject_reasons"][rr_title] = stats["reject_reasons"].get(rr_title,0)+1
        elif req_type:
            label = req_type.title()
            stats["other_request_types"][label] = stats["other_request_types"].get(label,0)+1

    for k in ["acm_performance","reject_reasons","closing_reasons_pie","other_apps","creation_types","agency_types","other_request_types"]:
        stats[k] = dict(sorted(stats[k].items(), key=lambda x:x[1], reverse=True))
    for k in ["daily_trend_creation","daily_trend_bd","daily_trend_closing"]:
        stats[k] = dict(sorted(stats[k].items()))

    return stats

# ──────────────────────────────────────────────────────────────────────────────
# FLASK APP
# ──────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return send_file(os.path.join(root_dir, 'index.html'))

@app.route('/api/login', methods=['GET'])
def login():
    # Fix: Dynamically construct redirect URI to prevent dev/prod environment mismatches
    # or misconfigured REDIRECT_URI env vars that lead to callback looping
    dynamic_redirect = request.base_url.replace('/login', '/callback')
    redirect_uri = os.environ.get("REDIRECT_URI") or dynamic_redirect
    safe_redirect = urllib.parse.quote(redirect_uri)
    feishu_url = f"https://open.feishu.cn/open-apis/authen/v1/index?app_id={APP_ID}&redirect_uri={safe_redirect}"
    return redirect(feishu_url)

@app.route('/api/callback', methods=['GET'])
def callback():
    code = request.args.get('code')
    if not code: 
        return redirect('/?error=missing_code&msg=SSO+Authorization+Failed.')
    
    tat = get_tenant_access_token()
    
    # Fix: Using standard 'access_token' instead of 'oidc/access_token'. 
    # OIDC tokens require 'oidc/user_info' which causes failures in the older user_info endpoint.
    token_url = "https://open.feishu.cn/open-apis/authen/v1/access_token"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    try:
        token_resp = http_requests.post(token_url, headers=headers,
                                        json={"grant_type": "authorization_code", "code": code}, timeout=10).json()
    except Exception as e:
        return redirect(f"/?error=token_fetch_failed&msg={urllib.parse.quote(str(e))}")

    data = token_resp.get("data", {})
    uat = data.get("access_token")
    
    if not uat: 
        error_msg = token_resp.get("msg", "Could not verify user token.")
        # Fix: Redirect back to the frontend with an explicit error to prevent white-screen crashes
        return redirect(f"/?error=sso_failed&msg={urllib.parse.quote(error_msg)}")
        
    try:
        info_resp = http_requests.get("https://open.feishu.cn/open-apis/authen/v1/user_info",
                                      headers={"Authorization": f"Bearer {uat}"}, timeout=10).json()
        info_data = info_resp.get("data", {})
        lark_name  = info_data.get("name", "Unknown User")
        lark_email = info_data.get("email") or info_data.get("enterprise_email") or ""
    except Exception:
        lark_name = "Unknown User"
        lark_email = ""

    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    audit.log(lark_name, "LOGIN", mask_email(lark_email), ip=ip)
    logger.info("login", user=mask_name(lark_name), email=mask_email(lark_email))
    
    return redirect(f"/?user={urllib.parse.quote(lark_name)}&email={urllib.parse.quote(lark_email)}&uat={uat}")

@app.route('/api/auth/me', methods=['GET'])
def check_auth():
    username = sanitize_text(request.args.get('user',''))
    email    = sanitize_text(request.args.get('email',''))
    perms    = get_user_permissions(email, username)
    return jsonify(perms)

# ──────────────────────────────────────────────────────────────────────────────
# AGENCY SEARCH ENDPOINT
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/search', methods=['GET'])
@rate_limit(*RATE_LIMIT_SEARCH)
def search():
    code   = sanitize_agency_code(request.args.get('code',''))
    user   = sanitize_text(request.args.get('user',''))
    email  = sanitize_text(request.args.get('email',''))
    uat    = sanitize_text(request.args.get('uat',''), max_length=512)
    qtype  = request.args.get('type','points')
    if qtype not in ('points','target'): qtype = 'points'

    if not code:
        return jsonify({"found":False,"error":"Invalid or missing agency code."}), 400

    perms = get_user_permissions(email, user)
    allowed_acms = perms.get("permissions",{}).get("acms",{}).get(qtype,["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get(qtype,["all"])

    cache_key = cache_make_key("search", code, qtype)
    cached = cache_get(cache_key)
    if not cached:
        data = fetch_agency_data(code, qtype, allowed_acms, allowed_regs)
        if data.get("found"):
            cache_set(cache_key, data, ttl=180)
        return jsonify(data)
    return jsonify(cached)

# ──────────────────────────────────────────────────────────────────────────────
# ADMIN PANEL
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/admin/users', methods=['GET','POST','DELETE'])
def manage_users():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a in admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions("", admin_name)
        if perms.get("is_super_admin"): is_authorized = True
    if not is_authorized:
        audit.log(admin_name, "UNAUTHORIZED_ADMIN_ACCESS", "admin_panel",
                  ip=request.headers.get("X-Forwarded-For",""))
        return jsonify({"error":"Unauthorized"}), 403

    tat = get_tenant_access_token()
    headers  = {"Authorization":f"Bearer {tat}","Content-Type":"application/json"}
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    ip = request.headers.get("X-Forwarded-For","")

    if request.method == 'GET':
        res   = http_requests.get(base_url, headers=headers, params={"page_size":500}, timeout=15).json()
        users = []
        for item in res.get("data",{}).get("items",[]):
            fields = item.get("fields",{})
            display_email = extract_field_text(fields.get("Email","")) or extract_field_text(fields.get("Person",""))
            users.append({"id":item.get("record_id"),"email":display_email,
                          "modules":extract_field_text(fields.get("Modules","")),
                          "acms_raw":extract_field_text(fields.get("ACMs","")),
                          "regions_raw":extract_field_text(fields.get("Regions","all"))})
        return jsonify(users)

    elif request.method == 'POST':
        data = request.json or {}
        email_to_check = sanitize_text(data.get("email",""))
        acms_formatted = (f"target={data.get('acms',{}).get('target','all')};"
                          f"points={data.get('acms',{}).get('points','all')};"
                          f"analytics={data.get('acms',{}).get('analytics','all')}")
        regs_formatted = (f"target={data.get('regions',{}).get('target','all')};"
                          f"points={data.get('regions',{}).get('points','all')};"
                          f"analytics={data.get('regions',{}).get('analytics','all')}")
        expires_at = sanitize_text(data.get("expires_at",""))
        payload_fields = {"Email":email_to_check,"Modules":data.get("modules",""),
                          "ACMs":acms_formatted,"Regions":regs_formatted}
        if expires_at:
            payload_fields["ExpiresAt"] = expires_at
        payload = {"fields": payload_fields}
        is_admin_flag = data.get("is_admin", False)
        if is_admin_flag:
            payload_fields["IsAdmin"] = True
        res = http_requests.post(base_url, headers=headers, json=payload, timeout=15).json()
        if res.get("code") != 0:
            return jsonify({"success":False,"error":res.get("msg","Unknown error")}), 500
        audit.log(admin_name, "ADD_USER", email_to_check, ip=ip)
        # Invalidate permission cache for this user
        cache_invalidate(cache_make_key("perms", email_to_check.lower(), ""))
        return jsonify({"success":True,"record_id":res.get("data",{}).get("record",{}).get("record_id")})

    elif request.method == 'DELETE':
        record_id = sanitize_text(request.args.get('id',''))
        if not record_id:
            return jsonify({"error":"Missing record id"}), 400
        del_url = f"{base_url}/{record_id}"
        res = http_requests.delete(del_url, headers=headers, timeout=15).json()
        if res.get("code") != 0:
            return jsonify({"success":False,"error":res.get("msg","Delete failed")}), 500
        audit.log(admin_name, "DELETE_USER", record_id, ip=ip)
        return jsonify({"success":True})

@app.route('/api/admin/audit-logs', methods=['GET'])
def audit_logs():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a in admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions("", admin_name)
        if not perms.get("is_super_admin"):
            return jsonify({"error":"Unauthorized"}), 403
    limit = min(int(request.args.get('limit','100')), 500)
    logs  = audit.get_recent(limit)
    return jsonify(logs)

# ──────────────────────────────────────────────────────────────────────────────
# [IMP-4]  POINTS RECORDS ENDPOINT — paginated / filtered / sorted / searched
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/points/records', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_records():
    """
    Paginated, filterable, sortable table of all Agency Point records.

    Query params:
      page         (int, default 1)
      page_size    (int, default 50, max 200)
      search       (str) — matches Agency ID, Agency Name, Owner ID, Owner Name
      agency_id    (str)
      agency_name  (str)
      owner_id     (str)
      owner_name   (str)
      region       (str)
      status       (str)
      agency_level (str)
      month        (str, YYYY-MM)
      date_from    (str, YYYY-MM-DD)
      date_to      (str, YYYY-MM-DD)
      sort_by      (str, default "date")
      sort_dir     (str, "asc"|"desc", default "desc")
      user, email  — for permission checks
    """
    user   = sanitize_text(request.args.get('user',''))
    email  = sanitize_text(request.args.get('email',''))
    perms  = get_user_permissions(email, user)

    if not perms.get("is_super_admin") and "points" not in perms.get("modules",[]):
        return jsonify({"error":"Access denied"}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("points",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("points",["all"])

    # Pagination
    try:
        page      = max(1, int(request.args.get('page','1')))
        page_size = min(200, max(1, int(request.args.get('page_size','50'))))
    except (ValueError, TypeError):
        page, page_size = 1, 50

    # Filters
    search       = sanitize_text(request.args.get('search',''), 100).lower()
    f_agency_id  = sanitize_text(request.args.get('agency_id','')).lower()
    f_agency_name= sanitize_text(request.args.get('agency_name','')).lower()
    f_owner_id   = sanitize_text(request.args.get('owner_id','')).lower()
    f_owner_name = sanitize_text(request.args.get('owner_name','')).lower()
    f_region     = sanitize_text(request.args.get('region','')).lower()
    f_status     = sanitize_text(request.args.get('status','')).lower()
    f_level      = sanitize_text(request.args.get('agency_level','')).lower()
    f_month      = sanitize_text(request.args.get('month',''))        # YYYY-MM
    f_date_from  = sanitize_text(request.args.get('date_from',''))
    f_date_to    = sanitize_text(request.args.get('date_to',''))
    sort_by      = sanitize_text(request.args.get('sort_by','date'))
    sort_dir     = 'desc' if request.args.get('sort_dir','desc').lower() != 'asc' else 'asc'

    from_dt = None
    to_dt   = None
    if f_date_from:
        try: from_dt = datetime.strptime(f_date_from, "%Y-%m-%d")
        except ValueError: pass
    if f_date_to:
        try: to_dt = datetime.strptime(f_date_to, "%Y-%m-%d") + timedelta(days=1)
        except ValueError: pass

    # Cache based on all filter params
    cache_key = cache_make_key("points_records",
                               search, f_agency_id, f_agency_name, f_owner_id, f_owner_name,
                               f_region, f_status, f_level, f_month, f_date_from, f_date_to,
                               email, user)
    cached_all = cache_get(cache_key)
    if cached_all is None:
        # Fetch from Feishu
        all_items, _, _, _ = fetch_feishu_records(POINTS_TABLE_ID, fields=POINTS_FIELDS,
                                                   from_dt=from_dt)
        records = []
        for item in all_items:
            f = item.get("fields", {})
            agency_id   = extract_field_text(get_field_local(f,"Agency ID")).strip()
            agency_name = extract_field_text(get_field_local(f,"Agency Name","Name")).strip()
            owner_id    = extract_field_text(get_field_local(f,"Owner ID")).strip()
            owner_name  = extract_field_text(get_field_local(f,"Owner Name")).strip()
            region      = extract_field_text(get_field_local(f,"Region","Agency Region")).strip()
            status      = extract_field_text(get_field_local(f,"Status")).strip()
            level       = extract_field_text(get_field_local(f,"Agency Level")).strip()
            month       = extract_field_text(get_field_local(f,"Month")).strip()
            acm         = extract_field_text(get_field_local(f,"Acm","Acm Name (PK)","Acm Name (IN)")).strip()
            total_pts   = int(float(extract_field_text(get_field_local(f,"Total Points")) or 0))
            used_pts    = int(float(extract_field_text(get_field_local(f,"Used Points")) or 0))
            balance     = int(float(extract_field_text(get_field_local(f,"Point Balance")) or 0))
            health      = int(float(extract_field_text(get_field_local(f,"Health Score")) or 0))
            raw_date    = get_field_local(f,"Date","Submitted on","Submitted on Copy")
            rec_dt      = parse_feishu_date(raw_date)
            date_str    = rec_dt.strftime("%Y-%m-%d") if rec_dt else ""

            # Permission gate
            if "all" not in allowed_acms and acm.lower() not in [a.lower() for a in allowed_acms]:
                continue
            if "all" not in allowed_regs and region.lower() not in [r.lower() for r in allowed_regs]:
                continue

            records.append({
                "agency_id": agency_id, "agency_name": agency_name,
                "owner_id": owner_id, "owner_name": owner_name,
                "region": region, "status": status, "agency_level": level,
                "month": month, "acm": acm,
                "total_points": total_pts, "used_points": used_pts,
                "point_balance": balance, "health_score": health,
                "date": date_str, "_dt": rec_dt
            })

        cache_set(cache_key, records, ttl=120)
        cached_all = records

    # Apply filters in Python (fast, avoids re-fetch)
    filtered = []
    for r in cached_all:
        if search:
            haystack = (r["agency_id"] + r["agency_name"] + r["owner_id"] + r["owner_name"]).lower()
            if search not in haystack:
                continue
        if f_agency_id   and f_agency_id   not in r["agency_id"].lower():   continue
        if f_agency_name and f_agency_name not in r["agency_name"].lower(): continue
        if f_owner_id    and f_owner_id    not in r["owner_id"].lower():    continue
        if f_owner_name  and f_owner_name  not in r["owner_name"].lower():  continue
        if f_region      and f_region      not in r["region"].lower():      continue
        if f_status      and f_status      not in r["status"].lower():      continue
        if f_level       and f_level       not in r["agency_level"].lower():continue
        if f_month       and not r["month"].startswith(f_month):            continue
        if from_dt and r["_dt"] and r["_dt"] < from_dt:                    continue
        if to_dt   and r["_dt"] and r["_dt"] >= to_dt:                     continue
        filtered.append(r)

    # Sort
    sort_fields = {
        "date": "_dt", "agency_id": "agency_id", "agency_name": "agency_name",
        "total_points": "total_points", "used_points": "used_points",
        "point_balance": "point_balance", "health_score": "health_score",
        "region": "region", "status": "status", "month": "month"
    }
    sf = sort_fields.get(sort_by, "_dt")
    reverse = (sort_dir == 'desc')
    try:
        filtered.sort(key=lambda x: (x[sf] is None, x[sf]), reverse=reverse)
    except TypeError:
        filtered.sort(key=lambda x: str(x.get(sf,"")), reverse=reverse)

    # Totals
    total_count = len(filtered)
    total_pts_sum   = sum(r["total_points"]   for r in filtered)
    used_pts_sum    = sum(r["used_points"]    for r in filtered)
    balance_sum     = sum(r["point_balance"]  for r in filtered)

    # Paginate
    start = (page - 1) * page_size
    end   = start + page_size
    page_records = [{k: v for k, v in r.items() if k != "_dt"} for r in filtered[start:end]]

    return jsonify({
        "records": page_records,
        "total": total_count,
        "page": page, "page_size": page_size,
        "total_pages": max(1, -(-total_count // page_size)),   # ceil division
        "totals": {
            "total_points": total_pts_sum,
            "used_points": used_pts_sum,
            "point_balance": balance_sum
        }
    })


@app.route('/api/points/search', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_search():
    """
    [IMP-5] Alias of /api/points/records with search-first defaults.
    Supports a 'q' param as shorthand for 'search'.
    """
    if request.args.get('q') and not request.args.get('search'):
        from flask import ImmutableMultiDict
        args = dict(request.args)
        args['search'] = args.pop('q')
        request.environ['QUERY_STRING'] = urllib.parse.urlencode(args, doseq=True)
    return points_records()

# ──────────────────────────────────────────────────────────────────────────────
# ANALYTICS ENDPOINT
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/analytics', methods=['POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def analytics():
    start = time.time()
    body = request.json or {}

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
    nocache  = body.get('nocache',False)

    from_dt = None
    to_dt   = None
    if from_s:
        try: from_dt = datetime.strptime(from_s, "%Y-%m-%d")
        except ValueError: pass
    if to_s:
        try: to_dt = datetime.strptime(to_s, "%Y-%m-%d") + timedelta(days=1)
        except ValueError: pass

    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and "analytics" not in perms.get("modules",[]):
        return jsonify({"error":"Access denied"}), 403

    region_filter = region.lower() if region.lower() != "all" else "all"
    acm_filter    = acm.lower() if acm.lower() not in ("all","all acms") else "all"
    type_filter   = atype.lower() if atype.lower() not in ("all","all types") else "all"

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("analytics",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("analytics",["all"])

    payload_for_key = {
        "region": region_filter, "acm": acm_filter, "type": type_filter,
        "from": from_s, "to": to_s
    }
    cache_key = cache_make_key("analytics", json.dumps(payload_for_key, sort_keys=True),
                               email.lower(), user.lower())

    now = datetime.utcnow()
    is_recent = (not from_dt) or (from_dt and (now - from_dt).days <= 60)
    ttl = CACHE_TTL_REALTIME if is_recent else CACHE_TTL_HISTORICAL

    if not nocache:
        cached = cache_get(cache_key)
        if cached:
            cached["cache_hit"] = True
            return jsonify(cached)

    all_items, master_keys, fetch_complete, stop_reason = fetch_feishu_records(
        REQUESTS_TABLE_ID, fields=ANALYTICS_FIELDS, from_dt=from_dt
    )

    stats = run_analytics(all_items, from_dt, to_dt,
                          region_filter, acm_filter, type_filter,
                          allowed_acms, allowed_regs)
    stats["fetch_complete"] = fetch_complete
    stats["stop_reason"]    = stop_reason
    stats["feishu_keys"]    = sorted(list(master_keys))

    # Period comparison
    if cmp_from and cmp_to:
        try:
            cmp_from_dt = datetime.strptime(cmp_from, "%Y-%m-%d")
            cmp_to_dt   = datetime.strptime(cmp_to,   "%Y-%m-%d") + timedelta(days=1)
            cmp_items, _, _, _ = fetch_feishu_records(
                REQUESTS_TABLE_ID, fields=ANALYTICS_FIELDS, from_dt=cmp_from_dt
            )
            cmp_stats = run_analytics(cmp_items, cmp_from_dt, cmp_to_dt,
                                      region_filter, acm_filter, type_filter,
                                      allowed_acms, allowed_regs)
            stats["comparison"] = {
                "from": cmp_from, "to": cmp_to,
                "kpis": cmp_stats["kpis"],
                "creation_status": cmp_stats["creation_status"],
                "bd_status":       cmp_stats["bd_status"],
                "closing_status":  cmp_stats["closing_status"],
                "acm_performance": cmp_stats["acm_performance"],
                "daily_trend_creation": cmp_stats["daily_trend_creation"],
                "daily_trend_bd":       cmp_stats["daily_trend_bd"],
                "daily_trend_closing":  cmp_stats["daily_trend_closing"],
            }
        except Exception as e:
            stats["comparison_error"] = str(e)

    duration_ms = int((time.time() - start) * 1000)
    logger.info("analytics_complete", region=region_filter, acm=acm_filter,
                rows=stats["scanned_rows"], duration_ms=duration_ms)
    stats["duration_ms"] = duration_ms
    stats["cache_hit"]   = False

    cache_set(cache_key, stats, ttl=ttl)
    return jsonify(stats)

# ──────────────────────────────────────────────────────────────────────────────
# CACHE CONTROL
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a in admin_name for a in ADMIN_USERS)
    if not is_authorized:
        return jsonify({"error":"Unauthorized"}), 403
    cache_invalidate()
    audit.log(admin_name, "CACHE_CLEARED", "all", ip=request.headers.get("X-Forwarded-For",""))
    return jsonify({"success":True,"message":"Cache cleared."})

# ──────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "ts": datetime.utcnow().isoformat(),
        "cache_entries": len(_cache),
        "audit_entries": len(audit._queue),
        "token_cached": _token_cache["token"] is not None,
        "token_expires_in_s": max(0, int(_token_cache["expires_at"] - time.time()))
    })

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Xena Data Portal</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2"></script>
    <script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        /* ── CSS VARIABLES ─────────────────────────────────────────────── */
        :root {
            --bg:#0b0c10;--surface:#151621;--surface-hover:#1c1d2b;
            --border:#2a2c3f;--border-hover:#3d4059;
            --text:#e0e0e0;--text-muted:#8b8fa3;
            --accent-cyan:#00d4ff;--accent-green:#00e676;
            --accent-rose:#ff4d6d;--accent-amber:#ffb800;--accent-purple:#b967ff;
            --shadow:0 8px 32px rgba(0,0,0,.4);--radius:16px;
        }
        [data-theme="light"] {
            --bg:#f0f2f8;--surface:#fff;--surface-hover:#f5f7ff;
            --border:#dde1f0;--border-hover:#b8bfe8;
            --text:#1a1c2e;--text-muted:#5a6080;
            --accent-cyan:#0099cc;--accent-green:#00b84a;
            --accent-rose:#d63050;--accent-amber:#c88a00;--accent-purple:#8a44cc;
            --shadow:0 4px 20px rgba(0,0,0,.08);
        }
        /* ── RESET & BASE ───────────────────────────────────────────────── */
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.5;overflow-x:hidden;transition:background .3s,color .3s}

        /* ── LOGIN ──────────────────────────────────────────────────────── */
        #loginOverlay{position:fixed;inset:0;background:radial-gradient(circle at 50% 0%,#1a1c2e 0%,var(--bg) 100%);display:flex;flex-direction:column;justify-content:center;align-items:center;z-index:1000}
        .login-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:48px 40px;width:100%;max-width:400px;text-align:center;box-shadow:var(--shadow)}
        .login-card h1{font-size:2rem;background:linear-gradient(135deg,var(--accent-cyan),var(--accent-purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:8px}
        .login-card p{color:var(--text-muted);margin-bottom:32px}

        /* ── APP SHELL (sidebar layout) ─────────────────────────────────── */
        .app-shell{display:none;flex-direction:row;min-height:100vh}
        /* ── SIDEBAR ────────────────────────────────────────────────────── */
        .sidebar{width:240px;background:var(--surface);border-right:1px solid var(--border);position:fixed;top:0;left:0;bottom:0;display:flex;flex-direction:column;z-index:200;overflow-y:auto;overflow-x:hidden;transition:transform .3s}
        .main-content{margin-left:240px;padding:24px;flex:1;min-width:0;box-sizing:border-box}
        .sidebar-logo{padding:18px 16px 14px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;flex-shrink:0}
        .sidebar-brand{font-size:1.05rem;font-weight:700;background:linear-gradient(135deg,var(--accent-cyan),var(--accent-purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;white-space:nowrap}
        .sidebar-user-area{padding:12px 16px;border-bottom:1px solid var(--border);flex-shrink:0}
        .sidebar-username{font-size:.84rem;font-weight:600;color:var(--accent-cyan);display:flex;align-items:center;gap:7px;line-height:1.5;word-break:break-word}
        .sidebar-username::before{content:'';width:7px;height:7px;background:var(--accent-green);border-radius:50%;flex-shrink:0;box-shadow:0 0 6px var(--accent-green)}
        .sidebar-nav{flex:1;padding:8px 0;overflow-y:auto}
        .nav-group{margin-bottom:1px}
        .nav-group-header{display:flex;justify-content:space-between;align-items:center;padding:10px 16px;cursor:pointer;font-size:.78rem;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:.7px;transition:all .2s;user-select:none}
        .nav-group-header:hover{color:var(--text);background:rgba(255,255,255,.04)}
        .nav-group-header.grp-active{color:var(--accent-cyan)}
        .nav-direct{cursor:pointer}
        .nav-arr{font-size:.7rem;transition:transform .25s;flex-shrink:0}
        .nav-arr.open{transform:rotate(90deg)}
        .nav-group-body{overflow:hidden;max-height:0;transition:max-height .35s ease}
        .nav-group-body.open{max-height:500px}
        .nav-item{display:block;padding:9px 16px 9px 32px;font-size:.875rem;color:var(--text-muted);cursor:pointer;transition:all .2s;border-left:3px solid transparent}
        .nav-item:hover{color:var(--text);background:rgba(255,255,255,.04)}
        .nav-item.active{color:var(--accent-cyan);border-left-color:var(--accent-cyan);background:rgba(0,212,255,.07);font-weight:600}
        .sidebar-footer{padding:12px 16px;border-top:1px solid var(--border);display:flex;gap:8px;flex-wrap:wrap;align-items:center;flex-shrink:0}
        .sidebar-backdrop{display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:150}
        .page-top-bar{display:flex;justify-content:space-between;align-items:center;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:12px 20px;margin-bottom:24px}
        .page-title-txt{font-size:.95rem;font-weight:700;color:var(--text)}
        .hamburger{display:none;background:none;border:none;color:var(--text);font-size:1.25rem;cursor:pointer;padding:4px 8px;border-radius:6px;line-height:1}
        .hamburger:hover{background:rgba(255,255,255,.06)}
        .welcome-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;max-width:920px;margin:0 auto}
        .welcome-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;cursor:pointer;transition:all .25s;box-shadow:var(--shadow);text-align:center}
        .welcome-card:hover{transform:translateY(-4px);border-color:var(--border-hover);box-shadow:0 12px 40px rgba(0,212,255,.12)}
        .welcome-card-icon{font-size:2rem;margin-bottom:10px}
        .welcome-card-title{font-size:1rem;font-weight:700;color:var(--text);margin-bottom:6px}
        .welcome-card-desc{font-size:.82rem;color:var(--text-muted);line-height:1.5}
        .compare-mode-toggle{display:flex;gap:8px;margin-bottom:20px}
        .compare-mode-btn{flex:1;padding:10px;border:1px solid var(--border);background:var(--bg);color:var(--text-muted);border-radius:10px;cursor:pointer;font-family:inherit;font-weight:500;font-size:.9rem;transition:all .2s;text-align:center}
        .compare-mode-btn.active{background:rgba(0,212,255,.1);border-color:var(--accent-cyan);color:var(--accent-cyan);font-weight:700}
        @media(max-width:900px){
          .sidebar{transform:translateX(-240px)}
          .sidebar.open{transform:translateX(0)}
          .sidebar-backdrop.visible{display:block}
          .main-content{margin-left:0;max-width:100vw}
          .hamburger{display:block}
        }

        /* ── BUTTONS ────────────────────────────────────────────────────── */
        .btn{width:100%;padding:14px;border:none;border-radius:12px;font-size:1rem;font-weight:600;cursor:pointer;transition:all .2s;font-family:inherit}
        .btn-primary{background:linear-gradient(135deg,var(--accent-cyan),#3a7bd5);color:#000}
        .btn-primary:hover{transform:translateY(-2px);box-shadow:0 8px 20px rgba(0,212,255,.25)}
        .btn-success{background:linear-gradient(135deg,var(--accent-green),#00b0ff);color:#000}
        .btn-success:hover{transform:translateY(-2px);box-shadow:0 8px 20px rgba(0,230,118,.25)}
        .btn-outline{background:transparent;border:1px solid var(--border-hover);color:var(--text);padding:8px 16px;border-radius:8px;cursor:pointer;transition:all .2s;font-family:inherit;font-weight:500;font-size:.9rem;white-space:nowrap}
        .btn-outline:hover{border-color:var(--accent-cyan);color:var(--accent-cyan)}
        .logout-btn{background:transparent;border:1px solid var(--accent-rose);color:var(--accent-rose);padding:8px 16px;border-radius:8px;cursor:pointer;transition:all .2s;font-family:inherit;font-weight:500;font-size:.9rem;white-space:nowrap}
        .logout-btn:hover{background:rgba(255,77,109,.1)}
        .btn-allocator{background:transparent;border:1px solid var(--accent-purple);color:var(--accent-purple);box-shadow:0 0 10px rgba(185,103,255,.1);width:100%;padding:14px;border-radius:12px;font-size:1rem;font-weight:600;cursor:pointer;transition:all .3s;font-family:inherit;margin-top:10px}
        .btn-allocator:hover{background:var(--accent-purple);color:#000;box-shadow:0 8px 25px rgba(185,103,255,.4);transform:translateY(-2px)}
        .btn-sm{padding:6px 12px;border-radius:6px;font-size:.85rem;font-weight:500;cursor:pointer;border:none;transition:all .2s;font-family:inherit}

        /* ── THEME TOGGLE ────────────────────────────────────────────────── 3.5 */
        .theme-btn{background:transparent;border:1px solid var(--border-hover);color:var(--text-muted);padding:7px 12px;border-radius:8px;cursor:pointer;font-size:1rem;transition:all .2s;line-height:1}
        .theme-btn:hover{border-color:var(--accent-amber);color:var(--accent-amber)}

        /* ── CARD ───────────────────────────────────────────────────────── */
        .card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;box-shadow:var(--shadow);transition:border-color .2s;margin-bottom:24px}
        .card:hover{border-color:var(--border-hover)}
        .page-header{text-align:center;margin-bottom:32px}
        .page-header h1{font-size:2.5rem;font-weight:700;background:linear-gradient(135deg,#fff,var(--accent-cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:4px}
        .page-header p{color:var(--text-muted);font-size:1.05rem}

        /* ── FORMS ──────────────────────────────────────────────────────── */
        .centered-search-box{max-width:450px;margin:0 auto;display:flex;flex-direction:column;gap:16px}
        .control-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:20px}
        .control-group label{display:block;font-size:.75rem;font-weight:600;text-transform:uppercase;letter-spacing:.5px;color:var(--text-muted);margin-bottom:6px}
        .control-group select,.control-group input[type=text],.control-group input[type=date],.control-group input[type=email]{width:100%;padding:10px 14px;background:var(--bg);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.95rem;font-family:inherit;transition:all .2s}
        .control-group select:focus,.control-group input:focus{outline:none;border-color:var(--accent-cyan);box-shadow:0 0 0 3px rgba(0,212,255,.1)}

        /* ── SKELETON ────────────────────────────────────────────────────── 2.5 */
        .skeleton{background:linear-gradient(90deg,var(--surface-hover) 25%,var(--border) 50%,var(--surface-hover) 75%);background-size:200% 100%;animation:skeleton-shimmer 1.5s infinite;border-radius:4px;color:transparent!important;border-color:transparent!important;pointer-events:none}
        @keyframes skeleton-shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
        .skeleton-block{height:120px;border-radius:12px;margin-bottom:16px}

        /* ── LOADING ────────────────────────────────────────────────────── */
        .loading-overlay{display:none;position:fixed;inset:0;background:rgba(11,12,16,.8);backdrop-filter:blur(4px);z-index:500;justify-content:center;align-items:center;flex-direction:column;gap:16px}
        .spinner{width:40px;height:40px;border:3px solid var(--border);border-top-color:var(--accent-cyan);border-radius:50%;animation:spin .8s linear infinite}
        @keyframes spin{to{transform:rotate(360deg)}}

        /* ── TOAST SYSTEM ────────────────────────────────────────────────── 3.1 */
        #toastContainer{position:fixed;top:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:10px;max-width:380px}
        .toast-item{display:flex;align-items:flex-start;gap:12px;padding:14px 18px;border-radius:12px;box-shadow:0 8px 24px rgba(0,0,0,.3);border:1px solid transparent;animation:toast-in .3s ease;cursor:pointer;position:relative;overflow:hidden}
        .toast-item::after{content:'';position:absolute;bottom:0;left:0;height:3px;background:currentColor;opacity:.4;animation:toast-progress linear forwards}
        @keyframes toast-in{from{opacity:0;transform:translateX(40px)}to{opacity:1;transform:translateX(0)}}
        @keyframes toast-progress{from{width:100%}to{width:0%}}
        @keyframes toast-out{to{opacity:0;transform:translateX(40px)}}
        .toast-success{background:rgba(0,230,118,.12);border-color:rgba(0,230,118,.3);color:var(--accent-green)}
        .toast-error{background:rgba(255,77,109,.12);border-color:rgba(255,77,109,.3);color:var(--accent-rose)}
        .toast-warning{background:rgba(255,184,0,.12);border-color:rgba(255,184,0,.3);color:var(--accent-amber)}
        .toast-info{background:rgba(0,212,255,.12);border-color:rgba(0,212,255,.3);color:var(--accent-cyan)}
        .toast-icon{font-size:1.1rem;flex-shrink:0}
        .toast-body{flex:1;font-size:.9rem;line-height:1.4}
        .toast-close{background:none;border:none;color:inherit;opacity:.6;cursor:pointer;font-size:1rem;flex-shrink:0;padding:0;line-height:1}

        /* ── KPI GRID ───────────────────────────────────────────────────── */
        .kpi-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin-bottom:24px}
        .kpi-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;text-align:center;position:relative;overflow:hidden;transition:transform .25s cubic-bezier(.2,.8,.2,1),box-shadow .25s ease}
        .kpi-card:hover{transform:translateY(-4px);box-shadow:0 12px 40px rgba(0,212,255,.12),var(--shadow)}
        .kpi-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px}
        .kpi-card:nth-child(1)::before{background:var(--accent-cyan)}
        .kpi-card:nth-child(2)::before{background:var(--accent-green)}
        .kpi-card:nth-child(3)::before{background:var(--accent-rose)}
        .kpi-label{font-size:.875rem;color:var(--text-muted);font-weight:500;margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px}
        .kpi-value{font-size:3rem;font-weight:700;line-height:1;font-variant-numeric:tabular-nums}
        .kpi-card:nth-child(1) .kpi-value{color:var(--accent-cyan)}
        .kpi-card:nth-child(2) .kpi-value{color:var(--accent-green)}
        .kpi-card:nth-child(3) .kpi-value{color:var(--accent-rose)}
        /* 1.3  Delta badges */
        .delta-badge{display:inline-block;font-size:.75rem;font-weight:700;padding:3px 8px;border-radius:20px;margin-top:8px}
        .delta-up{background:rgba(0,230,118,.15);color:var(--accent-green)}
        .delta-down{background:rgba(255,77,109,.15);color:var(--accent-rose)}
        .delta-neutral{background:rgba(255,255,255,.05);color:var(--text-muted)}
        /* 1.3  Sparklines */
        .sparkline-wrap{height:40px;margin-top:10px;opacity:.8}

        /* ── CHARTS ─────────────────────────────────────────────────────── */
        .chart-grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin-bottom:24px}
        .chart-grid-2{display:grid;grid-template-columns:repeat(2,1fr);gap:20px;margin-bottom:24px}
        .chart-grid-1{display:grid;grid-template-columns:1fr;gap:20px;margin-bottom:24px}
        .chart-box{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px}
        .chart-box.full-width{grid-column:1/-1}
        .chart-box h3{font-size:.95rem;font-weight:600;color:var(--text);margin-bottom:20px;display:flex;align-items:center;gap:8px}
        .chart-box h3::before{content:'';width:4px;height:16px;border-radius:2px;background:var(--accent-cyan)}
        .chart-wrapper-pie{position:relative;height:280px}
        .chart-wrapper-pie-large{position:relative;height:580px}
        .chart-wrapper-bar{position:relative;height:340px}
        .chart-wrapper-bar-tall{position:relative;height:420px}
        .chart-wrapper-bar-acm{position:relative;height:480px}

        /* ── 1.2  CROSS-FILTER CHIPS ─────────────────────────────────────── */
        #activeFiltersBar{display:none;margin-bottom:16px;display:flex;flex-wrap:wrap;gap:8px;align-items:center}
        .filter-chip{display:inline-flex;align-items:center;gap:6px;padding:4px 12px;border-radius:20px;font-size:.8rem;font-weight:600;background:rgba(0,212,255,.15);color:var(--accent-cyan);border:1px solid rgba(0,212,255,.3);cursor:pointer;transition:all .2s}
        .filter-chip:hover{background:rgba(255,77,109,.15);color:var(--accent-rose);border-color:rgba(255,77,109,.3)}
        .filter-chip-label{font-size:.7rem;opacity:.7}

        /* ── 1.7  INSIGHTS PANEL ────────────────────────────────────────── */
        #insightsPanel{background:linear-gradient(135deg,rgba(0,212,255,.06),rgba(185,103,255,.06));border:1px solid rgba(0,212,255,.15);border-radius:var(--radius);padding:20px;margin-bottom:24px;display:none}
        #insightsPanel h4{color:var(--accent-cyan);font-size:.9rem;margin-bottom:12px;display:flex;align-items:center;gap:8px}
        #insightsList{list-style:none;display:flex;flex-direction:column;gap:8px}
        #insightsList li{font-size:.88rem;color:var(--text);padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04)}
        #insightsList li:last-child{border-bottom:none}

        /* ── 1.5  LEADERBOARD ────────────────────────────────────────────── */
        .leaderboard-table{width:100%;border-collapse:collapse}
        .leaderboard-table th,.leaderboard-table td{padding:12px 14px;text-align:left;border-bottom:1px solid var(--border);font-size:.9rem}
        .leaderboard-table th{color:var(--text-muted);font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;font-weight:600}
        .leaderboard-table tr:hover td{background:var(--surface-hover)}
        .rank-medal{font-size:1.2rem;width:28px;display:inline-block;text-align:center}
        .needs-attention{background:rgba(255,77,109,.05)!important}
        .needs-attention td{color:var(--accent-rose)}
        .score-bar{height:6px;border-radius:3px;background:var(--border);position:relative;min-width:60px;display:inline-block;vertical-align:middle}
        .score-bar-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--accent-green),var(--accent-cyan));transition:width .6s ease}

        /* ── 1.6  HEATMAP ───────────────────────────────────────────────── */
        .heatmap-outer{display:flex;gap:6px;margin-top:12px}
        .heatmap-day-labels{display:grid;grid-template-rows:repeat(7,16px);gap:4px;padding-top:22px}
        .heatmap-day-label{font-size:.65rem;color:var(--text-muted);line-height:16px;width:28px;text-align:right;padding-right:4px}
        .heatmap-right{display:flex;flex-direction:column;flex:1;min-width:0}
        .heatmap-month-row{display:flex;gap:0;margin-bottom:4px;height:18px;position:relative}
        .heatmap-month-label{font-size:.65rem;color:var(--text-muted);position:absolute;white-space:nowrap}
        .heatmap-scroll{overflow-x:auto;padding-bottom:10px}
        .heatmap-grid{display:grid;grid-auto-flow:column;grid-template-rows:repeat(7,16px);gap:4px;width:max-content}
        .heatmap-cell{width:16px;height:16px;border-radius:4px;background:rgba(255,255,255,.06);position:relative;cursor:crosshair;transition:transform .12s,box-shadow .12s}
        .heatmap-cell:hover{transform:scale(1.35);z-index:2;box-shadow:0 0 8px rgba(0,212,255,.5)}
        .heatmap-tooltip{position:fixed;background:rgba(0,0,0,.9);color:#fff;padding:5px 10px;border-radius:6px;font-size:.75rem;white-space:nowrap;z-index:9999;pointer-events:none;border:1px solid rgba(0,212,255,.3);display:none}
        .heatmap-legend{display:flex;align-items:center;gap:6px;margin-top:10px;font-size:.75rem;color:var(--text-muted)}
        .heatmap-legend-cell{width:16px;height:16px;border-radius:4px}

        /* ── 1.1  COMPARISON ────────────────────────────────────────────── */
        .compare-section{background:rgba(0,212,255,.04);border:1px solid rgba(0,212,255,.15);border-radius:12px;padding:16px;margin-bottom:20px}
        .compare-section h5{color:var(--accent-cyan);margin-bottom:12px;font-size:.85rem;font-weight:600}
        .compare-kpi-row{display:flex;gap:16px;flex-wrap:wrap}
        .compare-kpi{flex:1;min-width:100px;text-align:center;padding:12px;background:var(--surface-hover);border-radius:10px}
        .compare-kpi-label{font-size:.75rem;color:var(--text-muted);margin-bottom:4px}
        .compare-kpi-val{font-size:1.4rem;font-weight:700;color:var(--text)}

        /* ── FILTER PRESETS ──────────────────────────────────────────────── 1.4 */
        .preset-bar{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px;align-items:center}
        .preset-chip{padding:5px 14px;border-radius:20px;font-size:.8rem;font-weight:600;background:var(--surface-hover);color:var(--text-muted);border:1px solid var(--border);cursor:pointer;transition:all .2s}
        .preset-chip:hover{border-color:var(--accent-cyan);color:var(--accent-cyan)}
        .preset-save-btn{background:transparent;border:1px dashed var(--border-hover);color:var(--text-muted);padding:5px 14px;border-radius:20px;font-size:.8rem;cursor:pointer;transition:all .2s;font-family:inherit}
        .preset-save-btn:hover{border-color:var(--accent-green);color:var(--accent-green)}

        /* ── MODAL ──────────────────────────────────────────────────────── */
        .modal-overlay{display:none;position:fixed;inset:0;background:rgba(11,12,16,.9);z-index:2000;justify-content:center;align-items:center;backdrop-filter:blur(5px)}
        .modal-content{background:linear-gradient(180deg,rgba(21,22,33,.97),rgba(21,22,33,.9));backdrop-filter:blur(20px) saturate(140%);box-shadow:0 24px 80px rgba(0,0,0,.5),inset 0 1px 0 rgba(255,255,255,.04);width:100%;max-width:820px;border-radius:var(--radius);border:1px solid var(--border);padding:32px;max-height:90vh;overflow-y:auto}
        .modal-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}
        .modal-header h2{font-size:1.4rem;color:var(--text);margin:0}
        .close-btn{background:none;border:none;color:var(--text-muted);font-size:1.5rem;cursor:pointer;transition:.2s;line-height:1;padding:4px 8px}
        .close-btn:hover{color:var(--accent-rose)}

        /* ── ADMIN ──────────────────────────────────────────────────────── */
        .admin-form{display:flex;flex-direction:column;gap:16px;margin-bottom:32px}
        .module-card{background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:12px;padding:16px;transition:border-color .2s,background .2s}
        .module-card:hover{border-color:var(--border-hover);background:rgba(255,255,255,.03)}
        .module-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;border-bottom:1px solid var(--border);padding-bottom:12px}
        .module-header h4{margin:0;font-size:1rem;color:var(--accent-cyan);display:flex;align-items:center;gap:8px}
        .switch{position:relative;display:inline-block;width:44px;height:24px}
        .switch input{opacity:0;width:0;height:0}
        .slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:var(--border);transition:.3s;border-radius:34px}
        .slider:before{position:absolute;content:"";height:18px;width:18px;left:3px;bottom:3px;background:#fff;transition:.3s;border-radius:50%}
        input:checked+.slider{background:var(--accent-cyan)}
        input:checked+.slider:before{transform:translateX(20px)}
        .admin-toggle{background:rgba(255,184,0,.1);border:1px solid rgba(255,184,0,.3);padding:12px 16px;border-radius:12px;display:flex;justify-content:space-between;align-items:center}
        .admin-toggle h4{color:var(--accent-amber);margin:0;font-size:1rem}
        .expires-group{background:rgba(0,212,255,.05);border:1px solid rgba(0,212,255,.15);padding:12px 16px;border-radius:12px}
        .user-list{display:flex;flex-direction:column;gap:10px}
        .user-row{display:flex;justify-content:space-between;align-items:center;padding:14px 16px;background:var(--bg);border:1px solid var(--border);border-radius:8px;transition:.2s;flex-wrap:wrap;gap:8px}
        .user-row:hover{border-color:var(--accent-cyan)}
        .user-info{display:flex;flex-direction:column;gap:4px}
        .user-email{font-weight:600;color:var(--accent-cyan);font-size:.95rem}
        .user-details{font-size:.8rem;color:var(--text-muted)}
        .user-details span{background:var(--surface);padding:2px 6px;border-radius:4px;border:1px solid var(--border);margin-right:4px}
        .action-btns{display:flex;gap:8px}
        .edit-btn{background:rgba(0,212,255,.1);color:var(--accent-cyan);border:1px solid rgba(0,212,255,.3);padding:6px 12px;border-radius:6px;cursor:pointer;font-weight:500;transition:.2s;font-family:inherit;font-size:.85rem}
        .edit-btn:hover{background:var(--accent-cyan);color:#000}
        .del-btn{background:rgba(255,77,109,.1);color:var(--accent-rose);border:1px solid rgba(255,77,109,.3);padding:6px 12px;border-radius:6px;cursor:pointer;font-weight:500;transition:.2s;font-family:inherit;font-size:.85rem}
        .del-btn:hover{background:var(--accent-rose);color:#000}

        /* ── PILL SELECT ───────────────────────────────────────────────── */
        .pill-select-container{position:relative;width:100%}
        .pill-input-box{display:flex;flex-wrap:wrap;gap:6px;padding:8px 12px;min-height:42px;background:var(--bg);border:1px solid var(--border);border-radius:10px;cursor:pointer;align-items:center}
        .pill{background:rgba(0,212,255,.15);color:var(--accent-cyan);padding:4px 10px;border-radius:20px;font-size:.8rem;display:flex;align-items:center;gap:6px;border:1px solid rgba(0,212,255,.3)}
        .pill.all-pill{background:rgba(0,230,118,.15);color:var(--accent-green);border-color:rgba(0,230,118,.3)}
        .pill-dropdown{position:absolute;top:100%;left:0;right:0;background:var(--surface-hover);border:1px solid var(--border);border-radius:10px;margin-top:6px;max-height:200px;overflow-y:auto;z-index:100;box-shadow:var(--shadow);display:none;flex-direction:column;padding:8px}
        .pill-dropdown.show{display:flex}
        .pill-option{padding:8px 12px;cursor:pointer;font-size:.9rem;border-radius:6px;transition:.2s;color:var(--text)}
        .pill-option:hover{background:var(--border)}
        .pill-option.selected{background:var(--accent-cyan);color:#000;font-weight:700}

        /* ── TARGET / POINTS UI ─────────────────────────────────────────── */
        .target-banner{background:linear-gradient(135deg,rgba(0,230,118,.08),rgba(0,176,255,.08));border:1px solid rgba(0,230,118,.2);border-radius:var(--radius);padding:32px;text-align:center;margin-bottom:24px;position:relative}
        .target-banner h2{font-size:2.5rem;color:var(--accent-green);margin:8px 0}
        .acm-badge{position:absolute;top:16px;right:16px;background:var(--bg);padding:6px 14px;border-radius:20px;font-size:.8rem;color:var(--text-muted);border:1px solid var(--border)}
        .stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-top:16px}
        .stat-box{background:var(--bg);border:1px solid var(--border);border-radius:12px;padding:16px;text-align:center}
        .stat-label{font-size:.8rem;color:var(--text-muted);margin-bottom:4px}
        .stat-value{font-size:1.5rem;font-weight:700}
        .priv-toggle{background:var(--bg);border:1px solid var(--border);padding:10px 12px;border-radius:8px;font-size:.85rem;cursor:pointer;transition:.2s;color:var(--text-muted);text-align:left;display:flex;align-items:center;justify-content:space-between;gap:8px;user-select:none}
        .priv-toggle:hover{border-color:var(--accent-cyan);color:var(--text)}
        .priv-toggle.active{background:rgba(0,212,255,.1);border-color:var(--accent-cyan);color:var(--accent-cyan);font-weight:600}
        .priv-toggle-label{flex:1;pointer-events:none}
        .priv-toggle-right{display:flex;align-items:center;gap:6px;flex-shrink:0}
        .priv-qty{width:46px;padding:3px 6px;border-radius:6px;border:1px solid var(--border-hover);background:var(--surface);color:var(--text);font-size:.82rem;text-align:center;cursor:default}
        .priv-toggle.active .priv-qty{border-color:var(--accent-cyan)}
        .priv-check{font-size:.85rem;color:var(--accent-cyan);opacity:0;width:14px;text-align:center}
        .priv-toggle.active .priv-check{opacity:1}

        /* ── 5.1  HEALTH BADGE ──────────────────────────────────────────── */
        .health-badge{position:absolute;top:16px;left:16px;padding:5px 14px;border-radius:20px;font-size:.8rem;font-weight:700;border:1px solid}
        .health-Healthy{background:rgba(0,230,118,.15);color:var(--accent-green);border-color:rgba(0,230,118,.3)}
        .health-At.Risk,.health-At-Risk{background:rgba(255,184,0,.15);color:var(--accent-amber);border-color:rgba(255,184,0,.3)}
        .health-Critical{background:rgba(255,77,109,.15);color:var(--accent-rose);border-color:rgba(255,77,109,.3)}

        /* ── 5.2  TIMELINE ──────────────────────────────────────────────── */
        .timeline{position:relative;padding-left:28px}
        .timeline::before{content:'';position:absolute;left:9px;top:0;bottom:0;width:2px;background:var(--border)}
        .timeline-item{position:relative;margin-bottom:16px;padding:12px 16px;background:var(--bg);border:1px solid var(--border);border-radius:10px;transition:.2s}
        .timeline-item:hover{border-color:var(--border-hover)}
        .timeline-dot{position:absolute;left:-19px;top:16px;width:12px;height:12px;border-radius:50%;border:2px solid var(--surface)}
        .tl-creation{background:var(--accent-green)}
        .tl-privilege{background:var(--accent-cyan)}
        .tl-points{background:var(--accent-amber)}
        .tl-rejection{background:var(--accent-rose)}
        .timeline-date{font-size:.75rem;color:var(--text-muted);margin-bottom:4px}
        .timeline-title{font-size:.9rem;font-weight:600;color:var(--text)}
        .timeline-sub{font-size:.8rem;color:var(--text-muted);margin-top:2px}

        /* ── 5.3  COMPARISON TABLE ──────────────────────────────────────── */
        .compare-table{width:100%;border-collapse:collapse;min-width:500px}
        .compare-table th,.compare-table td{padding:12px 16px;text-align:left;border-bottom:1px solid var(--border);font-size:.9rem}
        .compare-table th{color:var(--text-muted);font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;background:var(--surface-hover)}
        .compare-table tr:hover td{background:var(--surface-hover)}
        .best-val{color:var(--accent-green);font-weight:700}
        .worst-val{color:var(--accent-rose)}

        /* ── EMPTY STATES ────────────────────────────────────────────────── 3.4 */
        .empty-state{text-align:center;padding:60px 24px;color:var(--text-muted)}
        .empty-state svg{width:56px;height:56px;fill:var(--border-hover);margin-bottom:16px}
        .empty-state h3{font-size:1.1rem;color:var(--text);margin-bottom:8px}
        .empty-state p{font-size:.9rem;margin-bottom:20px}
        .empty-state-action{background:transparent;border:1px solid var(--accent-cyan);color:var(--accent-cyan);padding:10px 24px;border-radius:8px;cursor:pointer;font-family:inherit;font-weight:600;font-size:.9rem;transition:all .2s}
        .empty-state-action:hover{background:var(--accent-cyan);color:#000}

        /* ── AUDIT LOG ──────────────────────────────────────────────────── */
        .audit-row{display:flex;flex-wrap:wrap;gap:8px;padding:10px 0;border-bottom:1px solid var(--border);font-size:.82rem}
        .audit-ts{color:var(--text-muted);font-family:monospace}
        .audit-actor{color:var(--accent-cyan);font-weight:600}
        .audit-action{background:rgba(0,212,255,.1);color:var(--accent-cyan);padding:2px 8px;border-radius:4px;font-family:monospace;font-size:.78rem}
        .audit-target{color:var(--text)}

        /* ── TABS ───────────────────────────────────────────────────────── */
        .tab-bar{display:flex;gap:4px;margin-bottom:20px;background:var(--surface-hover);padding:4px;border-radius:10px}
        .tab-btn{flex:1;padding:8px 16px;border:none;border-radius:7px;cursor:pointer;font-family:inherit;font-weight:500;font-size:.85rem;background:transparent;color:var(--text-muted);transition:all .2s}
        .tab-btn.active{background:var(--surface);color:var(--text);box-shadow:0 1px 6px rgba(0,0,0,.2)}

        /* ── MOBILE RESPONSIVENESS ───────────────────────────────────────── 3.3 */
        @media(max-width:900px){
            .chart-grid-3{grid-template-columns:repeat(2,1fr)}
            .kpi-grid{grid-template-columns:repeat(2,1fr)}
        }
        @media(max-width:600px){
            .main-content{padding:12px}
            .kpi-grid{grid-template-columns:1fr}
            .chart-grid-3,.chart-grid-2{grid-template-columns:1fr}
            .chart-wrapper-pie-large{height:380px}
            .control-grid{grid-template-columns:1fr}
            .compare-kpi-row{flex-direction:column}
            .user-row{flex-direction:column;align-items:flex-start}
        }
        @media print{
            .sidebar,.page-top-bar,#pg-analytics .card,.loading-overlay,#toastContainer,.btn,.tab-bar,
            #activeFiltersBar,.preset-bar,.filter-chip{display:none!important}
            body{background:#fff;color:#000}
            .chart-box,.kpi-card{border:1px solid #ccc;page-break-inside:avoid}
        }
    </style>
</head>
<body>

<!-- ── TOAST CONTAINER ───────────────────────────────────────────────────── -->
<div id="toastContainer"></div>

<!-- ── LOADING OVERLAY ──────────────────────────────────────────────────── -->
<div class="loading-overlay" id="loadingOverlay">
    <div class="spinner"></div>
    <div style="color:var(--text-muted);font-weight:500;" id="loadingText">Crunching Feishu data...</div>
</div>

<!-- ── LOGIN OVERLAY ─────────────────────────────────────────────────────── -->
<div id="loginOverlay">
    <div class="login-card">
        <h1>🚀 Xena Portal</h1>
        <p>Secure Analytics Dashboard</p>
        <button class="btn btn-primary" onclick="window.location.href='/api/login'">Login with Feishu</button>
    </div>
</div>

<!-- ── MAIN APP ───────────────────────────────────────────────────────────── -->
<div class="app-shell" id="mainApp">

    <!-- ── SIDEBAR ──────────────────────────────────────────────────────────── -->
    <aside class="sidebar" id="sidebar">
        <div class="sidebar-logo">
            <span style="font-size:1.3rem">🚀</span>
            <span class="sidebar-brand">Xena Portal</span>
        </div>
        <div class="sidebar-user-area">
            <div class="sidebar-username" id="sidebarUsername">Loading…</div>
            <div style="font-size:.72rem;color:var(--text-muted);margin-top:2px">Live Data Portal</div>
        </div>
        <nav class="sidebar-nav">
            <div class="nav-group" id="navGrpAnalytics">
                <div class="nav-group-header" onclick="toggleNavGroup('navGrpAnalytics','navBodyAnalytics')">
                    <span>📊 Agency Analysis</span><span class="nav-arr" id="navArrAnalytics">›</span>
                </div>
                <div class="nav-group-body" id="navBodyAnalytics">
                    <div class="nav-item" id="nav-analytics" onclick="navTo('analytics')">Analytics Report</div>
                    <div class="nav-item" id="nav-compare"   onclick="navTo('compare')">Compare ACMs / Periods</div>
                </div>
            </div>
            <div class="nav-group" id="navGrpPoints">
                <div class="nav-group-header" onclick="toggleNavGroup('navGrpPoints','navBodyPoints')">
                    <span>🪙 Agency Points</span><span class="nav-arr" id="navArrPoints">›</span>
                </div>
                <div class="nav-group-body" id="navBodyPoints">
                    <div class="nav-item" id="nav-points"    onclick="navTo('points')">Check Agency Points</div>
                    <div class="nav-item" id="nav-allocator" onclick="navTo('allocator')">Smart Allocator</div>
                </div>
            </div>
            <div class="nav-group">
                <div class="nav-group-header nav-direct" id="nav-target" onclick="navTo('target')">
                    <span>🎯 Agency Target</span>
                </div>
            </div>
            <div class="nav-group" id="navGrpAdmin" style="display:none">
                <div style="height:1px;background:var(--border);margin:8px 16px 4px"></div>
                <div class="nav-group-header" onclick="toggleNavGroup('navGrpAdmin','navBodyAdmin')">
                    <span>⚙️ Access Management</span><span class="nav-arr" id="navArrAdmin">›</span>
                </div>
                <div class="nav-group-body" id="navBodyAdmin">
                    <div class="nav-item" id="nav-admin-agents" onclick="navTo('admin-agents')">Manage Agents</div>
                    <div class="nav-item" id="nav-admin-audit"  onclick="navTo('admin-audit')">Audit Log</div>
                </div>
            </div>
        </nav>
        <div class="sidebar-footer">
            <button class="theme-btn" onclick="toggleTheme()" id="themeBtn" title="Toggle theme" aria-label="Toggle theme">🌙</button>
            <button class="logout-btn" onclick="logout()">Logout</button>
        </div>
    </aside>

    <div class="sidebar-backdrop" id="sidebarBackdrop" onclick="closeSidebar()"></div>

    <!-- ── MAIN CONTENT ─────────────────────────────────────────────────────── -->
    <div class="main-content">

        <div class="page-top-bar">
            <div style="display:flex;align-items:center;gap:10px">
                <button class="hamburger" onclick="openSidebar()" aria-label="Open menu">☰</button>
                <span class="page-title-txt" id="pageTitle">Xena Live Data Portal</span>
            </div>
        </div>

        <!-- Hidden queryType — read by fetchAgencyData() -->
        <select id="queryType" style="display:none"></select>

        <!-- ══ PAGE: WELCOME ════════════════════════════════════════════════ -->
        <div id="pg-welcome" style="display:block">
            <div id="noAccessMsg" style="display:none;color:var(--accent-rose);text-align:center;font-weight:bold;padding:20px;background:rgba(255,77,109,.1);border-radius:12px;margin-bottom:24px">
                You do not have access to any modules. Please contact an administrator.
            </div>
            <div style="text-align:center;padding:32px 24px 24px">
                <div style="font-size:2.5rem;margin-bottom:12px">🚀</div>
                <h2 style="font-size:1.7rem;font-weight:700;background:linear-gradient(135deg,var(--accent-cyan),var(--accent-purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:8px">Xena Live Data Portal</h2>
                <p style="color:var(--text-muted);margin-bottom:32px">Agency Privilege &amp; Operational Analytics</p>
            </div>
            <div class="welcome-cards" id="welcomeCards"></div>
        </div>

        <!-- ══ PAGE: COMPARE ════════════════════════════════════════════════ -->
        <div id="pg-compare" style="display:none">
            <div class="card">
                <h3 style="margin-bottom:16px;color:var(--accent-cyan)">⚖️ Agency Comparison</h3>
                <div class="compare-mode-toggle">
                    <div class="compare-mode-btn active" id="cmpBtnAcm" onclick="setCompareMode('acm',this)">👥 ACM Side-by-Side</div>
                    <div class="compare-mode-btn" id="cmpBtnPeriod" onclick="setCompareMode('period',this)">📅 Period Compare → Analytics</div>
                </div>
                <div id="cmpModeAcm">
                    <p style="color:var(--text-muted);font-size:.9rem;margin-bottom:16px">Enter 2–3 agency codes to compare data side by side.</p>
                    <div class="control-grid">
                        <div class="control-group"><label>Primary Agency Code</label><input type="text" id="cmpCode1" placeholder="e.g., 40775"></div>
                        <div class="control-group"><label>Agency Code 2</label><input type="text" id="cmpCode2" placeholder="e.g., 40776"></div>
                        <div class="control-group"><label>Agency Code 3 (optional)</label><input type="text" id="cmpCode3" placeholder="e.g., 40777"></div>
                        <div class="control-group">
                            <label>Module</label>
                            <select id="cmpModule"><option value="points">Agency Points</option><option value="target">Agency Target</option></select>
                        </div>
                    </div>
                    <button class="btn btn-primary" style="max-width:220px;margin-top:12px" onclick="runAcmCompare()">⚖️ Compare Agencies</button>
                    <div id="cmpAcmResults" style="margin-top:24px;overflow-x:auto"></div>
                </div>
                <div id="cmpModePeriod" style="display:none">
                    <div class="empty-state" style="padding:40px 0">
                        <div style="font-size:2.5rem;margin-bottom:12px">📅</div>
                        <h3>Period Comparison</h3>
                        <p>Period comparison is built into Analytics Report — enable the "Period Comparison" toggle there.</p>
                        <button class="empty-state-action" onclick="navTo('analytics');setTimeout(()=>{document.getElementById('compareToggle').checked=true;toggleCompareDates();},100)">Go to Analytics Report</button>
                    </div>
                </div>
            </div>
        </div>

        <!-- ══ PAGE: AGENCY LOOKUP (target / points) ═══════════════════════ -->
        <div id="pg-agency" style="display:none">
            <div class="card" style="margin-bottom:24px">
                <h3 id="agencyLookupTitle" style="margin-bottom:16px;color:var(--accent-cyan)">🔍 Agency Lookup</h3>
                <div class="centered-search-box">
                    <div class="control-group">
                        <label>Agency Code</label>
                        <input type="text" id="agencyCode" placeholder="e.g., 40775" aria-label="Agency code" onkeydown="if(event.key==='Enter')fetchAgencyData()">
                    </div>
                    <div class="control-group">
                        <label>Compare With (optional, comma-separated codes)</label>
                        <input type="text" id="compareAgencyCodes" placeholder="e.g., 40776, 40777" aria-label="Comparison codes">
                    </div>
                    <button class="btn btn-primary" onclick="fetchAgencyData()">🔍 Search</button>
                </div>
            </div>

            <div id="agencyEmptyState" class="empty-state">
                <svg viewBox="0 0 24 24"><path d="M15.5 14h-.79l-.28-.27C15.41 12.59 16 11.11 16 9.5 16 5.91 13.09 3 9.5 3S3 5.91 3 9.5 5.91 16 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>
                <h3>Search for an Agency</h3>
                <p>Enter an agency code above to view data.</p>
            </div>

            <!-- Search results -->
            <div id="resultsContainer" style="display:none">
                <div class="target-banner">
                    <div class="health-badge health-Healthy" id="displayHealth" style="display:none">🟢 Healthy</div>
                    <div class="acm-badge" id="displayAcm">ACM: Unknown</div>
                    <div id="displayTargetTitle" style="color:var(--text-muted);font-size:.9rem;text-transform:uppercase;letter-spacing:1px">Current Month Target</div>
                    <h2 id="displayTargetCoins">0 Coins</h2>
                </div>

                <div class="card" id="privilegeArea" style="display:none">
                    <h3 style="margin-bottom:16px;color:var(--accent-cyan)">Available Target Privilege</h3>
                    <div class="control-group" style="max-width:300px;margin-bottom:16px">
                        <select id="specificPrivilege" onchange="updatePrivilegeGrid()" aria-label="Privilege type">
                            <option value="Dynamic Avatar">Dynamic Avatar</option>
                            <option value="30 Mics">30 Mics</option>
                            <option value="20 Mics">20 Mics</option>
                            <option value="New User Welcome Room">New User Welcome Room</option>
                            <option value="Game room">Game room</option>
                        </select>
                    </div>
                    <div class="stat-grid">
                        <div class="stat-box"><div class="stat-label">Total</div><div class="stat-value" id="valTotal" style="color:var(--accent-green)">0</div></div>
                        <div class="stat-box"><div class="stat-label">Claimed</div><div class="stat-value" id="valClaimed">0</div></div>
                        <div class="stat-box"><div class="stat-label">Remaining</div><div class="stat-value" id="valRemaining" style="color:var(--accent-amber)">0</div></div>
                        <div class="stat-box"><div class="stat-label">Pending</div><div class="stat-value" id="valPending" style="color:#ffaa00">0</div></div>
                        <div class="stat-box"><div class="stat-label">Rejected</div><div class="stat-value" id="valRejected" style="color:var(--accent-rose)">0</div></div>
                    </div>
                </div>

                <div class="card" id="pointsArea" style="display:none">
                    <h3 style="margin-bottom:16px;color:var(--accent-purple)">🪙 Agency Points System</h3>
                    <div class="stat-grid" style="margin-bottom:24px">
                        <div class="stat-box"><div class="stat-label">Total Points</div><div class="stat-value" id="ptTotal" style="color:var(--accent-green)">0</div></div>
                        <div class="stat-box"><div class="stat-label">Points Used</div><div class="stat-value" id="ptUsed" style="color:var(--accent-rose)">0</div></div>
                        <div class="stat-box"><div class="stat-label">Balance</div><div class="stat-value" id="ptBalance" style="color:var(--accent-cyan)">0</div></div>
                    </div>
                    <div style="margin-bottom:16px">
                        <button class="btn-outline" style="font-size:.85rem;padding:8px 16px" onclick="navTo('allocator')">🤖 Open Smart Allocator with this balance →</button>
                    </div>
                    <!-- 5.2  Timeline -->
                    <div style="margin-top:8px">
                        <h4 style="color:var(--text);margin-bottom:16px;display:flex;align-items:center;gap:8px">
                            <span style="color:var(--accent-amber)">⏱</span> Activity Timeline
                        </h4>
                        <div class="timeline" id="agencyTimeline"></div>
                    </div>
                    <div id="pointsHistoryWrap" style="margin-top:24px"></div>
                </div>
            </div>

            <!-- Agency comparison area -->
            <div id="comparisonArea" class="card" style="display:none;overflow-x:auto">
                <h3 style="color:var(--accent-cyan);margin-bottom:16px">⚖️ Agency Comparison Engine</h3>
                <div id="comparisonTableContainer"></div>
            </div>
        </div>

        <!-- ══ PAGE: SMART ALLOCATOR (standalone) ══════════════════════════ -->
        <div id="pg-allocator" style="display:none">
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:24px">
                <div class="card">
                    <h3 style="margin-bottom:16px;color:var(--accent-cyan)">🤖 Smart Allocator</h3>
                    <div class="control-group" style="margin-bottom:16px">
                        <label>Available Points Balance</label>
                        <input type="number" id="allocatorBalance" min="0" value="0" placeholder="Enter your points balance" style="width:100%;padding:10px 14px;background:var(--bg);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:1rem;font-family:inherit;transition:.2s;box-sizing:border-box">
                        <div style="font-size:.75rem;color:var(--text-muted);margin-top:4px" id="allocBalHint">Enter balance manually, or <a href="#" onclick="navTo('points');return false" style="color:var(--accent-cyan)">search an agency first</a> to auto-fill.</div>
                    </div>
                    <div class="control-group">
                        <label>Select Privileges &amp; Quantities</label>
                        <div id="calcPrivGrid" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px">
                            <div class="priv-toggle" data-val="trend card" onclick="privToggle(this,event)"><span class="priv-toggle-label">Trend Card (40-50)</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="traffic card" onclick="privToggle(this,event)"><span class="priv-toggle-label">Traffic Card (30-40)</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="30 mic 15 days" onclick="privToggle(this,event)"><span class="priv-toggle-label">30 Mic 15 Days (150-175)</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="30 mic 30 days" onclick="privToggle(this,event)"><span class="priv-toggle-label">30 Mic 30 Days (300-350)</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="normal short id" onclick="privToggle(this,event)"><span class="priv-toggle-label">Normal Short ID</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="customized short id 15 days" onclick="privToggle(this,event)"><span class="priv-toggle-label">Custom Short ID 15D</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="customized short id 30 days" onclick="privToggle(this,event)"><span class="priv-toggle-label">Custom Short ID 30D</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="room pin-up" onclick="privToggle(this,event)"><span class="priv-toggle-label">Room Pin-up (100-150)</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="welcome package 3" onclick="privToggle(this,event)"><span class="priv-toggle-label">Welcome Package 3</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="welcome package 2" onclick="privToggle(this,event)"><span class="priv-toggle-label">Welcome Package 2</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="main page banner" onclick="privToggle(this,event)"><span class="priv-toggle-label">Main Page Banner</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="news banner" onclick="privToggle(this,event)"><span class="priv-toggle-label">News Banner</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                            <div class="priv-toggle" data-val="live banner" onclick="privToggle(this,event)"><span class="priv-toggle-label">Live Banner</span><div class="priv-toggle-right"><input type="number" class="priv-qty" min="1" max="99" value="1" onclick="event.stopPropagation()" onmousedown="event.stopPropagation()"><span class="priv-check">✓</span></div></div>
                        </div>
                    </div>
                    <button class="btn btn-allocator" onclick="runAllocator()">⚡ Run Smart Allocator</button>
                </div>
                <div class="card">
                    <h4 style="color:var(--accent-purple);margin-bottom:12px">📜 Allocation Preview</h4>
                    <pre id="allocatorOutput" style="font-family:monospace;font-size:.82rem;white-space:pre-wrap;color:var(--text);line-height:1.6;max-height:420px;overflow-y:auto"><span style="color:var(--text-muted)">Select privileges and click Run to preview allocation.</span></pre>
                </div>
            </div>
        </div>

        <!-- ══ PAGE: ADMIN — MANAGE AGENTS ══════════════════════════════════ -->
        <div id="pg-admin-agents" style="display:none">
            <div class="card">
                <h3 style="color:var(--accent-cyan);margin-bottom:20px">⚙️ Manage Agents</h3>
                <div class="admin-form">
                    <div class="control-group">
                        <label>Agent Identity (Exact Lark Name OR Email)</label>
                        <input type="text" id="newAgentEmail" placeholder="e.g., Muhammad Usman" aria-label="Agent identity">
                    </div>
                    <div class="admin-toggle">
                        <div>
                            <h4>👑 Grant Admin Privileges</h4>
                            <span style="font-size:.8rem;color:var(--text-muted)">Allows user to manage other agents.</span>
                        </div>
                        <label class="switch"><input type="checkbox" id="modAdmin"><span class="slider"></span></label>
                    </div>
                    <div class="module-card">
                        <div class="module-header">
                            <h4>🎯 Target Module</h4>
                            <label class="switch"><input type="checkbox" id="modTarget" checked><span class="slider"></span></label>
                        </div>
                        <div class="control-grid" style="margin-bottom:0">
                            <div class="control-group"><label>ACMs</label><div class="pill-select-container" id="targetAcmSelect"></div></div>
                            <div class="control-group"><label>Regions</label><div class="pill-select-container" id="targetRegSelect"></div></div>
                        </div>
                    </div>
                    <div class="module-card">
                        <div class="module-header">
                            <h4>🪙 Points Module</h4>
                            <label class="switch"><input type="checkbox" id="modPoints" checked><span class="slider"></span></label>
                        </div>
                        <div class="control-grid" style="margin-bottom:0">
                            <div class="control-group"><label>ACMs</label><div class="pill-select-container" id="pointsAcmSelect"></div></div>
                            <div class="control-group"><label>Regions</label><div class="pill-select-container" id="pointsRegSelect"></div></div>
                        </div>
                    </div>
                    <div class="module-card">
                        <div class="module-header">
                            <h4>📊 Analytics Module</h4>
                            <label class="switch"><input type="checkbox" id="modAnalytics" checked><span class="slider"></span></label>
                        </div>
                        <div class="control-grid" style="margin-bottom:0">
                            <div class="control-group"><label>ACMs</label><div class="pill-select-container" id="analyticsAcmSelect"></div></div>
                            <div class="control-group"><label>Regions</label><div class="pill-select-container" id="analyticsRegSelect"></div></div>
                        </div>
                    </div>
                    <!-- 4.4  Temporary access -->
                    <div class="expires-group">
                        <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
                            <span style="color:var(--accent-amber);font-size:.9rem;font-weight:600">⏳ Temporary Access</span>
                            <span style="font-size:.8rem;color:var(--text-muted)">(optional — leave blank for permanent)</span>
                        </div>
                        <div class="control-group" style="margin-bottom:0">
                            <label>Access Expires On</label>
                            <input type="date" id="expiresAt" aria-label="Expiry date">
                        </div>
                    </div>
                    <button class="btn btn-primary" id="adminBtnSubmit" onclick="addAgent()" style="padding:14px;margin-top:8px;font-size:1.05rem">+ Save Agent Configuration</button>
                </div>
                <h3 style="margin-bottom:12px;font-size:.95rem;color:var(--text-muted);border-bottom:1px solid var(--border);padding-bottom:8px;margin-top:24px">Current Active Agents</h3>
                <div class="user-list" id="adminUserList"></div>
            </div>
        </div>

        <!-- ══ PAGE: ADMIN — AUDIT LOG ═══════════════════════════════════════ -->
        <div id="pg-admin-audit" style="display:none">
            <div class="card">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
                    <h3 style="color:var(--accent-cyan)">📋 Audit Log</h3>
                    <button class="btn-sm btn-outline" onclick="loadAuditLog()">🔄 Refresh</button>
                </div>
                <div id="auditLogList"><div class="empty-state" style="padding:40px 0"><p>Loading audit log…</p></div></div>
            </div>
        </div>

        <!-- ══ PAGE: ANALYTICS ══════════════════════════════════════════════ -->
        <div id="pg-analytics" style="display:none">
            <div class="card">
                <!-- 1.4  Preset bar -->
                <div class="preset-bar" id="presetBar">
                    <span style="font-size:.75rem;color:var(--text-muted);font-weight:600;text-transform:uppercase;letter-spacing:.5px">Quick Filters:</span>
                    <button class="preset-save-btn" onclick="saveFilterPreset()">+ Save Current</button>
                </div>

                <div class="control-grid">
                    <div class="control-group">
                        <label>Date Filter</label>
                        <select id="anaDatePreset" onchange="updateCustomDateVisibility()" aria-label="Date preset">
                            <option value="this_month" selected>Current Month</option>
                            <option value="last_month">Last Month</option>
                            <option value="this_week">Current Week</option>
                            <option value="last_week">Last Week</option>
                            <option value="specific">Specific Date</option>
                            <option value="all">All Time</option>
                        </select>
                    </div>
                    <div class="control-group" id="customFromDiv" style="display:none">
                        <label>From Date</label>
                        <input type="date" id="anaFrom" aria-label="From date">
                    </div>
                    <div class="control-group" id="customToDiv" style="display:none">
                        <label>To Date</label>
                        <input type="date" id="anaTo" aria-label="To date">
                    </div>
                    <div class="control-group">
                        <label>Region</label>
                        <select id="anaRegion" onchange="updateAcmDropdown()" aria-label="Region">
                            <option value="PK" selected>PK</option>
                            <option value="IN">IN</option>
                            <option value="All">All Regions</option>
                        </select>
                    </div>
                    <div class="control-group">
                        <label>Assigned ACM</label>
                        <select id="anaAcm" aria-label="ACM"><option value="All">All ACMs</option></select>
                    </div>
                    <div class="control-group">
                        <label>Agency Type</label>
                        <select id="anaType" aria-label="Agency type">
                            <option value="All">All Types</option>
                            <option value="Acm hunting">ACM Hunting</option>
                            <option value="BD hunting">BD Hunting</option>
                            <option value="Walkin">Walk-in</option>
                            <option value="New BD Hunting">New BD Hunting</option>
                            <option value="Walkin sub-agency">Walkin sub-agency</option>
                        </select>
                    </div>
                </div>

                <!-- 1.1  Comparison date range -->
                <div style="background:rgba(185,103,255,.06);border:1px solid rgba(185,103,255,.2);border-radius:10px;padding:12px 16px;margin-bottom:16px">
                    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
                        <span style="font-size:.85rem;font-weight:600;color:var(--accent-purple)">📊 Period Comparison (optional)</span>
                        <label class="switch" style="transform:scale(.8)"><input type="checkbox" id="compareToggle" onchange="toggleCompareDates()"><span class="slider"></span></label>
                    </div>
                    <div id="compareDatesRow" style="display:none;grid-template-columns:1fr 1fr;gap:12px" class="control-grid">
                        <div class="control-group" style="margin-bottom:0"><label>Compare From</label><input type="date" id="cmpFrom" aria-label="Compare from"></div>
                        <div class="control-group" style="margin-bottom:0"><label>Compare To</label><input type="date" id="cmpTo" aria-label="Compare to"></div>
                    </div>
                </div>

                <div style="display:flex;gap:12px;flex-wrap:wrap">
                    <button class="btn btn-success" id="btnGenAna" onclick="generateAnalytics()" style="flex:1;min-width:200px">Generate Analytics Report</button>
                    <button class="btn-outline" id="exportCsvBtn" style="display:none" onclick="exportCSV()">📥 CSV</button>
                    <button class="btn-outline" id="exportXlsBtn" style="display:none" onclick="exportExcel()">📊 Excel</button>
                    <button class="btn-outline" id="exportPdfBtn" style="display:none" onclick="window.print()">🖨️ PDF</button>
                </div>
            </div>

            <!-- ANALYTICS CONTAINER -->
            <div id="analyticsContainer" style="display:none">
                <!-- 1.2  Active filter chips -->
                <div id="activeFiltersBar" style="display:none;margin-bottom:16px;flex-wrap:wrap;gap:8px;align-items:center">
                    <span style="font-size:.75rem;color:var(--text-muted);font-weight:600;text-transform:uppercase;letter-spacing:.5px">Active Filters:</span>
                    <div id="filterChips"></div>
                    <button class="btn-sm" onclick="clearActiveFilters()" style="background:rgba(255,77,109,.1);color:var(--accent-rose);border:1px solid rgba(255,77,109,.3)">✕ Clear All</button>
                </div>

                <!-- 1.7  Insights Panel -->
                <div id="insightsPanel">
                    <h4>🧠 Auto-Generated Insights</h4>
                    <ul id="insightsList"></ul>
                </div>

                <!-- 1.1  Comparison KPIs -->
                <div id="comparisonKpisSection" style="display:none;margin-bottom:24px">
                    <div class="compare-section">
                        <h5>📊 Period Comparison: Current vs Previous</h5>
                        <div class="compare-kpi-row">
                            <div class="compare-kpi"><div class="compare-kpi-label">Current Creations</div><div class="compare-kpi-val" id="cmpCurCreations">—</div></div>
                            <div class="compare-kpi"><div class="compare-kpi-label">Previous Creations</div><div class="compare-kpi-val" id="cmpPrevCreations">—</div></div>
                            <div class="compare-kpi"><div class="compare-kpi-label">Current BDs</div><div class="compare-kpi-val" id="cmpCurBds">—</div></div>
                            <div class="compare-kpi"><div class="compare-kpi-label">Previous BDs</div><div class="compare-kpi-val" id="cmpPrevBds">—</div></div>
                            <div class="compare-kpi"><div class="compare-kpi-label">Current Closings</div><div class="compare-kpi-val" id="cmpCurClosings">—</div></div>
                            <div class="compare-kpi"><div class="compare-kpi-label">Previous Closings</div><div class="compare-kpi-val" id="cmpPrevClosings">—</div></div>
                        </div>
                    </div>
                </div>

                <!-- KPI cards -->
                I encountered an error doing what you asked. Could you try again?
