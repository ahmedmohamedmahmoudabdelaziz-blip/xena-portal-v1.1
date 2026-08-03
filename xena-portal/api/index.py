"""
Xena Data Portal — High-Speed Hybrid Backend (Enterprise Edition)
"""

import os, time, re, json, hashlib, logging, urllib.parse, threading, random, uuid
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, send_file, redirect, Response
import requests as http_requests

APP_ID       = os.environ.get("LARK_APP_ID")
APP_SECRET   = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://xena-portal-v1-1.vercel.app/api/callback")

UPSTASH_REDIS_REST_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
REDIS_ENABLED = bool(UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN)
REDIS_MAX_VALUE_BYTES = 900_000

MOCK_MODE = not bool(APP_ID and APP_SECRET)

BASE_ID           = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID   = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID   = "tbl3wweYCpmDmDSx"
AUDIT_TABLE_ID    = os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB")   

ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954']

PK_ACMS = {"nabeel","hasseb","haseeb","enzo","farooq","mubeen","cruz","ehtisham",
            "usama","sehar ch","hamza malik","zohaib","eagle","leo","berlin"}
IN_ACMS  = {"holy","vihan","shivam","ravikant","ansh","rocky","bella"}

CACHE_TTL_REALTIME   = 300    
CACHE_TTL_HISTORICAL = 3600   

RATE_LIMIT_SEARCH    = (50, 60)
RATE_LIMIT_ANALYTICS = (30, 60)
RATE_LIMIT_RECORDS   = (50, 60)

COINS_MULTIPLIER = 100000

QUERY_FIELD_ALIASES = {
    "user_id":     ["User ID"],
    "numbering":   ["Numbering"],
    "otherapp_id": ["Otherapp ID", "Otherapp Name", "Other App ID"],
    "nid_number":  ["NID Number", "NID"],
    "bd_code":     ["Bd Code", "BD Code"],
}

# Strictly block users from manually overriding system/staff-managed fields during form submission
EXCLUDED_SUBMIT_FIELDS = {
    "Numbering", "Submitted on", "Submitted on Copy", "Match ID", "Record ID Text",
    "Cleaned User ID", "Bot Color", "Bot Title", "Bot Message", "Ticket Details",
    "Duplicated Check", "Handle Time (Seconds)", "Base Points", "Formula",
    "Point Balance", "time of the requests", "Created By", "Webhook Lookup",
    "Mention this Group", "BD Nickname1", "BD Nickname2", "Respondents", "Lock Owner",
    "Assigned Member", "Done by", "Mentioned Person", "Assigned Time", "Completion Time",
    "Last Retry Time", "Ready to Archive", "Reward", "Approval", "Status", "Request Status"
}

MONTHLY_ALLOCATOR_LIMITS = {
    "trend card": 10, "traffic card": 50, "30 mic 15 days": 999, "30 mic 30 days": 999,
    "normal short id ( 2 levels above ) 15 days": 999, "normal short id ( 2 levels above ) 30 days": 999,
    "customized short id 15 days": 999, "customized short id 30 days": 999,
    "room pin-up": 999, "welcome package 3": 15, "welcome package 2": 50,
}
ORDER_TYPE_LIMITS = {
    "main page banner": 3, "news banner": 5, "live banner": 5, "splash": 10,
}

# ════════════════════════════════════════════════════════════════════
# CORE UTILITIES & TIMEZONE MANAGEMENT
# ════════════════════════════════════════════════════════════════════
CAIRO_OFFSET = timedelta(hours=3)

def cairo_now():
    """Authoritative 'current time' for the whole app. Vercel's server clock is UTC,
    but the business/users are in Cairo (UTC+3). Every 'Current Month' calculation
    MUST go through this function."""
    return datetime.now(timezone.utc) + CAIRO_OFFSET

_token_cache = {"token": None, "expires_at": 0, "lock": threading.Lock()}

def get_tenant_access_token():
    if MOCK_MODE: return "mock_tenant_token_12345"
    with _token_cache["lock"]:
        if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
            return _token_cache["token"]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = http_requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10).json()
    token = resp.get("tenant_access_token")
    expire = resp.get("expire", 7200)

    with _token_cache["lock"]:
        _token_cache["token"] = token
        _token_cache["expires_at"] = time.time() + max(expire - 300, 60)
    return token

_schema_cache = {"data": {}, "lock": threading.Lock()}

def get_table_schema(table_id, token, base_id, ttl=300):
    """Returns the set of live field names for a table, cached briefly. Used to make
    record-creation resilient: we drop any field we're about to write that Feishu
    doesn't actually recognize, instead of letting the whole write fail."""
    with _schema_cache["lock"]:
        cached = _schema_cache["data"].get(table_id)
        if cached and time.time() - cached["ts"] < ttl:
            return cached["fields"]

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/fields"
    try:
        resp = http_requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15).json()
        if resp.get("code") == 0:
            fields = set(f.get("field_name") for f in resp.get("data", {}).get("items", []))
            with _schema_cache["lock"]:
                _schema_cache["data"][table_id] = {"fields": fields, "ts": time.time()}
            return fields
    except Exception as e:
        logger.error("get_table_schema_failed", table_id=table_id, error=str(e))
    # On failure, fall back to whatever we last successfully cached (better than nothing)
    with _schema_cache["lock"]:
        cached = _schema_cache["data"].get(table_id)
        return cached["fields"] if cached else set()

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

def redis_cmd(*args, timeout=8):
    if not REDIS_ENABLED: return None
    try:
        resp = http_requests.post(
            UPSTASH_REDIS_REST_URL,
            headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
            json=list(args), timeout=timeout
        )
        return resp.json().get("result")
    except Exception as e:
        logger.warn("redis_cmd_failed", cmd=args[0] if args else "?", error=str(e))
        return None

def redis_get_json(key):
    raw = redis_cmd("GET", key)
    if not raw: return None
    try: return json.loads(raw)
    except Exception: return None

def redis_set_json(key, value, ttl=None):
    try:
        payload = json.dumps(value, default=str)
    except Exception as e:
        logger.warn("redis_serialize_failed", key=key, error=str(e))
        return False
    if len(payload) > REDIS_MAX_VALUE_BYTES:
        logger.warn("redis_value_too_large", key=key, size_bytes=len(payload))
        return False
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
_data_status = {}   # diagnostic info per filename, surfaced via /api/debug/data-status
SELF_BASE_URL = os.environ.get("SELF_BASE_URL", "").rstrip("/")

def _candidate_data_paths(filename):
    """Every plausible location the pre-fetched JSON could live at runtime on Vercel."""
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

def _fetch_data_over_http(filename):
    """Last-resort fallback: pull the pre-built JSON from this SAME deployment's own CDN-served static asset."""
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
                _data_status[filename] = {
                    "source": "disk", "path": file_path,
                    "records": len(data) if isinstance(data, list) else None,
                    "loaded_at": time.time(),
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
            "loaded_at": time.time(),
        }
        return data

    _data_status[filename] = {"source": "not_found", "tried_paths": tried_paths, "loaded_at": time.time()}
    logger.warn("local_json_not_found_anywhere", file=filename, tried_paths=tried_paths)
    return None

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

class AuditLogger:
    def __init__(self):
        self._queue = []
        self._lock  = threading.Lock()

    def log(self, actor, action, target, details="", ip="", severity="Info"):
        full_target = f"{target} | {details}" if details else target
        entry = {
            "actor": mask_name(actor) or "Unknown", "action": action, "target": full_target,
            "ip": ip, "severity": severity, "ts": datetime.utcnow().isoformat(),
        }
        logger.info("audit", **entry)
        if AUDIT_TABLE_ID and not MOCK_MODE:
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

def extract_attachments(field_data):
    """For Attachment-type fields: returns [{name, proxy_url}]. Feishu's own attachment
    'url' already includes a required '?extra=...' permission token scoped to this table -
    rebuilding the URL from file_token alone (dropping that param) is what was causing
    downloads to fail. We pass Feishu's exact url through our proxy instead."""
    if not field_data or not isinstance(field_data, list): return []
    out = []
    for item in field_data:
        if isinstance(item, dict) and item.get("url"):
            encoded = urllib.parse.quote(item["url"], safe="")
            out.append({"name": item.get("name", "file"), "url": f"/api/attachments/proxy?src={encoded}"})
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
        return [s.strip() for s in field_data.split(',') if s.strip()]
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

def get_user_permissions(email, name):
    name_clean = name.strip().lower() if name else ""
    email_clean = email.strip().lower() if email else ""
    
    if any(admin == name_clean for admin in ADMIN_USERS):
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin", "query", "submit", "submit_new_request", "submit_my_requests", "query_requests", "query_agency_list", "export_data"], 
            "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]},
                            "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}}
        }

    if not email_clean and not name_clean: 
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

    if MOCK_MODE:
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin", "query", "submit", "submit_new_request", "submit_my_requests", "query_requests", "query_agency_list", "export_data"], 
            "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]},
                            "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}}
        }

    cache_key = cache_make_key("perms", email_clean, name_clean)
    cached = cache_get(cache_key)
    if cached: return cached

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
                return result
    except Exception: pass
    
    fallback = {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}
    return fallback

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

def fetch_feishu_records(table_id, from_dt=None):
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

    page_token = None
    for _ in range(200):
        params = {"page_size": 500, "automatic_fields": "true"} 
        if table_id == REQUESTS_TABLE_ID: params["sort"] = '["Numbering DESC"]'
        if page_token: params["page_token"] = page_token
        
        try:
            resp = session.get(url, params=params, timeout=45) 
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

def _date_filter_value(dt):
    return [str(int(dt.timestamp() * 1000))]

def _peek_newest_date(tat, table_id):
    try:
        session = http_requests.Session()
        session.headers.update({"Authorization": f"Bearer {tat}"})
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records"
        resp = session.get(url, params={"page_size": 1, "sort": '["Numbering DESC"]'}, timeout=15)
        data = resp.json()
        items = data.get("data", {}).get("items", [])
        if items:
            raw = get_field_local(items[0].get("fields", {}), "Submitted on Copy", "Submitted on", "Created Time")
            dt = parse_feishu_date(raw)
            if dt: return dt
    except Exception as e:
        logger.warn("peek_newest_date_failed", table=table_id, error=str(e))
    return datetime.utcnow()

def _date_buckets(from_dt, to_dt, n_buckets):
    total_seconds = max((to_dt - from_dt).total_seconds(), 1)
    step = total_seconds / n_buckets
    return [(from_dt + timedelta(seconds=step * i), from_dt + timedelta(seconds=step * (i + 1)))
            for i in range(n_buckets)]

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

def _merge_shards(shard_results):
    all_items, seen, master_keys = [], set(), set()
    fetch_complete, stop_reason = True, ""
    for items, complete, reason in shard_results:
        if not complete:
            fetch_complete, stop_reason = False, reason
        for item in items:
            rid = item.get("record_id")
            if rid and rid not in seen:
                seen.add(rid)
                all_items.append(item)
                master_keys.update(item.get("fields", {}).keys())
    return all_items, master_keys, fetch_complete, stop_reason

def _run_shards_timed(table_id, tat, shards, field_names):
    def _timed(shard_filter):
        t0 = time.time()
        items, complete, reason = _fetch_bitable_shard(table_id, tat, shard_filter, field_names)
        logger.info("shard_fetch", table=table_id, ms=int((time.time() - t0) * 1000),
                    rows=len(items), complete=complete, reason=reason or "")
        return items, complete, reason

    with ThreadPoolExecutor(max_workers=len(shards)) as executor:
        futures = [executor.submit(_timed, f) for f in shards]
        return [f.result() for f in futures]

REQUESTS_SHARD_COUNT = 10 
REQUESTS_LOOKBACK_DAYS_DEFAULT = 365 * 3  

def fetch_requests_sharded(from_dt=None, to_dt=None, field_names=REQUESTS_ANALYTICS_FIELDS, n_shards=REQUESTS_SHARD_COUNT):
    """Bypassing the Bitable sharded filter that breaks on mixed column types, and explicitly filtering the results purely in Python side using `fetch_feishu_records`."""
    if MOCK_MODE:
        items = MockFeishuDB.generate_requests(300)
        keys = set(items[0]["fields"].keys()) if items else set()
        return items, keys, True, ""

    items, keys, fetch_complete, stop_reason = fetch_feishu_records(REQUESTS_TABLE_ID, from_dt=from_dt)

    # Clean pure-Python filtering
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
        
        if balance == 0 and total_pts > 0: balance = total_pts - used_pts

        health_score, health_status = 100, "Healthy"
        if total_pts > 0:
            utilization = used_pts / total_pts
            if utilization > 0.90: health_score, health_status = 40, "Critical"
            elif utilization > 0.70: health_score, health_status = 70, "At Risk"
            else: health_score, health_status = 95, "Healthy"
        else: 
            health_score, health_status = 0, "Inactive"

        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw.upper(), "acm": acm_raw.title(),
            "total_points": total_pts, "used_points": used_pts,
            "point_balance": balance, "health_score": health_score,
            "health_status": health_status,
            "history": history_points, 
            "allocator_status": allocator_status,
            "accepted_user_ids": sorted(set(accepted_ids_all)),
            "requests": [r.get("fields", {}) for r in all_records]
        }
    else:  
        raw_base_pts = parse_float_safe(extract_field_text(get_field_local(first, "Base Points", "base_points")))
        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw.upper(), "acm": acm_raw.title(),
            "base_points": raw_base_pts * COINS_MULTIPLIER, "health_score": 100, "health_status": "Healthy",
            "privileges_claimed": dict(privileges_claimed),  
            "history": history_target, 
            "accepted_user_ids": sorted(set(accepted_ids_all)),
            "requests": [r.get("fields", {}) for r in all_records]
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

    acm_filter_c     = acm_filter.strip().lower() if acm_filter else "all"
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
        if acm_filter_c != "all" and acm_filter_c != acm: continue

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

def _background_sync_requests_table():
    with _bg_sync_lock:
        if _bg_sync["syncing"]: return
        _bg_sync["syncing"] = True
    try:
        items, keys, complete, reason = fetch_feishu_records(REQUESTS_TABLE_ID)
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

def get_requests_table_snapshot(from_dt=None):
    local_data = load_local_json("requests.json")
    if local_data:
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

    return fetch_requests_sharded(from_dt=from_dt) + (False,)

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

def get_points_table_snapshot():
    local_data = load_local_json("points.json")
    if local_data:
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

    items, _keys, complete, reason = fetch_feishu_records(POINTS_TABLE_ID)
    return items, complete, reason, False


app = Flask(__name__)

# === NEW: FEISHU WEBHOOK ENDPOINT (HYBRID APPROACH) ===
@app.route('/api/webhook/feishu', methods=['GET', 'POST'])
def feishu_webhook():
    """Receives events from Feishu Event Subscriptions (like record updates)."""
    try:
        # force=True guarantees Flask reads the payload even if Feishu sends unusual HTTP headers
        # silent=True prevents Flask from crashing if the payload is empty
        data = request.get_json(force=True, silent=True) or {}
        
        # 1. Pass the Feishu Verification Challenge instantly
        if "challenge" in data:
            return jsonify({"challenge": data["challenge"]})
            
        # 2. Handle Record Changes (Optional but powerful)
        # When a record changes in Feishu, we instantly trigger a background refresh
        # so our memory cache stays perfectly up-to-date without waiting 3 minutes.
        header = data.get("header", {})
        event = data.get("event", {})
        
        if header.get("event_type") in ["bitable.application.record.created_v1", "bitable.application.record.updated_v1"]:
            if event.get("table_id") == REQUESTS_TABLE_ID:
                threading.Thread(target=_background_sync_requests_table, daemon=True).start()
            elif event.get("table_id") == POINTS_TABLE_ID:
                threading.Thread(target=_background_sync_points_table, daemon=True).start()
                
        # Always return 200 OK so Feishu knows we successfully received it
        return jsonify({"code": 0, "msg": "success"}), 200
        
    except Exception as e:
        logger.error("webhook_parsing_error", error=str(e))
        # Even if parsing fails completely, reply 200 so Feishu doesn't timeout/block us
        return jsonify({"code": 0, "msg": "success"}), 200

@app.route('/', methods=['GET'])
def home():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return send_file(os.path.join(root_dir, 'index.html'))

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

        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
        audit.log(lark_name, "LOGIN", mask_email(lark_email), ip=ip)
        
        return redirect(f"/?user={urllib.parse.quote(lark_name, safe='')}&email={urllib.parse.quote(lark_email, safe='')}&uat={urllib.parse.quote(uat, safe='')}&avatar={urllib.parse.quote(avatar_url, safe='')}")

    except Exception as exc:
        return redirect("/?auth_error=" + urllib.parse.quote(f"Login error: {str(exc)[:120]}", safe=''))

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
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions("", admin_name)
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
                          f"analytics={data.get('acms',{}).get('analytics','all')};"
                          f"query={data.get('acms',{}).get('query','all')}")
        regs_formatted = (f"target={data.get('regions',{}).get('target','all')};"
                          f"points={data.get('regions',{}).get('points','all')};"
                          f"analytics={data.get('regions',{}).get('analytics','all')};"
                          f"query={data.get('regions',{}).get('query','all')}")
                          
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
        cache_invalidate(cache_make_key("perms", email_to_check.lower(), ""))
        return jsonify({"success":True,"record_id":res.get("data",{}).get("record",{}).get("record_id")})

    elif request.method == 'DELETE':
        record_id = sanitize_text(request.args.get('id',''))
        res = http_requests.delete(f"{base_url}/{record_id}", headers=headers, timeout=15).json()
        if res.get("code") != 0: return jsonify({"success":False,"error":res.get("msg","Delete failed")}), 500
        audit.log(admin_name, "DELETE_USER", record_id, ip=ip, severity="Warning")
        return jsonify({"success":True})

@app.route('/api/admin/audit-logs', methods=['GET'])
def audit_logs():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions("", admin_name)
        if not perms.get("is_super_admin"): return jsonify({"error":"Unauthorized"}), 403
        
    if MOCK_MODE: return jsonify(audit.get_recent(50))
    
    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{AUDIT_TABLE_ID}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    payload = {
        "page_size": min(int(request.args.get('limit','100')), 500),
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
        return jsonify(logs)
    except Exception as e:
        logger.error("audit_log_fetch_failed", error=str(e))
        return jsonify(audit.get_recent(min(int(request.args.get('limit','100')), 500)))

@app.route('/api/requests/submit', methods=['POST'])
def submit_request():
    """Flawless Hybrid Engine: Creates an empty shell record using User's context first to capture identity natively. 
    Then, the System token (TAT) immediately populates all the user's submitted fields and system tracking fields."""
    user = sanitize_text(request.form.get('user', ''))
    email = sanitize_text(request.form.get('email', ''))
    uat = request.form.get('uat', '')
    
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

    tat = get_tenant_access_token()
    
    # Pre-fetch the User's Feishu Open ID using their UAT
    # (Used to explicitly stamp their profile into 'Respondents' if UAT creation fails)
    open_id = None
    if uat:
        try:
            u_info = http_requests.get("https://open.feishu.cn/open-apis/authen/v1/user_info", headers={"Authorization": f"Bearer {uat}"}, timeout=5).json()
            open_id = u_info.get("data", {}).get("open_id")
        except: pass

    # Prepare final fields
    final_fields = {}
    actual_fields = get_table_schema(REQUESTS_TABLE_ID, tat, BASE_ID) or set()

    for key, val in user_fields.items():
        if key not in EXCLUDED_SUBMIT_FIELDS and val not in (None, "", []):
            if actual_fields and key not in actual_fields: continue
            final_fields[key] = val

    # Upload files exclusively using TAT to prevent permission issues
    for field_name in request.files:
        if field_name in EXCLUDED_SUBMIT_FIELDS: continue
        if actual_fields and field_name not in actual_fields: continue
            
        file_list = request.files.getlist(field_name)
        tokens = []
        for f in file_list:
            b = f.read()
            if not b: continue
            try:
                form_data = {
                    'file_name': f.filename,
                    'parent_type': 'bitable_file',
                    'parent_node': BASE_ID,
                    'size': str(len(b))
                }
                files = {'file': (f.filename, b, f.mimetype)}
                h = {"Authorization": f"Bearer {tat}"}
                up_res = http_requests.post("https://open.feishu.cn/open-apis/drive/v1/medias/upload_all", headers=h, data=form_data, files=files, timeout=30).json()
                if up_res.get("code") == 0:
                    tokens.append({"file_token": up_res["data"]["file_token"]})
                else:
                    raise Exception(f"File upload failed: {up_res.get('msg')}")
            except Exception as e:
                logger.error("file_upload_failed", error=str(e), filename=f.filename)
                return jsonify({"error": f"Failed to upload {f.filename}. Error: {str(e)}"}), 502
        if tokens:
            final_fields[field_name] = tokens

    system_fields = {"Request Status": "Pending", "Submitted By": user}
    if actual_fields:
        system_fields = {k: v for k, v in system_fields.items() if k in actual_fields}

    success = False
    record_id = None
    used_tat_for_creation = False

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records"

    # ATTEMPT 1: Create an empty/minimal record using User's Token (UAT)
    # This purely registers their Feishu Name/Avatar into the "Created By" column natively.
    if uat:
        h_uat = {"Authorization": f"Bearer {uat}", "Content-Type": "application/json"}
        
        # We try a completely empty record first. If Bitable complains about missing required fields,
        # we try passing just the Request Type.
        attempts = [
            {},                             # 1. Blank record (Guaranteed no permission block)
            {"Request Type": req_type}      # 2. Minimal fields (If Bitable validation requires a field)
        ]
        
        for fields_to_try in attempts:
            try:
                resp = http_requests.post(url, headers=h_uat, json={"fields": fields_to_try}, timeout=10)
                data = resp.json()
                code = data.get("code")
                
                if code == 0:
                    success = True
                    record_id = data.get("data", {}).get("record", {}).get("record_id")
                    break
                elif code in [99991668, 99991664, 99991663]:
                    # EXPIRED TOKEN: Halt immediately and inform the user.
                    return jsonify({"error": "Your Feishu session has expired. Please refresh the page to log in again."}), 401
                else:
                    logger.warn("uat_creation_attempt_failed", code=code, msg=data.get("msg"))
            except Exception as e:
                logger.warn("uat_creation_exception", error=str(e))
                break # Network error, break loop to fallback

    # ATTEMPT 2: Fallback to System Token (TAT) ONLY if UAT fundamentally lacks table creation rights
    if not success:
        used_tat_for_creation = True
        create_fields = final_fields.copy()
        create_fields.update(system_fields)
        if open_id and (not actual_fields or "Respondents" in actual_fields):
            create_fields["Respondents"] = [{"id": open_id}]
            
        h_tat = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
        for attempt in range(3):
            try:
                resp = http_requests.post(url, headers=h_tat, json={"fields": create_fields}, timeout=15)
                data = resp.json()
                if data.get("code") == 0:
                    success = True
                    record_id = data.get("data", {}).get("record", {}).get("record_id")
                    break
                else:
                    feishu_err = data.get('msg', 'Unknown Error')
                    if attempt == 2: raise Exception(feishu_err)
            except Exception as e:
                if attempt == 2:
                    return jsonify({"error": f"Failed to create record: {str(e)}"}), 502
                time.sleep(1 * (attempt + 1))

    # PHASE 2: Data Population via TAT (System)
    # The UAT created the empty row. Now the System immediately injects all form data + system tracking.
    if success and not used_tat_for_creation and record_id:
        update_fields = final_fields.copy()
        update_fields.update(system_fields)
        
        tat_headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
        update_url = f"{url}/{record_id}"
        
        for attempt in range(3):
            try:
                # Use PUT to completely update the empty row with the full payload
                resp = http_requests.put(update_url, headers=tat_headers, json={"fields": update_fields}, timeout=15)
                data = resp.json()
                if data.get("code") == 0:
                    break
                else:
                    if attempt == 2:
                        logger.error("tat_population_failed", msg=data.get("msg"))
            except Exception as e:
                if attempt == 2: logger.error("tat_population_exception", error=str(e))
                time.sleep(1 * (attempt + 1))

    # Audit Trace Action
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    audit.log(user, "SUBMIT_NEW_REQUEST", f"Type: {req_type} | Code: {user_fields.get('Agency Code', 'N/A')} | Mode: {'TAT-Fallback' if used_tat_for_creation else 'UAT-Blank+TAT-Populate'}", ip=ip, severity="Info")

    return jsonify({"success": True, "message": f"Successfully submitted {req_type}!"})

@app.route('/api/requests/single', methods=['GET'])
def get_single_request():
    """Fetches one fresh record by record_id - used by the ticket workspace's
    Duplicated Check flow to re-pull a record after Feishu's formulas recalculate."""
    user = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    record_id = sanitize_text(request.args.get('record_id',''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("tickets" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403
    if not record_id:
        return jsonify({"error": "Missing record_id"}), 400

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}"
    try:
        resp = http_requests.get(url, headers={"Authorization": f"Bearer {tat}"}, timeout=15)
        data = resp.json()
        if data.get("code") != 0:
            return jsonify({"error": data.get("msg", "Record not found")}), 404
        return jsonify(build_ticket_payload(data.get("data", {}).get("record", {})))
    except Exception as e:
        logger.error("get_single_request_failed", record_id=record_id, error=str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/requests/update', methods=['POST'])
def update_request():
    """Patches an existing record - used both by the Duplicated Check 'save progress
    before waiting for formulas' step, and the final Submit & Resolve action."""
    body = request.get_json(force=True, silent=True) or {}
    user = sanitize_text(request.args.get('user','') or body.get('user',''))
    email = sanitize_text(request.args.get('email','') or body.get('email',''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("tickets" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    record_id = sanitize_text(body.get('record_id', ''))
    if not record_id:
        return jsonify({"error": "Missing record_id"}), 400

    # Only these are ever legal to edit from the ticket workspace - anything else in the
    # body (record_id, user, email, etc.) is deliberately ignored, not just filtered later.
    EDITABLE_TICKET_FIELDS = {
        "status": "Status", "audition_note": "Audition note",
    }
    fields = {}
    for key, feishu_name in EDITABLE_TICKET_FIELDS.items():
        if key in body and body[key] not in (None, ""):
            fields[feishu_name] = body[key]
    # Reject Reason is a MultiSelect in Feishu - accepts a list of option strings
    if body.get('reject_reason'):
        rr = body['reject_reason']
        fields["Reject Reason"] = rr if isinstance(rr, list) else [rr]
    if body.get('create_way'):
        fields["Create Way"] = body['create_way']
    if body.get('mentioned_person'):
        fields["Mentioned Person"] = body['mentioned_person']
    if body.get('chinese_note'):
        fields["chinese Note"] = body['chinese_note']
    if body.get('type_of_action_approval'):
        fields["Type of Action"] = body['type_of_action_approval']
    # Allow raw Feishu field names too, for callers editing basic/detail fields directly
    for feishu_name in TICKET_FIELD_KEY_MAP:
        if feishu_name in body and body[feishu_name] not in (None, ""):
            fields[feishu_name] = body[feishu_name]

    if not fields:
        return jsonify({"error": "No editable fields provided"}), 400

    tat = get_tenant_access_token()
    actual_fields = get_table_schema(REQUESTS_TABLE_ID, tat, BASE_ID)
    if actual_fields:
        dropped = [k for k in fields if k not in actual_fields]
        if dropped:
            logger.warn("update_dropped_unknown_fields", fields=dropped)
        fields = {k: v for k, v in fields.items() if k in actual_fields}

    if not fields:
        return jsonify({"error": "None of the provided fields exist in the live Feishu schema"}), 400

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    try:
        resp = http_requests.put(url, headers=headers, json={"fields": fields}, timeout=15)
        data = resp.json()
        if data.get("code") != 0:
            err = data.get("msg", "Update failed")
            if data.get("code") == 1254045:
                err = "One of the provided fields does not match the exact Feishu column schema."
            return jsonify({"error": err}), 502
    except Exception as e:
        logger.error("update_request_failed", record_id=record_id, error=str(e))
        return jsonify({"error": str(e)}), 502

    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    audit.log(user, "TICKET_UPDATE", f"record={record_id} fields={list(fields.keys())}", ip=ip, severity="Info")
    return jsonify({"success": True})

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
            "point_balance": balance, "health_score": health,
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

    is_export = request.args.get('export', 'false').lower() == 'true'
    if is_export:
        if not perms.get("is_super_admin") and not any("export" in m for m in perms.get("modules",[])):
            audit.log(user, "UNAUTHORIZED_EXPORT", "Point Records", ip=ip, severity="Critical")
            return jsonify({"error":"Export access denied."}), 403
        
        audit.log(user, "EXPORT_DATA", f"Point Records ({total_count} rows)", ip=ip, severity="Info")
        return jsonify({
            "records": filtered[:5000], "total": total_count, "page": 1, "page_size": total_count,
            "total_pages": 1,
            "totals": {"total_points": total_pts_sum, "used_points": used_pts_sum, "point_balance": balance_sum},
            "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason),
            "served_from_background_cache": served_from_cache,
            "duration_ms": int((time.time() - start) * 1000), "raw_rows_fetched": len(all_items)
        })

    slice_start, slice_end = (page - 1) * page_size, (page - 1) * page_size + page_size
    page_records = filtered[slice_start:slice_end]

    return jsonify({
        "records": page_records, "total": total_count, "page": page, "page_size": page_size,
        "total_pages": max(1, -(-total_count // page_size)),
        "totals": {"total_points": total_pts_sum, "used_points": used_pts_sum, "point_balance": balance_sum},
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
    user  = sanitize_text(request.args.get('user', request.headers.get('X-User-Name','')))
    email = sanitize_text(request.args.get('email',''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not perms.get("modules"): return jsonify({"error":"Access denied"}), 403

    cache_invalidate()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(_background_sync_requests_table), executor.submit(_background_sync_points_table)]
        for f in futures: f.result()

    with _bg_sync_lock:
        req_count, req_updated, req_complete = len(_bg_sync["requests_items"]), _bg_sync["updated_at"], _bg_sync["fetch_complete"]
    with _bg_points_lock:
        pts_count, pts_updated, pts_complete = len(_bg_points_sync["items"]), _bg_points_sync["updated_at"], _bg_points_sync["fetch_complete"]

    audit.log(user, "MANUAL_SYNC_REFRESH", "grand_table+points_table", ip=request.headers.get("X-Forwarded-For",""), severity="Info")
    return jsonify({
        "success": True,
        "requests_table": {"record_count": req_count, "updated_at": req_updated, "fetch_complete": req_complete},
        "points_table":   {"record_count": pts_count, "updated_at": pts_updated, "fetch_complete": pts_complete},
        "redis_enabled": REDIS_ENABLED,
    })

@app.route('/api/points/search', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_search():
    if request.args.get('q') and not request.args.get('search'):
        args = dict(request.args)
        args['search'] = args.pop('q')
        request.environ['QUERY_STRING'] = urllib.parse.urlencode(args, doseq=True)
    return points_records()

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
        search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
        headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

        aliases = QUERY_FIELD_ALIASES[field]
        combos = []
        for alias in aliases:
            for op in ["contains", "is", "="]:
                if op == "=" and not value.isdigit(): continue
                val_array = (int(value),) if op == "=" else (value,)
                combos.append((alias, op, val_array))

        def try_combo(combo, projection=QUERY_RECORDS_FIELDS):
            alias, op, val_array = combo
            payload = {"page_size": 500, "filter": {"conjunction": "and", "conditions": [{"field_name": alias, "operator": op, "value": val_array}]}}
            if projection: payload["field_names"] = projection
            try:
                resp = http_requests.post(search_url, headers=headers, json=payload, timeout=10)
                data = resp.json()
                if data.get("code") == 1254045 and projection:
                    return try_combo(combo, projection=None)
                if data.get("code") == 0:
                    return {"combo": combo, "ok": True, "items": data.get("data", {}).get("items", [])}
                elif data.get("code") not in (1254011, 1254402, 1254010):
                    return {"combo": combo, "ok": False, "error": data.get("msg"), "fatal": data.get("code") == 99991663}
                return {"combo": combo, "ok": False, "error": None}
            except Exception as e:
                return {"combo": combo, "ok": False, "error": str(e)}

        results_by_combo = {}
        with ThreadPoolExecutor(max_workers=min(9, len(combos) or 1)) as executor:
            for res in executor.map(try_combo, combos):
                results_by_combo[res["combo"]] = res

        all_items, fetch_complete, stop_reason, success = [], False, "", False
        for combo in combos:
            res = results_by_combo.get(combo)
            if res and res.get("ok"):
                all_items, success, fetch_complete = res["items"], True, True
                break
            if res and res.get("error"):
                stop_reason = res["error"]

        if not success: return jsonify({"error": f"Data fetch failed: Feishu API Error: {stop_reason or 'Invalid Filter.'}"}), 502

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
            "numbering":        extract_field_text(get_field_local(fields, "Numbering")),
            "request_type":     extract_field_text(get_field_local(fields, "Request Type", "Type")),
            "submitted_on":     submitted_dt.strftime("%Y-%m-%d") if submitted_dt else extract_field_text(submitted_raw),
            "respondents":      extract_field_text(get_field_local(fields, "Respondents")),
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
    perms = get_user_permissions(email, user)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    
    if not perms.get("is_super_admin") and not any("submit_my_requests" in m or "submit" in m for m in perms.get("modules",[])):
        return jsonify({"error":"Access denied"}), 403
    
    _cairo_now = cairo_now()
    from_dt = _cairo_now - timedelta(days=15)
    
    if MOCK_MODE:
        all_items = MockFeishuDB.generate_requests(50)
        fetch_complete, stop_reason = True, ""
    else:
        tat = get_tenant_access_token()
        headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
        search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"

        # Refactored: Fetch pages strictly sorted by Numbering and filter in Python.
        # This completely avoids the 1254018 InvalidFilter error caused by Feishu rejecting the person/date filter.
        user_clean = user.strip().lower()
        all_items = []
        page_token = None
        fetch_complete = True
        stop_reason = ""
        
        for _ in range(10): # Fetch up to ~5000 recent records
            params = {
                "page_size": 500,
                "sort": [{"field_name": "Numbering", "desc": True}]
            }
            if page_token:
                params["page_token"] = page_token
                
            try:
                resp = http_requests.post(search_url, headers=headers, json=params, timeout=15)
                data = resp.json()
                
                if data.get("code") != 0:
                    fetch_complete = False
                    stop_reason = data.get("msg") or f"Feishu error {data.get('code')}"
                    logger.warn("my_requests_fetch_error", code=data.get("code"), msg=stop_reason)
                    break
                    
                items = data.get("data", {}).get("items", [])
                if not items:
                    break
                    
                reached_older_records = False
                for it in items:
                    f = it.get("fields", {})
                    
                    raw_date = get_field_local(f, "Submitted on Copy", "Submitted on", "Created Time")
                    dt = parse_feishu_date(raw_date)
                    
                    if dt and dt < from_dt:
                        reached_older_records = True
                        continue
                        
                    sb = extract_field_text(get_field_local(f, "Submitted By", "Created By")).lower()
                    if user_clean in sb:
                        all_items.append(it)
                        
                if reached_older_records:
                    break
                    
                page_token = data.get("data", {}).get("page_token")
                if not page_token or not data.get("data", {}).get("has_more"):
                    break
                    
            except Exception as e:
                fetch_complete = False
                stop_reason = str(e)
                logger.warn("my_requests_exception", error=str(e))
                break
    
    results = []
    
    for item in all_items:
        fields = item.get("fields", {})
        
        raw_date = get_field_local(fields, "Submitted on Copy", "Submitted on", "Created Time")
        dt = parse_feishu_date(raw_date)
        
        # Apply the exact 15-day limit on the ultra-small result set
        if not dt or dt < from_dt:
            continue
        
        region = clean(get_field_local(fields, "Region", "Agency Region"))
        acm_pk = clean(get_field_local(fields, "Acm Name (PK)"))
        acm_in = clean(get_field_local(fields, "Acm Name (IN)"))
        acm_fb = clean(get_field_local(fields, "Acm", "Assigned Member"))
        if region in ("", "none"):
            if acm_pk in PK_ACMS or acm_fb in PK_ACMS: region = "pk"
            elif acm_in in IN_ACMS or acm_fb in IN_ACMS: region = "in"
        acm = (acm_in if region == "in" else acm_pk) or acm_fb
        
        submitted_by = extract_field_text(get_field_local(fields, "Submitted By"))
        respondents = extract_field_text(get_field_local(fields, "Respondents", "Created By"))
        
        results.append({
            "record_id": item.get("record_id"),
            "numbering": extract_field_text(get_field_local(fields, "Numbering")),
            "request_type": extract_field_text(get_field_local(fields, "Request Type", "Type")),
            "submitted_on": dt.strftime("%Y-%m-%d %H:%M") if dt else extract_field_text(raw_date),
            "respondents": submitted_by or respondents,
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
            "_sort_ts": dt.timestamp() if dt else 0,
        })
    
    results.sort(key=lambda r: r["_sort_ts"], reverse=True)
    for r in results: r.pop("_sort_ts", None)
    
    audit.log(user, "MY_REQUESTS_VIEW", f"Fetched {len(results)} records", ip=ip, severity="Info")
    
    return jsonify({
        "results": results,
        "count": len(results),
        "fetch_complete": fetch_complete,
        "stop_reason": ("" if fetch_complete else stop_reason),
    })

@app.route('/api/live-queue', methods=['GET'])
def live_queue():
    """Lightweight endpoint to be polled every 5 seconds by the frontend"""
    user = sanitize_text(request.args.get('user',''))
    
    if not user or MOCK_MODE:
        return jsonify({"success": True, "tickets": []})
        
    tat = get_tenant_access_token()
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    # Fast query: Find where Assigned Member == User AND Status contains "In Progress"
    payload = {
        "page_size": 50,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": "Assigned Member",
                    "operator": "contains",
                    "value": [user]
                },
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
            payload["filter"]["conditions"][1]["field_name"] = "Status"
            resp = http_requests.post(search_url, headers=headers, json=payload, timeout=5)
            data = resp.json()
            
        if data.get("code") == 0:
            items = data.get("data", {}).get("items", [])
            tickets = []
            for item in items:
                # Fixed: Properly map the whole payload so the ticket modal renders all data.
                tickets.append(build_ticket_payload(item))
            return jsonify({"success": True, "tickets": tickets})
            
        return jsonify({"success": False, "error": data.get("msg")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/requests/agent-update', methods=['POST'])
def update_request_agent():
    """Allows the agent to write status updates back to Feishu"""
    user = sanitize_text(request.json.get('user', ''))
    record_id = sanitize_text(request.json.get('record_id', ''))
    fields = request.json.get('fields', {})
    
    if not user or not record_id or not fields:
        return jsonify({"success": False, "error": "Missing data"}), 400
        
    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    try:
        # Using PUT (or PATCH) updates only the specific record in Feishu
        resp = http_requests.put(url, headers=headers, json={"fields": fields}, timeout=10)
        data = resp.json()
        
        if data.get("code") == 0:
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
            audit.log(user, "UPDATE_TICKET", f"Record: {record_id}", ip=ip, severity="Info")
            return jsonify({"success": True})
            
        return jsonify({"success": False, "error": data.get("msg")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

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
    
    if MOCK_MODE:
        all_items = MockFeishuDB.generate_requests(300)
        fetch_complete, stop_reason = True, ""
    else:
        # Prevent timeout: Use the high-speed background cache and pull everything
        all_items, master_keys, fetch_complete, stop_reason, _ = get_requests_table_snapshot(from_dt=None)
    
    results = []
    target_types = ["agency creation", "agency applied already by acm or bd link ( follow-up )", "applied already", "follow-up"]
    
    for item in all_items:
        fields = item.get("fields", {})
        req_type = clean(get_field_local(fields, "Request Type", "Type"))
        
        if not any(t in req_type for t in target_types):
            continue
        
        raw_date = get_field_local(fields, "Submitted on Copy", "Submitted on", "Created Time")
        dt = parse_feishu_date(raw_date)
        
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
        agency_name = extract_field_text(get_field_local(fields, "Agency Name", "Name"))
        
        if f_region and f_region not in region: continue
        if f_agency_code and f_agency_code not in agency_code.lower(): continue
        if f_agency_name and f_agency_name not in agency_name.lower(): continue
        if f_acm and f_acm not in acm.lower(): continue
        
        manager_raw = extract_field_text(get_field_local(fields, "User ID", "Agency Manager ID", "Manager ID"))
        manager_name = extract_field_text(get_field_local(fields, "Applier real name", "Manager Name", "Agency Manager Name"))
        manager_display = manager_raw + (f" ({manager_name})" if manager_name else "")
        
        results.append({
            "record_id": item.get("record_id", ""),
            "region": region.upper() if region else "",
            "agency_code": agency_code,
            "agency_name": agency_name,
            "agency_manager": manager_display,
            "country": extract_field_text(get_field_local(fields, "Country")) or (region.upper() if region else ""),
            "create_time": dt.strftime("%Y-%m-%d %H:%M") if dt else extract_field_text(raw_date),
            "agency_members": extract_field_text(get_field_local(fields, "Agency Members", "Members", "Member Count")) or "0",
            "agency_type": extract_field_text(get_field_local(fields, "Agency Type", "Type of Agency")),
            "parent_agency": extract_field_text(get_field_local(fields, "Parent Agency", "Parent-Agency ID")),
            "sub_agency": extract_field_text(get_field_local(fields, "Sub Agency", "Sub-Agency")),
            "acm_name": acm.title() if acm else "",
            "status": extract_field_text(get_field_local(fields, "Status", "Request Status")),
            "_sort_ts": dt.timestamp() if dt else 0,
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
    if not perms.get("is_super_admin") and not any("analytics" in m for m in perms.get("modules",[])):
        return jsonify({"error":"Access denied"}), 403

    region_filter = region.lower() if region.lower() != "all" else "all"
    acm_filter    = acm.lower() if acm.lower() not in ("all","all acms") else "all"
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
    if not perms.get("is_super_admin") and not any("analytics" in m for m in perms.get("modules",[])):
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

    groups = []
    for label, from_dt, to_dt, acm_filter in groups_spec:
        raw = run_analytics(all_items, from_dt, to_dt, region_filter, acm_filter, type_filter, allowed_acms, allowed_regs)
        groups.append(_shape_compare_group(label, raw))

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
        "self_base_url_env": SELF_BASE_URL or None,
    })

# Maps Feishu field names -> the flat snake_case keys the Live Ticket Queue UI expects
# (same convention used in the frontend's LT_FIELD_MAP). Keeping this as one dict makes
# it easy to extend if new fields need to show up in the ticket workspace.
TICKET_FIELD_KEY_MAP = {
    "Region": "region", "User ID": "user_id", "Agency Code": "agency_code",
    "Agency Name": "agency_name", "Applier real name": "applier_real_name",
    "Otherapp Name": "otherapp_name", "Otherapp ID": "otherapp_id", "Country": "country",
    "Whatsapp Number": "whatsapp_number", "NID Number": "nid_number", "Agency Type": "agency_type",
    "Bd Code": "bd_code", "Acm Name (PK)": "acm", "Acm Name (IN)": "acm",
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

def build_ticket_payload(item):
    """Flattens a raw Feishu record into the shape the Live Ticket Queue frontend expects."""
    fields = item.get("fields", {})
    ticket = {"record_id": item.get("record_id")}
    for feishu_name, key in TICKET_FIELD_KEY_MAP.items():
        val = extract_field_text(fields.get(feishu_name))
        if val:
            ticket[key] = val
    for att_field in TICKET_ATTACHMENT_FIELDS:
        pics = extract_attachments(fields.get(att_field))
        if pics:
            ticket.setdefault("attachments", []).extend(pics)
    ticket["request_type"] = extract_field_text(get_field_local(fields, "Request Type", "Type"))
    ticket["assigned_member"] = extract_field_text(get_field_local(fields, "Assigned Member"))
    ticket["status"] = extract_field_text(get_field_local(fields, "Status"))
    ticket["reject_reason"] = extract_field_text(get_field_local(fields, "Reject Reason", "Rejection Reason"))
    ticket["audition_note"] = extract_field_text(get_field_local(fields, "Audition note", "Audition Note"))
    ticket["duplicated_check"] = extract_field_text(get_field_local(fields, "Duplicated Check"))
    ticket["create_way"] = extract_field_text(get_field_local(fields, "Create Way"))
    ticket["mentioned_person"] = extract_field_text(get_field_local(fields, "Mentioned Person"))
    ticket["chinese_note"] = extract_field_text(get_field_local(fields, "chinese Note"))
    ticket["type_of_action_approval"] = extract_field_text(get_field_local(fields, "Type of Action"))
    return ticket

@app.route('/api/attachments/proxy', methods=['GET'])
def proxy_attachment():
    """Streams a Feishu attachment back through our own server (adding the Bearer token)
    so the browser never needs the tenant token directly. `src` must be Feishu's own
    attachment URL exactly as returned in the record (already includes the required
    '?extra=...' permission token) - we only add auth, we don't rebuild the URL."""
    user = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    perms = get_user_permissions(email, user)
    if not perms.get("modules") and not perms.get("is_super_admin"):
        return jsonify({"error": "Access denied"}), 403

    src = request.args.get('src', '')
    if not src.startswith("https://open.feishu.cn/"):
        return jsonify({"error": "Invalid attachment source"}), 400
    try:
        tat = get_tenant_access_token()
        resp = http_requests.get(src, headers={"Authorization": f"Bearer {tat}"}, timeout=20)
        if resp.status_code != 200:
            return jsonify({"error": "Could not fetch attachment"}), 502
        return Response(resp.content, content_type=resp.headers.get("Content-Type", "application/octet-stream"))
    except Exception as e:
        logger.error("attachment_proxy_failed", error=str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/tickets/pull-assigned', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def pull_assigned_ticket():
    """Manual 'pull' alternative to the live push feed: looks up whatever's currently
    assigned to this agent with Request Status = In Progress, right now, on demand."""
    user = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("tickets" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403
    if not user:
        return jsonify({"error": "Missing user"}), 400

    if MOCK_MODE:
        return jsonify({"tickets": []})

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    user_clean = user.strip().lower()

    # Assigned Member is a Person-type field - Feishu's filter API needs an open_id to
    # filter it reliably, not a display name, so we can't filter on it server-side here.
    # But Request Status IS a plain SingleSelect field, which filters perfectly - and
    # "In Progress" narrows a 12k+ record table down to a small handful almost always,
    # so a single filtered call + a quick Python pass on that small set is fast (~1-3s)
    # instead of paginating the whole table.
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    payload = {
        "page_size": 500,
        "filter": {"conjunction": "and", "conditions": [
            {"field_name": "Request Status", "operator": "is", "value": ["In Progress"]}
        ]},
        "sort": [{"field_name": "Numbering", "desc": True}]
    }
    matches = []
    try:
        resp = http_requests.post(search_url, headers=headers, json=payload, timeout=15)
        data = resp.json()
        if data.get("code") == 0:
            for it in data.get("data", {}).get("items", []):
                f = it.get("fields", {})
                assigned = extract_field_text(get_field_local(f, "Assigned Member")).lower()
                if user_clean in assigned:
                    matches.append(build_ticket_payload(it))
        else:
            logger.warn("pull_assigned_filter_rejected", code=data.get("code"), msg=data.get("msg"))
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

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
