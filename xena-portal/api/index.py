"""
Xena Data Portal — High-Speed Hybrid Backend
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Combines the speed of v2.0 (Token Caching, Normalized Analytics, Session Re-use)
with the bulletproof parsing of v1.1 (Fuzzy Aliases, Deep JSON Extraction, Health Score).
Includes concurrent ThreadPoolExecutor for 2x faster Analytics processing.
"""

import os, time, re, json, hashlib, logging, urllib.parse, threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
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
# Hardcoded to the specific Audit Log table ID provided
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

# Universal Query — maps a frontend "search by" key to the real Feishu column name(s)
QUERY_FIELD_ALIASES = {
    "user_id":     ["User ID"],
    "numbering":   ["Numbering"],
    "otherapp_id": ["Otherapp ID", "Otherapp Name", "Other App ID"],
    "nid_number":  ["NID Number", "NID"],
    "bd_code":     ["Bd Code", "BD Code"],
}

MONTHLY_ALLOCATOR_LIMITS = {
    "trend card": 10,
    "traffic card": 10, 
}

# ──────────────────────────────────────────────────────────────────────────────
# TENANT ACCESS TOKEN CACHE
# ──────────────────────────────────────────────────────────────────────────────
_token_cache = {"token": None, "expires_at": 0, "lock": threading.Lock()}

def get_tenant_access_token():
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

def mask_email(email):
    if not email or "@" not in email: return email[:3] + "***" if email else ""
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
# 7.1  RATE LIMITER & INPUT SANITISATION
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# 4.1  AUDIT LOGGER
# ──────────────────────────────────────────────────────────────────────────────
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
        if AUDIT_TABLE_ID:
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

# ──────────────────────────────────────────────────────────────────────────────
# BATTLE-TESTED PARSERS 
# ──────────────────────────────────────────────────────────────────────────────
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

def parse_monthly_usage_tracker(raw_field):
    text = extract_field_text(raw_field)
    if not text: return {}
    usage = {}
    matches = re.findall(r'([A-Za-z][A-Za-z0-9 /\-]*?)\s*:\s*(\d+)', text)
    for item_name, qty in matches:
        usage[item_name.strip().lower()] = int(qty)
    return usage

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
    
    # EXACT MATCH CHECK for Super Admins
    if any(admin == name_clean for admin in ADMIN_USERS):
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin", "query", "export"], 
            "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]},
                            "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}}
        }

    if not email_clean and not name_clean: 
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

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
            
            # EXACT MATCH CHECK for standard users
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

# ──────────────────────────────────────────────────────────────────────────────
# HIGH-SPEED SESSION FETCHING (Using Fast GET Method)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_feishu_records(table_id, from_dt=None):
    tat = get_tenant_access_token()
    
    all_items = []
    seen_ids  = set()
    master_keys = set()
    fetch_complete = True
    stop_reason = ""
    consecutive_old_pages = 0

    session = http_requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records"

    page_token = None
    for _ in range(200):
        # Using GET params, avoiding InvalidFilter crashes 
        params = {"page_size": 500, "automatic_fields": "true"} 
        if table_id == REQUESTS_TABLE_ID:
            params["sort"] = '["Numbering DESC"]'
        
        if page_token: params["page_token"] = page_token
        
        try:
            resp = session.get(url, params=params, timeout=45) 
            if resp.status_code != 200:
                fetch_complete = False
                stop_reason = f"HTTP {resp.status_code}: {resp.text}"
                break
                
            data = resp.json()
            if data.get("code") != 0:
                fetch_complete = False
                stop_reason = f"Feishu Error Code {data.get('code')}: {data.get('msg')}"
                break
            
            block = data.get("data", {})
            items = block.get("items", [])
            if not items: break

            page_old_count = 0
            valid_dates_in_page = 0
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
            else:
                consecutive_old_pages = 0

            if consecutive_old_pages >= 3:
                stop_reason = "Safely reached pages with all older records."
                break

            page_token = block.get("page_token")
            if not page_token or not block.get("has_more", False):
                break

        except Exception as e:
            fetch_complete = False
            stop_reason = str(e)
            break

    return all_items, master_keys, fetch_complete, stop_reason

# ──────────────────────────────────────────────────────────────────────────────
# AGENCY SEARCH (target / points) DUAL ENGINE
# ──────────────────────────────────────────────────────────────────────────────
def fetch_agency_data(code, query_type="points", allowed_acms=None, allowed_regs=None):
    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    points_payload = {
        "filter": {
            "conjunction": "and",
            "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [code]}]
        }
    }
    
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    
    try:
        resp = http_requests.post(search_url, headers=headers, json=points_payload, timeout=30).json()
        if resp.get("code") != 0: return {"found": False, "error": f"Feishu API Error: {resp.get('msg')}"}
        all_records = resp.get("data", {}).get("items", [])
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

    history_points = []
    history_target = []
    privileges_claimed = defaultdict(int)
    usage_this_month = defaultdict(int)
    cm, cy = datetime.now().month, datetime.now().year
    
    try:
        hist_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
        hist_resp = http_requests.post(hist_url, headers=headers, json=points_payload, timeout=30).json()
        
        if hist_resp.get("code") == 0:
            for r in hist_resp.get("data", {}).get("items", []):
                hf = r.get("fields", {})
                h_date = parse_feishu_date(get_field_local(hf, "Submitted on Copy", "Submitted on", "Created Time"))
                
                if not h_date or h_date.month != cm or h_date.year != cy: continue

                req_type      = extract_field_text(get_field_local(hf, "Request Type", "Type")).strip()
                status_val    = extract_field_text(get_field_local(hf, "Status", "Request Status")).strip()
                req_type_lower = req_type.lower()
                s_lower = status_val.lower()

                target_type   = extract_field_text(get_field_local(hf, "Target Type")).strip()
                point_balance = extract_field_text(get_field_local(hf, "Point Balance")).strip()

                if "target" in req_type_lower:
                    privilege_val = extract_field_text(get_field_local(hf, "Agency Point Privilege", "Privilege", "Agency Privilege")).strip()
                    raw_counter = extract_field_text(get_field_local(hf, "Counter", "Qty")).strip()
                    qty = 1
                    if raw_counter:
                        m = re.search(r'\d+', str(raw_counter))
                        if m: qty = int(m.group())

                    history_target.append({
                        "date": h_date.strftime("%Y-%m-%d"),
                        "_dt": h_date,
                        "request_type": req_type,
                        "status": status_val,
                        "privilege": privilege_val,
                        "quantities_input": str(qty) 
                    })

                    if s_lower in ("done", "done ", "completed", "approved", "confirm") and privilege_val:
                        privileges_claimed[privilege_val] += qty

                else:
                    latest_usage  = extract_field_text(get_field_local(hf, "Latest Usage Tracker")).strip()
                    parsed_items = re.findall(r'🔹\s*(.*?):\s*(\d+)', latest_usage)
                    
                    if parsed_items:
                        privilege_val = " + ".join([f"{k.strip()} ({v})" for k, v in parsed_items])
                    else:
                        privilege_val = extract_field_text(get_field_local(hf, "Agency Point Privilege", "Privilege", "Agency Privilege")).strip()

                    history_points.append({
                        "date": h_date.strftime("%Y-%m-%d"),
                        "_dt": h_date,
                        "request_type": req_type,
                        "status": status_val,
                        "target_type": target_type,
                        "point_balance": point_balance,
                        "privilege": privilege_val,
                        "quantities_input": extract_field_text(get_field_local(hf, "Quantities Input")).strip()
                    })

                    if not ("reject" in s_lower or "fail" in s_lower or "decline" in s_lower):
                        for item_name, item_qty in parsed_items:
                            name_clean = item_name.strip().lower()
                            qty_int = int(item_qty)
                            
                            matched = False
                            for key in MONTHLY_ALLOCATOR_LIMITS.keys():
                                if key in name_clean:
                                    usage_this_month[key] += qty_int
                                    matched = True
                                    break
                            if not matched:
                                usage_this_month[name_clean] += qty_int

            history_points.sort(key=lambda x: (x["_dt"] is None, x["_dt"]), reverse=True)
            history_target.sort(key=lambda x: (x["_dt"] is None, x["_dt"]), reverse=True)
            for h in history_points: h.pop("_dt", None)
            for h in history_target: h.pop("_dt", None)
    except Exception as e:
        logger.error("Points history fetch failed", agency=code, error=str(e))

    allocator_status = compute_allocator_status(usage_this_month)
    
    if query_type == "points":
        total_pts = parse_float_safe(extract_field_text(get_field_local(first, '# Total Points', 'Total Points', 'Total', 'Total points')))
        used_pts  = parse_float_safe(extract_field_text(get_field_local(first, 'Used Points', 'Used', 'Used points')))
        balance   = parse_float_safe(extract_field_text(get_field_local(first, 'Point Balance', 'Balance', 'Point balance')))
        
        if balance == 0 and total_pts > 0:
            balance = total_pts - used_pts

        health_score = 100
        health_status = "Healthy"
        if total_pts > 0:
            utilization = used_pts / total_pts
            if utilization > 0.90: 
                health_score = 40
                health_status = "Critical"
            elif utilization > 0.70: 
                health_score = 70
                health_status = "At Risk"
            else: 
                health_score = 95
                health_status = "Healthy"
        else: 
            health_score = 0
            health_status = "Inactive"

        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw.upper(), "acm": acm_raw.title(),
            "total_points": total_pts, "used_points": used_pts,
            "point_balance": balance, "health_score": health_score,
            "health_status": health_status,
            "history": history_points, 
            "monthly_usage": {}, 
            "allocator_status": allocator_status,
            "requests": [r.get("fields", {}) for r in all_records]
        }
    else:  
        raw_base_pts = parse_float_safe(extract_field_text(get_field_local(first, "Base Points", "base_points")))
        base_pts_coins = raw_base_pts * COINS_MULTIPLIER

        return {
            "found": True, "agency_code": code, "agency_name": agency_name,
            "region": region_raw.upper(), "acm": acm_raw.title(),
            "base_points": base_pts_coins, "health_score": 100, "health_status": "Healthy",
            "privileges_claimed": dict(privileges_claimed),  
            "history": history_target, 
            "requests": [r.get("fields", {}) for r in all_records]
        }

# ──────────────────────────────────────────────────────────────────────────────
# NORMALISED FIELD MAP (ANALYTICS LOOP OPTIMIZATION)
# ──────────────────────────────────────────────────────────────────────────────
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
    raw_cl_rsn = get_field_local(fields,"Closing Reason","Closing Agencies Reason","PK Closing Agencies Reason")
    raw_o_app  = get_field_local(fields,"Otherapp Name","Other App Name","Other Apps")
    raw_rj_rsn = get_field_local(fields,"Reject Reason","Rejection Reason","Agencies Rejection Reason","PK Agencies Rejection reason")
    raw_cr_way = get_field_local(fields,"Create Way","Creation Type","Agency Creation Type","PK Agencies Creation Type")

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
        "fetch_complete": True, "stop_reason": ""
    }

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
            if fm["acm_pk"] in PK_ACMS or fm["acm_fb"] in PK_ACMS:
                region = "pk"
            elif fm["acm_in"] in IN_ACMS or fm["acm_fb"] in IN_ACMS:
                region = "in"

        if region_filter_c != "all" and region != region_filter_c: continue
        if "all" not in allowed_regs_set and region not in allowed_regs_set: continue

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
# FLASK APP & BACKGROUND CACHE LOOP
# ──────────────────────────────────────────────────────────────────────────────
BACKGROUND_SYNC_INTERVAL = 180   # re-sync every 3 minutes
BACKGROUND_SYNC_MAX_AGE  = 600   # snapshot considered "fresh enough" for 10 minutes

_bg_sync = {
    "requests_items": [], "requests_keys": set(),
    "updated_at": 0, "fetch_complete": True, "stop_reason": "",
    "syncing": False,
}
_bg_sync_lock = threading.Lock()
_bg_thread_started = False
_bg_thread_lock = threading.Lock()

def _background_sync_requests_table():
    with _bg_sync_lock:
        if _bg_sync["syncing"]: return
        _bg_sync["syncing"] = True
    try:
        items, keys, complete, reason = fetch_feishu_records(REQUESTS_TABLE_ID)
        with _bg_sync_lock:
            _bg_sync["requests_items"]  = items
            _bg_sync["requests_keys"]   = keys
            _bg_sync["updated_at"]      = time.time()
            _bg_sync["fetch_complete"]  = complete
            _bg_sync["stop_reason"]     = reason
        logger.info("background_sync_complete", table="grand_table", count=len(items), complete=complete)
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
    """Serves the Grand Table from the warm in-memory snapshot when it's fresh enough."""
    ensure_background_sync_started()
    with _bg_sync_lock:
        items, keys = _bg_sync["requests_items"], _bg_sync["requests_keys"]
        updated_at   = _bg_sync["updated_at"]
        complete     = _bg_sync["fetch_complete"]
        reason       = _bg_sync["stop_reason"]
    if items and (time.time() - updated_at) < BACKGROUND_SYNC_MAX_AGE:
        return items, keys, complete, reason, True
    items, keys, complete, reason = fetch_feishu_records(REQUESTS_TABLE_ID, from_dt=from_dt)
    return items, keys, complete, reason, False

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return send_file(os.path.join(root_dir, 'index.html'))

@app.route('/api/login', methods=['GET'])
def login():
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

        uat = (token_resp.get("data") or {}).get("access_token")
        if not uat: uat = token_resp.get("access_token")

        if not uat:
            err = token_resp.get("msg") or token_resp.get("error_description") or "Token exchange failed"
            return redirect("/?auth_error=" + urllib.parse.quote(f"Login failed: {err}", safe=''))

        info_resp  = http_requests.get(
            "https://open.feishu.cn/open-apis/authen/v1/user_info",
            headers={"Authorization": f"Bearer {uat}"}, timeout=15
        ).json()
        
        user_data  = info_resp.get("data", {})
        lark_name  = user_data.get("name", "Unknown User")
        lark_email = user_data.get("email") or user_data.get("enterprise_email") or ""

        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
        audit.log(lark_name, "LOGIN", mask_email(lark_email), ip=ip)
        
        return redirect(f"/?user={urllib.parse.quote(lark_name, safe='')}&email={urllib.parse.quote(lark_email, safe='')}&uat={urllib.parse.quote(uat, safe='')}")

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
    
    code   = sanitize_agency_code(req_data.get('code',''))
    user   = sanitize_text(req_data.get('user',''))
    email  = sanitize_text(req_data.get('email',''))
    uat    = sanitize_text(req_data.get('uat',''), max_length=512)
    qtype  = req_data.get('type','points')
    nocache = req_data.get('nocache', '0') in ['1', 'true', True]
    
    if qtype not in ('points','target'): qtype = 'points'

    if not code: return jsonify({"found":False,"error":"Invalid or missing agency code."}), 400

    perms = get_user_permissions(email, user)
    
    # Sub-Module Check Support
    if not perms.get("is_super_admin") and not any(qtype in m for m in perms.get("modules", [])):
        return jsonify({"found": False, "error": f"Access Denied: You do not have permission to view {qtype.title()}."}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get(qtype,["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get(qtype,["all"])

    cache_key = cache_make_key("search", code, qtype)
    
    if not nocache:
        cached = cache_get(cache_key)
        if cached: return jsonify(cached)
        
    data = fetch_agency_data(code, qtype, allowed_acms, allowed_regs)
    if data.get("found"):
        cache_set(cache_key, data, ttl=180)
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

        if existing_record_id:
            res = http_requests.put(f"{base_url}/{existing_record_id}", headers=headers, json=payload, timeout=15).json()
        else:
            res = http_requests.post(base_url, headers=headers, json=payload, timeout=15).json()

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
        
    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{AUDIT_TABLE_ID}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    payload = {
        "page_size": min(int(request.args.get('limit','100')), 500)
    }
    try:
        res = http_requests.post(url, headers=headers, json=payload, timeout=10).json()
        if res.get("code") != 0:
            raise Exception(res.get("msg", "Feishu API Error"))
        
        logs = []
        for item in res.get("data", {}).get("items", []):
            f = item.get("fields", {})
            ts_val = f.get("Timestamp")
            if isinstance(ts_val, (int, float)):
                dt_str = datetime.fromtimestamp(ts_val/1000.0).isoformat()
            else:
                dt_str = str(ts_val)
                
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

@app.route('/api/points/records', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_records():
    user   = sanitize_text(request.args.get('user',''))
    email  = sanitize_text(request.args.get('email',''))
    perms  = get_user_permissions(email, user)

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

    raw_points_cache_key = "points_table_raw_items"
    cached_bundle = cache_get(raw_points_cache_key)

    if cached_bundle is not None:
        all_items      = cached_bundle["items"]
        fetch_complete = cached_bundle["fetch_complete"]
        stop_reason    = cached_bundle["stop_reason"]
    else:
        all_items, _, fetch_complete, stop_reason = fetch_feishu_records(POINTS_TABLE_ID)
        if not fetch_complete and not all_items:
            return jsonify({"error": f"Feishu sync failed: {stop_reason}"}), 502

        cache_set(raw_points_cache_key,
                  {"items": all_items, "fetch_complete": fetch_complete, "stop_reason": stop_reason},
                  ttl=300)

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

        if balance == 0 and total_pts > 0:
            balance = total_pts - used_pts

        health = 100
        if total_pts > 0:
            utilization = used_pts / total_pts
            if utilization > 0.90: health = 40
            elif utilization > 0.70: health = 70
            else: health = 95
        else:
            health = 0

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

    # Allow bypassing pagination if an export is requested
    is_export = request.args.get('export', 'false').lower() == 'true'
    if is_export:
        if not perms.get("is_super_admin") and not any("export" in m for m in perms.get("modules",[])):
            audit.log(user.lower(), "UNAUTHORIZED_EXPORT", "Point Records", ip=request.headers.get("X-Forwarded-For",""), severity="Critical")
            return jsonify({"error":"Export access denied. You lack the necessary permissions."}), 403
        
        audit.log(user.lower(), "EXPORT_DATA", f"Point Records ({total_count} rows)", ip=request.headers.get("X-Forwarded-For",""), severity="Info")
        return jsonify({
            "records": filtered[:5000], "total": total_count, "page": 1, "page_size": total_count,
            "total_pages": 1,
            "totals": {"total_points": total_pts_sum, "used_points": used_pts_sum, "point_balance": balance_sum},
            "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason)
        })

    start = (page - 1) * page_size
    end   = start + page_size
    page_records = filtered[start:end]

    return jsonify({
        "records": page_records, "total": total_count, "page": page, "page_size": page_size,
        "total_pages": max(1, -(-total_count // page_size)),
        "totals": {"total_points": total_pts_sum, "used_points": used_pts_sum, "point_balance": balance_sum},
        "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason)
    })

@app.route('/api/audit/log', methods=['POST'])
def client_audit_log():
    data = request.json or {}
    user = sanitize_text(data.get('user', ''))
    email = sanitize_text(data.get('email', ''))
    action = sanitize_text(data.get('action', ''))
    target = sanitize_text(data.get('target', ''))
    severity = sanitize_text(data.get('severity', 'Info'))
    
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not perms.get("modules"):
        return jsonify({"error":"Unauthorized"}), 403
        
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
    _background_sync_requests_table()
    with _bg_sync_lock:
        count, updated_at, complete = len(_bg_sync["requests_items"]), _bg_sync["updated_at"], _bg_sync["fetch_complete"]
        
    audit.log(user.lower(), "MANUAL_SYNC_REFRESH", "grand_table", ip=request.headers.get("X-Forwarded-For",""), severity="Info")
    return jsonify({"success": True, "record_count": count, "updated_at": updated_at, "fetch_complete": complete})

@app.route('/api/points/search', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_search():
    if request.args.get('q') and not request.args.get('search'):
        args = dict(request.args)
        args['search'] = args.pop('q')
        request.environ['QUERY_STRING'] = urllib.parse.urlencode(args, doseq=True)
    return points_records()

def _norm_query_val(v):
    return re.sub(r'\s+', '', str(v).strip().lower())

@app.route('/api/query', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def query_records():
    user  = sanitize_text(request.args.get('user',''))
    email = sanitize_text(request.args.get('email',''))
    field = sanitize_text(request.args.get('field','')).strip().lower()
    value = sanitize_text(request.args.get('value',''), 200).strip()

    if field not in QUERY_FIELD_ALIASES:
        return jsonify({"error": "Invalid search field."}), 400
    if not value:
        return jsonify({"error": "Please enter a value to search."}), 400

    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("query" in m for m in perms.get("modules", [])):
        return jsonify({"error": "Access denied"}), 403

    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("query",["all"])
    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("query",["all"])
    allowed_acms_set = set(a.lower() for a in allowed_acms) if allowed_acms else {"all"}
    allowed_regs_set = set(r.lower() for r in allowed_regs) if allowed_regs else {"all"}

    tat = get_tenant_access_token()
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    all_items = []
    fetch_complete = False
    stop_reason = ""
    success = False

    aliases = QUERY_FIELD_ALIASES[field]
    
    for alias in aliases:
        for op in ["contains", "is", "="]:
            val_array = [value]
            
            if op == "=":
                if value.isdigit(): val_array = [int(value)]
                else: continue

            payload = {
                "page_size": 500,
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": alias, "operator": op, "value": val_array}]
                }
            }
            
            try:
                resp = http_requests.post(search_url, headers=headers, json=payload, timeout=10)
                data = resp.json()
                if data.get("code") == 0:
                    all_items = data.get("data", {}).get("items", [])
                    success = True
                    fetch_complete = True
                    break
                elif data.get("code") not in (1254011, 1254402, 1254010): 
                    stop_reason = data.get("msg")
                    if data.get("code") == 99991663: break 
            except Exception as e:
                stop_reason = str(e)
        
        if success: break

    if not success:
        return jsonify({"error": f"Data fetch failed: Feishu API Error: {stop_reason or 'Invalid Filter for this column type.'}"}), 502

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

        if "all" not in allowed_acms_set and acm.lower().strip() not in allowed_acms_set:
            continue
        if "all" not in allowed_regs_set and region not in allowed_regs_set:
            continue

        submitted_raw = get_field_local(fields, "Submitted on Copy", "Submitted on", "Created Time")
        submitted_dt  = parse_feishu_date(submitted_raw)

        results.append({
            "numbering":        extract_field_text(get_field_local(fields, "Numbering")),
            "request_type":     extract_field_text(get_field_local(fields, "Request Type", "Type")),
            "submitted_on":     submitted_dt.strftime("%Y-%m-%d") if submitted_dt else extract_field_text(submitted_raw),
            "respondents":      extract_field_text(get_field_local(fields, "Respondents")),
            "user_id":          extract_field_text(get_field_local(fields, "User ID")),
            "otherapp_id":      extract_field_text(get_field_local(fields, "Otherapp ID", "Otherapp Name", "Other App Name")),
            "acm":              acm.title() if acm else "",
            "region":           region.upper() if region else "",
            "bd_code":          extract_field_text(get_field_local(fields, "Bd Code", "BD Code")),
            "status":           extract_field_text(get_field_local(fields, "Status", "Request Status")),
            "reject_reason":    extract_field_text(get_field_local(fields, "Reject Reason", "Rejection Reason")),
            "audition_note":    extract_field_text(get_field_local(fields, "Audition note", "Audition Note")),
            "duplicated_check": extract_field_text(get_field_local(fields, "Duplicated Check")),
            "_sort_ts": submitted_dt.timestamp() if submitted_dt else 0,
        })

    results.sort(key=lambda r: r["_sort_ts"], reverse=True)
    for r in results:
        r.pop("_sort_ts", None)

    audit.log(user.lower(), "QUERY_SEARCH", f"{field}={value}", ip=request.headers.get("X-Forwarded-For",""), severity="Info")

    return jsonify({
        "results": results, "count": len(results),
        "field": field, "value": value,
        "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason),
        "served_from_background_cache": False
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
    nocache  = body.get('nocache',False)

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

    cache_key = cache_make_key("analytics", json.dumps({
        "region": region_filter, "acm": acm_filter, "type": type_filter, "from": from_s, "to": to_s
    }, sort_keys=True), email.lower(), user.lower())

    now = datetime.utcnow()
    ttl = CACHE_TTL_REALTIME if (not from_dt) or ((now - from_dt).days <= 60) else CACHE_TTL_HISTORICAL

    if not nocache:
        cached = cache_get(cache_key)
        if cached:
            cached["cache_hit"] = True
            return jsonify(cached)

    oldest_dt = from_dt
    if cmp_from and cmp_to:
        try:
            cmp_from_dt = datetime.strptime(cmp_from, "%Y-%m-%d")
            cmp_to_dt   = datetime.strptime(cmp_to,   "%Y-%m-%d") + timedelta(days=1)
            if cmp_from_dt and (not oldest_dt or cmp_from_dt < oldest_dt): oldest_dt = cmp_from_dt
        except ValueError: pass

    all_items, master_keys, fetch_complete, stop_reason, from_bg_cache = get_requests_table_snapshot(from_dt=oldest_dt)

    if not fetch_complete and not all_items:
        return jsonify({"error": f"Data fetch failed: {stop_reason}"}), 502

    stats = run_analytics(all_items, from_dt, to_dt, region_filter, acm_filter, type_filter, allowed_acms, allowed_regs)
    stats["fetch_complete"] = fetch_complete
    stats["stop_reason"]    = stop_reason
    stats["feishu_keys"]    = sorted(list(master_keys))
    stats["served_from_background_cache"] = from_bg_cache

    if cmp_from and cmp_to:
        try:
            cmp_stats = run_analytics(all_items, cmp_from_dt, cmp_to_dt, region_filter, acm_filter, type_filter, allowed_acms, allowed_regs)
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
    logger.info("analytics_complete", region=region_filter, acm=acm_filter, rows=stats["scanned_rows"], duration_ms=duration_ms)
    stats["duration_ms"] = duration_ms
    stats["cache_hit"]   = False

    cache_set(cache_key, stats, ttl=ttl)
    return jsonify(stats)

@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a == admin_name for a in ADMIN_USERS)
    if not is_authorized: return jsonify({"error":"Unauthorized"}), 403
    cache_invalidate()
    audit.log(admin_name, "CACHE_CLEARED", "all", ip=request.headers.get("X-Forwarded-For",""), severity="Warning")
    return jsonify({"success":True,"message":"Cache cleared."})

@app.route('/api/health', methods=['GET'])
def health():
    with _bg_sync_lock:
        bg_info = {
            "record_count": len(_bg_sync["requests_items"]),
            "age_seconds": (time.time() - _bg_sync["updated_at"]) if _bg_sync["updated_at"] else None,
            "syncing": _bg_sync["syncing"],
        }
    return jsonify({
        "status": "ok", "ts": datetime.utcnow().isoformat(),
        "cache_entries": len(_cache), "audit_entries": len(audit._queue),
        "token_cached": _token_cache["token"] is not None,
        "token_expires_in_s": max(0, int(_token_cache["expires_at"] - time.time())),
        "background_sync": bg_info
    })

# Start the background sync thread right at boot time
ensure_background_sync_started()

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
