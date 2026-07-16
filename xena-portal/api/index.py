"""
Xena Data Portal — Improved Backend
Improvements implemented:
  2.1  In-memory response caching with TTL
  7.1  Rate limiting + input sanitization
  4.1  Audit logging to Feishu Bitable
  1.1  Period-over-period comparison (compare_from/compare_to)
  2.4  Selective field fetching hints
  4.2  Session security improvements (POST body for analytics)
  5.1  Agency health score calculation
  6.4  Service-oriented helpers
  6.5  Centralized configuration constants
  7.2  Structured JSON logging
  7.3  PII masking in logs
"""

import os, time, re, json, hashlib, logging, urllib.parse, threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import wraps
from flask import Flask, request, jsonify, send_file, redirect
import requests

# ──────────────────────────────────────────────────────────────────────────────
# 6.5  CENTRALIZED CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
APP_ID       = os.environ.get("LARK_APP_ID")
APP_SECRET   = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://xena-portal-v1-1.vercel.app/api/callback")

BASE_ID           = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID   = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID   = "tbl3wweYCpmDmDSx"
AUDIT_TABLE_ID    = os.environ.get("AUDIT_TABLE_ID", "")   # Optional: create in Feishu

ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954']

# ACM lists for region auto-detection
PK_ACMS = ["nabeel","hasseb","haseeb","enzo","farooq","mubeen","cruz","ehtisham",
           "usama","sehar ch","hamza malik","zohaib","eagle","leo","berlin"]
IN_ACMS  = ["holy","vihan","shivam","ravikant","ansh","rocky","bella"]

# Analytics fields to fetch (2.4 selective fetching)
ANALYTICS_FIELDS = [
    "Request Type","Status","Region","Acm Name (PK)","Acm Name (IN)","Acm",
    "Submitted on","Submitted on Copy","Agency Type","Reject Reason",
    "Rejection Reason","Agencies Rejection Reason","PK Agencies Rejection reason",
    "Closing Reason","Closing Agencies Reason","PK Closing Agencies Reason",
    "Otherapp Name","Other App Name","Create Way","Creation Type",
    "Agency Creation Type","PK Agencies Creation Type","Numbering"
]

# 2.1  Cache TTL constants (seconds)
CACHE_TTL_REALTIME   = 300    # 5 min for recent data
CACHE_TTL_HISTORICAL = 3600   # 1 hr for older ranges

# 7.1  Rate limit: (requests, window_seconds)
RATE_LIMIT_SEARCH    = (20, 60)   # 20 req/min per IP
RATE_LIMIT_ANALYTICS = (4, 60)    # 4 req/min per IP

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
    parts = name.split()
    return parts[0][:2] + "*** " + (parts[-1][0] + "***" if len(parts) > 1 else "")

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
        keys = [k for k in list(_cache.keys()) if k.startswith(prefix)]
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
# 7.1  INPUT SANITIZATION
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
    # Remove control characters
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
        # Async write to Feishu if table configured
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
                "Actor":   entry["actor"],
                "Action":  entry["action"],
                "Target":  entry["target"],
                "Details": entry["details"],
                "IP":      entry["ip"],
                "Timestamp": entry["ts"],
            }}
            requests.post(url, headers=hdrs, json=payload, timeout=8)
        except Exception as e:
            logger.error("audit_write_failed", error=str(e))

    def recent(self, limit=100):
        with self._lock:
            return list(reversed(self._queue[-limit:]))

audit = AuditLogger()

# ──────────────────────────────────────────────────────────────────────────────
# FEISHU HELPERS
# ──────────────────────────────────────────────────────────────────────────────
_tat_cache = {"token": None, "expires": 0}
_tat_lock  = threading.Lock()

def get_tenant_access_token():
    with _tat_lock:
        if _tat_cache["token"] and time.time() < _tat_cache["expires"]:
            return _tat_cache["token"]
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10).json()
    tok = resp.get("tenant_access_token", "")
    with _tat_lock:
        _tat_cache["token"]   = tok
        _tat_cache["expires"] = time.time() + resp.get("expire", 7000) - 60
    return tok

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
        clean = date_str[:10].replace('/','−').replace('.','-')
        return datetime.strptime(clean, "%Y-%m-%d")
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

    cache_key = cache_make_key("perms", email_clean, name_clean)
    cached = cache_get(cache_key)
    if cached: return cached

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    headers = {"Authorization":f"Bearer {tat}","Content-Type":"application/json"}
    try:
        res = requests.get(url, headers=headers, params={"page_size":500}, timeout=10).json()
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
                regions_raw = extract_field_text(get_field_local(fields,"Regions"))
                # 4.4  Check expires_at
                expires_raw = fields.get("ExpiresAt","")
                if expires_raw:
                    exp_dt = parse_feishu_date(expires_raw)
                    if exp_dt and exp_dt < datetime.now():
                        result = {"is_super_admin":False,"modules":[],"permissions":{"acms":{},"regions":{}}}
                        cache_set(cache_key, result, ttl=60)
                        return result
                modules  = [m.strip().lower() for m in modules_raw.split(",") if m.strip()]
                is_admin = "admin" in modules
                result = {"is_super_admin":is_admin,"modules":modules,
                          "permissions":{"acms":parse_granular_string(acms_raw),
                                         "regions":parse_granular_string(regions_raw)}}
                cache_set(cache_key, result, ttl=120)
                return result
        result = {"is_super_admin":False,"modules":[],"permissions":{"acms":{},"regions":{}}}
        cache_set(cache_key, result, ttl=60)
        return result
    except Exception as e:
        logger.error("auth_error", error=str(e))
        return {"is_super_admin":False,"modules":[],"permissions":{"acms":{},"regions":{}}}

# ──────────────────────────────────────────────────────────────────────────────
# 5.1  AGENCY HEALTH SCORE
# ──────────────────────────────────────────────────────────────────────────────
def calculate_health_score(point_balance, total_points, used_points, requests_this_month):
    score = 100
    # Balance ratio (30% weight)
    if total_points > 0:
        balance_ratio = point_balance / total_points
        if balance_ratio < 0.1:   score -= 30
        elif balance_ratio < 0.3: score -= 15
        elif balance_ratio < 0.5: score -= 5
    else:
        score -= 20  # No points at all
    # Activity (25% weight) — any request this month is good
    if not requests_this_month:
        score -= 25
    elif len(requests_this_month) < 2:
        score -= 10
    # Rejection rate (25% weight)
    if requests_this_month:
        total_reqs = len(requests_this_month)
        rejected   = sum(1 for r in requests_this_month
                         if "reject" in extract_field_text(r.get("Status","")).lower())
        rej_rate   = rejected / total_reqs if total_reqs > 0 else 0
        if rej_rate > 0.5:   score -= 25
        elif rej_rate > 0.3: score -= 15
        elif rej_rate > 0.1: score -= 5
    # Usage ratio (20% weight)
    if total_points > 0:
        usage_ratio = used_points / total_points
        if usage_ratio > 0.9: score -= 20  # Over-used
    score = max(0, min(100, score))
    if score >= 75:  status = "Healthy"
    elif score >= 50: status = "At Risk"
    else:            status = "Critical"
    return {"score": score, "status": status}

# ──────────────────────────────────────────────────────────────────────────────
# ANALYTICS AGGREGATION HELPER  (shared between current and compare periods)
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_and_aggregate(session, from_dt, to_dt, region_filter, acm_filter,
                          type_filter, allowed_regs, allowed_acms):
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records"
    all_items = []; seen_ids = set(); master_keys = set()
    page_token = ""; fetch_complete = True; stop_reason = None
    consecutive_old_pages = 0

    for page_num in range(150):
        params = {"page_size":500,"automatic_fields":"true",'sort':'["Numbering DESC"]'}
        if page_token: params["page_token"] = page_token
        try:
            res = session.get(base_url, params=params, timeout=12)
            if res.status_code != 200:
                fetch_complete = False; stop_reason = f"HTTP {res.status_code}"; break
            res_json = res.json()
            if res_json.get("code") != 0:
                fetch_complete = False; stop_reason = res_json.get("msg"); break
            data_block = res_json.get("data",{})
            items = data_block.get("items",[])
            if not items: break
            page_old_count = 0; valid_dates_in_page = 0
            for item in items:
                rid = item.get("record_id")
                if rid and rid not in seen_ids:
                    seen_ids.add(rid); all_items.append(item)
                    master_keys.update(item.get('fields',{}).keys())
                    raw_date = get_field_local(item.get('fields',{}),
                                               'Submitted on Copy','Submitted on','Created Time')
                    record_dt = parse_feishu_date(raw_date)
                    if record_dt:
                        valid_dates_in_page += 1
                        if from_dt and record_dt < (from_dt - timedelta(days=1)):
                            page_old_count += 1
            if valid_dates_in_page > 0 and page_old_count == valid_dates_in_page:
                consecutive_old_pages += 1
            else:
                consecutive_old_pages = 0
            if consecutive_old_pages >= 3: stop_reason = "Reached old records."; break
            page_token = data_block.get("page_token")
            if not page_token or not data_block.get("has_more",False): break
        except Exception as e:
            fetch_complete = False; stop_reason = str(e); break

    # Aggregate
    stats = {
        "kpis": {"creations":0,"bds":0,"closings":0},
        "creation_status": {"Done":0,"Rejected":0,"Under Investigation":0},
        "bd_status":       {"Done":0,"Rejected":0,"Under Investigation":0},
        "closing_status":  {"Done":0,"Rejected":0,"Under Investigation":0},
        "acm_performance":{}, "creation_types":{}, "agency_types":{},
        "other_apps":{}, "reject_reasons":{}, "closing_reasons_pie":{},
        "acm_closing_reasons":{},
        "daily_trend_creation":{}, "daily_trend_bd":{}, "daily_trend_closing":{},
        "other_request_types":{}, "scanned_rows":len(all_items),
        "fetch_complete":fetch_complete,"stop_reason":stop_reason,
        "feishu_keys":sorted(list(master_keys))
    }

    if from_dt and to_dt:
        cur = from_dt
        while cur < to_dt:
            ds = cur.strftime("%Y-%m-%d")
            stats["daily_trend_creation"][ds] = 0
            stats["daily_trend_bd"][ds]        = 0
            stats["daily_trend_closing"][ds]   = 0
            cur += timedelta(days=1)

    for item in all_items:
        fields = item.get('fields',{})
        record_dt = parse_feishu_date(get_field_local(fields,'Submitted on','Submitted on Copy','Created Time'))
        if from_dt or to_dt:
            if not record_dt or (from_dt and record_dt < from_dt) or (to_dt and record_dt >= to_dt):
                continue

        region    = clean(get_field_local(fields,'Region','Agency Region'))
        acm_pk    = clean(get_field_local(fields,'Acm Name (PK)'))
        acm_in    = clean(get_field_local(fields,'Acm Name (IN)'))
        acm_fallback = clean(get_field_local(fields,'Acm','Assigned Member'))

        if region in ('','none'):
            if acm_pk in PK_ACMS or acm_fallback in PK_ACMS: region = 'pk'

        if region_filter != 'all' and region != region_filter: continue

        req_type      = clean(get_field_local(fields,'Request Type','Request type','Type','Category'))
        status        = clean(get_field_local(fields,'Status','Request Status','Agency Status','State'))
        agency_type   = clean(get_field_local(fields,'Agency Type','Type of Agency'))
        closing_reason= clean(get_field_local(fields,'Closing Reason','Closing Agencies Reason','PK Closing Agencies Reason'))
        other_app     = clean(get_field_local(fields,'Otherapp Name','Other App Name','Other Apps'))

        is_done     = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        acm = acm_in if region == "in" else acm_pk
        if not acm: acm = acm_fallback

        if "all" not in allowed_acms and acm.lower().strip() not in allowed_acms: continue
        if acm_filter != 'all' and acm_filter != acm: continue

        agency_type_title = agency_type.title() if agency_type else "Unknown"
        if type_filter != 'all' and type_filter != agency_type: continue

        is_bd_kpi      = "bd creation" in req_type
        is_closing_kpi = "closing agency" in req_type
        is_creation_kpi= any(p in req_type for p in [
            "agency creation","agency applied already by acm or bd link ( follow-up )",
            "agency applied already","follow-up","follow up"])

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
            raw_ct = get_field_local(fields,'Create Way','Creation Type','Agency Creation Type','PK Agencies Creation Type')
            for ct in extract_field_list(raw_ct):
                if ct:
                    ct_title = ct.title()
                    stats["creation_types"][ct_title] = stats["creation_types"].get(ct_title,0)+1
            if is_rejected:
                raw_rr = get_field_local(fields,'Reject Reason','Rejection Reason','Agencies Rejection Reason','PK Agencies Rejection reason')
                for rr in extract_field_list(raw_rr):
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
    safe_redirect = urllib.parse.quote(REDIRECT_URI)
    feishu_url = f"https://open.feishu.cn/open-apis/authen/v1/index?app_id={APP_ID}&redirect_uri={safe_redirect}"
    return redirect(feishu_url)

@app.route('/api/callback', methods=['GET'])
def callback():
    code = request.args.get('code')
    if not code: return "SSO Authorization Failed.", 400
    tat = get_tenant_access_token()
    token_url = "https://open.feishu.cn/open-apis/authen/v1/oidc/access_token"
    headers = {"Authorization":f"Bearer {tat}","Content-Type":"application/json"}
    token_resp = requests.post(token_url, headers=headers,
                               json={"grant_type":"authorization_code","code":code}, timeout=10).json()
    uat = token_resp.get("data",{}).get("access_token")
    if not uat: return "SSO Error: Could not verify user token.", 500
    info_resp = requests.get("https://open.feishu.cn/open-apis/authen/v1/user_info",
                             headers={"Authorization":f"Bearer {uat}"}, timeout=10).json()
    data = info_resp.get("data",{})
    lark_name  = data.get("name","Unknown User")
    lark_email = data.get("email") or data.get("enterprise_email") or ""
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
        res   = requests.get(base_url, headers=headers, params={"page_size":500}).json()
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
        data = request.json
        email_to_check = sanitize_text(data.get("email",""))
        acms_formatted = f"target={data.get('acms',{}).get('target','all')};points={data.get('acms',{}).get('points','all')};analytics={data.get('acms',{}).get('analytics','all')}"
        regs_formatted = f"target={data.get('regions',{}).get('target','all')};points={data.get('regions',{}).get('points','all')};analytics={data.get('regions',{}).get('analytics','all')}"
        # Handle optional expires_at
        expires_at = sanitize_text(data.get("expires_at",""))
        payload_fields = {"Email":email_to_check,"Modules":data.get("modules",""),
                          "ACMs":acms_formatted,"Regions":regs_formatted}
        if expires_at:
            payload_fields["ExpiresAt"] = expires_at
        payload = {"fields": payload_fields}
        # Check for existing record
        res_all = requests.get(base_url, headers=headers, params={"page_size":500}).json()
        existing_id = None
        for item in res_all.get("data",{}).get("items",[]):
            db_email  = extract_field_text(item.get("fields",{}).get("Email","")).lower().strip()
            db_person = extract_field_text(item.get("fields",{}).get("Person","")).lower().strip()
            tgt = email_to_check.lower().strip()
            if tgt and (tgt == db_email or tgt == db_person):
                existing_id = item["record_id"]; break
        if existing_id:
            res = requests.put(f"{base_url}/{existing_id}", headers=headers, json=payload).json()
            action = "AGENT_EDITED"
        else:
            res = requests.post(base_url, headers=headers, json=payload).json()
            action = "AGENT_ADDED"
        if res.get("code") != 0:
            return jsonify({"success":False,"error":res.get("msg")}), 400
        cache_invalidate()  # Invalidate perms cache
        audit.log(admin_name, action, email_to_check, ip=ip)
        return jsonify({"success":True})

    elif request.method == 'DELETE':
        record_id = sanitize_text(request.args.get('id',''))
        res = requests.delete(f"{base_url}/{record_id}", headers=headers).json()
        cache_invalidate()
        audit.log(admin_name, "AGENT_DELETED", record_id, ip=ip)
        return jsonify({"success": res.get("code") == 0})

@app.route('/api/admin/audit-logs', methods=['GET'])
def get_audit_logs():
    """4.1  Return recent audit log entries (in-memory)."""
    admin_name = sanitize_text(request.headers.get('X-User-Name','')).lower()
    is_authorized = any(a in admin_name for a in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions("", admin_name)
        if not perms.get("is_super_admin"):
            return jsonify({"error":"Unauthorized"}), 403
    limit = min(int(request.args.get('limit', 100)), 500)
    return jsonify(audit.recent(limit))

# ──────────────────────────────────────────────────────────────────────────────
# SEARCH ENDPOINT
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/search', methods=['GET'])
@rate_limit(*RATE_LIMIT_SEARCH)
def search_agency():
    username     = sanitize_text(request.args.get('user',''))
    email        = sanitize_text(request.args.get('email',''))
    agency_code  = sanitize_agency_code(request.args.get('code',''))
    uat          = request.args.get('uat','')
    inquiry_type = sanitize_text(request.args.get('type','target')).lower()

    if not uat:          return jsonify({"error":"Unauthorized session."}), 401
    if not agency_code:  return jsonify({"error":"Invalid agency code. Must be 3-8 digits."}), 400

    perms = get_user_permissions(email, username)
    if inquiry_type not in perms["modules"] and not perms.get("is_super_admin"):
        return jsonify({"error":f"Access Denied: No permission for {inquiry_type.title()} module."}), 403

    start = time.time()
    tat = get_tenant_access_token()
    headers = {"Authorization":f"Bearer {tat}","Content-Type":"application/json"}
    points_payload = {"filter":{"conjunction":"and","conditions":[
        {"field_name":"Agency Code","operator":"is","value":[agency_code]}]}}
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    points_response = requests.post(points_url, headers=headers, json=points_payload, timeout=10).json()
    if points_response.get("code") != 0:
        return jsonify({"error":f"Feishu API Blocked: {points_response.get('msg')}"}), 403

    items = points_response.get('data',{}).get('items',[])
    if not items: return jsonify({"error":f"⚠️ Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields',{})
    region = clean(get_field_local(fields,'Region','Agency Region'))
    sheet_acm_name = extract_field_text(get_field_local(fields,'Acm Name (PK)','Acm Name (IN)','Acm','Assigned Member')).strip()

    if region in ('','none'):
        if sheet_acm_name.lower() in PK_ACMS: region = 'pk'
        elif sheet_acm_name.lower() in IN_ACMS: region = 'in'

    allowed_regs = perms.get("permissions",{}).get("regions",{}).get(inquiry_type,["all"])
    allowed_acms = perms.get("permissions",{}).get("acms",{}).get(inquiry_type,["all"])

    if "all" not in allowed_regs and region not in allowed_regs:
        return jsonify({"error":f"Access Denied: Region {region.upper()} restricted."}), 403
    if "all" not in allowed_acms and sheet_acm_name.lower() not in allowed_acms:
        return jsonify({"error":f"Access Denied: ACM {sheet_acm_name} restricted."}), 403

    try: base_points  = float(extract_field_text(get_field_local(fields,'Base Points')).replace(',',''))
    except: base_points = 0
    try: total_points = float(extract_field_text(get_field_local(fields,'# Total Points','Total Points','Total','Total points')).replace(',',''))
    except: total_points = 0
    try: used_points  = float(extract_field_text(get_field_local(fields,'Used Points','Used','Used points')).replace(',',''))
    except: used_points = 0
    try: point_balance= float(extract_field_text(get_field_local(fields,'Point Balance','Balance','Point balance')).replace(',',''))
    except: point_balance = 0
    if point_balance == 0 and total_points > 0:
        point_balance = total_points - used_points
    monthly_tracker = extract_field_text(get_field_local(fields,'Monthly Usage Tracker','Monthly Usage','Usage Tracker','Latest Usage Tracker'))

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=points_payload, timeout=10).json()

    valid_requests = []
    if req_response.get("code") == 0:
        cm, cy = datetime.now().month, datetime.now().year
        for item in req_response.get('data',{}).get('items',[]):
            r_fields = item.get('fields',{})
            ts = parse_feishu_date(get_field_local(r_fields,'Submitted on Copy','Submitted on'))
            if ts and ts.month == cm and ts.year == cy:
                valid_requests.append(r_fields)

    # 5.1  Calculate health score
    health = calculate_health_score(point_balance, total_points, used_points, valid_requests)

    logger.info("search_agency", agency=agency_code, inquiry=inquiry_type,
                duration_ms=int((time.time()-start)*1000))

    return jsonify({
        "base_points":base_points,"total_points":total_points,"used_points":used_points,
        "point_balance":point_balance,"monthly_tracker":monthly_tracker,
        "requests":valid_requests,"acm":sheet_acm_name.title(),"role":"Verified by Feishu",
        "health_score":health["score"],"health_status":health["status"]
    })

# ──────────────────────────────────────────────────────────────────────────────
# ANALYTICS ENDPOINT  (supports both GET and POST, with optional comparison)
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/api/analytics', methods=['GET','POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def get_analytics():
    # 7.3  Accept params from body (POST) to avoid PII in query strings
    if request.method == 'POST' and request.content_type == 'application/json':
        body = request.json or {}
    else:
        body = request.args

    username     = sanitize_text(body.get('user','')).lower()
    email        = sanitize_text(body.get('email',''))
    uat          = body.get('uat','')
    nocache      = body.get('nocache','0') == '1'

    if not uat: return jsonify({"error":"Unauthorized session."}), 401

    perms = get_user_permissions(email, username)
    if "analytics" not in perms["modules"] and not perms.get("is_super_admin"):
        return jsonify({"error":"Unauthorized. Analytics module restricted."}), 403

    allowed_regs = perms.get("permissions",{}).get("regions",{}).get("analytics",["all"])
    allowed_acms = perms.get("permissions",{}).get("acms",{}).get("analytics",["all"])

    region_filter = sanitize_text(body.get('region','PK')).lower() or 'pk'
    if region_filter == 'all' and "all" not in allowed_regs:
        return jsonify({"error":"Access Denied: Specify a region."}), 403
    if region_filter != 'all' and "all" not in allowed_regs and region_filter not in allowed_regs:
        return jsonify({"error":f"Access Denied: Region {region_filter.upper()} restricted."}), 403

    acm_filter  = sanitize_text(body.get('acm','All')).lower()
    if acm_filter == 'hasseb': acm_filter = 'haseeb'
    type_filter = sanitize_text(body.get('type','All')).lower()
    date_from   = sanitize_text(body.get('from',''))
    date_to     = sanitize_text(body.get('to',''))
    # 1.1  Comparison period params
    cmp_from    = sanitize_text(body.get('compare_from',''))
    cmp_to      = sanitize_text(body.get('compare_to',''))

    def parse_date_range(f, t):
        if f and t:
            dt1 = datetime.strptime(f, "%Y-%m-%d")
            dt2 = datetime.strptime(t, "%Y-%m-%d")
            if dt1 > dt2: dt1, dt2 = dt2, dt1
            return dt1, dt2 + timedelta(days=1)
        now = datetime.now()
        from_d = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12: to_d = now.replace(year=now.year+1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:               to_d = now.replace(month=now.month+1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return from_d, to_d

    from_dt, to_dt = parse_date_range(date_from, date_to)

    # Build cache key
    cache_key_parts = (region_filter, acm_filter, type_filter, date_from, date_to)
    cache_key = cache_make_key("analytics", *cache_key_parts)

    # Determine TTL: historical data gets longer cache
    now = datetime.now()
    if to_dt.date() < now.date():
        ttl = CACHE_TTL_HISTORICAL
    else:
        ttl = CACHE_TTL_REALTIME

    # Check cache (skip if nocache=1)
    if not nocache:
        cached_result = cache_get(cache_key)
        if cached_result:
            logger.info("analytics_cache_hit", region=region_filter, acm=acm_filter)
            if cmp_from and cmp_to:
                cached_result["cache_hit"] = True
            return jsonify(cached_result)

    start = time.time()
    tat = get_tenant_access_token()
    session = requests.Session()
    session.headers.update({"Authorization":f"Bearer {tat}"})

    stats = _fetch_and_aggregate(session, from_dt, to_dt, region_filter,
                                  acm_filter, type_filter, allowed_regs, allowed_acms)

    # 1.1  Period-over-period comparison
    if cmp_from and cmp_to:
        try:
            cmp_from_dt, cmp_to_dt = parse_date_range(cmp_from, cmp_to)
            cmp_tat = get_tenant_access_token()
            cmp_session = requests.Session()
            cmp_session.headers.update({"Authorization":f"Bearer {cmp_tat}"})
            cmp_stats = _fetch_and_aggregate(cmp_session, cmp_from_dt, cmp_to_dt,
                                              region_filter, acm_filter, type_filter,
                                              allowed_regs, allowed_acms)
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

    duration_ms = int((time.time()-start)*1000)
    logger.info("analytics_complete", region=region_filter, acm=acm_filter,
                rows=stats["scanned_rows"], duration_ms=duration_ms)
    stats["duration_ms"] = duration_ms

    # Store in cache
    cache_set(cache_key, stats, ttl=ttl)
    return jsonify(stats)

# ──────────────────────────────────────────────────────────────────────────────
# CACHE CONTROL ENDPOINT
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
        "status":"ok","ts":datetime.utcnow().isoformat(),
        "cache_entries":len(_cache),"audit_entries":len(audit._queue)
    })

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT',5000)))
