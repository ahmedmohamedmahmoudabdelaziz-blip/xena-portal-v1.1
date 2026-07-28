"""
Xena Data Portal — High-Speed Live Backend (Performance Optimized)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Refactored to eliminate background caching and slow sequential parsing.
Now uses Field Projection, Server-Side Filtering, and Parallel Pagination
to deliver 100% LIVE data within seconds.
"""

import os, time, re, json, hashlib, logging, urllib.parse, threading, random, uuid
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, send_file, redirect
import requests as http_requests

# ──────────────────────────────────────────────────────────────────────────────
# CENTRALISED CONFIGURATION & ENV
# ──────────────────────────────────────────────────────────────────────────────
APP_ID       = os.environ.get("LARK_APP_ID")
APP_SECRET   = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://xena-portal-v1-1.vercel.app/api/callback")

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

MONTHLY_ALLOCATOR_LIMITS = {
    "trend card": 10,
    "traffic card": 50,
    "30 mic 15 days": 999,
    "30 mic 30 days": 999,
    "normal short id ( 2 levels above ) 15 days": 999,
    "normal short id ( 2 levels above ) 30 days": 999,
    "customized short id 15 days": 999,
    "customized short id 30 days": 999,
    "room pin-up": 999,
    "welcome package 3": 15,
    "welcome package 2": 50,
}
ORDER_TYPE_LIMITS = {
    "main page banner": 3,
    "news banner": 5,
    "live banner": 5,
    "splash": 10,
}

# ──────────────────────────────────────────────────────────────────────────────
# TENANT ACCESS TOKEN
# ──────────────────────────────────────────────────────────────────────────────
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
    return " ".join(p[:1] + "***" if len(p) > 1 else p for p in name.strip().split())

# ──────────────────────────────────────────────────────────────────────────────
# RATE LIMITER & SANITISATION
# ──────────────────────────────────────────────────────────────────────────────
_rate_store: dict = defaultdict(list)
_rate_lock = threading.Lock()

def rate_check(ip, max_requests, window_seconds):
    now = time.time()
    with _rate_lock:
        _rate_store[ip] = [t for t in _rate_store[ip] if now - t < window_seconds]
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
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', str(text).strip()[:max_length])

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
            threading.Thread(target=self._write_feishu, args=(entry,), daemon=True).start()
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
                "Action": entry["action"], "Target": entry["target"],
                "IP Address": entry["ip"], "Severity": entry["severity"]
            }}
            http_requests.post(url, headers=hdrs, json=payload, timeout=8)
        except Exception as e: logger.error("audit_write_failed", error=str(e))

    def get_recent(self, limit=100):
        with self._lock: return list(reversed(self._queue[-limit:]))

audit = AuditLogger()

# ──────────────────────────────────────────────────────────────────────────────
# MOCK DB ENGINE (For local dev without keys)
# ──────────────────────────────────────────────────────────────────────────────
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
            items.append({
                "record_id": str(uuid.uuid4()),
                "fields": {
                    "Numbering": f"REQ-{1000+i}", "Request Type": req_type, "Status": status,
                    "Region": "PK" if is_pk else "IN", "Acm Name (PK)": acm if is_pk else "",
                    "Acm Name (IN)": acm if not is_pk else "", "Agency Type": random.choice(["Acm hunting", "BD hunting", "Walkin"]),
                    "Submitted on Copy": int(dt.timestamp() * 1000), "Agency Code": str(40000 + random.randint(1, 999)),
                    "Agency Point Privilege": req_type if "Card" in req_type else "", "Target Type": "Agency" if "Target" in req_type else "",
                    "Quantities Input": str(random.randint(1, 3)), "Point Balance": str(random.randint(100, 5000))
                }
            })
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
                "Agency Code": code, "Agency Name": f"Mock Agency {code}", "Region": "PK" if is_pk else "IN",
                "Acm": acm, "Base Points": str(random.randint(100, 500)), "Total Points": str(total),
                "Used Points": str(used), "Point Balance": str(total - used)
            }
        }]

# ──────────────────────────────────────────────────────────────────────────────
# HIGH-SPEED OPTIMIZED DATA FETCHERS
# ──────────────────────────────────────────────────────────────────────────────
def fetch_feishu_records_optimized(table_id, filter_obj=None, field_names=None):
    """
    Core function for Server-Side Filtering and Field Projection.
    Replaces the old caching loop.
    """
    if MOCK_MODE:
        items = MockFeishuDB.generate_requests(100) if table_id == REQUESTS_TABLE_ID else MockFeishuDB.generate_agency("123")
        return items, True, ""

    tat = get_tenant_access_token()
    all_items = []
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records/search"
    payload = {"page_size": 500, "automatic_fields": True}
    if filter_obj: payload["filter"] = filter_obj
    if field_names: payload["field_names"] = field_names

    session = http_requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})

    page_token = None
    while True:
        if page_token: payload["page_token"] = page_token
        try:
            resp = session.post(url, json=payload, timeout=20)
            data = resp.json()
            if data.get("code") != 0:
                return all_items, False, f"Feishu Error: {data.get('msg')}"
            
            block = data.get("data", {})
            items = block.get("items", [])
            all_items.extend(items)
            
            page_token = block.get("page_token")
            if not page_token or not block.get("has_more", False): break
        except Exception as e:
            return all_items, False, str(e)

    return all_items, True, ""

def fetch_feishu_parallel_by_date(table_id, date_field, start_dt, end_dt, base_conditions, field_names):
    """
    Slices a date range into parallel chunks, fetching them concurrently.
    Massively reduces load time for large Analytics queries.
    """
    if MOCK_MODE: return MockFeishuDB.generate_requests(300), True, ""
    
    if not start_dt: start_dt = datetime(2023, 1, 1)
    if not end_dt: end_dt = datetime.now() + timedelta(days=1)
    
    delta = end_dt - start_dt
    chunk_size = max(timedelta(days=1), delta / 5) # Distribute across max 5 workers
    
    chunks = []
    curr = start_dt
    while curr < end_dt:
        nxt = min(curr + chunk_size, end_dt)
        chunks.append((curr, nxt))
        curr = nxt
        
    all_items = []
    has_errors = False
    error_msg = ""
    
    def fetch_chunk(c_start, c_end):
        conds = list(base_conditions) if base_conditions else []
        conds.append({"field_name": date_field, "operator": "isGreaterEqual", "value": [int(c_start.timestamp()*1000)]})
        conds.append({"field_name": date_field, "operator": "isLess", "value": [int(c_end.timestamp()*1000)]})
        filter_obj = {"conjunction": "and", "conditions": conds}
        return fetch_feishu_records_optimized(table_id, filter_obj, field_names)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_chunk, c[0], c[1]): c for c in chunks}
        for future in as_completed(futures):
            try:
                items, success, msg = future.result()
                all_items.extend(items)
                if not success:
                    has_errors = True
                    error_msg = msg
            except Exception as e:
                has_errors = True
                error_msg = str(e)
                
    return all_items, not has_errors, error_msg

def normalize_key(k): return " ".join(str(k).lower().strip().split())

def get_field_local(fields, *aliases):
    if not fields: return None
    for alias in aliases:
        if alias in fields and fields[alias] not in (None, "", []): return fields[alias]
    for alias in aliases:
        tgt = normalize_key(alias)
        for k, v in fields.items():
            if normalize_key(k) == tgt and v not in (None, "", []): return v
    return None

def extract_field_text(field_data):
    if not field_data: return ""
    if isinstance(field_data, (str, int, float)): return str(field_data)
    if isinstance(field_data, dict):
        for key in ['text', 'name', 'en_name', 'email', 'value', 'label', 'id']:
            if key in field_data: return str(field_data[key])
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
    if isinstance(field_data, str): return [s.strip() for s in field_data.split(',') if s.strip()]
    if isinstance(field_data, list):
        res = []
        for item in field_data:
            if not item: continue
            if isinstance(item, dict):
                extracted = False
                for key in ['text', 'name', 'en_name', 'email', 'value', 'label']:
                    if key in item and item[key] not in (None, ""):
                        res.append(str(item[key]).strip()); extracted = True; break
                if not extracted and 'id' in item and item['id'] not in (None, ""): res.append(str(item['id']).strip())
                elif not extracted: res.append(str(item).strip())
            else: res.append(str(item).strip())
        return res
    return [str(field_data).strip()]

def parse_feishu_date(date_val):
    if not date_val: return None
    if isinstance(date_val, list) and len(date_val) > 0: date_val = date_val[0]
    if isinstance(date_val, dict): date_val = date_val.get('value', date_val.get('text', ''))
    try:
        if isinstance(date_val, (int, float)) or (isinstance(date_val, str) and date_val.isdigit()):
            dt_utc = datetime.fromtimestamp(int(date_val) / 1000.0, tz=timezone.utc)
            return dt_utc.replace(tzinfo=None) + timedelta(hours=3) # Cairo Time
        clean_str = str(date_val).strip()[:10].replace('/', '-').replace('.', '-')
        return datetime.strptime(clean_str, "%Y-%m-%d")
    except Exception: return None

def clean(field_data): return extract_field_text(field_data).strip().lower()

def compute_allocator_status(usage_dict):
    status = {}
    for item, used in usage_dict.items():
        limit = MONTHLY_ALLOCATOR_LIMITS.get(item)
        status[item] = {"used": used, "limit": limit, "remaining": (max(0, limit - used) if limit is not None else None)}
    return status

def parse_granular_string(raw_str):
    default = {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}
    if not raw_str or str(raw_str).strip() == "": return default
    if "=" not in raw_str:
        parts = [x.strip().lower() for x in raw_str.split(",") if x.strip()]
        return {"target": parts or ["all"], "points": parts or ["all"], "analytics": parts or ["all"], "query": parts or ["all"]}
    res = {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}
    for chunk in raw_str.split(";"):
        if "=" in chunk:
            mod, vals = chunk.split("=", 1)
            val_list = [v.strip().lower() for v in vals.split(",") if v.strip()]
            if mod.strip().lower() in res: res[mod.strip().lower()] = val_list or ["all"]
    return res

def get_user_permissions(email, name):
    name_clean = name.strip().lower() if name else ""
    email_clean = email.strip().lower() if email else ""
    
    if any(admin == name_clean for admin in ADMIN_USERS) or MOCK_MODE:
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin", "query", "export_data"], 
            "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]},
                            "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"], "query": ["all"]}}
        }

    if not email_clean and not name_clean: 
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records/search"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    # Use field projection for lightning-fast auth check
    payload = {
        "page_size": 500,
        "field_names": ["Email", "Person", "Modules", "ACMs", "Regions"]
    }
    
    try:
        res = http_requests.post(url, headers=headers, json=payload, timeout=10).json()
        for item in res.get("data", {}).get("items", []):
            fields = item.get("fields", {})
            db_email = extract_field_text(fields.get("Email", "")).lower().strip()
            db_person = extract_field_text(fields.get("Person", "")).lower().strip()
            
            if (email_clean and email_clean == db_email) or (name_clean and name_clean == db_person):
                modules = [m.strip().lower() for m in extract_field_text(get_field_local(fields, "Modules")).split(",") if m.strip()]
                parsed_acms = parse_granular_string(extract_field_text(get_field_local(fields, "ACMs")))
                parsed_regions = parse_granular_string(extract_field_text(get_field_local(fields, "Regions")))
                return {"is_super_admin": "admin" in modules, "modules": modules, "permissions": {"acms": parsed_acms, "regions": parsed_regions}}
    except Exception: pass
    return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

def generate_executive_insights(stats, cmp_stats=None):
    insights = []
    kpis = stats.get("kpis", {})
    creations, bds, closings = kpis.get("creations", 0), kpis.get("bds", 0), kpis.get("closings", 0)

    if creations > 0 and bds > 0:
        ratio = creations / bds
        insights.append(f"Pipeline Analysis: Creation-to-BD ratio sits at {ratio:.1f}x, indicating {'highly effective Top-of-Funnel organic acquisition' if ratio > 2.5 else 'a BD-reliant growth strategy this period'}.")

    if creations > 0 and closings > 0:
        eff = (closings / creations) * 100
        insights.append(f"Closing Efficiency: Converting at {eff:.1f}% relative to new creations.")

    acm_perf = stats.get("acm_performance", {})
    if acm_perf:
        top_acm = max(acm_perf, key=acm_perf.get)
        share = (acm_perf[top_acm] / creations * 100) if creations > 0 else 0
        insights.append(f"Leadership: {top_acm} is driving {share:.1f}% of total volume, establishing a strong regional benchmark.")

    if cmp_stats and cmp_stats.get("kpis", {}).get("creations", 0) > 0:
        prev_creations = cmp_stats["kpis"]["creations"]
        delta = ((creations - prev_creations) / prev_creations) * 100
        insights.append(f"Period Momentum: Demonstrating a {abs(delta):.1f}% {'growth' if delta >= 0 else 'decline'} in agency creations compared to the previous cycle.")

    return insights

# ──────────────────────────────────────────────────────────────────────────────
# AGENCY SEARCH ENGINE (DUAL ENGINE)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_agency_data(code, query_type="points", allowed_acms=None, allowed_regs=None):
    if MOCK_MODE: return _process_agency_data(code, MockFeishuDB.generate_agency(code), MockFeishuDB.generate_requests(50), query_type, allowed_acms, allowed_regs)

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    # N+1 ELIMINATION: Fetch Points and History in parallel using ThreadPoolExecutor
    search_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search"
    hist_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search"
    
    points_payload = {
        "automatic_fields": True, "page_size": 1,
        "filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [code]}]},
        "field_names": ["Agency Code", "Agency Name", "Region", "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member", "Base Points", "Total Points", "Used Points", "Point Balance"]
    }
    
    cm, cy = datetime.now().month, datetime.now().year
    month_start = datetime(cy, cm, 1).timestamp() * 1000
    
    hist_payload = {
        "automatic_fields": True, "page_size": 500,
        "filter": {"conjunction": "and", "conditions": [
            {"field_name": "Agency Code", "operator": "is", "value": [code]},
            {"field_name": "Submitted on Copy", "operator": "isGreaterEqual", "value": [int(month_start)]}
        ]},
        "field_names": ["Submitted on Copy", "Request Type", "Type", "Status", "Request Status", "Target Type", "Point Balance", "Agency Point Privilege", "Privilege", "Quantities Input", "Qty", "Latest Usage Tracker"]
    }

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            f_points = executor.submit(http_requests.post, search_url, headers=headers, json=points_payload, timeout=15)
            f_hist = executor.submit(http_requests.post, hist_url, headers=headers, json=hist_payload, timeout=15)
            
            resp = f_points.result().json()
            if resp.get("code") != 0: return {"found": False, "error": f"Feishu API Error: {resp.get('msg')}"}
            all_records = resp.get("data", {}).get("items", [])
            if not all_records: return {"found": False, "error": f"Notice: Agency {code} not found or no records."}
            
            hist_resp = f_hist.result().json()
            hist_items = hist_resp.get("data", {}).get("items", []) if hist_resp.get("code") == 0 else []
            
        return _process_agency_data(code, all_records, hist_items, query_type, allowed_acms, allowed_regs)
    except Exception as e:
        return {"found": False, "error": f"Search connection error: {str(e)}"}

def _process_agency_data(code, all_records, hist_items, query_type, allowed_acms, allowed_regs):
    first = all_records[0].get("fields", {})
    agency_name  = extract_field_text(get_field_local(first,"Agency Name","Name"))
    region_raw   = clean(get_field_local(first,"Region","Agency Region"))
    acm_raw      = extract_field_text(get_field_local(first,"Acm Name (PK)","Acm Name (IN)","Acm","Assigned Member"))

    if region_raw in ('', 'none'):
        if acm_raw.lower() in PK_ACMS: region_raw = 'pk'
        elif acm_raw.lower() in IN_ACMS: region_raw = 'in'

    if allowed_acms and "all" not in allowed_acms and acm_raw.strip().lower() not in [a.lower() for a in allowed_acms]:
        return {"found": False, "error": f"Access Denied: Not authorized to view ACM {acm_raw}."}
    if allowed_regs and "all" not in allowed_regs and region_raw.strip().lower() not in [r.lower() for r in allowed_regs]:
        return {"found": False, "error": f"Access Denied: Not authorized to view Region {region_raw.upper()}."}

    history_points, history_target = [], []
    privileges_claimed, usage_this_month = defaultdict(int), defaultdict(int)
    cm, cy = datetime.now().month, datetime.now().year
    
    for r in hist_items:
        hf = r.get("fields", {})
        h_date = parse_feishu_date(get_field_local(hf, "Submitted on Copy", "Submitted on", "Created Time"))
        if not h_date or h_date.month != cm or h_date.year != cy: continue

        req_type      = extract_field_text(get_field_local(hf, "Request Type", "Type")).strip()
        status_val    = extract_field_text(get_field_local(hf, "Status", "Request Status")).strip()
        req_type_lower = req_type.lower()
        s_lower = status_val.lower()

        if "target" in req_type_lower:
            privilege_val = extract_field_text(get_field_local(hf, "Agency Point Privilege", "Privilege", "Agency Privilege")).strip()
            qty = 1
            raw_counter = extract_field_text(get_field_local(hf, "Counter", "Qty", "Quantities Input")).strip()
            if raw_counter:
                m = re.search(r'\d+', str(raw_counter))
                if m: qty = int(m.group())

            history_target.append({"date": h_date.strftime("%Y-%m-%d"), "_dt": h_date, "request_type": req_type, "status": status_val, "privilege": privilege_val, "quantities_input": str(qty)})
            if s_lower in ("done", "done ", "completed", "approved", "confirm") and privilege_val:
                privileges_claimed[privilege_val] += qty
        else:
            latest_usage  = extract_field_text(get_field_local(hf, "Latest Usage Tracker")).strip()
            parsed_items = re.findall(r'🔹\s*(.*?):\s*(\d+)', latest_usage)
            privilege_val = " + ".join([f"{k.strip()} ({v})" for k, v in parsed_items]) if parsed_items else extract_field_text(get_field_local(hf, "Agency Point Privilege", "Privilege")).strip()

            history_points.append({"date": h_date.strftime("%Y-%m-%d"), "_dt": h_date, "request_type": req_type, "status": status_val, "target_type": extract_field_text(get_field_local(hf, "Target Type")).strip(), "point_balance": extract_field_text(get_field_local(hf, "Point Balance")).strip(), "privilege": privilege_val, "quantities_input": extract_field_text(get_field_local(hf, "Quantities Input")).strip()})

            if not ("reject" in s_lower or "fail" in s_lower or "decline" in s_lower):
                for item_name, item_qty in parsed_items:
                    name_clean = item_name.strip().lower()
                    matched = False
                    for key in MONTHLY_ALLOCATOR_LIMITS.keys():
                        if key in name_clean:
                            usage_this_month[key] += int(item_qty); matched = True; break
                    if not matched: usage_this_month[name_clean] += int(item_qty)

    history_points.sort(key=lambda x: (x["_dt"] is None, x["_dt"]), reverse=True)
    history_target.sort(key=lambda x: (x["_dt"] is None, x["_dt"]), reverse=True)
    for h in history_points: h.pop("_dt", None)
    for h in history_target: h.pop("_dt", None)
    
    if query_type == "points":
        total_pts = parse_float_safe(extract_field_text(get_field_local(first, '# Total Points', 'Total Points', 'Total')))
        used_pts  = parse_float_safe(extract_field_text(get_field_local(first, 'Used Points', 'Used')))
        balance   = parse_float_safe(extract_field_text(get_field_local(first, 'Point Balance', 'Balance')))
        if balance == 0 and total_pts > 0: balance = total_pts - used_pts

        health_score, health_status = (95, "Healthy") if total_pts > 0 and (used_pts / total_pts) <= 0.70 else (40 if total_pts > 0 and (used_pts / total_pts) > 0.90 else 70, "Critical" if total_pts > 0 and (used_pts / total_pts) > 0.90 else "At Risk") if total_pts > 0 else (0, "Inactive")

        return {"found": True, "agency_code": code, "agency_name": agency_name, "region": region_raw.upper(), "acm": acm_raw.title(), "total_points": total_pts, "used_points": used_pts, "point_balance": balance, "health_score": health_score, "health_status": health_status, "history": history_points, "allocator_status": compute_allocator_status(usage_this_month), "requests": [r.get("fields", {}) for r in all_records]}
    else:  
        raw_base_pts = parse_float_safe(extract_field_text(get_field_local(first, "Base Points", "base_points")))
        return {"found": True, "agency_code": code, "agency_name": agency_name, "region": region_raw.upper(), "acm": acm_raw.title(), "base_points": raw_base_pts * COINS_MULTIPLIER, "health_score": 100, "health_status": "Healthy", "privileges_claimed": dict(privileges_claimed), "history": history_target, "requests": [r.get("fields", {}) for r in all_records]}

def _build_field_map_safe(item: dict) -> dict:
    fields = item.get("fields", {})
    return {
        "date":      parse_feishu_date(get_field_local(fields,"Submitted on Copy","Submitted on","Created Time")),
        "req_type":  clean(get_field_local(fields,"Request Type","Request type","Type","Category")),
        "status":    clean(get_field_local(fields,"Status","Request Status","Agency Status","State")),
        "region":    clean(get_field_local(fields,"Region","Agency Region")),
        "acm_pk":    clean(get_field_local(fields,"Acm Name (PK)")),
        "acm_in":    clean(get_field_local(fields,"Acm Name (IN)")),
        "acm_fb":    clean(get_field_local(fields,"Acm","Assigned Member")),
        "a_type":    clean(get_field_local(fields,"Agency Type","Type of Agency")),
        "cl_rsn":    clean(get_field_local(fields,"Closing Reason","Closing Agencies Reason")),
        "o_app":     clean(get_field_local(fields,"Otherapp Name","Other App Name","Other Apps")),
        "rj_rsns":   extract_field_list(get_field_local(fields,"Reject Reason","Rejection Reason")),
        "cr_ways":   extract_field_list(get_field_local(fields,"Create Way","Creation Type")),
    }

def run_analytics(all_items, from_dt, to_dt, region_filter, acm_filter, type_filter, allowed_acms, allowed_regs):
    stats = {
        "kpis": {"creations":0,"bds":0,"closings":0},
        "creation_status": {"Done":0,"Rejected":0,"Under Investigation":0},
        "bd_status":       {"Done":0,"Rejected":0,"Under Investigation":0},
        "closing_status":  {"Done":0,"Rejected":0,"Under Investigation":0},
        "acm_performance":{}, "creation_types":{}, "agency_types":{},
        "other_apps":{}, "reject_reasons":{}, "closing_reasons_pie":{},
        "acm_closing_reasons":{}, "daily_trend_creation":{}, "daily_trend_bd":{}, "daily_trend_closing":{},
        "other_request_types":{}, "scanned_rows": len(all_items), "fetch_complete": True, "stop_reason": "",
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
        if type_filter_c != "all" and type_filter_c != fm["a_type"]: continue

        req_type, status, agency_type, closing_reason, other_app = fm["req_type"], fm["status"], fm["a_type"], fm["cl_rsn"], fm["o_app"]
        is_done     = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status
        is_bd_kpi      = "bd creation" in req_type
        is_closing_kpi = "closing agency" in req_type
        is_creation_kpi= any(p in req_type for p in ["agency creation","applied already","follow-up"])
        
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
            if is_done and acm: stats["acm_performance"][acm.title()] = stats["acm_performance"].get(acm.title(),0)+1
            if is_done and other_app: stats["other_apps"][other_app.title()] = stats["other_apps"].get(other_app.title(),0)+1
            if agency_type: stats["agency_types"][agency_type.title()] = stats["agency_types"].get(agency_type.title(),0)+1
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

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return send_file(os.path.join(root_dir, 'index.html'))

@app.route('/api/login', methods=['GET'])
def login():
    if MOCK_MODE: return redirect(f"/?user=Test%20User&email=test@example.com&uat=mock_token_123&avatar=https://ui-avatars.com/api/?name=Test+User")
    return redirect(f"https://open.feishu.cn/open-apis/authen/v1/index?app_id={APP_ID}&redirect_uri={urllib.parse.quote(REDIRECT_URI)}")

@app.route('/api/callback', methods=['GET'])
def callback():
    code = request.args.get('code')
    if not code: return redirect("/?auth_error=" + urllib.parse.quote("Authorization failed: no code returned.", safe=''))
    try:
        token_resp = http_requests.post("https://open.feishu.cn/open-apis/authen/v1/access_token", headers={"Content-Type": "application/json"}, json={"app_id": APP_ID, "app_secret": APP_SECRET, "grant_type": "authorization_code", "code": code}, timeout=15).json()
        uat = (token_resp.get("data") or {}).get("access_token") or token_resp.get("access_token")
        if not uat: return redirect("/?auth_error=" + urllib.parse.quote(f"Login failed: {token_resp.get('msg') or 'Token exchange failed'}", safe=''))
        
        user_data = http_requests.get("https://open.feishu.cn/open-apis/authen/v1/user_info", headers={"Authorization": f"Bearer {uat}"}, timeout=15).json().get("data", {})
        lark_name, lark_email, avatar_url = user_data.get("name", "Unknown"), user_data.get("email", ""), user_data.get("avatar_72", "")
        audit.log(lark_name, "LOGIN", mask_email(lark_email), ip=request.headers.get("X-Forwarded-For", ""))
        return redirect(f"/?user={urllib.parse.quote(lark_name, safe='')}&email={urllib.parse.quote(lark_email, safe='')}&uat={urllib.parse.quote(uat, safe='')}&avatar={urllib.parse.quote(avatar_url, safe='')}")
    except Exception as exc: return redirect("/?auth_error=" + urllib.parse.quote(f"Login error: {str(exc)[:120]}", safe=''))

@app.route('/api/auth/me', methods=['GET'])
def check_auth(): return jsonify(get_user_permissions(sanitize_text(request.args.get('email','')), sanitize_text(request.args.get('user',''))))

@app.route('/api/search', methods=['GET', 'POST'])
@rate_limit(*RATE_LIMIT_SEARCH)
def search():
    req_data = request.json if request.method == 'POST' else request.args
    code, user, email, qtype = sanitize_agency_code(req_data.get('code','')), sanitize_text(req_data.get('user','')), sanitize_text(req_data.get('email','')), req_data.get('type','points')
    if qtype not in ('points','target'): qtype = 'points'
    if not code: return jsonify({"found":False,"error":"Invalid or missing agency code."}), 400

    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any(qtype in m for m in perms.get("modules", [])): return jsonify({"found": False, "error": f"Access Denied: You do not have permission to view {qtype.title()}."}), 403

    data = fetch_agency_data(code, qtype, perms.get("permissions",{}).get("acms",{}).get(qtype,["all"]), perms.get("permissions",{}).get("regions",{}).get(qtype,["all"]))
    if data.get("found"):
        audit.log(user, "AGENCY_SEARCH", f"Code: {code} | Type: {qtype}", ip=request.headers.get("X-Forwarded-For", ""), severity="Info")
        return jsonify(data)
    return jsonify(data), 404

@app.route('/api/points/records', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def points_records():
    user, email = sanitize_text(request.args.get('user','')), sanitize_text(request.args.get('email',''))
    perms = get_user_permissions(email, user)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    if not perms.get("is_super_admin") and not any("points" in m for m in perms.get("modules",[])): return jsonify({"error":"Access denied"}), 403
    allowed_acms, allowed_regs = perms.get("permissions",{}).get("acms",{}).get("points",["all"]), perms.get("permissions",{}).get("regions",{}).get("points",["all"])

    try: page, page_size = max(1, int(request.args.get('page','1'))), min(200, max(1, int(request.args.get('page_size','50'))))
    except ValueError: page, page_size = 1, 50

    search, f_agency_code, f_region, f_acm = [sanitize_text(request.args.get(k,'')).lower() for k in ('search', 'agency_id', 'region', 'acm')]
    sort_by, sort_dir = sanitize_text(request.args.get('sort_by','point_balance')), 'desc' if request.args.get('sort_dir','desc').lower() != 'asc' else 'asc'

    # STRICT SERVER-SIDE FILTERING & PROJECTION
    field_names = ["Agency Code", "Agency Name", "Region", "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member", "Base Points", "Bonus Points", "Total Points", "# Total Points", "Used Points", "Point Balance", "Status", "Date"]
    conds = []
    
    # We apply strict Feishu conditions if simple matching is requested
    if f_agency_code: conds.append({"field_name": "Agency Code", "operator": "contains", "value": [f_agency_code]})
    
    filter_obj = {"conjunction": "and", "conditions": conds} if conds else None
    all_items, fetch_complete, stop_reason = fetch_feishu_records_optimized(POINTS_TABLE_ID, filter_obj, field_names)

    if not fetch_complete and not all_items: return jsonify({"error": f"Feishu API Error: {stop_reason}"}), 502

    filtered = []
    for item in all_items:
        f = item.get("fields", {})
        agency_code = extract_field_text(get_field_local(f, "Agency Code")).strip()
        acm         = extract_field_text(get_field_local(f, "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member")).strip()
        region      = 'PK' if acm.lower() in PK_ACMS else ('IN' if acm.lower() in IN_ACMS else '')

        if "all" not in allowed_acms and acm.lower() not in [a.lower() for a in allowed_acms]: continue
        if "all" not in allowed_regs and region.lower() not in [r.lower() for r in allowed_regs]: continue
        if search and search not in (agency_code + acm).lower(): continue
        if f_region and f_region not in region.lower(): continue
        if f_acm and f_acm not in acm.lower(): continue

        total_pts = parse_float_safe(extract_field_text(get_field_local(f, "Total Points", "# Total Points")))
        used_pts  = parse_float_safe(extract_field_text(get_field_local(f, "Used Points")))
        balance   = parse_float_safe(extract_field_text(get_field_local(f, "Point Balance")))
        if balance == 0 and total_pts > 0: balance = total_pts - used_pts

        filtered.append({
            "agency_id": agency_code, "acm": acm, "region": region,
            "agency_name": extract_field_text(get_field_local(f, "Agency Name", "Name")),
            "base_points": parse_float_safe(extract_field_text(get_field_local(f, "Base Points"))),
            "bonus_points": parse_float_safe(extract_field_text(get_field_local(f, "Bonus Points"))),
            "total_points": total_pts, "used_points": used_pts, "point_balance": balance,
            "health_score": 40 if (total_pts > 0 and (used_pts/total_pts)>0.90) else (70 if total_pts > 0 and (used_pts/total_pts)>0.70 else (95 if total_pts > 0 else 0)),
            "status": extract_field_text(get_field_local(f, "Status")),
            "date": extract_field_text(get_field_local(f, "Date")),
        })

    sf = {"agency_id": "agency_id", "acm": "acm", "region": "region", "base_points": "base_points", "total_points": "total_points", "used_points": "used_points", "point_balance": "point_balance", "health_score": "health_score"}.get(sort_by, "point_balance")
    try: filtered.sort(key=lambda x: (x[sf] is None, x[sf], x["agency_id"]), reverse=(sort_dir == 'desc'))
    except TypeError: filtered.sort(key=lambda x: (str(x.get(sf,"")), x["agency_id"]), reverse=(sort_dir == 'desc'))

    if request.args.get('export', 'false').lower() == 'true':
        if not perms.get("is_super_admin") and not any("export" in m for m in perms.get("modules",[])): return jsonify({"error":"Export access denied."}), 403
        audit.log(user, "EXPORT_DATA", f"Point Records ({len(filtered)} rows)", ip=ip, severity="Info")
        return jsonify({"records": filtered[:5000], "fetch_complete": fetch_complete})

    start, end = (page - 1) * page_size, (page - 1) * page_size + page_size
    return jsonify({
        "records": filtered[start:end], "total": len(filtered), "page": page, "page_size": page_size, "total_pages": max(1, -(-len(filtered) // page_size)),
        "totals": {"total_points": sum(r["total_points"] for r in filtered), "used_points": sum(r["used_points"] for r in filtered), "point_balance": sum(r["point_balance"] for r in filtered)},
        "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason)
    })

@app.route('/api/analytics', methods=['GET', 'POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def analytics():
    start = time.time()
    body = request.json if request.method == 'POST' else request.args
    user, email, region, acm, atype = [sanitize_text(body.get(k, default)) for k, default in [('user',''), ('email',''), ('region','PK'), ('acm','All'), ('type','All')]]
    from_s, to_s, cmp_from, cmp_to = body.get('from',''), body.get('to',''), body.get('compare_from',''), body.get('compare_to','')

    audit.log(user, "GENERATE_ANALYTICS", f"R:{region}|ACM:{acm}", ip=request.headers.get("X-Forwarded-For", ""), severity="Info")
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("analytics" in m for m in perms.get("modules",[])): return jsonify({"error":"Access denied"}), 403

    def parse_d(s, end=False):
        if not s: return None
        try: return datetime.strptime(s, "%Y-%m-%d") + (timedelta(days=1) if end else timedelta(0))
        except ValueError: return None
    from_dt, to_dt = parse_d(from_s), parse_d(to_s, end=True)
    cmp_from_dt, cmp_to_dt = parse_d(cmp_from), parse_d(cmp_to, end=True)
    
    # ─────────────────────────────────────────────────────────────
    # MASSIVE PERFORMANCE GAIN: Field Projection
    # ─────────────────────────────────────────────────────────────
    field_names = ["Submitted on Copy", "Submitted on", "Created Time", "Request Type", "Type", "Status", "Request Status", "Agency Status", "State", "Region", "Agency Region", "Acm Name (PK)", "Acm Name (IN)", "Acm", "Assigned Member", "Agency Type", "Type of Agency", "Closing Reason", "Closing Agencies Reason", "Otherapp Name", "Other App Name", "Other Apps", "Reject Reason", "Rejection Reason", "Create Way", "Creation Type"]

    # Parallel Date Slicing fetch
    all_items, fetch_complete, stop_reason = fetch_feishu_parallel_by_date(REQUESTS_TABLE_ID, "Submitted on Copy", from_dt or cmp_from_dt, to_dt or datetime.now()+timedelta(days=1), None, field_names)
    
    if not fetch_complete and not all_items: return jsonify({"error": f"Data fetch failed: {stop_reason}"}), 502

    stats = run_analytics(all_items, from_dt, to_dt, region, acm, atype, perms.get("permissions",{}).get("acms",{}).get("analytics",["all"]), perms.get("permissions",{}).get("regions",{}).get("analytics",["all"]))
    stats["fetch_complete"], stats["stop_reason"] = fetch_complete, stop_reason
    
    cmp_stats = None
    if cmp_from and cmp_to:
        try:
            cmp_stats = run_analytics(all_items, cmp_from_dt, cmp_to_dt, region, acm, atype, perms.get("permissions",{}).get("acms",{}).get("analytics",["all"]), perms.get("permissions",{}).get("regions",{}).get("analytics",["all"]))
            stats["comparison"] = {"from": cmp_from, "to": cmp_to, "kpis": cmp_stats["kpis"], "creation_status": cmp_stats["creation_status"], "bd_status": cmp_stats["bd_status"], "closing_status": cmp_stats["closing_status"], "acm_performance": cmp_stats["acm_performance"], "daily_trend_creation": cmp_stats["daily_trend_creation"], "daily_trend_bd": cmp_stats["daily_trend_bd"], "daily_trend_closing": cmp_stats["daily_trend_closing"]}
        except Exception as e: stats["comparison_error"] = str(e)

    stats["executive_insights"] = generate_executive_insights(stats, cmp_stats)
    stats["duration_ms"] = int((time.time() - start) * 1000)
    return jsonify(stats)

@app.route('/api/compare', methods=['GET', 'POST'])
@rate_limit(*RATE_LIMIT_ANALYTICS)
def compare():
    start = time.time()
    body = request.json if request.method == 'POST' else request.args
    user, email, mode, region, rtype = [sanitize_text(body.get(k, default)) for k, default in [('user',''), ('email',''), ('mode','acm'), ('region','All'), ('type','All')]]
    
    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("analytics" in m for m in perms.get("modules",[])): return jsonify({"error":"Access denied"}), 403

    def parse_d(s, end=False): return datetime.strptime(s, "%Y-%m-%d") + (timedelta(days=1) if end else timedelta(0)) if s else None
    groups_spec = []

    if mode.lower() == "period":
        acm_filter = sanitize_text(body.get('acm','All')).lower()
        periods = json.loads(body.get('periods', '[]')) if isinstance(body.get('periods'), str) else (body.get('periods') or [])
        if len(periods) < 2 or len(periods) > 4: return jsonify({"error": "Provide 2 to 4 periods."}), 400
        for i, p in enumerate(periods): groups_spec.append((sanitize_text(p.get('label') or f"Period {i+1}"), parse_d(p.get('from')), parse_d(p.get('to'), True), acm_filter))
    else:
        acms = [a.strip() for a in (body.get('acms').split(",") if isinstance(body.get('acms'), str) else (body.get('acms') or [])) if a.strip()]
        if len(acms) < 2 or len(acms) > 4: return jsonify({"error": "Provide 2 to 4 ACMs."}), 400
        for acm in acms: groups_spec.append((acm.title(), parse_d(body.get('from')), parse_d(body.get('to'), True), acm.lower()))

    # Fetch with Projection and Parallel Slicing
    oldest_dt = min([g[1] for g in groups_spec if g[1] is not None], default=datetime(2023,1,1))
    newest_dt = max([g[2] for g in groups_spec if g[2] is not None], default=datetime.now()+timedelta(days=1))
    
    field_names = ["Submitted on Copy", "Submitted on", "Created Time", "Request Type", "Type", "Status", "Request Status", "Agency Status", "State", "Region", "Agency Region", "Acm Name (PK)", "Acm Name (IN)", "Acm", "Assigned Member", "Agency Type", "Type of Agency", "Closing Reason", "Closing Agencies Reason", "Otherapp Name", "Other App Name", "Other Apps", "Reject Reason", "Rejection Reason", "Create Way", "Creation Type"]
    all_items, fetch_complete, stop_reason = fetch_feishu_parallel_by_date(REQUESTS_TABLE_ID, "Submitted on Copy", oldest_dt, newest_dt, None, field_names)
    
    if not fetch_complete and not all_items: return jsonify({"error": f"Data fetch failed: {stop_reason}"}), 502

    groups = []
    for label, from_dt, to_dt, acm_filter in groups_spec:
        raw = run_analytics(all_items, from_dt, to_dt, region, acm_filter, rtype, perms.get("permissions",{}).get("acms",{}).get("analytics",["all"]), perms.get("permissions",{}).get("regions",{}).get("analytics",["all"]))
        kpis = raw.get("kpis", {})
        groups.append({
            "label": label, "kpis": {"creations": kpis.get("creations",0), "bds": kpis.get("bds",0), "closings": kpis.get("closings",0)},
            "closing_efficiency_pct": round((kpis.get("closings",0)/kpis.get("creations",1))*100, 1) if kpis.get("creations",0) else 0.0,
            "status_mix": {k: raw.get("creation_status",{}).get(k,0) + raw.get("bd_status",{}).get(k,0) + raw.get("closing_status",{}).get(k,0) for k in ["Done", "Rejected", "Under Investigation"]},
            "daily_trend": [{"date": d, "creations": raw.get("daily_trend_creation",{}).get(d,0), "bds": raw.get("daily_trend_bd",{}).get(d,0), "closings": raw.get("daily_trend_closing",{}).get(d,0)} for d in sorted(set(raw.get("daily_trend_creation",{})) | set(raw.get("daily_trend_bd",{})) | set(raw.get("daily_trend_closing",{})))],
            "top_reject_reasons": dict(list(raw.get("reject_reasons",{}).items())[:5]),
            "acm_performance": dict(list(raw.get("acm_performance",{}).items())[:8]),
            "scanned_rows": raw.get("scanned_rows", 0)
        })

    audit.log(user, "COMPARE_RUN", f"mode:{mode}|groups:{len(groups)}", ip=request.headers.get("X-Forwarded-For", ""), severity="Info")
    return jsonify({"mode": mode, "groups": groups, "fetch_complete": fetch_complete, "stop_reason": ("" if fetch_complete else stop_reason), "duration_ms": int((time.time() - start) * 1000)})

@app.route('/api/query', methods=['GET'])
@rate_limit(*RATE_LIMIT_RECORDS)
def query_records():
    user, email, field, value = [sanitize_text(request.args.get(k,'')).strip() for k in ('user','email','field','value')]
    if field not in QUERY_FIELD_ALIASES: return jsonify({"error": "Invalid search field."}), 400
    if not value: return jsonify({"error": "Please enter a value to search."}), 400

    perms = get_user_permissions(email, user)
    if not perms.get("is_super_admin") and not any("query" in m for m in perms.get("modules", [])): return jsonify({"error": "Access denied"}), 403

    audit.log(user, "QUERY_SEARCH", f"{field}={value}", ip=request.headers.get("X-Forwarded-For", ""), severity="Info")

    # Native Server-Side query translated directly from Frontend to Feishu
    field_names = ["Numbering", "Request Type", "Type", "Submitted on Copy", "Submitted on", "Created Time", "Respondents", "User ID", "Otherapp ID", "Otherapp Name", "Other App Name", "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member", "Region", "Agency Region", "Bd Code", "BD Code", "Status", "Request Status", "Reject Reason", "Rejection Reason", "Audition note", "Audition Note", "Duplicated Check"]
    
    aliases = QUERY_FIELD_ALIASES[field]
    combos = [(alias, op, (int(value),) if op == "=" else (value,)) for alias in aliases for op in (["contains", "is", "="] if not (op=="=" and not value.isdigit()) else [])]

    def try_combo(combo):
        alias, op, val_array = combo
        return {"combo": combo, "ok": True, "items": fetch_feishu_records_optimized(REQUESTS_TABLE_ID, {"conjunction": "and", "conditions": [{"field_name": alias, "operator": op, "value": val_array}]}, field_names)[0]}

    all_items = []
    with ThreadPoolExecutor(max_workers=min(5, len(combos) or 1)) as executor:
        for future in as_completed({executor.submit(try_combo, c): c for c in combos}):
            res = future.result()
            if res["ok"] and res["items"]:
                all_items = res["items"]
                break  # Stop as soon as we find a matching record set

    allowed_acms, allowed_regs = perms.get("permissions",{}).get("acms",{}).get("query",["all"]), perms.get("permissions",{}).get("regions",{}).get("query",["all"])
    allowed_acms_set, allowed_regs_set = set(a.lower() for a in allowed_acms), set(r.lower() for r in allowed_regs)

    results = []
    for item in all_items:
        f = item.get("fields", {})
        region = clean(get_field_local(f, "Region", "Agency Region"))
        acm_pk, acm_in, acm_fb = clean(get_field_local(f, "Acm Name (PK)")), clean(get_field_local(f, "Acm Name (IN)")), clean(get_field_local(f, "Acm", "Assigned Member"))
        if region in ("", "none"): region = "pk" if (acm_pk in PK_ACMS or acm_fb in PK_ACMS) else ("in" if (acm_in in IN_ACMS or acm_fb in IN_ACMS) else "")
        acm = (acm_in if region == "in" else acm_pk) or acm_fb

        if "all" not in allowed_acms_set and acm.lower().strip() not in allowed_acms_set: continue
        if "all" not in allowed_regs_set and region not in allowed_regs_set: continue

        submitted_raw = get_field_local(f, "Submitted on Copy", "Submitted on", "Created Time")
        submitted_dt  = parse_feishu_date(submitted_raw)

        results.append({
            "numbering":        extract_field_text(get_field_local(f, "Numbering")), "request_type": extract_field_text(get_field_local(f, "Request Type", "Type")),
            "submitted_on":     submitted_dt.strftime("%Y-%m-%d") if submitted_dt else extract_field_text(submitted_raw),
            "respondents":      extract_field_text(get_field_local(f, "Respondents")), "user_id": extract_field_text(get_field_local(f, "User ID")),
            "otherapp_id":      extract_field_text(get_field_local(f, "Otherapp ID", "Otherapp Name", "Other App Name")),
            "acm":              acm.title() if acm else "", "region": region.upper() if region else "",
            "bd_code":          extract_field_text(get_field_local(f, "Bd Code", "BD Code")), "status": extract_field_text(get_field_local(f, "Status", "Request Status")),
            "reject_reason":    extract_field_text(get_field_local(f, "Reject Reason", "Rejection Reason")), "audition_note": extract_field_text(get_field_local(f, "Audition note", "Audition Note")),
            "duplicated_check": extract_field_text(get_field_local(f, "Duplicated Check")), "_sort_ts": submitted_dt.timestamp() if submitted_dt else 0,
        })
    results.sort(key=lambda r: r["_sort_ts"], reverse=True)
    return jsonify({"results": results, "count": len(results), "field": field, "value": value})

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok", "ts": datetime.utcnow().isoformat(), "token_cached": _token_cache["token"] is not None,
        "token_expires_in_s": max(0, int(_token_cache["expires_at"] - time.time())), "mock_mode_active": MOCK_MODE
    })

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
