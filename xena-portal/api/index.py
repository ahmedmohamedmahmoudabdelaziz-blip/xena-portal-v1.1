import os
import time
import urllib.parse
import logging
import threading
from flask import Flask, request, jsonify, send_file, redirect
import requests
from datetime import datetime, timedelta, timezone

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- SECURE CONFIGURATION ---
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = "https://xena-portal-v1-1.vercel.app/api/callback"
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"

# 🚨 ACCESS MANAGEMENT TABLE ID
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx"

# 🚨 ONLY YOU ARE MASTER ADMIN NOW.
ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954']

# ACM Lists for auto-detecting blank regions
PK_ACMS = ["nabeel", "hasseb", "haseeb", "enzo", "farooq", "mubeen", "cruz", "ehtisham", "usama", "sehar ch", "hamza malik", "zohaib", "eagle", "leo", "berlin"]
IN_ACMS = ["holy", "vihan", "shivam", "ravikant", "ansh", "rocky", "bella"]

# =============================================================================
# 🚨 HIGH-PERFORMANCE CACHING LAYER
# =============================================================================
_cache_lock = threading.Lock()
_token_cache = {"value": None, "expires_at": 0}
_access_table_cache = {"items": None, "expires_at": 0}
ACCESS_TABLE_TTL_SECONDS = 60  

_analytics_cache = {}
_analytics_cache_lock = threading.Lock()
ANALYTICS_CACHE_TTL_SECONDS = 20 

def get_tenant_access_token():
    now = time.time()
    with _cache_lock:
        if _token_cache["value"] and now < _token_cache["expires_at"]:
            return _token_cache["value"]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    try:
        response = requests.post(url, json=payload, timeout=10).json()
        token = response.get("tenant_access_token")
        expire_in = response.get("expire", 7200) 

        with _cache_lock:
            _token_cache["value"] = token
            _token_cache["expires_at"] = now + max(expire_in - 60, 60)
        return token
    except Exception as e:
        logging.error(f"Token Error: {e}")
        return _token_cache["value"]

def _get_access_table_items():
    now = time.time()
    with _cache_lock:
        if _access_table_cache["items"] is not None and now < _access_table_cache["expires_at"]:
            return _access_table_cache["items"]

    tat = get_tenant_access_token()
    if not tat: return []
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    try:
        res = requests.get(url, headers=headers, params={"page_size": 500}, timeout=10).json()
        items = res.get("data", {}).get("items", [])
        with _cache_lock:
            _access_table_cache["items"] = items
            _access_table_cache["expires_at"] = now + ACCESS_TABLE_TTL_SECONDS
        return items
    except Exception as e:
        logging.error(f"Access Table Fetch Error: {e}")
        return _access_table_cache["items"] or []

def invalidate_access_cache():
    with _cache_lock:
        _access_table_cache["expires_at"] = 0

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
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
            if tgt in normalize_key(k):
                if v not in (None, "", []):
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

def parse_granular_string(raw_str):
    default = {"target": ["all"], "points": ["all"], "analytics": ["all"]}
    if not raw_str or str(raw_str).strip() == "": return default
    
    if "=" not in raw_str:
        parts = [x.strip().lower() for x in raw_str.split(",") if x.strip()]
        if not parts: parts = ["all"]
        return {"target": parts, "points": parts, "analytics": parts}
    
    res = {"target": ["all"], "points": ["all"], "analytics": ["all"]}
    for chunk in raw_str.split(";"):
        if "=" in chunk:
            mod, vals = chunk.split("=", 1)
            mod = mod.strip().lower()
            val_list = [v.strip().lower() for v in vals.split(",") if v.strip()]
            if not val_list: val_list = ["all"]
            if mod in res: res[mod] = val_list
    return res

# =============================================================================
# 🚨 BULLETPROOF ACCESS CONTROL (Now using Cache)
# =============================================================================
def get_user_permissions(email, name):
    name_clean = name.strip().lower() if name else ""
    email_clean = email.strip().lower() if email else ""
    
    if any(admin in name_clean for admin in ADMIN_USERS):
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin"], 
            "permissions": {
                "acms": {"target": ["all"], "points": ["all"], "analytics": ["all"]},
                "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"]}
            }
        }

    if not email_clean and not name_clean: 
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

    try:
        items = _get_access_table_items()
        
        for item in items:
            fields = item.get("fields", {})
            db_email = extract_field_text(fields.get("Email", "")).lower()
            db_person = extract_field_text(fields.get("Person", "")).lower()
            
            match_found = False
            if email_clean and (email_clean in db_email or email_clean in db_person): match_found = True
            if name_clean and (name_clean in db_email or name_clean in db_person): match_found = True
                
            if match_found:
                modules_raw = extract_field_text(get_field_local(fields, "Modules"))
                acms_raw = extract_field_text(get_field_local(fields, "ACMs"))
                regions_raw = extract_field_text(get_field_local(fields, "Regions"))
                
                modules = [m.strip().lower() for m in modules_raw.split(",") if m.strip()]
                is_admin = "admin" in modules
                
                parsed_acms = parse_granular_string(acms_raw)
                parsed_regions = parse_granular_string(regions_raw)
                
                return {
                    "is_super_admin": is_admin, 
                    "modules": modules, 
                    "permissions": {
                        "acms": parsed_acms,
                        "regions": parsed_regions
                    }
                }
                
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}
    except Exception as e:
        logging.error(f"Auth Error: {e}")
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

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
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    payload = {"grant_type": "authorization_code", "code": code}
    token_resp = requests.post(token_url, headers=headers, json=payload, timeout=10).json()

    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token: return "SSO Error: Could not verify user token.", 500

    info_url = "https://open.feishu.cn/open-apis/authen/v1/user_info"
    info_resp = requests.get(info_url, headers={"Authorization": f"Bearer {user_access_token}"}, timeout=10).json()

    data = info_resp.get("data", {})
    lark_name = data.get("name", "Unknown User")
    lark_email = data.get("email") or data.get("enterprise_email") or "" 
    
    return redirect(f"/?user={urllib.parse.quote(lark_name)}&email={urllib.parse.quote(lark_email)}&uat={user_access_token}")

@app.route('/api/auth/me', methods=['GET'])
def check_auth():
    username = request.args.get('user', '')
    email = request.args.get('email', '')
    perms = get_user_permissions(email, username)
    return jsonify(perms)

# =============================================================================
# 🚨 ADMIN PANEL ROUTES (Added Cache Invalidation)
# =============================================================================
@app.route('/api/admin/users', methods=['GET', 'POST', 'DELETE'])
def manage_users():
    admin_name = request.headers.get('X-User-Name', '').lower()
    
    is_authorized = any(admin in admin_name for admin in ADMIN_USERS)
    if not is_authorized:
        perms = get_user_permissions("", admin_name)
        if perms.get("is_super_admin"): is_authorized = True

    if not is_authorized: return jsonify({"error": "Unauthorized"}), 403

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"

    if request.method == 'GET':
        items = _get_access_table_items() # Uses cache to save time
        users = []
        for item in items:
            fields = item.get("fields", {})
            display_email = extract_field_text(fields.get("Email", ""))
            if not display_email: display_email = extract_field_text(fields.get("Person", ""))

            users.append({
                "id": item.get("record_id"),
                "email": display_email,
                "modules": extract_field_text(fields.get("Modules", "")),
                "acms_raw": extract_field_text(fields.get("ACMs", "")),
                "regions_raw": extract_field_text(fields.get("Regions", "all"))
            })
        return jsonify(users)

    elif request.method == 'POST':
        data = request.json
        email_to_check = data.get("email", "").strip()
        
        acms_formatted = f"target={data.get('acms', {}).get('target', 'all')};points={data.get('acms', {}).get('points', 'all')};analytics={data.get('acms', {}).get('analytics', 'all')}"
        regs_formatted = f"target={data.get('regions', {}).get('target', 'all')};points={data.get('regions', {}).get('points', 'all')};analytics={data.get('regions', {}).get('analytics', 'all')}"

        payload = {
            "fields": {
                "Email": email_to_check, 
                "Modules": data.get("modules", ""), 
                "ACMs": acms_formatted, 
                "Regions": regs_formatted
            }
        }
        
        items = _get_access_table_items()
        existing_record_id = None
        for item in items:
            db_email = extract_field_text(item.get("fields", {}).get("Email", "")).lower().strip()
            db_person = extract_field_text(item.get("fields", {}).get("Person", "")).lower().strip()
            target_check = email_to_check.lower().strip()
            
            if target_check and (target_check == db_email or target_check == db_person):
                existing_record_id = item["record_id"]
                break
        
        if existing_record_id:
            res = requests.put(f"{base_url}/{existing_record_id}", headers=headers, json=payload).json()
        else:
            res = requests.post(base_url, headers=headers, json=payload).json()
        
        if res.get("code") != 0:
            return jsonify({"success": False, "error": res.get("msg")}), 400
            
        invalidate_access_cache() # 🚨 Instantly clears cache so changes apply
        return jsonify({"success": True})

    elif request.method == 'DELETE':
        record_id = request.args.get('id')
        res = requests.delete(f"{base_url}/{record_id}", headers=headers).json()
        invalidate_access_cache() # 🚨 Instantly clears cache so changes apply
        return jsonify({"success": res.get("code") == 0})

@app.route('/api/search', methods=['GET'])
def search_agency():
    username = request.args.get('user', '')
    email = request.args.get('email', '')
    agency_code = request.args.get('code')
    uat = request.args.get('uat', '')
    inquiry_type = request.args.get('type', 'target').strip().lower()

    if not uat: return jsonify({"error": "Unauthorized session."}), 401
    if not agency_code: return jsonify({"error": "No agency code provided"}), 400

    perms = get_user_permissions(email, username)
    if inquiry_type not in perms["modules"] and not perms.get("is_super_admin"):
        return jsonify({"error": f"Access Denied: You do not have permission to view the {inquiry_type.title()} module."}), 403

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    points_payload = {
        "filter": {
            "conjunction": "and",
            "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]
        }
    }
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"

    points_response = requests.post(points_url, headers=headers, json=points_payload, timeout=10).json()
    if points_response.get("code") != 0: return jsonify({"error": f"Feishu API Blocked: {points_response.get('msg')}"}), 403

    items = points_response.get('data', {}).get('items', [])
    if not items: return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    region = clean(get_field_local(fields, 'Region', 'Agency Region'))
    sheet_acm_name = extract_field_text(get_field_local(fields, 'Acm Name (PK)', 'Acm Name (IN)', 'Acm', 'Assigned Member')).strip()
    
    if region in ('', 'none'):
        if sheet_acm_name.lower() in PK_ACMS:
            region = 'pk'
        elif sheet_acm_name.lower() in IN_ACMS:
            region = 'in'
    
    allowed_regs = perms.get("permissions", {}).get("regions", {}).get(inquiry_type, ["all"])
    allowed_acms = perms.get("permissions", {}).get("acms", {}).get(inquiry_type, ["all"])

    if "all" not in allowed_regs and region not in allowed_regs:
        display_reg = region.upper() if region else 'UNKNOWN'
        return jsonify({"error": f"Access Denied: Your profile restricts querying Region: {display_reg}"}), 403
        
    if "all" not in allowed_acms and sheet_acm_name.lower() not in allowed_acms:
        return jsonify({"error": f"Access Denied: You are not authorized to view data for ACM: {sheet_acm_name}"}), 403

    try: base_points = float(extract_field_text(get_field_local(fields, 'Base Points')).replace(',', '').strip())
    except ValueError: base_points = 0

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=points_payload, timeout=10).json()

    valid_requests = []
    if req_response.get("code") == 0:
        cm, cy = datetime.now().month, datetime.now().year
        for item in req_response.get('data', {}).get('items', []):
            r_fields = item.get('fields', {})
            ts = parse_feishu_date(get_field_local(r_fields, 'Submitted on Copy', 'Submitted on'))
            if ts and ts.month == cm and ts.year == cy:
                valid_requests.append(r_fields)

    return jsonify({"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified by Feishu"})

@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    username = request.args.get('user', '').lower()
    email = request.args.get('email', '')
    uat = request.args.get('uat', '')
    if not uat: return jsonify({"error": "Unauthorized session. Please log in again."}), 401
    
    perms = get_user_permissions(email, username)
    if "analytics" not in perms["modules"] and not perms.get("is_super_admin"): 
        return jsonify({"error": "Unauthorized. Analytics module restricted."}), 403

    allowed_regs = perms.get("permissions", {}).get("regions", {}).get("analytics", ["all"])
    allowed_acms = perms.get("permissions", {}).get("acms", {}).get("analytics", ["all"])

    region_filter = request.args.get('region', 'PK').strip().lower()
    if not region_filter: region_filter = 'pk'
    
    if region_filter == 'all' and "all" not in allowed_regs:
        return jsonify({"error": "Access Denied: Please specify a specific region filter you own."}), 403
    if region_filter != 'all' and "all" not in allowed_regs and region_filter not in allowed_regs:
        return jsonify({"error": f"Access Denied: You lack permissions for Region: {region_filter.upper()}."}), 403

    acm_filter = request.args.get('acm', 'All').strip().lower()
    if acm_filter == 'hasseb': acm_filter = 'haseeb'
    type_filter = request.args.get('type', 'All').strip().lower()
    date_from = request.args.get('from', '').strip()
    date_to = request.args.get('to', '').strip()

    # 🚨 HIGH-PERFORMANCE ANALYTICS CACHING
    cache_key = f"{region_filter}|{acm_filter}|{type_filter}|{date_from}|{date_to}"
    now = time.time()
    with _analytics_cache_lock:
        cached = _analytics_cache.get(cache_key)
        if cached and now < cached["expires_at"]:
            return jsonify(cached["data"])

    if date_from and date_to:
        dt1 = datetime.strptime(date_from, "%Y-%m-%d")
        dt2 = datetime.strptime(date_to, "%Y-%m-%d")
        if dt1 > dt2: dt1, dt2 = dt2, dt1
        from_dt, to_dt = dt1, dt2 + timedelta(days=1)
    else:
        now_dt = datetime.now()
        from_dt = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now_dt.month == 12: to_dt = now_dt.replace(year=now_dt.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else: to_dt = now_dt.replace(month=now_dt.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)

    tat = get_tenant_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}"})
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records"

    all_items, seen_ids, master_keys = [], set(), set()
    page_token, error_msg, fetch_complete, stop_reason, consecutive_old_pages = "", None, True, None, 0

    for page_num in range(150):
        params = {"page_size": 500, "automatic_fields": "true", "sort": '["Numbering DESC"]'}
        if page_token: params["page_token"] = page_token

        try:
            res = session.get(base_url, params=params, timeout=12)
            if res.status_code != 200:
                fetch_complete, stop_reason, error_msg = False, f"HTTP Error {res.status_code}: {res.text}", f"HTTP Error {res.status_code}: {res.text}"
                break

            res_json = res.json()
            if res_json.get("code") != 0:
                fetch_complete, stop_reason, error_msg = False, res_json.get("msg"), res_json.get("msg")
                break

            data_block = res_json.get("data", {})
            items = data_block.get("items", [])
            if not items: break

            page_old_count, valid_dates_in_page = 0, 0

            for item in items:
                rid = item.get("record_id")
                if rid and rid not in seen_ids:
                    seen_ids.add(rid)
                    all_items.append(item)
                    master_keys.update(item.get('fields', {}).keys())

                    raw_date = get_field_local(item.get('fields', {}), 'Submitted on Copy', 'Submitted on', 'Created Time')
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

            page_token = data_block.get("page_token")
            if not page_token or not data_block.get("has_more", False): break

        except Exception as e:
            fetch_complete, stop_reason, error_msg = False, str(e), str(e)
            break

    sample_keys = sorted(list(master_keys))

    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {},
        "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {},
        "acm_closing_reasons": {}, "daily_trend_creation": {}, "daily_trend_bd": {},        
        "other_request_types": {}, "scanned_rows": len(all_items),
        "error_debug": error_msg, "feishu_keys": sample_keys,
        "fetch_complete": fetch_complete, "stop_reason": stop_reason
    }

    if from_dt and to_dt:
        cur = from_dt
        while cur < to_dt:
            date_str = cur.strftime("%Y-%m-%d")
            stats["daily_trend_creation"][date_str] = 0
            stats["daily_trend_bd"][date_str] = 0
            cur += timedelta(days=1)

    for item in all_items:
        fields = item.get('fields', {})

        record_dt = parse_feishu_date(get_field_local(fields, 'Submitted on', 'Submitted on Copy', 'Created Time'))
        if from_dt or to_dt:
            if not record_dt or (from_dt and record_dt < from_dt) or (to_dt and record_dt >= to_dt): continue

        region = clean(get_field_local(fields, 'Region', 'Agency Region'))
        acm_pk = clean(get_field_local(fields, 'Acm Name (PK)'))
        acm_in = clean(get_field_local(fields, 'Acm Name (IN)'))
        acm_fallback = clean(get_field_local(fields, 'Acm', 'Assigned Member'))
        
        if region in ('', 'none'):
            if acm_pk in PK_ACMS or acm_fallback in PK_ACMS: region = 'pk'

        if region_filter != 'all' and region != region_filter: continue

        req_type = clean(get_field_local(fields, 'Request Type', 'Request type', 'Type', 'Category', 'Request Category'))
        status = clean(get_field_local(fields, 'Status', 'Request Status', 'Agency Status', 'State'))
        agency_type = clean(get_field_local(fields, 'Agency Type', 'Type of Agency'))
        closing_reason = clean(get_field_local(fields, 'Closing Reason', 'Closing Agencies Reason', 'PK Closing Agencies Reason'))
        other_app = clean(get_field_local(fields, 'Otherapp Name', 'Other App Name', 'Other Apps'))

        is_done = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        acm = acm_in if region == "in" else acm_pk
        if not acm: acm = acm_fallback
        
        if "all" not in allowed_acms and acm.lower().strip() not in allowed_acms: continue
        if acm_filter != 'all' and acm_filter != acm: continue

        agency_type_title = agency_type.title() if agency_type else "Unknown"
        if type_filter != 'all' and type_filter != agency_type: continue

        is_bd_kpi = "bd creation" in req_type
        is_closing_kpi = "closing agency" in req_type
        is_creation_kpi = any(p in req_type for p in [
            "agency creation",
            "agency applied already by acm or bd link ( follow-up )",
            "agency applied already",
            "follow-up",
            "follow up"
        ])

        if is_done and record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            if is_creation_kpi and date_str in stats["daily_trend_creation"]: stats["daily_trend_creation"][date_str] += 1
            if is_bd_kpi and date_str in stats["daily_trend_bd"]: stats["daily_trend_bd"][date_str] += 1

        if is_closing_kpi:
            stats["kpis"]["closings"] += 1
            if is_done: stats["closing_status"]["Done"] += 1
            elif is_rejected: stats["closing_status"]["Rejected"] += 1
            else: stats["closing_status"]["Under Investigation"] += 1

            if closing_reason:
                cr_title = closing_reason.title()
                stats["closing_reasons_pie"][cr_title] = stats["closing_reasons_pie"].get(cr_title, 0) + 1
                if acm:
                    clean_acm = acm.title()
                    if clean_acm not in stats["acm_closing_reasons"]: stats["acm_closing_reasons"][clean_acm] = {"User Request": 0, "Duplicated Hosting": 0}
                    if "user" in closing_reason: stats["acm_closing_reasons"][clean_acm]["User Request"] += 1
                    elif "dup" in closing_reason: stats["acm_closing_reasons"][clean_acm]["Duplicated Hosting"] += 1

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
                clean_acm = acm.title()
                stats["acm_performance"][clean_acm] = stats["acm_performance"].get(clean_acm, 0) + 1
            if is_done and other_app:
                oa_title = other_app.title()
                stats["other_apps"][oa_title] = stats["other_apps"].get(oa_title, 0) + 1
            if agency_type_title != "Unknown":
                stats["agency_types"][agency_type_title] = stats["agency_types"].get(agency_type_title, 0) + 1

            raw_creation_types = get_field_local(fields, 'Create Way', 'Creation Type', 'Agency Creation Type', 'PK Agencies Creation Type')
            for ct in extract_field_list(raw_creation_types):
                if ct:
                    ct_title = ct.title()
                    stats["creation_types"][ct_title] = stats["creation_types"].get(ct_title, 0) + 1

            if is_rejected:
                raw_reject_reasons = get_field_local(fields, 'Reject Reason', 'Rejection Reason', 'Agencies Rejection Reason', 'PK Agencies Rejection reason')
                for rr in extract_field_list(raw_reject_reasons):
                    if rr:
                        rr_title = rr.title()
                        stats["reject_reasons"][rr_title] = stats["reject_reasons"].get(rr_title, 0) + 1

        elif req_type:
            label = req_type.title()
            stats["other_request_types"][label] = stats["other_request_types"].get(label, 0) + 1

    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend_creation"] = dict(sorted(stats["daily_trend_creation"].items()))
    stats["daily_trend_bd"] = dict(sorted(stats["daily_trend_bd"].items()))
    stats["creation_types"] = dict(sorted(stats["creation_types"].items(), key=lambda x: x[1], reverse=True))
    stats["agency_types"] = dict(sorted(stats["agency_types"].items(), key=lambda x: x[1], reverse=True))
    stats["other_request_types"] = dict(sorted(stats["other_request_types"].items(), key=lambda x: x[1], reverse=True))

    # 🚨 SAVE TO MEMORY FOR 90 SECONDS
    with _analytics_cache_lock:
        _analytics_cache[cache_key] = {"data": stats, "expires_at": time.time() + ANALYTICS_CACHE_TTL_SECONDS}
        # Keep cache clean
        if len(_analytics_cache) > 50:
            oldest = min(_analytics_cache.items(), key=lambda kv: kv[1]["expires_at"])[0]
            del _analytics_cache[oldest]

    return jsonify(stats)
