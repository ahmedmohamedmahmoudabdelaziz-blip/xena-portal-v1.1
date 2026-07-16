import os
import time
import urllib.parse
import logging
import re
from flask import Flask, request, jsonify, send_file, redirect
import requests
from datetime import datetime, timedelta, timezone
from flask_caching import Cache
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# --- Structured Logging & Cache & Limits ---
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [XenaPortal] - %(message)s')
logger = logging.getLogger(__name__)

cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 300})
limiter = Limiter(get_remote_address, app=app, default_limits=["1000 per day", "200 per hour"])

# --- Centralized Config ---
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = "https://xena-portal-v1-1.vercel.app/api/callback"
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx"

ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954']
PK_ACMS = ["nabeel", "haseeb", "enzo", "farooq", "mubeen", "cruz", "ehtisham", "usama", "sehar ch", "hamza malik", "zohaib", "eagle", "leo", "berlin"]
IN_ACMS = ["holy", "vihan", "shivam", "ravikant", "ansh", "rocky", "bella"]

def mask_email(email):
    if not email or '@' not in email: return email
    name, domain = email.split('@', 1)
    return f"{name[0]}***{name[-1]}@{domain}" if len(name) > 2 else f"***@{domain}"

@cache.memoize(timeout=3500)
def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    res = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10).json()
    return res.get("tenant_access_token")

def normalize_key(k): return " ".join(str(k).lower().strip().split())

def get_field_local(fields, *aliases):
    if not fields: return None
    for alias in aliases:
        if alias in fields and fields[alias] not in (None, "", []): return fields[alias]
    for alias in aliases:
        tgt = normalize_key(alias)
        for k, v in fields.items():
            if normalize_key(k) == tgt and v not in (None, "", []): return v
    for alias in aliases:
        tgt = normalize_key(alias)
        for k, v in fields.items():
            if tgt in normalize_key(k) and v not in (None, "", []): return v
    return None

def extract_field_text(field_data):
    if not field_data: return ""
    if isinstance(field_data, (str, int, float)): return str(field_data)
    if isinstance(field_data, dict):
        for k in ['text', 'name', 'en_name', 'email', 'value', 'label', 'id']:
            if k in field_data: return str(field_data[k])
        return str(field_data.get('id', field_data))
    if isinstance(field_data, list):
        if not field_data: return ""
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                extracted = False
                for k in ['text', 'name', 'en_name', 'email', 'value', 'id']:
                    if k in item:
                        texts.append(str(item[k]))
                        extracted = True
                        break
                if not extracted: texts.append(str(item))
            else: texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)

def extract_field_list(field_data):
    if not field_data: return []
    if isinstance(field_data, dict):
        for k in ['text', 'name', 'en_name', 'email', 'value', 'label']:
            if k in field_data and field_data[k] not in (None, ""): return [str(field_data[k]).strip()]
        return [str(field_data.get('id', field_data)).strip()]
    if isinstance(field_data, str): return [s.strip() for s in field_data.split(',') if s.strip()]
    if isinstance(field_data, list):
        res = []
        for item in field_data:
            if not item: continue
            if isinstance(item, dict):
                ext = False
                for k in ['text', 'name', 'en_name', 'email', 'value', 'label']:
                    if k in item and item[k] not in (None, ""):
                        res.append(str(item[k]).strip())
                        ext = True
                        break
                if not ext and 'id' in item and item['id'] not in (None, ""): res.append(str(item['id']).strip())
                elif not ext: res.append(str(item).strip())
            else: res.append(str(item).strip())
        return res
    return [str(field_data).strip()]

def parse_feishu_date(date_val):
    if not date_val: return None
    if isinstance(date_val, list) and len(date_val) > 0: date_val = date_val[0]
    if isinstance(date_val, dict): date_val = date_val.get('value', date_val.get('text', ''))
    try:
        if isinstance(date_val, (int, float)) or (isinstance(date_val, str) and date_val.strip().isdigit()):
            ts = float(date_val) if isinstance(date_val, (int, float)) else float(date_val.strip())
            dt_utc = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            dt_cairo = dt_utc + timedelta(hours=3)
            return dt_cairo.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        clean_str = str(date_val)[:10].replace('/', '-').replace('.', '-')
        return datetime.strptime(clean_str, "%Y-%m-%d")
    except: return None

def clean(field_data): return extract_field_text(field_data).strip().lower()

def parse_granular_string(raw_str):
    default = {"target": ["all"], "points": ["all"], "analytics": ["all"]}
    if not raw_str or str(raw_str).strip() == "": return default
    if "=" not in raw_str:
        parts = [x.strip().lower() for x in raw_str.split(",") if x.strip()]
        return {"target": parts or ["all"], "points": parts or ["all"], "analytics": parts or ["all"]}
    
    res = {"target": ["all"], "points": ["all"], "analytics": ["all"]}
    for chunk in raw_str.split(";"):
        if "=" in chunk:
            mod, vals = chunk.split("=", 1)
            val_list = [v.strip().lower() for v in vals.split(",") if v.strip()]
            if mod.strip().lower() in res: res[mod.strip().lower()] = val_list or ["all"]
    return res

# --- Core Access Control ---
@cache.memoize(timeout=60)
def get_user_permissions(email, name):
    nc, ec = (name or "").strip().lower(), (email or "").strip().lower()
    if any(admin in nc for admin in ADMIN_USERS):
        return {"is_super_admin": True, "modules": ["target", "points", "analytics", "admin"], "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"]}, "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"]}}}

    if not ec and not nc: return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    try:
        res = requests.get(url, headers={"Authorization": f"Bearer {tat}"}, params={"page_size": 500}, timeout=10).json()
        for item in res.get("data", {}).get("items", []):
            f = item.get("fields", {})
            db_e, db_p = extract_field_text(f.get("Email", "")).lower(), extract_field_text(f.get("Person", "")).lower()
            if (ec and (ec in db_e or ec in db_p)) or (nc and (nc in db_e or nc in db_p)):
                mods = [m.strip().lower() for m in extract_field_text(get_field_local(f, "Modules")).split(",") if m.strip()]
                return {"is_super_admin": "admin" in mods, "modules": mods, "permissions": {"acms": parse_granular_string(extract_field_text(get_field_local(f, "ACMs"))), "regions": parse_granular_string(extract_field_text(get_field_local(f, "Regions")))}}
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}
    except Exception as e:
        logger.error(f"Auth Error: {str(e)}")
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

@app.route('/')
def home(): return send_file(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'index.html'))

@app.route('/api/login')
@limiter.limit("10 per minute")
def login(): return redirect(f"https://open.feishu.cn/open-apis/authen/v1/index?app_id={APP_ID}&redirect_uri={urllib.parse.quote(REDIRECT_URI)}")

@app.route('/api/callback')
def callback():
    code = request.args.get('code')
    if not code: return "SSO Authorization Failed.", 400
    tat = get_tenant_access_token()
    token_resp = requests.post("https://open.feishu.cn/open-apis/authen/v1/oidc/access_token", headers={"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}, json={"grant_type": "authorization_code", "code": code}, timeout=10).json()
    uat = token_resp.get("data", {}).get("access_token")
    if not uat: return "SSO Error: Could not verify user token.", 500
    info_resp = requests.get("https://open.feishu.cn/open-apis/authen/v1/user_info", headers={"Authorization": f"Bearer {uat}"}, timeout=10).json()
    data = info_resp.get("data", {})
    ln, le = data.get("name", "Unknown User"), data.get("email") or data.get("enterprise_email") or "" 
    logger.info(f"USER_LOGIN: {ln} ({mask_email(le)})")
    return redirect(f"/?user={urllib.parse.quote(ln)}&email={urllib.parse.quote(le)}&uat={uat}")

@app.route('/api/auth/me')
def check_auth(): return jsonify(get_user_permissions(request.args.get('email', ''), request.args.get('user', '')))

@app.route('/api/admin/users', methods=['GET', 'POST', 'DELETE'])
def manage_users():
    admin_name = request.headers.get('X-User-Name', '').lower()
    is_authorized = any(admin in admin_name for admin in ADMIN_USERS) or get_user_permissions("", admin_name).get("is_super_admin")
    if not is_authorized: return jsonify({"error": "Unauthorized"}), 403

    tat = get_tenant_access_token()
    headers, base_url = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}, f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"

    if request.method == 'GET':
        res = requests.get(base_url, headers=headers, params={"page_size": 500}).json()
        users = [{"id": i.get("record_id"), "email": extract_field_text(i.get("fields", {}).get("Email", "")) or extract_field_text(i.get("fields", {}).get("Person", "")), "modules": extract_field_text(i.get("fields", {}).get("Modules", "")), "acms_raw": extract_field_text(i.get("fields", {}).get("ACMs", "")), "regions_raw": extract_field_text(i.get("fields", {}).get("Regions", "all"))} for i in res.get("data", {}).get("items", [])]
        return jsonify(users)

    elif request.method == 'POST':
        data = request.json
        email_check = data.get("email", "").strip()
        logger.info(f"ADMIN_ACTION: {admin_name} updated user {mask_email(email_check)}")
        
        payload = {"fields": {"Email": email_check, "Modules": data.get("modules", ""), "ACMs": f"target={data.get('acms', {}).get('target', 'all')};points={data.get('acms', {}).get('points', 'all')};analytics={data.get('acms', {}).get('analytics', 'all')}", "Regions": f"target={data.get('regions', {}).get('target', 'all')};points={data.get('regions', {}).get('points', 'all')};analytics={data.get('regions', {}).get('analytics', 'all')"}}
        
        res_all = requests.get(base_url, headers=headers, params={"page_size": 500}).json()
        existing_id = next((i["record_id"] for i in res_all.get("data", {}).get("items", []) if email_check.lower() in (extract_field_text(i.get("fields", {}).get("Email", "")).lower(), extract_field_text(i.get("fields", {}).get("Person", "")).lower())), None)
        
        res = requests.put(f"{base_url}/{existing_id}", headers=headers, json=payload).json() if existing_id else requests.post(base_url, headers=headers, json=payload).json()
        cache.delete_memoized(get_user_permissions)
        return jsonify({"success": res.get("code") == 0, "error": res.get("msg")})

    elif request.method == 'DELETE':
        record_id = request.args.get('id')
        logger.info(f"ADMIN_ACTION: {admin_name} deleted user record {record_id}")
        res = requests.delete(f"{base_url}/{record_id}", headers=headers).json()
        cache.delete_memoized(get_user_permissions)
        return jsonify({"success": res.get("code") == 0})

@app.route('/api/search', methods=['GET'])
@limiter.limit("20 per minute")
def search_agency():
    username, email, agency_code, uat, inquiry_type = request.args.get('user', ''), request.args.get('email', ''), request.args.get('code'), request.args.get('uat', ''), request.args.get('type', 'target').strip().lower()

    if not uat: return jsonify({"error": "Unauthorized session."}), 401
    if not agency_code or not re.match(r'^\d+$', agency_code): return jsonify({"error": "Invalid agency code format"}), 400

    perms = get_user_permissions(email, username)
    if inquiry_type not in perms["modules"] and not perms.get("is_super_admin"): return jsonify({"error": f"Access Denied: {inquiry_type.title()} module."}), 403

    headers = {"Authorization": f"Bearer {get_tenant_access_token()}", "Content-Type": "application/json"}
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    
    p_res = requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true", headers=headers, json=points_payload, timeout=10).json()
    if p_res.get("code") != 0: return jsonify({"error": f"Feishu API Blocked: {p_res.get('msg')}"}), 403

    items = p_res.get('data', {}).get('items', [])
    if not items: return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    region, acm = clean(get_field_local(fields, 'Region', 'Agency Region')), extract_field_text(get_field_local(fields, 'Acm Name (PK)', 'Acm Name (IN)', 'Acm', 'Assigned Member')).strip()
    if region in ('', 'none'): region = 'pk' if acm.lower() in PK_ACMS else 'in' if acm.lower() in IN_ACMS else region
    
    a_regs, a_acms = perms.get("permissions", {}).get("regions", {}).get(inquiry_type, ["all"]), perms.get("permissions", {}).get("acms", {}).get(inquiry_type, ["all"])
    if "all" not in a_regs and region not in a_regs: return jsonify({"error": f"Access Denied: Region {region.upper()}"}), 403
    if "all" not in a_acms and acm.lower() not in a_acms: return jsonify({"error": f"Access Denied: ACM {acm}"}), 403

    try: bp = float(extract_field_text(get_field_local(fields, 'Base Points')).replace(',', '').strip())
    except ValueError: bp = 0
    try: tp = float(extract_field_text(get_field_local(fields, '# Total Points', 'Total Points', 'Total')).replace(',', '').strip())
    except ValueError: tp = 0
    try: up = float(extract_field_text(get_field_local(fields, 'Used Points', 'Used')).replace(',', '').strip())
    except ValueError: up = 0
    try: pb = float(extract_field_text(get_field_local(fields, 'Point Balance', 'Balance')).replace(',', '').strip())
    except ValueError: pb = 0
    
    if pb == 0 and tp > 0: pb = tp - up
    
    # Phase 5: Agency Health Score Calculation
    health_score = 100
    health_status = "Healthy"
    if tp > 0:
        utilization = up / tp
        if utilization > 0.90: health_score = 40; health_status = "Critical"
        elif utilization > 0.70: health_score = 70; health_status = "At Risk"
        else: health_score = 95; health_status = "Healthy"
    else: health_score = 0; health_status = "Inactive"

    # Get ALL historical requests (no month filter)
    req_res = requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true", headers=headers, json=points_payload, timeout=10).json()
    
    valid_requests = []
    if req_res.get("code") == 0:
        for item in req_res.get('data', {}).get('items', []):
            r_fields = item.get('fields', {})
            r_fields['_timestamp'] = parse_feishu_date(get_field_local(r_fields, 'Submitted on Copy', 'Submitted on'))
            valid_requests.append(r_fields)

    return jsonify({
        "base_points": bp,
        "total_points": tp,
        "used_points": up,
        "point_balance": pb,
        "monthly_tracker": extract_field_text(get_field_local(fields, 'Monthly Usage Tracker', 'Latest Usage Tracker')),
        "requests": valid_requests,
        "acm": acm.title(),
        "health_score": health_score,
        "health_status": health_status
    })

@app.route('/api/analytics', methods=['GET'])
@limiter.limit("5 per minute")
@cache.cached(timeout=300, query_string=True)
def get_analytics():
    username, email, uat = request.args.get('user', '').lower(), request.args.get('email', ''), request.args.get('uat', '')
    if not uat: return jsonify({"error": "Unauthorized session. Please log in again."}), 401
    
    perms = get_user_permissions(email, username)
    if "analytics" not in perms["modules"] and not perms.get("is_super_admin"): 
        return jsonify({"error": "Unauthorized. Analytics module restricted."}), 403

    allowed_regs = perms.get("permissions", {}).get("regions", {}).get("analytics", ["all"])
    allowed_acms = perms.get("permissions", {}).get("acms", {}).get("analytics", ["all"])

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {get_tenant_access_token()}"})

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

    if date_from and date_to:
        dt1 = datetime.strptime(date_from, "%Y-%m-%d")
        dt2 = datetime.strptime(date_to, "%Y-%m-%d")
        if dt1 > dt2: dt1, dt2 = dt2, dt1
        from_dt, to_dt = dt1, dt2 + timedelta(days=1)
    else:
        now = datetime.now()
        from_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12: to_dt = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else: to_dt = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)

    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records"

    all_items = []
    seen_ids = set()
    master_keys = set()
    page_token = ""
    error_msg = None
    fetch_complete = True
    stop_reason = None
    consecutive_old_pages = 0

    for _ in range(150):
        params = {"page_size": 500, "automatic_fields": "true", "sort": '["Numbering DESC"]'}
        if page_token: params["page_token"] = page_token

        try:
            res = session.get(base_url, params=params, timeout=12)
            if res.status_code != 200:
                fetch_complete = False
                error_msg = f"HTTP Error {res.status_code}: {res.text}"
                stop_reason = error_msg
                break

            res_json = res.json()
            if res_json.get("code") != 0:
                fetch_complete = False
                error_msg = res_json.get("msg")
                stop_reason = error_msg
                break

            data_block = res_json.get("data", {})
            items = data_block.get("items", [])
            if not items: break

            page_old_count = 0
            valid_dates_in_page = 0

            for item in items:
                rid = item.get("record_id")
                if rid and rid not in seen_ids:
                    seen_ids.add(rid)
                    all_items.append(item)
                    master_keys.update(item.get('fields', {}).keys())

                    record_dt = parse_feishu_date(get_field_local(item.get('fields', {}), 'Submitted on Copy', 'Submitted on', 'Created Time'))
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
            fetch_complete = False
            error_msg = str(e)
            stop_reason = error_msg
            break

    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {},
        "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {},
        "acm_closing_reasons": {}, 
        "daily_trend_creation": {},  
        "daily_trend_bd": {},        
        "other_request_types": {}, "scanned_rows": len(all_items),
        "error_debug": error_msg, "feishu_keys": sorted(list(master_keys)),
        "fetch_complete": fetch_complete, "stop_reason": stop_reason
    }

    if from_dt and to_dt:
        cur = from_dt
        while cur < to_dt:
            d_str = cur.strftime("%Y-%m-%d")
            stats["daily_trend_creation"][d_str] = 0
            stats["daily_trend_bd"][d_str] = 0
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
            if acm_pk in PK_ACMS or acm_fallback in PK_ACMS:
                region = 'pk'

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
            d_str = record_dt.strftime("%Y-%m-%d")
            if is_creation_kpi and d_str in stats["daily_trend_creation"]:
                stats["daily_trend_creation"][d_str] += 1
            if is_bd_kpi and d_str in stats["daily_trend_bd"]:
                stats["daily_trend_bd"][d_str] += 1

        if is_closing_kpi:
            stats["kpis"]["closings"] += 1
            stats["closing_status"]["Done" if is_done else "Rejected" if is_rejected else "Under Investigation"] += 1
            if closing_reason:
                cr_title = closing_reason.title()
                stats["closing_reasons_pie"][cr_title] = stats["closing_reasons_pie"].get(cr_title, 0) + 1
                if acm:
                    clean_acm = acm.title()
                    if clean_acm not in stats["acm_closing_reasons"]:
                        stats["acm_closing_reasons"][clean_acm] = {"User Request": 0, "Duplicated Hosting": 0}
                    stats["acm_closing_reasons"][clean_acm]["User Request" if "user" in closing_reason else "Duplicated Hosting"] += 1

        elif is_bd_kpi:
            stats["kpis"]["bds"] += 1
            stats["bd_status"]["Done" if is_done else "Rejected" if is_rejected else "Under Investigation"] += 1

        elif is_creation_kpi:
            stats["kpis"]["creations"] += 1
            stats["creation_status"]["Done" if is_done else "Rejected" if is_rejected else "Under Investigation"] += 1
            if is_done and acm: stats["acm_performance"][acm.title()] = stats["acm_performance"].get(acm.title(), 0) + 1
            if is_done and other_app: stats["other_apps"][other_app.title()] = stats["other_apps"].get(other_app.title(), 0) + 1
            if agency_type: stats["agency_types"][agency_type.title()] = stats["agency_types"].get(agency_type.title(), 0) + 1
            for ct in extract_field_list(get_field_local(fields, 'Create Way', 'Creation Type', 'Agency Creation Type', 'PK Agencies Creation Type')):
                if ct: stats["creation_types"][ct.title()] = stats["creation_types"].get(ct.title(), 0) + 1
            if is_rejected:
                for rr in extract_field_list(get_field_local(fields, 'Reject Reason', 'Rejection Reason', 'Agencies Rejection Reason', 'PK Agencies Rejection reason')):
                    if rr: stats["reject_reasons"][rr.title()] = stats["reject_reasons"].get(rr.title(), 0) + 1

        elif req_type:
            label = req_type.title()
            stats["other_request_types"][label] = stats["other_request_types"].get(label, 0) + 1

    # Sorting for consistent output
    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend_creation"] = dict(sorted(stats["daily_trend_creation"].items()))
    stats["daily_trend_bd"] = dict(sorted(stats["daily_trend_bd"].items()))
    stats["creation_types"] = dict(sorted(stats["creation_types"].items(), key=lambda x: x[1], reverse=True))
    stats["agency_types"] = dict(sorted(stats["agency_types"].items(), key=lambda x: x[1], reverse=True))
    stats["other_request_types"] = dict(sorted(stats["other_request_types"].items(), key=lambda x: x[1], reverse=True))

    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True)
