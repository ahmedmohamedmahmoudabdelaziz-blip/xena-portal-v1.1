import os
import time
import urllib.parse
import logging
from flask import Flask, request, jsonify, send_file, redirect
import requests
from datetime import datetime, timedelta, timezone

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SECURE CONFIGURATION ---
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = "https://xena-portal-v1-1.vercel.app/api/callback"
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx"

# 🚨 ONLY YOU ARE MASTER ADMIN NOW. Everyone else must be added via the Website Admin Panel.
ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954']

PK_ACMS = ["nabeel", "hasseb", "haseeb", "enzo", "farooq", "mubeen", "cruz", "ehtisham", "usama", "sehar ch", "hamza malik", "zohaib", "eagle", "leo", "berlin"]
IN_ACMS = ["holy", "vihan", "shivam", "ravikant", "ansh", "rocky", "bella"]

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, json=payload, timeout=10).json()
    return response.get("tenant_access_token")

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
            if key in field_data and field_data[key] not in (None, ""): return [str(field_data[key]).strip()]
        if 'id' in field_data and field_data['id'] not in (None, ""): return [str(field_data['id']).strip()]
        return [str(field_data).strip()]
    if isinstance(field_data, str): return [s.strip() for s in field_data.split(',') if s.strip()]
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
        if isinstance(date_val, (int, float)):
            dt_utc = datetime.fromtimestamp(date_val / 1000.0, tz=timezone.utc)
            return (dt_utc + timedelta(hours=3)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        date_str = str(date_val).strip()
        if date_str.isdigit():
            dt_utc = datetime.fromtimestamp(int(date_str) / 1000.0, tz=timezone.utc)
            return (dt_utc + timedelta(hours=3)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        clean_str = date_str[:10].replace('/', '-').replace('.', '-')
        return datetime.strptime(clean_str, "%Y-%m-%d")
    except Exception: return None

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

def get_user_permissions(email, name):
    name_clean = name.strip().lower() if name else ""
    email_clean = email.strip().lower() if email else ""
    
    if any(admin in name_clean for admin in ADMIN_USERS):
        return {
            "is_super_admin": True, "modules": ["target", "points", "analytics", "admin"], 
            "permissions": {"acms": {"target": ["all"], "points": ["all"], "analytics": ["all"]}, "regions": {"target": ["all"], "points": ["all"], "analytics": ["all"]}}
        }

    if not email_clean and not name_clean: return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"
    try:
        res = requests.get(url, headers={"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}, params={"page_size": 500}, timeout=10).json()
        for item in res.get("data", {}).get("items", []):
            fields = item.get("fields", {})
            db_email = extract_field_text(fields.get("Email", "")).lower()
            db_person = extract_field_text(fields.get("Person", "")).lower()
            
            if (email_clean and (email_clean in db_email or email_clean in db_person)) or (name_clean and (name_clean in db_email or name_clean in db_person)):
                modules_raw = extract_field_text(get_field_local(fields, "Modules"))
                modules = [m.strip().lower() for m in modules_raw.split(",") if m.strip()]
                return {
                    "is_super_admin": "admin" in modules, "modules": modules, 
                    "permissions": { "acms": parse_granular_string(extract_field_text(get_field_local(fields, "ACMs"))), "regions": parse_granular_string(extract_field_text(get_field_local(fields, "Regions"))) }
                }
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}
    except Exception as e:
        logging.error(f"Auth Error: {str(e)}")
        return {"is_super_admin": False, "modules": [], "permissions": {"acms": {}, "regions": {}}}

@app.route('/', methods=['GET'])
def home():
    return send_file(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'index.html'))

@app.route('/api/login', methods=['GET'])
def login():
    return redirect(f"https://open.feishu.cn/open-apis/authen/v1/index?app_id={APP_ID}&redirect_uri={urllib.parse.quote(REDIRECT_URI)}")

@app.route('/api/callback', methods=['GET'])
def callback():
    code = request.args.get('code')
    if not code: return "SSO Authorization Failed.", 400
    tat = get_tenant_access_token()
    token_resp = requests.post("https://open.feishu.cn/open-apis/authen/v1/oidc/access_token", headers={"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}, json={"grant_type": "authorization_code", "code": code}, timeout=10).json()
    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token: return "SSO Error: Could not verify user token.", 500
    info_resp = requests.get("https://open.feishu.cn/open-apis/authen/v1/user_info", headers={"Authorization": f"Bearer {user_access_token}"}, timeout=10).json()
    data = info_resp.get("data", {})
    return redirect(f"/?user={urllib.parse.quote(data.get('name', 'Unknown User'))}&email={urllib.parse.quote(data.get('email') or data.get('enterprise_email') or '')}&uat={user_access_token}")

@app.route('/api/auth/me', methods=['GET'])
def check_auth():
    return jsonify(get_user_permissions(request.args.get('email', ''), request.args.get('user', '')))

@app.route('/api/admin/users', methods=['GET', 'POST', 'DELETE'])
def manage_users():
    admin_name = request.headers.get('X-User-Name', '').lower()
    if not any(admin in admin_name for admin in ADMIN_USERS) and not get_user_permissions("", admin_name).get("is_super_admin"): return jsonify({"error": "Unauthorized"}), 403
    tat = get_tenant_access_token()
    headers, base_url = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}, f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"

    if request.method == 'GET':
        res = requests.get(base_url, headers=headers, params={"page_size": 500}).json()
        return jsonify([{"id": i.get("record_id"), "email": extract_field_text(i.get("fields", {}).get("Email", "")) or extract_field_text(i.get("fields", {}).get("Person", "")), "modules": extract_field_text(i.get("fields", {}).get("Modules", "")), "acms_raw": extract_field_text(i.get("fields", {}).get("ACMs", "")), "regions_raw": extract_field_text(i.get("fields", {}).get("Regions", "all"))} for i in res.get("data", {}).get("items", [])])

    elif request.method == 'POST':
        data = request.json
        email_check = data.get("email", "").strip().lower()
        payload = {"fields": {"Email": email_check, "Modules": data.get("modules", ""), "ACMs": f"target={data.get('acms', {}).get('target', 'all')};points={data.get('acms', {}).get('points', 'all')};analytics={data.get('acms', {}).get('analytics', 'all')}", "Regions": f"target={data.get('regions', {}).get('target', 'all')};points={data.get('regions', {}).get('points', 'all')};analytics={data.get('regions', {}).get('analytics', 'all')}"}}
        
        existing_id = next((i["record_id"] for i in requests.get(base_url, headers=headers, params={"page_size": 500}).json().get("data", {}).get("items", []) if email_check in [extract_field_text(i.get("fields", {}).get("Email", "")).lower().strip(), extract_field_text(i.get("fields", {}).get("Person", "")).lower().strip()]), None)
        
        res = requests.put(f"{base_url}/{existing_id}", headers=headers, json=payload).json() if existing_id else requests.post(base_url, headers=headers, json=payload).json()
        return jsonify({"success": True}) if res.get("code") == 0 else jsonify({"success": False, "error": res.get("msg")}), 400

    elif request.method == 'DELETE':
        return jsonify({"success": requests.delete(f"{base_url}/{request.args.get('id')}", headers=headers).json().get("code") == 0})

@app.route('/api/search', methods=['GET'])
def search_agency():
    if not request.args.get('uat', ''): return jsonify({"error": "Unauthorized session."}), 401
    if not request.args.get('code'): return jsonify({"error": "No agency code provided"}), 400
    inquiry_type = request.args.get('type', 'target').strip().lower()
    perms = get_user_permissions(request.args.get('email', ''), request.args.get('user', ''))
    if inquiry_type not in perms["modules"] and not perms.get("is_super_admin"): return jsonify({"error": f"Access Denied: You do not have permission to view the {inquiry_type.title()} module."}), 403

    headers = {"Authorization": f"Bearer {get_tenant_access_token()}", "Content-Type": "application/json"}
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [request.args.get('code')]}]}}
    
    start_time = time.time()
    points_resp = requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true", headers=headers, json=points_payload, timeout=10).json()
    logging.info(f"Points DB Fetch Time: {time.time() - start_time:.2f}s")
    
    if points_resp.get("code") != 0: return jsonify({"error": f"Feishu API Blocked: {points_resp.get('msg')}"}), 403
    items = points_resp.get('data', {}).get('items', [])
    if not items: return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {request.args.get('code')} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    region = clean(get_field_local(fields, 'Region', 'Agency Region'))
    sheet_acm_name = extract_field_text(get_field_local(fields, 'Acm Name (PK)', 'Acm Name (IN)', 'Acm', 'Assigned Member')).strip()
    if region in ('', 'none'): region = 'pk' if sheet_acm_name.lower() in PK_ACMS else 'in' if sheet_acm_name.lower() in IN_ACMS else region
    
    if "all" not in perms.get("permissions", {}).get("regions", {}).get(inquiry_type, ["all"]) and region not in perms.get("permissions", {}).get("regions", {}).get(inquiry_type, ["all"]): return jsonify({"error": f"Access Denied: Your profile restricts querying Region: {region.upper() if region else 'UNKNOWN'}"}), 403
    if "all" not in perms.get("permissions", {}).get("acms", {}).get(inquiry_type, ["all"]) and sheet_acm_name.lower() not in perms.get("permissions", {}).get("acms", {}).get(inquiry_type, ["all"]): return jsonify({"error": f"Access Denied: You are not authorized to view data for ACM: {sheet_acm_name}"}), 403

    try: base_points = float(extract_field_text(get_field_local(fields, 'Base Points')).replace(',', '').strip())
    except ValueError: base_points = 0
    try: total_points = float(extract_field_text(get_field_local(fields, '# Total Points', 'Total Points', 'Total', 'Total points')).replace(',', '').strip())
    except ValueError: total_points = 0
    try: used_points = float(extract_field_text(get_field_local(fields, 'Used Points', 'Used', 'Used points')).replace(',', '').strip())
    except ValueError: used_points = 0
    try: point_balance = float(extract_field_text(get_field_local(fields, 'Point Balance', 'Balance', 'Point balance')).replace(',', '').strip())
    except ValueError: point_balance = 0
    
    if point_balance == 0 and total_points > 0: point_balance = total_points - used_points
    monthly_tracker = extract_field_text(get_field_local(fields, 'Monthly Usage Tracker', 'Monthly Usage', 'Usage Tracker', 'Latest Usage Tracker'))

    req_response = requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true", headers=headers, json=points_payload, timeout=10).json()
    valid_requests = [r.get('fields', {}) for r in req_response.get('data', {}).get('items', []) if parse_feishu_date(get_field_local(r.get('fields', {}), 'Submitted on Copy', 'Submitted on')) and parse_feishu_date(get_field_local(r.get('fields', {}), 'Submitted on Copy', 'Submitted on')).month == datetime.now().month and parse_feishu_date(get_field_local(r.get('fields', {}), 'Submitted on Copy', 'Submitted on')).year == datetime.now().year] if req_response.get("code") == 0 else []

    return jsonify({"base_points": base_points, "total_points": total_points, "used_points": used_points, "point_balance": point_balance, "monthly_tracker": monthly_tracker, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified by Feishu"})

@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    if not request.args.get('uat', ''): return jsonify({"error": "Unauthorized session. Please log in again."}), 401
    perms = get_user_permissions(request.args.get('email', ''), request.args.get('user', '').lower())
    if "analytics" not in perms["modules"] and not perms.get("is_super_admin"): return jsonify({"error": "Unauthorized. Analytics module restricted."}), 403

    allowed_regs, allowed_acms = perms.get("permissions", {}).get("regions", {}).get("analytics", ["all"]), perms.get("permissions", {}).get("acms", {}).get("analytics", ["all"])
    region_filter = request.args.get('region', 'PK').strip().lower() or 'pk'
    if region_filter == 'all' and "all" not in allowed_regs: return jsonify({"error": "Access Denied: Please specify a specific region filter you own."}), 403
    if region_filter != 'all' and "all" not in allowed_regs and region_filter not in allowed_regs: return jsonify({"error": f"Access Denied: You lack permissions for Region: {region_filter.upper()}."}), 403

    acm_filter, type_filter = 'haseeb' if request.args.get('acm', 'All').strip().lower() == 'hasseb' else request.args.get('acm', 'All').strip().lower(), request.args.get('type', 'All').strip().lower()
    
    if request.args.get('from', '').strip() and request.args.get('to', '').strip():
        dt1, dt2 = datetime.strptime(request.args.get('from', '').strip(), "%Y-%m-%d"), datetime.strptime(request.args.get('to', '').strip(), "%Y-%m-%d")
        if dt1 > dt2: dt1, dt2 = dt2, dt1
        from_dt, to_dt = dt1, dt2 + timedelta(days=1)
    else:
        now = datetime.now()
        from_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        to_dt = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0) if now.month == 12 else now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {get_tenant_access_token()}"})
    all_items, seen_ids, master_keys, page_token, error_msg, fetch_complete, stop_reason, consecutive_old_pages = [], set(), set(), "", None, True, None, 0
    start_time = time.time()

    for page_num in range(150):
        params = {"page_size": 500, "automatic_fields": "true", "sort": '["Numbering DESC"]'}
        if page_token: params["page_token"] = page_token

        try:
            res = session.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records", params=params, timeout=12)
            if res.status_code != 200 or res.json().get("code") != 0:
                fetch_complete, error_msg, stop_reason = False, res.json().get("msg") if res.status_code == 200 else f"HTTP Error {res.status_code}: {res.text}", res.json().get("msg") if res.status_code == 200 else f"HTTP Error {res.status_code}: {res.text}"
                break
            
            data_block = res.json().get("data", {})
            items = data_block.get("items", [])
            if not items: break

            page_old_count, valid_dates_in_page = 0, 0
            for item in items:
                rid = item.get("record_id")
                if rid and rid not in seen_ids:
                    seen_ids.add(rid); all_items.append(item); master_keys.update(item.get('fields', {}).keys())
                    record_dt = parse_feishu_date(get_field_local(item.get('fields', {}), 'Submitted on Copy', 'Submitted on', 'Created Time'))
                    if record_dt:
                        valid_dates_in_page += 1
                        if from_dt and record_dt < (from_dt - timedelta(days=1)): page_old_count += 1

            consecutive_old_pages = consecutive_old_pages + 1 if valid_dates_in_page > 0 and page_old_count == valid_dates_in_page else 0
            if consecutive_old_pages >= 3: stop_reason = "Safely reached pages with all older records."; break
            
            page_token = data_block.get("page_token")
            if not page_token or not data_block.get("has_more", False): break
        except Exception as e: fetch_complete, error_msg, stop_reason = False, str(e), str(e); break

    logging.info(f"Analytics Data Fetched in {time.time() - start_time:.2f}s. Scanned {len(all_items)} records.")

    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0}, "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0}, "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0}, "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {}, "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {}, "acm_closing_reasons": {}, "daily_trend_creation": {}, "daily_trend_bd": {}, "other_request_types": {}, 
        "scanned_rows": len(all_items), "error_debug": error_msg, "feishu_keys": sorted(list(master_keys)), "fetch_complete": fetch_complete, "stop_reason": stop_reason
    }

    if from_dt and to_dt:
        cur = from_dt
        while cur < to_dt:
            stats["daily_trend_creation"][cur.strftime("%Y-%m-%d")] = 0; stats["daily_trend_bd"][cur.strftime("%Y-%m-%d")] = 0; cur += timedelta(days=1)

    for item in all_items:
        fields = item.get('fields', {})
        record_dt = parse_feishu_date(get_field_local(fields, 'Submitted on', 'Submitted on Copy', 'Created Time'))
        if from_dt or to_dt:
            if not record_dt or (from_dt and record_dt < from_dt) or (to_dt and record_dt >= to_dt): continue

        region, acm_pk, acm_in, acm_fallback = clean(get_field_local(fields, 'Region', 'Agency Region')), clean(get_field_local(fields, 'Acm Name (PK)')), clean(get_field_local(fields, 'Acm Name (IN)')), clean(get_field_local(fields, 'Acm', 'Assigned Member'))
        if region in ('', 'none'): region = 'pk' if acm_pk in PK_ACMS or acm_fallback in PK_ACMS else region
        if region_filter != 'all' and region != region_filter: continue

        req_type, status, agency_type, closing_reason, other_app = clean(get_field_local(fields, 'Request Type', 'Request type', 'Type', 'Category', 'Request Category')), clean(get_field_local(fields, 'Status', 'Request Status', 'Agency Status', 'State')), clean(get_field_local(fields, 'Agency Type', 'Type of Agency')), clean(get_field_local(fields, 'Closing Reason', 'Closing Agencies Reason', 'PK Closing Agencies Reason')), clean(get_field_local(fields, 'Otherapp Name', 'Other App Name', 'Other Apps'))
        is_done, is_rejected = "done" in status or "complet" in status or "approv" in status, "reject" in status or "fail" in status or "decline" in status
        acm = acm_in if region == "in" else acm_pk
        if not acm: acm = acm_fallback
        
        if "all" not in allowed_acms and acm.lower().strip() not in allowed_acms: continue
        if acm_filter != 'all' and acm_filter != acm: continue
        if type_filter != 'all' and type_filter != agency_type: continue

        is_bd_kpi, is_closing_kpi, is_creation_kpi = "bd creation" in req_type, "closing agency" in req_type, any(p in req_type for p in ["agency creation", "agency applied already by acm or bd link ( follow-up )", "agency applied already", "follow-up", "follow up"])

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
                    if acm.title() not in stats["acm_closing_reasons"]: stats["acm_closing_reasons"][acm.title()] = {"User Request": 0, "Duplicated Hosting": 0}
                    if "user" in closing_reason: stats["acm_closing_reasons"][acm.title()]["User Request"] += 1
                    elif "dup" in closing_reason: stats["acm_closing_reasons"][acm.title()]["Duplicated Hosting"] += 1

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
            if is_done and acm: stats["acm_performance"][acm.title()] = stats["acm_performance"].get(acm.title(), 0) + 1
            if is_done and other_app: stats["other_apps"][other_app.title()] = stats["other_apps"].get(other_app.title(), 0) + 1
            if agency_type: stats["agency_types"][agency_type.title() if agency_type else "Unknown"] = stats["agency_types"].get(agency_type.title() if agency_type else "Unknown", 0) + 1
            for ct in extract_field_list(get_field_local(fields, 'Create Way', 'Creation Type', 'Agency Creation Type', 'PK Agencies Creation Type')):
                if ct: stats["creation_types"][ct.title()] = stats["creation_types"].get(ct.title(), 0) + 1
            if is_rejected:
                for rr in extract_field_list(get_field_local(fields, 'Reject Reason', 'Rejection Reason', 'Agencies Rejection Reason', 'PK Agencies Rejection reason')):
                    if rr: stats["reject_reasons"][rr.title()] = stats["reject_reasons"].get(rr.title(), 0) + 1
        elif req_type: stats["other_request_types"][req_type.title()] = stats["other_request_types"].get(req_type.title(), 0) + 1

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
