import os
import time
import urllib.parse
import logging
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
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx" 

MASTER_ADMINS = ['ahmed samurai', 'ahmed samurai 1954']
PK_ACMS = ["nabeel", "hasseb", "haseeb", "enzo", "farooq", "mubeen", "cruz", "ehtisham", "usama", "sehar ch", "hamza malik", "zohaib", "eagle", "leo", "berlin"]

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
        for key in ['text', 'name', 'en_name', 'value', 'label', 'id']:
            if key in field_data: return str(field_data[key])
        return str(field_data)
    if isinstance(field_data, list):
        if len(field_data) == 0: return ""
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                extracted = False
                for key in ['text', 'name', 'en_name', 'value', 'id']:
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
        for key in ['text', 'name', 'en_name', 'value', 'label']:
            if key in field_data and field_data[key] not in (None, ""):
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
                for key in ['text', 'name', 'en_name', 'value', 'label']:
                    if key in item and item[key] not in (None, ""):
                        res.append(str(item[key]).strip())
                        extracted = True
                        break
                if not extracted and 'id' in item and item['id'] not in (None, ""):
                    res.append(str(item['id']).strip())
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

def clean(field_data):
    return extract_field_text(field_data).strip().lower()

# =============================================================================
# 🚨 DYNAMIC ACCESS CONTROL RULES (With Regional Enforcements)
# =============================================================================
def get_user_permissions(email, name):
    name_clean = name.strip().lower()
    
    if any(admin in name_clean for admin in MASTER_ADMINS):
        return {"is_super_admin": True, "modules": ["target", "points", "analytics"], "acms": ["all"], "regions": ["all"]}

    if not email: 
        return {"is_super_admin": False, "modules": [], "acms": [], "regions": []}

    tat = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records/search"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Email", "operator": "is", "value": [email.strip().lower()]}]}}
    
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=10).json()
        items = res.get("data", {}).get("items", [])
        if not items: return {"is_super_admin": False, "modules": [], "acms": [], "regions": []}
        
        fields = items[0].get("fields", {})
        modules_raw = extract_field_text(get_field_local(fields, "Modules"))
        acms_raw = extract_field_text(get_field_local(fields, "ACMs"))
        regions_raw = extract_field_text(get_field_local(fields, "Regions")) # 🚨 Fetch new column from DB
        
        modules = [m.strip().lower() for m in modules_raw.split(",") if m.strip()]
        acms = [a.strip().lower() for a in acms_raw.split(",") if a.strip()]
        
        # If blank or empty, safely default to "all" to prevent blocking older users
        regions = [r.strip().lower() for r in regions_raw.split(",") if r.strip()]
        if not regions: regions = ["all"]
        
        return {"is_super_admin": False, "modules": modules, "acms": acms, "regions": regions}
    except Exception:
        return {"is_super_admin": False, "modules": [], "acms": [], "regions": []}

@app.route('/api/admin/users', methods=['GET', 'POST', 'DELETE'])
def manage_users():
    admin_name = request.headers.get('X-User-Name', '').lower()
    if not any(admin in admin_name for admin in MASTER_ADMINS):
        return jsonify({"error": "Unauthorized"}), 403

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ACCESS_TABLE_ID}/records"

    if request.method == 'GET':
        res = requests.get(base_url, headers=headers).json()
        users = []
        for item in res.get("data", {}).get("items", []):
            fields = item.get("fields", {})
            users.append({
                "id": item.get("record_id"),
                "email": extract_field_text(fields.get("Email", "")),
                "modules": extract_field_text(fields.get("Modules", "")),
                "acms": extract_field_text(fields.get("ACMs", "")),
                "regions": extract_field_text(fields.get("Regions", "all")) # 🚨 Expose to UI List View
            })
        return jsonify(users)

    elif request.method == 'POST':
        data = request.json
        # 🚨 DB INJECTION: Map fields dynamically to the new row
        payload = {
            "fields": {
                "Email": data.get("email").strip().lower(), 
                "Modules": data.get("modules"), 
                "ACMs": data.get("acms"),
                "Regions": data.get("regions").strip().lower() if data.get("regions") else "all"
            }
        }
        res = requests.post(base_url, headers=headers, json=payload).json()
        return jsonify({"success": res.get("code") == 0})

    elif request.method == 'DELETE':
        record_id = request.args.get('id')
        res = requests.delete(f"{base_url}/{record_id}", headers=headers).json()
        return jsonify({"success": res.get("code") == 0})

@app.route('/api/search', methods=['GET'])
def search_agency():
    username = request.args.get('user', '')
    email = request.args.get('email', '')
    agency_code = request.args.get('code')
    inquiry_type = request.args.get('type', 'target').strip().lower()
    
    perms = get_user_permissions(email, username)
    if inquiry_type not in perms["modules"]:
        return jsonify({"error": f"Access Denied: Permission layout absent for {inquiry_type}."}), 403

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_response = requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true", headers=headers, json=points_payload, timeout=10).json()

    items = points_response.get('data', {}).get('items', [])
    if not items: return jsonify({"error": f"Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    region = clean(get_field_local(fields, 'Region', 'Agency Region'))
    sheet_acm_name = extract_field_text(get_field_local(fields, 'Acm Name (PK)', 'Acm Name (IN)', 'Acm', 'Assigned Member')).strip()
    
    # 🚨 SECURITY GATEWAY: Enforce Region RLS Block
    if "all" not in perms.get("regions", ["all"]) and region not in perms.get("regions", []):
        return jsonify({"error": f"Access Denied: Your profile does not allow you to query records from Region: {region.upper()}."}), 403

    # 🚨 SECURITY GATEWAY: Enforce ACM RLS Block
    if "all" not in perms["acms"] and sheet_acm_name.lower() not in perms["acms"]:
        return jsonify({"error": f"Access Denied: You are not authorized to view data for ACM: {sheet_acm_name}."}), 403

    try: base_points = float(extract_field_text(get_field_local(fields, 'Base Points')).replace(',', '').strip())
    except ValueError: base_points = 0

    req_response = requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true", headers=headers, json=points_payload, timeout=10).json()
    valid_requests = []
    if req_response.get("code") == 0:
        cm, cy = datetime.now().month, datetime.now().year
        for item in req_response.get('data', {}).get('items', []):
            r_fields = item.get('fields', {})
            ts = parse_feishu_date(get_field_local(r_fields, 'Submitted on Copy', 'Submitted on'))
            if ts and ts.month == cm and ts.year == cy: valid_requests.append(r_fields)

    return jsonify({"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified"})

@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    username = request.args.get('user', '')
    email = request.args.get('email', '')
    perms = get_user_permissions(email, username)
    
    if "analytics" not in perms["modules"]: return jsonify({"error": "Access Blocked."}), 403

    tat = get_tenant_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}"})

    region_filter = request.args.get('region', 'PK').strip().lower()
    if not region_filter: region_filter = 'pk'
    
    # 🚨 SECURITY GATEWAY: If an agent tries to open all region data but only owns PK, trigger error block
    if region_filter == 'all' and "all" not in perms.get("regions", ["all"]):
        return jsonify({"error": f"Access Denied: Your access profile requires you to specify a clear single region selection filter."}), 403
    if region_filter != 'all' and "all" not in perms.get("regions", ["all"]) and region_filter not in perms.get("regions", []):
        return jsonify({"error": f"Access Denied: You do not have permission to view metrics for Region: {region_filter.upper()}."}), 403

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
    page_token = ""
    consecutive_old_pages = 0

    for page_num in range(150):
        params = {"page_size": 500, "automatic_fields": "true", "sort": '["Numbering DESC"]'}
        if page_token: params["page_token"] = page_token
        try:
            res = session.get(base_url, params=params, timeout=12)
            if res.status_code != 200: break
            res_json = res.json(); data_block = res_json.get("data", {}); items = data_block.get("items", [])
            if not items: break

            page_old_count = 0
            valid_dates_in_page = 0
            for item in items:
                rid = item.get("record_id")
                if rid and rid not in seen_ids:
                    seen_ids.add(rid)
                    all_items.append(item)
                    raw_date = get_field_local(item.get('fields', {}), 'Submitted on Copy', 'Submitted on', 'Created Time')
                    record_dt = parse_feishu_date(raw_date)
                    if record_dt:
                        valid_dates_in_page += 1
                        if from_dt and record_dt < (from_dt - timedelta(days=1)): page_old_count += 1

            if valid_dates_in_page > 0 and page_old_count == valid_dates_in_page: consecutive_old_pages += 1
            else: consecutive_old_pages = 0
            if consecutive_old_pages >= 3: break
            page_token = data_block.get("page_token")
            if not page_token or not data_block.get("has_more", False): break
        except Exception: break

    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {},
        "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {},
        "acm_closing_reasons": {}, "daily_trend_creation": {}, "daily_trend_bd": {},
        "other_request_types": {}, "scanned_rows": len(all_items), "fetch_complete": True
    }

    if from_dt and to_dt:
        cur = from_dt
        while cur < to_dt:
            date_str = cur.strftime("%Y-%m-%d")
            stats["daily_trend_creation"][date_str] = 0; stats["daily_trend_bd"][date_str] = 0
            cur += timedelta(days=1)

    for item in all_items:
        fields = item.get('fields', {})
        record_dt = parse_feishu_date(get_field_local(fields, 'Submitted on', 'Submitted on Copy', 'Created Time'))
        if from_dt or to_dt:
            if not record_dt or (from_dt and record_dt < from_dt) or (to_dt and record_dt >= to_dt): continue

        region = clean(get_field_local(fields, 'Region', 'Agency Region'))
        acm_pk = clean(get_field_local(fields, 'Acm Name (PK)'))
        acm_fallback = clean(get_field_local(fields, 'Acm', 'Assigned Member'))
        if region in ('', 'none') and (acm_pk in PK_ACMS or acm_fallback in PK_ACMS): region = 'pk'
        
        # 🚨 Enforce Region Limits inside the execution scan loop
        if "all" not in perms.get("regions", ["all"]) and region not in perms.get("regions", []): continue
        if region_filter != 'all' and region != region_filter: continue

        req_type = clean(get_field_local(fields, 'Request Type', 'Request type'))
        status = clean(get_field_local(fields, 'Status', 'Request Status'))
        is_done = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        acm = clean(get_field_local(fields, 'Acm Name (IN)')) if region == "in" else acm_pk
        if not acm: acm = acm_fallback
        
        if "all" not in perms["acms"] and acm.lower().strip() not in perms["acms"]: continue
        if acm_filter != 'all' and acm_filter != acm: continue

        agency_type = clean(get_field_local(fields, 'Agency Type', 'Type of Agency'))
        closing_reason = clean(get_field_local(fields, 'Closing Reason', 'Closing Agencies Reason'))
        other_app = clean(get_field_local(fields, 'Otherapp Name', 'Other App Name'))
        agency_type_title = agency_type.title() if agency_type else "Unknown"
        if type_filter != 'all' and type_filter != agency_type: continue

        is_bd_kpi = "bd creation" in req_type
        is_closing_kpi = "closing agency" in req_type
        is_creation_kpi = any(p in req_type for p in ["agency creation", "agency applied already by acm or bd link ( follow-up )", "agency applied already", "follow-up", "follow up"])

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
            if is_done && other_app:
                oa_title = other_app.title()
                stats["other_apps"][oa_title] = stats["other_apps"].get(oa_title, 0) + 1
            if agency_type_title != "Unknown": stats["agency_types"][agency_type_title] = stats["agency_types"].get(agency_type_title, 0) + 1

            raw_creation_types = get_field_local(fields, 'Create Way', 'Creation Type', 'Agency Creation Type')
            for ct in extract_field_list(raw_creation_types):
                if ct: stats["creation_types"][ct.title()] = stats["creation_types"].get(ct_title, 0) + 1
            if is_rejected:
                raw_reject_reasons = get_field_local(fields, 'Reject Reason', 'Rejection Reason')
                for rr in extract_field_list(raw_reject_reasons):
                    if rr: stats["reject_reasons"][rr.title()] = stats["reject_reasons"].get(rr_title, 0) + 1

    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend_creation"] = dict(sorted(stats["daily_trend_creation"].items()))
    stats["daily_trend_bd"] = dict(sorted(stats["daily_trend_bd"].items()))
    return jsonify(stats)
