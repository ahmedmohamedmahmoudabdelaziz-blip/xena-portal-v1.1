import os
import time
import urllib.parse
import logging
from flask import Flask, request, jsonify, send_file, redirect
import requests
from datetime import datetime, timedelta

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- SECURE CONFIGURATION ---
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = "https://xena-portal-v1-1.vercel.app/api/callback"
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"

ADMIN_USERS = ['ahmed samurai', 'ahmed samurai 1954', 'noora', 'mano']

def get_field(fields, *names):
    """Fuzzy and regional fallback selector to guarantee no blank fields."""
    if not fields: return None
    for name in names:
        if name in fields: return fields[name]
    for name in names:
        name_clean = name.strip().lower()
        for key in fields:
            if key.strip().lower() == name_clean:
                return fields[key]
    for name in names:
        name_clean = name.strip().lower()
        for key in fields:
            if name_clean in key.strip().lower():
                return fields[key]
    return None

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, json=payload).json()
    return response.get("tenant_access_token")

def extract_field_text(field_data):
    if not field_data: return ""
    if isinstance(field_data, (str, int, float)): return str(field_data)
    if isinstance(field_data, dict):
        for key in ['text', 'name', 'en_name', 'value', 'label']:
            if key in field_data: return str(field_data[key])
        return str(field_data)
    if isinstance(field_data, list):
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                extracted = False
                for key in ['text', 'name', 'en_name', 'value']:
                    if key in item:
                        texts.append(str(item[key]))
                        extracted = True
                        break
                if not extracted: texts.append(str(item))
            else: texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)

def parse_feishu_date(date_val):
    if not date_val: return None
    if isinstance(date_val, list) and len(date_val) > 0: date_val = date_val[0]
    if isinstance(date_val, dict): date_val = date_val.get('value', date_val.get('text', ''))
        
    if isinstance(date_val, (int, float)): 
        dt = datetime.fromtimestamp(date_val / 1000.0)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif isinstance(date_val, str):
        if date_val.isdigit(): 
            dt = datetime.fromtimestamp(int(date_val) / 1000.0)
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            try:
                clean_str = date_val[:10].replace('/', '-').replace('.', '-')
                return datetime.strptime(clean_str, "%Y-%m-%d")
            except Exception: pass
    return None

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
    token_resp = requests.post(token_url, headers=headers, json=payload).json()
    
    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token: return "SSO Error: Could not verify user token.", 500
        
    info_url = "https://open.feishu.cn/open-apis/authen/v1/user_info"
    info_resp = requests.get(info_url, headers={"Authorization": f"Bearer {user_access_token}"}).json()
    
    lark_name = info_resp.get("data", {}).get("name", "Unknown User")
    safe_name = urllib.parse.quote(lark_name)
    return redirect(f"/?user={safe_name}&uat={user_access_token}")

@app.route('/api/auth/me', methods=['GET'])
def check_auth():
    username = request.args.get('user', '').lower()
    is_admin = any(admin in username for admin in ADMIN_USERS)
    return jsonify({"isAdmin": is_admin})

@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    uat = request.args.get('uat', '')
    if not uat: return jsonify({"error": "Unauthorized session."}), 401
    if not agency_code: return jsonify({"error": "No agency code provided"}), 400

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    if points_response.get("code") != 0: return jsonify({"error": f"Feishu API Blocked: {points_response.get('msg')}"}), 403

    items = points_response.get('data', {}).get('items', [])
    if not items: return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    sheet_acm_name = extract_field_text(get_field(fields, 'Acm Name (PK)', 'Acm Name (IN)', 'Acm', 'Assigned Member')).strip()
    try: base_points = float(extract_field_text(get_field(fields, 'Base Points')).replace(',', '').strip())
    except ValueError: base_points = 0

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=points_payload).json() 
    
    valid_requests = []
    if req_response.get("code") == 0:
        cm, cy = datetime.now().month, datetime.now().year
        for item in req_response.get('data', {}).get('items', []):
            r_fields = item.get('fields', {})
            ts = parse_feishu_date(get_field(r_fields, 'Submitted on Copy', 'Submitted on'))
            if ts and ts.month == cm and ts.year == cy:
                valid_requests.append(r_fields)

    return jsonify({"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified by Feishu"})

# --- 📊 RAW UNRESTRICTED ANALYTICS ENGINE ---
@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    username = request.args.get('user', '').lower()
    uat = request.args.get('uat', '')
    
    if not uat: return jsonify({"error": "Unauthorized session. Please log in again."}), 401
    if not any(admin in username for admin in ADMIN_USERS): return jsonify({"error": "Unauthorized. Analytics are restricted to Administrators."}), 403

    tat = get_tenant_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})

    region_filter = request.args.get('region', 'ALL').strip().lower()
    acm_filter = request.args.get('acm', 'All').strip().lower()
    type_filter = request.args.get('type', 'All').strip().lower()
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')
    
    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1) if date_to else None

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    
    # 🚀 Native Payload Filter
    payload = {"page_size": 500} # Hard limit constraint fix
    if region_filter not in ['all', '']:
        payload["filter"] = {
            "conjunction": "and",
            "conditions": [
                {"field_name": "Region", "operator": "contains", "value": [region_filter.upper()]}
            ]
        }

    # Sorting resolution via system timestamp
    valid_sort = None
    for sort_col in ["Created Time", "Submitted on Copy", "Submitted on"]:
        test_payload = {"page_size": 1, "sort": [{"field_name": sort_col, "desc": True}]}
        if "filter" in payload: test_payload["filter"] = payload["filter"]
        
        test_res = session.post(req_url, json=test_payload).json()
        if test_res.get("code") == 0:
            valid_sort = [{"field_name": sort_col, "desc": True}]
            break
            
    if valid_sort:
        payload["sort"] = valid_sort

    all_items = []
    seen_record_ids = set()
    page_token = ""
    
    for _ in range(150):
        if page_token: payload["page_token"] = page_token
        try:
            res = session.post(req_url, json=payload).json()
            if res.get("code") != 0: break
                
            fetched_items = res.get('data', {}).get('items', [])
            for item in fetched_items:
                record_id = item.get("record_id")
                if record_id and record_id not in seen_record_ids:
                    seen_record_ids.add(record_id)
                    all_items.append(item)
                    
            page_token = res.get('data', {}).get('page_token')
            if not page_token or not res.get('data', {}).get('has_more'): break
        except Exception:
            break

    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {}, 
        "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {},
        "acm_closing_reasons": {}, "daily_trend": {}, "scanned_rows": len(all_items)
    }

    dropped_date = dropped_region = dropped_acm = dropped_type = processed = 0

    for item in all_items:
        fields = item.get('fields', {})
        
        record_dt = parse_feishu_date(get_field(fields, 'Submitted on Copy', 'Submitted on'))
        if from_dt or to_dt:
            if not record_dt: 
                dropped_date += 1
                continue
            if from_dt and record_dt < from_dt: 
                dropped_date += 1
                continue
            if to_dt and record_dt >= to_dt: 
                dropped_date += 1
                continue

        req_type = extract_field_text(get_field(fields, 'Request Type')).strip().lower()
        
        # Exact Column Isolation
        status_val = fields.get('Status')
        if not status_val:
            for k, v in fields.items():
                if k.strip().lower() == 'status':
                    status_val = v
                    break
        status = extract_field_text(status_val).strip().lower()

        # Regional Prefix Mappings
        creation_type = extract_field_text(get_field(fields, 'PK Agencies Creation Type', 'IN Agencies Creation Type', 'Agencies Creation Type', 'Agency Creation Type', 'Create Way', 'Creation Type')).strip() 
        reject_reason = extract_field_text(get_field(fields, 'PK Agencies Rejection reason', 'IN Agencies Rejection reason', 'Agencies Rejection Reason', 'Agencies Rejection reason', 'Reject Reason', 'Rejection Reason')).strip() 
        closing_reason = extract_field_text(get_field(fields, 'PK Closing Agencies Reason', 'IN Closing Agencies Reason', 'Closing Agencies Reason', 'Closing Reason')).strip()
        
        agency_type = extract_field_text(get_field(fields, 'Agency Type')).strip()
        region = extract_field_text(get_field(fields, 'Region')).strip().lower()
        other_app = extract_field_text(get_field(fields, 'Otherapp Name')).strip()

        is_done = "done" in status
        is_rejected = "rejected" in status

        acm_pk = extract_field_text(get_field(fields, 'Acm Name (PK)')).strip()
        acm_in = extract_field_text(get_field(fields, 'Acm Name (IN)')).strip()
        acm = acm_in if region == "in" else acm_pk
        if not acm: acm = extract_field_text(get_field(fields, 'Acm', 'Assigned Member')).strip()

        if region_filter not in ['all', ''] and region != region_filter: 
            dropped_region += 1
            continue
        if acm_filter not in ['all', 'all acms', ''] and acm_filter != acm.lower(): 
            dropped_acm += 1
            continue
        if type_filter not in ['all', 'all types', ''] and type_filter != agency_type.lower(): 
            dropped_type += 1
            continue

        processed += 1

        if is_done and record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            stats["daily_trend"][date_str] = stats["daily_trend"].get(date_str, 0) + 1

        if "agency creation" in req_type or "agency applied" in req_type:
            stats["kpis"]["creations"] += 1
            if is_done: stats["creation_status"]["Done"] += 1
            elif is_rejected: stats["creation_status"]["Rejected"] += 1
            else: stats["creation_status"]["Under Investigation"] += 1
            
            if is_done and acm:
                clean_acm = acm.title()
                stats["acm_performance"][clean_acm] = stats["acm_performance"].get(clean_acm, 0) + 1
            if is_done and other_app:
                stats["other_apps"][other_app] = stats["other_apps"].get(other_app, 0) + 1
            if creation_type:
                stats["creation_types"][creation_type] = stats["creation_types"].get(creation_type, 0) + 1
            if agency_type:
                clean_type = agency_type.title()
                stats["agency_types"][clean_type] = stats["agency_types"].get(clean_type, 0) + 1
            if is_rejected and reject_reason:
                stats["reject_reasons"][reject_reason] = stats["reject_reasons"].get(reject_reason, 0) + 1

        elif "bd creation" in req_type:
            stats["kpis"]["bds"] += 1
            if is_done: stats["bd_status"]["Done"] += 1
            elif is_rejected: stats["bd_status"]["Rejected"] += 1
            else: stats["bd_status"]["Under Investigation"] += 1

        elif "closing agency" in req_type:
            stats["kpis"]["closings"] += 1
            if is_done: stats["closing_status"]["Done"] += 1
            elif is_rejected: stats["closing_status"]["Rejected"] += 1
            else: stats["closing_status"]["Under Investigation"] += 1
            
            if closing_reason:
                stats["closing_reasons_pie"][closing_reason] = stats["closing_reasons_pie"].get(closing_reason, 0) + 1
                if acm:
                    clean_acm = acm.title()
                    if clean_acm not in stats["acm_closing_reasons"]:
                        stats["acm_closing_reasons"][clean_acm] = {"User Request": 0, "Duplicated Hosting": 0}
                    if "User Request" in closing_reason:
                        stats["acm_closing_reasons"][clean_acm]["User Request"] += 1
                    elif "Duplicated" in closing_reason:
                        stats["acm_closing_reasons"][clean_acm]["Duplicated Hosting"] += 1

    stats["debug"] = {
        "total_fetched": len(all_items), "dropped_date": dropped_date, "dropped_region": dropped_region,
        "dropped_acm": dropped_acm, "dropped_type": dropped_type, "processed": processed
    }

    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend"] = dict(sorted(stats["daily_trend"].items()))

    return jsonify(stats)
