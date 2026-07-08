import os
import urllib.parse
import logging
from flask import Flask, request, jsonify, send_file, redirect
import requests
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- CONFIGURATION ---
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = "https://xena-portal-v1-1.vercel.app/api/callback"
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    return requests.post(url, json=payload).json().get("tenant_access_token")

def extract_field_text(field_data):
    """Robust extractor designed to handle single-select, person arrays, and text objects."""
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
                for key in ['text', 'name', 'en_name', 'value']:
                    if key in item:
                        texts.append(str(item[key]))
                        break
                else: texts.append(str(item))
            else: texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)

def parse_feishu_date(date_val):
    if not date_val: return None
    if isinstance(date_val, dict) and 'value' in date_val: date_val = date_val['value']
    if isinstance(date_val, list) and len(date_val) > 0:
        if isinstance(date_val[0], dict) and 'text' in date_val[0]: date_val = date_val[0]['text']
    if isinstance(date_val, (int, float)): return datetime.fromtimestamp(date_val / 1000.0)
    if isinstance(date_val, str):
        if date_val.isdigit(): return datetime.fromtimestamp(int(date_val) / 1000.0)
        try: return datetime.strptime(date_val[:10], "%Y-%m-%d")
        except ValueError: pass
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

@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    uat = request.args.get('uat', '')
    
    if not uat: return jsonify({"error": "Unauthorized session."}), 401
    if not agency_code: return jsonify({"error": "No agency code provided"}), 400

    headers = {"Authorization": f"Bearer {uat}", "Content-Type": "application/json"}
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    if points_response.get("code") != 0:
        return jsonify({"error": "Feishu API error or unauthorized access configuration."}), 403

    items = points_response.get('data', {}).get('items', [])
    if not items:
        return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    sheet_acm_name = extract_field_text(fields.get('Acm')).strip()
    try: base_points = float(extract_field_text(fields.get('Base Points')).replace(',', '').strip())
    except ValueError: base_points = 0

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=points_payload).json() 
    
    valid_requests = []
    if req_response.get("code") == 0:
        cm, cy = datetime.now().month, datetime.now().year
        for item in req_response.get('data', {}).get('items', []):
            r_fields = item.get('fields', {})
            ts = parse_feishu_date(r_fields.get('Submitted on'))
            if ts and ts.month == cm and ts.year == cy:
                valid_requests.append(r_fields)

    return jsonify({"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified by Feishu"})

# --- 📊 MASTERPIECE ANALYTICS DASHBOARD ENGINE ---
@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    username = request.args.get('user', '').lower()
    uat = request.args.get('uat', '')
    
    if not uat:
        return jsonify({"error": "Unauthorized session. Please log in again."}), 401

    admin_users = ['ahmed samurai', 'ahmed samurai 1954', 'noora', 'mano']
    if not any(admin in username for admin in admin_users):
        return jsonify({"error": "Unauthorized. Analytics are restricted to Administrators."}), 403

    headers = {"Authorization": f"Bearer {uat}", "Content-Type": "application/json"}

    # Capture incoming dynamic filter states
    region_filter = request.args.get('region', 'ALL').strip().upper()
    acm_filter = request.args.get('acm', 'All').strip().lower()
    type_filter = request.args.get('type', 'All').strip().lower()
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')
    
    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") if date_to else None

    # Pagination data processing engine
    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    all_items = []
    page_token = ""
    for _ in range(4): 
        payload = {"page_size": 500}
        if page_token: payload["page_token"] = page_token
        try:
            res = requests.post(req_url, headers=headers, json=payload, timeout=10).json()
            if res.get("code") != 0: 
                return jsonify({"error": f"Feishu API Error: {res.get('msg')} (Code {res.get('code')})"}), 400
            all_items.extend(res.get('data', {}).get('items', []))
            page_token = res.get('data', {}).get('page_token')
            if not res.get('data', {}).get('has_more'): break
        except Exception as e:
            return jsonify({"error": f"Server processing bottleneck: {str(e)}"}), 500

    # Initialization of precise chart structures observed in metrics configurations
    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "reject_reasons": {},
        "closing_reasons_pie": {},
        "acm_closing_reasons": {}, 
        "acm_performance": {}, 
        "creation_types": {},
        "agency_types": {}, 
        "other_apps": {}, 
        "daily_trend": {}
    }

    for item in all_items:
        fields = item.get('fields', {})
        
        # 1. Execution of Date Constraint Checks
        record_dt = parse_feishu_date(fields.get("Submitted on"))
        if record_dt:
            if from_dt and record_dt.date() < from_dt.date(): continue
            if to_dt and record_dt.date() > to_dt.date(): continue

        # 2. Variable Extraction via Explicit Column Schemas
        req_type = extract_field_text(fields.get('Request Type')).strip()
        status = extract_field_text(fields.get('Status', fields.get('Request Status', ''))).strip()
        agency_type = extract_field_text(fields.get('Agency Type')).strip()
        creation_type = extract_field_text(fields.get('Create Way')).strip()
        region = extract_field_text(fields.get('Region')).strip().upper()
        reject_reason = extract_field_text(fields.get('Reject Reason')).strip()
        closing_reason = extract_field_text(fields.get('Closing Reason')).strip()
        other_app = extract_field_text(fields.get('Otherapp Name')).strip()

        # Dynamic cross-regional check for proper ACM allocation arrays
        acm_pk = extract_field_text(fields.get('Acm Name (PK)')).strip()
        acm_in = extract_field_text(fields.get('Acm Name (IN)')).strip()
        acm = acm_in if region == "IN" else acm_pk

        # 3. Dynamic Global Filter Validation Engine
        if region_filter not in ['ALL', ''] and region != region_filter: continue
        if acm_filter not in ['all', 'all acms', ''] and acm_filter != acm.lower(): continue
        if type_filter not in ['all', 'all types', ''] and type_filter != agency_type.lower(): continue

        # 4. Metric Routing and Aggregation Pipeline
        if "Agency Creation" in req_type:
            stats["kpis"]["creations"] += 1
            if "Done" in status: stats["creation_status"]["Done"] += 1
            elif "Rejected" in status: stats["creation_status"]["Rejected"] += 1
            elif status: stats["creation_status"]["Under Investigation"] += 1
            
            if "Done" in status and acm:
                stats["acm_performance"][acm] = stats["acm_performance"].get(acm, 0) + 1
            if "Done" in status and other_app:
                stats["other_apps"][other_app] = stats["other_apps"].get(other_app, 0) + 1
            if creation_type:
                stats["creation_types"][creation_type] = stats["creation_types"].get(creation_type, 0) + 1
            if "Done" in status and agency_type:
                stats["agency_types"][agency_type] = stats["agency_types"].get(agency_type, 0) + 1
            if "Rejected" in status and reject_reason:
                stats["reject_reasons"][reject_reason] = stats["reject_reasons"].get(reject_reason, 0) + 1

        elif "BD Creation" in req_type:
            stats["kpis"]["bds"] += 1
            if "Done" in status: stats["bd_status"]["Done"] += 1
            elif "Rejected" in status: stats["bd_status"]["Rejected"] += 1
            elif status: stats["bd_status"]["Under Investigation"] += 1

        elif "Closing Agency" in req_type:
            stats["kpis"]["closings"] += 1
            if "Done" in status: stats["closing_status"]["Done"] += 1
            elif "Rejected" in status: stats["closing_status"]["Rejected"] += 1
            elif status: stats["closing_status"]["Under Investigation"] += 1
            
            if closing_reason:
                stats["closing_reasons_pie"][closing_reason] = stats["closing_reasons_pie"].get(closing_reason, 0) + 1
                if acm:
                    if acm not in stats["acm_closing_reasons"]:
                        stats["acm_closing_reasons"][acm] = {"User Request": 0, "Duplicated Hosting": 0}
                    if "User Request" in closing_reason:
                        stats["acm_closing_reasons"][acm]["User Request"] += 1
                    elif "Duplicated" in closing_reason:
                        stats["acm_closing_reasons"][acm]["Duplicated Hosting"] += 1

        # Real-time parsing for curved graph trend tracking
        if "Done" in status and record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            stats["daily_trend"][date_str] = stats["daily_trend"].get(date_str, 0) + 1

    # Sorting Operations for Top-Performance Metrics
    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend"] = dict(sorted(stats["daily_trend"].items()))

    return jsonify(stats)
