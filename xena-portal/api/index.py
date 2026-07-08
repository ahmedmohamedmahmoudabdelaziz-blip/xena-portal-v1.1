import os
import urllib.parse
from flask import Flask, request, jsonify, send_file, redirect
import requests
from datetime import datetime

app = Flask(__name__)

# --- SECURE CONFIGURATION ---
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
REDIRECT_URI = "https://xena-portal-v1-1.vercel.app/api/callback"
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"  # The Grand Table
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    return requests.post(url, json=payload).json().get("tenant_access_token")

def extract_field_text(field_data):
    if not field_data: return ""
    if isinstance(field_data, (str, int, float)): return str(field_data)
    if isinstance(field_data, dict):
        return str(field_data.get('text', field_data.get('name', field_data.get('en_name', field_data))))
    if isinstance(field_data, list):
        return " ".join([extract_field_text(item) for item in field_data])
    return str(field_data)

@app.route('/', methods=['GET'])
def home():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return send_file(os.path.join(root_dir, 'index.html'))

# --- SSO LOGIN ---
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

# --- SEARCH AGENCY ---
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
        return jsonify({"error": f"Feishu API Blocked: Waiting for IT Admin to approve the app release."}), 403

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
            ts = r_fields.get('Submitted on') 
            if ts:
                try:
                    rd = datetime.fromtimestamp(int(ts) / 1000.0)
                    if rd.month == cm and rd.year == cy: valid_requests.append(r_fields)
                except Exception: pass

    return jsonify({"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified by Feishu"})

# --- 📊 GRAND TABLE ANALYTICS ENGINE ---
@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    uat = request.args.get('uat', '')
    if not uat: return jsonify({"error": "Unauthorized session."}), 401

    region_filter = request.args.get('region', 'All')
    acm_filter = request.args.get('acm', 'All').lower()
    type_filter = request.args.get('type', 'All').lower()
    
    # Custom Date Parsing
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')
    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") if date_to else None

    headers = {"Authorization": f"Bearer {uat}", "Content-Type": "application/json"}
    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    
    # Fetch up to 2000 records to ensure accuracy
    all_items = []
    page_token = ""
    for _ in range(4): 
        payload = {"page_size": 500}
        if page_token: payload["page_token"] = page_token
        res = requests.post(req_url, headers=headers, json=payload).json()
        if res.get("code") != 0: break
        all_items.extend(res.get('data', {}).get('items', []))
        page_token = res.get('data', {}).get('page_token')
        if not res.get('data', {}).get('has_more'): break

    # Data Aggregation
    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {},
        "reject_reasons": {},
        "closing_reasons": {},
        "other_apps": {},
        "daily_trend": {}
    }

    for item in all_items:
        fields = item.get('fields', {})
        
        # 1. Strict Date Check
        ts = fields.get('Submitted on')
        if not ts: continue
        try:
            record_dt = datetime.fromtimestamp(int(ts) / 1000.0)
            if from_dt and record_dt.date() < from_dt.date(): continue
            if to_dt and record_dt.date() > to_dt.date(): continue
        except: continue

        # 2. Extract Fields safely using your exact column names
        req_type = extract_field_text(fields.get('Request Type')).strip()
        status = extract_field_text(fields.get('Status')).strip()
        acm = extract_field_text(fields.get('Acm')).strip()
        agency_type = extract_field_text(fields.get('Agency Type')).strip().lower()
        region = extract_field_text(fields.get('Region')).strip()
        reject_reason = extract_field_text(fields.get('Reject Reason')).strip()
        closing_reason = extract_field_text(fields.get('Closing Reason')).strip()
        other_app = extract_field_text(fields.get('Otherapp Name')).strip()

        # 3. Apply Custom Filters
        if region_filter != 'All' and region_filter not in region: continue
        if acm_filter != 'all' and acm_filter not in acm.lower(): continue
        if type_filter != 'all' and type_filter not in agency_type: continue

        # Daily Trend Mapping
        date_str = record_dt.strftime("%Y-%m-%d")
        stats["daily_trend"][date_str] = stats["daily_trend"].get(date_str, 0) + 1

        # Logic Routing
        if "Agency Creation" in req_type:
            stats["kpis"]["creations"] += 1
            if "Done" in status: stats["creation_status"]["Done"] += 1
            elif "Rejected" in status: stats["creation_status"]["Rejected"] += 1
            else: stats["creation_status"]["Under Investigation"] += 1
            
            if "Done" in status and acm:
                clean_acm = acm.title()
                stats["acm_performance"][clean_acm] = stats["acm_performance"].get(clean_acm, 0) + 1
            if other_app:
                stats["other_apps"][other_app] = stats["other_apps"].get(other_app, 0) + 1

        elif "BD Creation" in req_type:
            stats["kpis"]["bds"] += 1
            if "Done" in status: stats["bd_status"]["Done"] += 1
            elif "Rejected" in status: stats["bd_status"]["Rejected"] += 1
            else: stats["bd_status"]["Under Investigation"] += 1

        elif "Closing Agency" in req_type:
            stats["kpis"]["closings"] += 1
            if "Done" in status: stats["closing_status"]["Done"] += 1
            elif "Rejected" in status: stats["closing_status"]["Rejected"] += 1
            else: stats["closing_status"]["Under Investigation"] += 1
            
            if closing_reason:
                stats["closing_reasons"][closing_reason] = stats["closing_reasons"].get(closing_reason, 0) + 1

        # Rejection Reasons (Global for all request types)
        if "Rejected" in status and reject_reason:
            stats["reject_reasons"][reject_reason] = stats["reject_reasons"].get(reject_reason, 0) + 1

    # Sorting
    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons"] = dict(sorted(stats["closing_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend"] = dict(sorted(stats["daily_trend"].items()))

    return jsonify(stats)
