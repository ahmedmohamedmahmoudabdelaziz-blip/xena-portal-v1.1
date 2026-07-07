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
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    return requests.post(url, json=payload).json().get("tenant_access_token")

# Extracts data cleanly
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
    
    # We now exclusively use the Feishu User Access Token (uat)
    return redirect(f"/?user={safe_name}&uat={user_access_token}")

# --- SEARCH: CONTROLLED 100% BY FEISHU ADVANCED PERMISSIONS ---
@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    uat = request.args.get('uat', '')
    
    if not uat:
        return jsonify({"error": "Unauthorized session. Please log in via Feishu again."}), 401
    if not agency_code:
        return jsonify({"error": "No agency code provided"}), 400

    # We use the USER'S token. Feishu acts as the supreme judge of who sees what!
    headers = {"Authorization": f"Bearer {uat}", "Content-Type": "application/json"}

    # 1. FETCH AGENCY DATA
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    
    # 🚨 DIAGNOSTIC CHECK: Did you add the permissions in Step 1?
    code = points_response.get("code")
    if code != 0:
        msg = points_response.get("msg", "Unknown error")
        return jsonify({"error": f"FEISHU API BLOCKED YOU! Code {code}: {msg}. You MUST add 'Bitable' permissions in the Feishu Developer Console and publish a new version!"}), 403

    items = points_response.get('data', {}).get('items', [])
    
    # 🎯 ADVANCED PERMISSIONS SUCCESS: 
    # If the API succeeds (code 0) but returns 0 items, it means Feishu's 
    # Advanced Permissions successfully hid the row from this user!
    if not items:
        return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    # If Feishu allows them to see it, extract the data normally
    fields = items[0].get('fields', {})
    sheet_acm_name = extract_field_text(fields.get('Acm')).strip()
    
    raw_bp = extract_field_text(fields.get('Base Points')).replace(',', '').strip()
    try: base_points = float(raw_bp)
    except ValueError: base_points = 0

    # 2. FETCH REQUESTS (Also strictly using the User's Token)
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

    final_data = {
        "base_points": base_points, 
        "requests": valid_requests, 
        "acm": sheet_acm_name.title(), 
        "role": "Verified by Feishu"
    }
    return jsonify(final_data)
