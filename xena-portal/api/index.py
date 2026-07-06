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
    response = requests.post(url, json=payload).json()
    return response.get("tenant_access_token")

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
    
    # WE GRAB THE USER'S PERSONAL IDENTITY TOKEN
    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token: return "SSO Error: Could not verify user token.", 500
        
    info_url = "https://open.feishu.cn/open-apis/authen/v1/user_info"
    info_headers = {"Authorization": f"Bearer {user_access_token}"}
    info_resp = requests.get(info_url, headers=info_headers).json()
    
    lark_name = info_resp.get("data", {}).get("name", "Unknown User")
    
    safe_name = urllib.parse.quote(lark_name)
    # Pass their token securely to the dashboard
    return redirect(f"/?user={safe_name}&uat={user_access_token}")

@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    uat = request.args.get('uat', '') 
    
    if not uat:
        return jsonify({"error": "Unauthorized session. Please Log In via Feishu."}), 401
    if not agency_code:
        return jsonify({"error": "No agency code provided"}), 400

    # ----------------------------------------------------
    # MAGIC HAPPENS HERE: We use the USER'S token (uat).
    # Feishu checks the Advanced Permissions natively!
    # ----------------------------------------------------
    headers = {"Authorization": f"Bearer {uat}", "Content-Type": "application/json"}

    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    
    # We ask Feishu for the data on behalf of the user
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    
    # If Feishu's Advanced Permissions blocks them, it returns an error code!
    if points_response.get("code") != 0:
        return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team"}), 403

    base_points = 0
    sheet_acm_name = ""

    # If Feishu allowed them to see it, extract the data normally
    if 'data' in points_response and 'items' in points_response['data'] and len(points_response['data']['items']) > 0:
        fields = points_response['data']['items'][0].get('fields', {})
        
        raw_acm = fields.get('Acm', '')
        if isinstance(raw_acm, dict): sheet_acm_name = raw_acm.get('text', '')
        elif isinstance(raw_acm, list) and len(raw_acm) > 0: sheet_acm_name = raw_acm[0].get('text', '')
        else: sheet_acm_name = str(raw_acm)

        raw_bp = fields.get('Base Points', 0)
        if isinstance(raw_bp, dict): raw_bp = raw_bp.get('text', 0)
        elif isinstance(raw_bp, list) and len(raw_bp) > 0: raw_bp = raw_bp[0].get('text', 0)
        try: base_points = float(str(raw_bp).replace(',', '').strip())
        except ValueError: base_points = 0
    else:
        # If the query succeeds but finds 0 rows, the agency simply doesn't exist.
        return jsonify({"error": f"Agency code '{agency_code}' does not exist in the database."}), 404

    # ----------------------------------------------------
    # FETCH REQUESTS (Also strictly using the User's Token)
    # ----------------------------------------------------
    req_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}, {"field_name": "Request Type", "operator": "is", "value": ["Agency Target Privilege"]}]}}
    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=req_payload).json()
    
    valid_requests = []
    cm, cy = datetime.now().month, datetime.now().year
    if 'data' in req_response and 'items' in req_response['data']:
        for item in req_response['data']['items']:
            fields = item.get('fields', {})
            ts = fields.get('Submitted on') 
            if ts:
                try:
                    rd = datetime.fromtimestamp(int(ts) / 1000.0)
                    if rd.month == cm and rd.year == cy: valid_requests.append(fields)
                except Exception: pass

    final_data = {"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title()}
    return jsonify(final_data)
