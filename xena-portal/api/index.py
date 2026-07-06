import os
import time
import urllib.parse
import hashlib
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

# --- NEW: YOUR ROLES TABLE ID (From screenshot) ---
ROLES_TABLE_ID = "tblpgMD1AfAXQYZd"

cache = {}
CACHE_EXPIRY = 600 

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
    if not code: return "SSO Authorization Failed. No code provided.", 400
        
    tat = get_tenant_access_token()
    token_url = "https://open.feishu.cn/open-apis/authen/v1/oidc/access_token"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    payload = {"grant_type": "authorization_code", "code": code}
    token_resp = requests.post(token_url, headers=headers, json=payload).json()
    
    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token: return f"SSO Error: Could not verify user token.", 500
        
    info_url = "https://open.feishu.cn/open-apis/authen/v1/user_info"
    info_headers = {"Authorization": f"Bearer {user_access_token}"}
    info_resp = requests.get(info_url, headers=info_headers).json()
    
    lark_name = info_resp.get("data", {}).get("name", "Unknown User")
    
    safe_name = urllib.parse.quote(lark_name)
    return redirect(f"/?user={safe_name}&uat={user_access_token}")

@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    username = request.args.get('user', '')
    uat = request.args.get('uat', '') 
    
    if not username or not uat:
        return jsonify({"error": "Unauthorized. Please Log In via Feishu."}), 401
    if not agency_code:
        return jsonify({"error": "No agency code provided"}), 400

    cache_key = f"{agency_code}_{username}"
    if cache_key in cache and (time.time() - cache[cache_key]['time'] < CACHE_EXPIRY):
        return jsonify(cache[cache_key]['data'])

    tat = get_tenant_access_token()
    user_lower = username.lower()

    # ------------------------------------------------------------------
    # STEP 1: CHECK THE ROLES TABLE
    # ------------------------------------------------------------------
    user_role = "ACM" # Default role
    roles_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ROLES_TABLE_ID}/records/search?automatic_fields=true"
    roles_resp = requests.post(roles_url, headers={"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}, json={}).json()

    if 'data' in roles_resp and 'items' in roles_resp['data']:
        for item in roles_resp['data']['items']:
            fields = item.get('fields', {})
            
            # Check both Agent columns for the user's Feishu name
            agents1 = str(fields.get('Agent Name', '')).lower()
            agents2 = str(fields.get('Agent name 2', '')).lower()
            
            if user_lower in agents1 or user_lower in agents2:
                raw_role = fields.get('Role', '')
                if isinstance(raw_role, dict): raw_role = raw_role.get('text', '')
                elif isinstance(raw_role, list) and len(raw_role) > 0: raw_role = raw_role[0].get('text', '')
                user_role = str(raw_role).upper()
                break # Found their role, stop searching

    # ------------------------------------------------------------------
    # STEP 2: HYBRID TOKEN ROUTING
    # ------------------------------------------------------------------
    is_super_user = ("ADMIN" in user_role or "CS" in user_role)
    
    # Admins/CS get the Super-Robot Token (TAT). ACMs get their strict User Token (UAT).
    search_token = tat if is_super_user else uat 
    headers = {"Authorization": f"Bearer {search_token}", "Content-Type": "application/json"}

    # ------------------------------------------------------------------
    # STEP 3: FETCH DATA
    # ------------------------------------------------------------------
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    
    # If Feishu rejects the UAT token due to Advanced Permissions blocking them
    if points_response.get("code") != 0:
        return jsonify({"error": "Access Denied by Feishu Advanced Permissions. You are not authorized to view this agency."}), 403

    base_points = 0
    acm_name = ""

    if 'data' in points_response and 'items' in points_response['data'] and len(points_response['data']['items']) > 0:
        fields = points_response['data']['items'][0].get('fields', {})
        
        raw_acm = fields.get('Acm', '')
        if isinstance(raw_acm, list) and len(raw_acm) > 0: acm_name = raw_acm[0].get('text', '')
        elif isinstance(raw_acm, dict): acm_name = raw_acm.get('text', '')
        else: acm_name = str(raw_acm)

        raw_bp = fields.get('Base Points', 0)
        if isinstance(raw_bp, list): raw_bp = raw_bp[0].get('text', 0) if len(raw_bp) > 0 else 0
        elif isinstance(raw_bp, dict): raw_bp = raw_bp.get('text', 0)
        try: base_points = float(str(raw_bp).replace(',', '').strip())
        except ValueError: base_points = 0
    else:
        # Agency doesn't exist, OR Feishu returned 0 items because the ACM is blocked.
        return jsonify({"error": f"Access Denied: Agency '{agency_code}' does not exist, or you do not have permission to view it."}), 404

    # Fetch Requests
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

    # Return the data PLUS their detected role so the frontend can display it
    final_data = {"base_points": base_points, "requests": valid_requests, "acm": acm_name.title(), "role": user_role}
    cache[cache_key] = {"time": time.time(), "data": final_data}
    return jsonify(final_data)
