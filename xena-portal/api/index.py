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
REDIRECT_URI = "https://xena-portal-v1-1.vercel.app/api/callback" # Your exact Vercel URL
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"

# --- MASTER ADMINS (Can see all agencies) ---
# Type your exact Lark name here in lowercase
ADMIN_USERS = ["Ahmed Samurai", "xena admin"] 

# --- SMART CACHE (Lightning Fast Searches) ---
cache = {}
CACHE_EXPIRY = 600 # Remembers searches for 10 minutes

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, json=payload).json()
    return response.get("tenant_access_token")

def generate_secure_token(name):
    # Creates an un-hackable session token using your secret key
    raw = f"{name}-{APP_SECRET}"
    return hashlib.sha256(raw.encode()).hexdigest()

# --- FRONT DOOR: Serve HTML ---
@app.route('/', methods=['GET'])
def home():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return send_file(os.path.join(root_dir, 'index.html'))

# --- SSO STEP 1: Send user to Lark to login ---
@app.route('/api/login', methods=['GET'])
def login():
    safe_redirect = urllib.parse.quote(REDIRECT_URI)
    lark_url = f"https://open.larksuite.com/open-apis/authen/v1/user_auth_page_ctrl?app_id={APP_ID}&redirect_uri={safe_redirect}"
    return redirect(lark_url)

# --- SSO STEP 2: Lark sends them back here with a verification code ---
@app.route('/api/callback', methods=['GET'])
def callback():
    code = request.args.get('code')
    if not code:
        return "SSO Authorization Failed. No code provided.", 400
        
    tat = get_tenant_access_token()
    
    # 1. Exchange code for User Token
    token_url = "https://open.larksuite.com/open-apis/authen/v1/oidc/access_token"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    payload = {"grant_type": "authorization_code", "code": code}
    token_resp = requests.post(token_url, headers=headers, json=payload).json()
    
    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token:
        return "SSO Error: Could not verify user token.", 500
        
    # 2. Get User's Real Lark Name
    info_url = "https://open.larksuite.com/open-apis/authen/v1/user_info"
    info_headers = {"Authorization": f"Bearer {user_access_token}"}
    info_resp = requests.get(info_url, headers=info_headers).json()
    
    lark_name = info_resp.get("data", {}).get("name", "Unknown User")
    
    # 3. Create a secure session token & send them to the dashboard!
    secure_token = generate_secure_token(lark_name)
    safe_name = urllib.parse.quote(lark_name)
    return redirect(f"/?user={safe_name}&token={secure_token}")

# --- BACKDOOR: Serve Data ---
@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    username = request.args.get('user', '')
    token = request.args.get('token', '')
    
    # SECURITY: Verify the user is actually logged in
    if not username or token != generate_secure_token(username):
        return jsonify({"error": "Unauthorized. Please Log In via Lark."}), 401

    if not agency_code:
        return jsonify({"error": "No agency code provided"}), 400

    # CACHE CHECK
    cache_key = f"{agency_code}_{username}"
    if cache_key in cache and (time.time() - cache[cache_key]['time'] < CACHE_EXPIRY):
        return jsonify(cache[cache_key]['data'])

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    # FETCH BASE POINTS & CHECK ACM
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    
    base_points = 0
    acm_name = ""

    if 'data' in points_response and 'items' in points_response['data'] and len(points_response['data']['items']) > 0:
        fields = points_response['data']['items'][0].get('fields', {})
        
        raw_acm = fields.get('Acm', '')
        if isinstance(raw_acm, list) and len(raw_acm) > 0: acm_name = raw_acm[0].get('text', '')
        elif isinstance(raw_acm, dict): acm_name = raw_acm.get('text', '')
        else: acm_name = str(raw_acm)

        # ROLE-BASED ACCESS CONTROL
        user_lower = username.lower()
        acm_lower = acm_name.lower()
        
        is_admin = any(admin in user_lower for admin in ADMIN_USERS)
        is_owner = (acm_lower in user_lower) or (user_lower in acm_lower)
        
        if not is_admin and not is_owner:
            return jsonify({"error": f"Access Denied: Agency {agency_code} is assigned to {acm_name.title()}, not you."}), 403

        raw_bp = fields.get('Base Points', 0)
        if isinstance(raw_bp, list): raw_bp = raw_bp[0].get('text', 0) if len(raw_bp) > 0 else 0
        elif isinstance(raw_bp, dict): raw_bp = raw_bp.get('text', 0)
        try: base_points = float(str(raw_bp).replace(',', '').strip())
        except ValueError: base_points = 0
    else:
        return jsonify({"error": f"Agency code '{agency_code}' does not exist in the database."}), 404

    # FETCH REQUESTS
    req_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}, {"field_name": "Request Type", "operator": "is", "value": ["Agency Target Privilege"]}]}}
    req_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
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

    # SAVE TO CACHE & RETURN
    final_data = {"base_points": base_points, "requests": valid_requests, "acm": acm_name.title()}
    cache[cache_key] = {"time": time.time(), "data": final_data}
    return jsonify(final_data)
