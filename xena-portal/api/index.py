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
ROLES_TABLE_ID = "tblpgMD1AfAXQYZd"

cache = {}
CACHE_EXPIRY = 600 

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, json=payload).json()
    return response.get("tenant_access_token")

def generate_secure_token(name):
    raw = f"{name}-{APP_SECRET}"
    return hashlib.sha256(raw.encode()).hexdigest()

# --- THE UNIVERSAL TEXT EXTRACTOR ---
# This forces Feishu's complicated Person/Option fields into plain readable text
def extract_field_text(field_data):
    if not field_data: 
        return ""
    if isinstance(field_data, (str, int, float)): 
        return str(field_data)
    if isinstance(field_data, dict):
        if 'text' in field_data: return str(field_data['text'])
        if 'name' in field_data: return str(field_data['name'])
        if 'en_name' in field_data: return str(field_data['en_name'])
        return str(field_data)
    if isinstance(field_data, list):
        return " ".join([extract_field_text(item) for item in field_data])
    return str(field_data)

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
    info_headers = {"Authorization": f"Bearer {user_access_token}"}
    info_resp = requests.get(info_url, headers=info_headers).json()
    
    lark_name = info_resp.get("data", {}).get("name", "Unknown User")
    
    secure_token = generate_secure_token(lark_name)
    safe_name = urllib.parse.quote(lark_name)
    return redirect(f"/?user={safe_name}&token={secure_token}")

@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    raw_username = request.args.get('user', '') 
    username = raw_username.lower().strip() 
    token = request.args.get('token', '') 
    
    if not raw_username or token != generate_secure_token(raw_username):
        return jsonify({"error": "Unauthorized session. Please Log In via Feishu."}), 401
    if not agency_code:
        return jsonify({"error": "No agency code provided"}), 400

    cache_key = f"{agency_code}_{username}"
    if cache_key in cache and (time.time() - cache[cache_key]['time'] < CACHE_EXPIRY):
        return jsonify(cache[cache_key]['data'])

    tat = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    # ----------------------------------------------------
    # 1. FETCH USER ROLE & MAPPING FROM BITABLE (Max 500 rows to ensure we don't miss you)
    # ----------------------------------------------------
    roles_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{ROLES_TABLE_ID}/records/search?automatic_fields=true"
    roles_payload = {"page_size": 500}
    roles_resp = requests.post(roles_url, headers=headers, json=roles_payload).json()
    
    user_role = "ACM" # Default fallback
    acm_value = ""
    
    if 'data' in roles_resp and 'items' in roles_resp['data']:
        for item in roles_resp['data']['items']:
            fields = item.get('fields', {})
            
            # Using the new Extractor to guarantee we get plain text!
            agents1 = extract_field_text(fields.get('Agent Name')).lower()
            agents2 = extract_field_text(fields.get('Agent name 2')).lower()
            
            if username in agents1 or username in agents2:
                user_role = extract_field_text(fields.get('Role')).upper().strip()
                acm_value = extract_field_text(fields.get('Acm Value')).lower().strip()
                break

    # ----------------------------------------------------
    # 2. FETCH AGENCY DATA
    # ----------------------------------------------------
    points_payload = {"filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}}
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    
    base_points = 0
    sheet_acm_name = ""

    if 'data' in points_response and 'items' in points_response['data'] and len(points_response['data']['items']) > 0:
        fields = points_response['data']['items'][0].get('fields', {})
        
        sheet_acm_name = extract_field_text(fields.get('Acm')).strip()
        sheet_acm_clean = sheet_acm_name.lower()

        # --- BULLETPROOF SECURITY CHECK ENGINE ---
        is_super_user = ("ADMIN" in user_role or "CS" in user_role)
        is_authorized_owner = (acm_value != "" and acm_value in sheet_acm_clean)
        
        # New requested error message!
        if not is_super_user and not is_authorized_owner:
            return jsonify({"error": f"Access Denied: Agency {agency_code} is not related to your team"}), 403

        raw_bp = extract_field_text(fields.get('Base Points')).replace(',', '').strip()
        try: base_points = float(raw_bp)
        except ValueError: base_points = 0
    else:
        return jsonify({"error": f"Agency code '{agency_code}' does not exist in the database."}), 404

    # ----------------------------------------------------
    # 3. FETCH REQUESTS
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

    final_data = {"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": user_role}
    cache[cache_key] = {"time": time.time(), "data": final_data}
    return jsonify(final_data)
