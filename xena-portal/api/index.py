import os
from flask import Flask, request, jsonify, send_file
import requests
from datetime import datetime

app = Flask(__name__)

# --- SECURE CONFIGURATION ---
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"

def get_tenant_access_token():
    if not APP_ID or not APP_SECRET:
        return None
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, json=payload).json()
    return response.get("tenant_access_token")

# --- 1. FRONT DOOR: Serve the HTML Dashboard ---
@app.route('/', methods=['GET'])
def home():
    # This automatically looks up one folder to securely grab your UI
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    html_path = os.path.join(root_dir, 'index.html')
    return send_file(html_path)

# --- 2. BACKDOOR: Serve the API Data ---
@app.route('/api/search', methods=['GET'])
def search_agency():
    agency_code = request.args.get('code')
    
    if not agency_code:
        return jsonify({"error": "No agency code provided"}), 400

    token = get_tenant_access_token()
    
    if not token:
        return jsonify({"error": "Authentication Failed. Please check Vercel Environment Variables."}), 500

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    current_month = datetime.now().month
    current_year = datetime.now().year

    # FETCH BASE POINTS
    points_payload = {
        "filter": {"conjunction": "and", "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]}
    }
    points_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    points_response = requests.post(points_url, headers=headers, json=points_payload).json()
    
    base_points = 0
    if 'data' in points_response and 'items' in points_response['data'] and len(points_response['data']['items']) > 0:
        latest_points_record = points_response['data']['items'][0]
        fields = latest_points_record.get('fields', {})
        raw_bp = fields.get('Base Points', 0)
        
        if isinstance(raw_bp, list):
            raw_bp = raw_bp[0].get('text', 0) if len(raw_bp) > 0 else 0
        elif isinstance(raw_bp, dict):
            raw_bp = raw_bp.get('text', 0)
        
        clean_bp = str(raw_bp).replace(',', '').strip()
        try:
            base_points = float(clean_bp)
        except ValueError:
            base_points = 0

    # FETCH REQUESTS
    req_payload = {
        "filter": {"conjunction": "and", "conditions": [
                {"field_name": "Agency Code", "operator": "is", "value": [agency_code]},
                {"field_name": "Request Type", "operator": "is", "value": ["Agency Target Privilege"]}
            ]}
    }
    req_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=req_payload).json()
    
    valid_requests = []
    if 'data' in req_response and 'items' in req_response['data']:
        for item in req_response['data']['items']:
            fields = item.get('fields', {})
            timestamp_ms = fields.get('Submitted on') 
            if timestamp_ms:
                try:
                    record_date = datetime.fromtimestamp(int(timestamp_ms) / 1000.0)
                    if record_date.month == current_month and record_date.year == current_year:
                        valid_requests.append(fields)
                except Exception:
                    pass

    return jsonify({
        "base_points": base_points,
        "requests": valid_requests
    })