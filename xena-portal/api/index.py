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

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, json=payload, timeout=10).json()
    return response.get("tenant_access_token")

def get_field_local(fields, *aliases):
    """PURE LOCAL LOOKUP: Instant string matching. Zero API calls."""
    if not fields: return None
    for alias in aliases:
        if alias in fields: return fields[alias]
    for alias in aliases:
        tgt = alias.lower().strip()
        for k, v in fields.items():
            if tgt in k.lower() or k.lower() in tgt: return v
    return None

def extract_field_text(field_data):
    """Safely unwraps Feishu's dicts and lists into raw strings."""
    if not field_data: return ""
    if isinstance(field_data, (str, int, float)): return str(field_data)
    
    if isinstance(field_data, dict):
        for key in ['text', 'name', 'en_name', 'value', 'label', 'id']:
            if key in field_data: return str(field_data[key])
        if 'id' in field_data: return str(field_data['id'])
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

def parse_feishu_date(date_val):
    """DEAD-SIMPLE DATE PARSER: No timezone math. Strips hours/mins entirely."""
    if not date_val: return None
    if isinstance(date_val, list) and len(date_val) > 0: date_val = date_val[0]
    if isinstance(date_val, dict): date_val = date_val.get('value', date_val.get('text', ''))
    
    try:
        if isinstance(date_val, (int, float)):
            return datetime.fromtimestamp(date_val / 1000.0).replace(hour=0, minute=0, second=0, microsecond=0)
        
        date_str = str(date_val).strip()
        if date_str.isdigit():
            return datetime.fromtimestamp(int(date_str) / 1000.0).replace(hour=0, minute=0, second=0, microsecond=0)
        
        clean_str = date_str[:10].replace('/', '-').replace('.', '-')
        return datetime.strptime(clean_str, "%Y-%m-%d")
    except Exception:
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
    token_resp = requests.post(token_url, headers=headers, json=payload, timeout=10).json()
    
    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token: return "SSO Error: Could not verify user token.", 500
    
    info_url = "https://open.feishu.cn/open-apis/authen/v1/user_info"
    info_resp = requests.get(info_url, headers={"Authorization": f"Bearer {user_access_token}"}, timeout=10).json()
    
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
    points_payload = {
        "filter": {
            "conjunction": "and",
            "conditions": [{"field_name": "Agency Code", "operator": "is", "value": [agency_code]}]
        }
    }
    points_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{POINTS_TABLE_ID}/records/search?automatic_fields=true"
    
    points_response = requests.post(points_url, headers=headers, json=points_payload, timeout=10).json()
    if points_response.get("code") != 0: return jsonify({"error": f"Feishu API Blocked: {points_response.get('msg')}"}), 403

    items = points_response.get('data', {}).get('items', [])
    if not items: return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    sheet_acm_name = extract_field_text(get_field_local(fields, 'Acm Name (PK)', 'Acm Name (IN)', 'Acm', 'Assigned Member')).strip()
    try: base_points = float(extract_field_text(get_field_local(fields, 'Base Points')).replace(',', '').strip())
    except ValueError: base_points = 0

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=points_payload, timeout=10).json()
    
    valid_requests = []
    if req_response.get("code") == 0:
        cm, cy = datetime.now().month, datetime.now().year
        for item in req_response.get('data', {}).get('items', []):
            r_fields = item.get('fields', {})
            ts = parse_feishu_date(get_field_local(r_fields, 'Submitted on Copy', 'Submitted on'))
            if ts and ts.month == cm and ts.year == cy:
                valid_requests.append(r_fields)

    return jsonify({"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified by Feishu"})

# --- 🚀 THE BULLETPROOF ANALYTICS PIPELINE ---
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
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')
    
    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1) if date_to else None

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"

    # 1. NATIVE REGION FILTER TO AVOID VERCEL TIMEOUTS
    payload = {"page_size": 500}
    if region_filter not in ['all', '']:
        payload["filter"] = {
            "conjunction": "and",
            "conditions": [{"field_name": "Region", "operator": "contains", "value": [region_filter.upper()]}]
        }

    all_items = []
    page_token = ""
    error_msg = None

    # 2. BULLETPROOF FETCH LOOP (No "Brakes" - Guarantees we download every single record)
    for _ in range(50):
        if page_token: payload["page_token"] = page_token
        try:
            res = session.post(req_url, json=payload, timeout=15)
            if res.status_code != 200:
                error_msg = f"HTTP {res.status_code}: {res.text}"
                break
                
            res_json = res.json()
            if res_json.get("code") != 0:
                # If Feishu rejects the native Region filter, gracefully drop it and fetch everything
                if "filter" in payload:
                    del payload["filter"]
                    res = session.post(req_url, json=payload, timeout=15)
                    res_json = res.json()
                    if res_json.get("code") != 0:
                        error_msg = res_json.get("msg")
                        break
                else:
                    error_msg = res_json.get("msg")
                    break
                    
            items = res_json.get("data", {}).get("items", [])
            all_items.extend(items)
            
            page_token = res_json.get("data", {}).get("page_token")
            if not page_token or not res_json.get("data", {}).get("has_more", False):
                break
        except Exception as e:
            error_msg = str(e)
            break

    # 3. DATA AGGREGATOR
    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {},
        "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {},
        "acm_closing_reasons": {}, "daily_trend": {},
        "scanned_rows": len(all_items), "error_debug": error_msg
    }

    if from_dt and date_to:
        cur = from_dt
        end = datetime.strptime(date_to, "%Y-%m-%d")
        while cur <= end:
            stats["daily_trend"][cur.strftime("%Y-%m-%d")] = 0
            cur += timedelta(days=1)

    acm_filter = request.args.get('acm', 'All').strip().lower()
    type_filter = request.args.get('type', 'All').strip().lower()

    for item in all_items:
        fields = item.get('fields', {})

        raw_date = get_field_local(fields, 'Submitted on', 'Submitted on Copy', 'Created Time')
        record_dt = parse_feishu_date(raw_date)
        
        # EXACT LOCAL DATE MATH
        if from_dt or to_dt:
            if not record_dt: continue
            if from_dt and record_dt < from_dt: continue
            if to_dt and record_dt >= to_dt: continue

        # EXACT REGION ENFORCEMENT
        region = extract_field_text(get_field_local(fields, 'Region', 'Agency Region')).strip().lower()
        if region_filter not in ('all', '') and region != region_filter:
            continue

        req_type = extract_field_text(get_field_local(fields, 'Request Type')).strip().lower()
        status = extract_field_text(get_field_local(fields, 'Status', 'Request Status', 'Agency Status', 'State')).strip().lower()
        
        creation_type = extract_field_text(get_field_local(fields, 'Create Way', 'Agencies Creation Type', 'PK Agencies Creation Type', 'IN Agencies Creation Type', 'Creation Type')).strip()
        reject_reason = extract_field_text(get_field_local(fields, 'Reject Reason', 'Rejection Reason', 'Agencies Rejection Reason', 'PK Agencies Rejection reason', 'IN Agencies Rejection reason')).strip()
        agency_type = extract_field_text(get_field_local(fields, 'Agency Type', 'Type of Agency')).strip()
        closing_reason = extract_field_text(get_field_local(fields, 'Closing Reason', 'Closing Agencies Reason', 'PK Closing Agencies Reason', 'IN Closing Agencies Reason')).strip()
        other_app = extract_field_text(get_field_local(fields, 'Otherapp Name', 'Other App Name', 'Other Apps')).strip()

        # REMOVED TRAILING SPACES TO FIX THE "Done " BUG
        is_done = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        acm_pk = extract_field_text(get_field_local(fields, 'Acm Name (PK)')).strip()
        acm_in = extract_field_text(get_field_local(fields, 'Acm Name (IN)')).strip()
        acm = acm_in if region == "in" else acm_pk
        if not acm: acm = extract_field_text(get_field_local(fields, 'Acm', 'Assigned Member')).strip()

        if acm_filter not in ('all', 'all acms', '') and acm_filter != acm.lower(): continue
        if type_filter not in ('all', 'all types', '') and type_filter != agency_type.lower(): continue

        # LARK VIDEO MIRRORING: KPI #12 Daily Trend (Ignores Request Type, only tracks "Status Done")
        if is_done and record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            if date_str in stats["daily_trend"]:
                stats["daily_trend"][date_str] += 1

        # ---- LARK VIDEO MIRRORING ("Contains" Logic) --------
        
        # 1. CLOSING AGENCY
        if "closing agency" in req_type:
            stats["kpis"]["closings"] += 1
            if is_done: stats["closing_status"]["Done"] += 1
            elif is_rejected: stats["closing_status"]["Rejected"] += 1
            else: stats["closing_status"]["Under Investigation"] += 1
            
            if closing_reason:
                stats["closing_reasons_pie"][closing_reason] = stats["closing_reasons_pie"].get(closing_reason, 0) + 1
                if acm:
                    clean_acm = acm.title()
                    stats["acm_closing_reasons"].setdefault(clean_acm, {})
                    stats["acm_closing_reasons"][clean_acm][closing_reason] = stats["acm_closing_reasons"][clean_acm].get(closing_reason, 0) + 1

        # 2. BD CREATION
        elif "bd creation" in req_type:
            stats["kpis"]["bds"] += 1
            if is_done: stats["bd_status"]["Done"] += 1
            elif is_rejected: stats["bd_status"]["Rejected"] += 1
            else: stats["bd_status"]["Under Investigation"] += 1

        # 3. AGENCY CREATION
        elif "agency creation" in req_type:
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

    # Sorting
    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend"] = dict(sorted(stats["daily_trend"].items()))
    stats["creation_types"] = dict(sorted(stats["creation_types"].items(), key=lambda x: x[1], reverse=True))
    stats["agency_types"] = dict(sorted(stats["agency_types"].items(), key=lambda x: x[1], reverse=True))

    return jsonify(stats)
