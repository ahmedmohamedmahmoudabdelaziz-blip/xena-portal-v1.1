import os
import time
import urllib.parse
import logging
from flask import Flask, request, jsonify, send_file, redirect
import requests
from datetime import datetime, timedelta, timezone

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

# --- 🎯 CANONICAL REQUEST TYPES ---
# These are the EXACT strings Feishu sends in the "Request Type" field.
# We match on these instead of loose substrings, because loose substrings
# ("bd" in text, or "everything else = creation") were silently swallowing
# unrelated request types into the wrong KPI bucket.
# "Agency Creation" also folds in the follow-up type below by design - a
# follow-up on an agency already applied for via an ACM/BD link is still
# part of the Agency Creation KPI, per how the native Feishu dashboard counts it.
RT_CREATION_SET = {"agency creation", "agency applied already by acm or bd link ( follow-up )"}
RT_BD = "bd creation"
RT_CLOSING = "closing agency"

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
    if not date_val: return None
    if isinstance(date_val, list) and len(date_val) > 0: date_val = date_val[0]
    if isinstance(date_val, dict): date_val = date_val.get('value', date_val.get('text', ''))

    dt = None
    if isinstance(date_val, (int, float)):
        dt = datetime.fromtimestamp(date_val / 1000.0)
    elif isinstance(date_val, str):
        if date_val.isdigit():
            dt = datetime.fromtimestamp(int(date_val) / 1000.0)
        else:
            try:
                clean_str = str(date_val)[:10].replace('/', '-').replace('.', '-')
                dt = datetime.strptime(clean_str, "%Y-%m-%d")
            except Exception: pass
    if dt: return dt.replace(hour=0, minute=0, second=0, microsecond=0)
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

    all_items = []
    seen_ids = set()
    page_token = ""
    error_msg = None

    # 1. ATTEMPT NATIVE DATE FILTERING (Fastest Method)
    native_worked = False
    conditions = []
    if region_filter not in ['all', '']:
        conditions.append({"field_name": "Region", "operator": "contains", "value": [region_filter.upper()]})
    if from_dt:
        from_ts = int(from_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        conditions.append({"field_name": "Submitted on", "operator": "isGreaterEqual", "value": [from_ts]})
    if to_dt:
        to_ts = int(to_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        conditions.append({"field_name": "Submitted on", "operator": "isLess", "value": [to_ts]})

    payload = {"page_size": 500}
    if conditions: payload["filter"] = {"conjunction": "and", "conditions": conditions}

    for _ in range(50):
        if page_token: payload["page_token"] = page_token
        res = session.post(req_url, json=payload, timeout=12).json()

        if res.get("code") != 0:
            break  # Native filter rejected, break to fallback

        native_worked = True
        items = res.get("data", {}).get("items", [])
        for item in items:
            rid = item.get("record_id")
            if rid not in seen_ids:
                seen_ids.add(rid)
                all_items.append(item)

        page_token = res.get("data", {}).get("page_token")
        if not page_token: break

    # 2. FALLBACK: NATIVE FILTER FAILED -> FETCH WITH INFINITE LOOP PROTECTION
    if not native_worked:
        all_items = []
        seen_ids = set()
        page_token = ""

        payload = {"page_size": 500}
        if region_filter not in ['all', '']:
            payload["filter"] = {"conjunction": "and", "conditions": [{"field_name": "Region", "operator": "contains", "value": [region_filter.upper()]}]}
        # 🚨 FIX: "Submitted on" is a date field - many records share the same day,
        # so it is NOT a unique sort key. Feishu's pagination cursor can get confused
        # when sorting on a non-unique field and re-send the same page forever. Our
        # "Infinite Loop Destroyer" below was catching that and bailing out early,
        # which silently truncated the fetch to the first ~500 records of the month.
        # "Numbering" is a true auto-increment unique field, so sorting on it gives
        # Feishu a stable, unambiguous cursor and pagination proceeds correctly
        # through the entire table.
        payload["sort"] = [{"field_name": "Numbering", "desc": True}]

        for _ in range(50):
            if page_token: payload["page_token"] = page_token
            try:
                res = session.post(req_url, json=payload, timeout=12).json()
                if res.get("code") != 0: break

                items = res.get("data", {}).get("items", [])
                new_records_in_page = 0

                for item in items:
                    rid = item.get("record_id")
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_items.append(item)
                        new_records_in_page += 1

                # NOTE: we no longer early-exit based on date once we cross from_dt.
                # That shortcut only worked when results arrived in descending date
                # order (the old, unstable "Submitted on" sort). Now that we sort by
                # "Numbering" for pagination stability, records are NOT in date order,
                # so we must walk every page and let the per-record date filter further
                # down in this function do the actual date-range filtering.

                # 🚨 INFINITE LOOP DESTROYER: kept as a genuine safety net - if Feishu
                # really does resend a page with zero new record_ids, stop instead of
                # spinning. With a unique sort key this should no longer trigger
                # prematurely.
                if new_records_in_page == 0: break

                page_token = res.get("data", {}).get("page_token")
                if not page_token: break
            except Exception as e:
                error_msg = str(e)
                break

    # 3. DIAGNOSTIC SCANNER: Aggregate ALL columns found in the downloaded records
    master_keys = set()
    for item in all_items:
        master_keys.update(item.get('fields', {}).keys())
    sample_keys = sorted(list(master_keys))

    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {},
        "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {},
        "acm_closing_reasons": {}, "daily_trend": {},
        "other_request_types": {},  # 🔎 visibility into everything that is NOT a KPI (was silently miscounted before)
        "scanned_rows": len(all_items), "error_debug": error_msg, "feishu_keys": sample_keys
    }

    if from_dt and date_to:
        cur = from_dt
        end = datetime.strptime(date_to, "%Y-%m-%d")
        while cur <= end:
            stats["daily_trend"][cur.strftime("%Y-%m-%d")] = 0
            cur += timedelta(days=1)

    for item in all_items:
        fields = item.get('fields', {})

        record_dt = parse_feishu_date(get_field_local(fields, 'Submitted on Copy', 'Submitted on', 'Created Time'))
        if from_dt or to_dt:
            if not record_dt or (from_dt and record_dt < from_dt) or (to_dt and record_dt >= to_dt): continue

        req_type = extract_field_text(get_field_local(fields, 'Request Type')).strip().lower()

        status_val = get_field_local(fields, 'Status', 'Request Status', 'Agency Status', 'State')
        status = extract_field_text(status_val).strip().lower()

        creation_type = extract_field_text(get_field_local(fields, 'Create Way', 'Creation Type', 'Agency Creation Type')).strip()
        reject_reason = extract_field_text(get_field_local(fields, 'Reject Reason', 'Rejection Reason', 'Agencies Rejection Reason')).strip()
        agency_type = extract_field_text(get_field_local(fields, 'Agency Type', 'Type of Agency')).strip()
        region = extract_field_text(get_field_local(fields, 'Region', 'Agency Region')).strip().lower()
        closing_reason = extract_field_text(get_field_local(fields, 'Closing Reason', 'Closing Agencies Reason')).strip()
        other_app = extract_field_text(get_field_local(fields, 'Otherapp Name', 'Other App Name', 'Other Apps')).strip()

        # 🚀 STATUS MATCHING (kept fuzzy on purpose - Done/Rejected/Under Investigation are stable Feishu labels)
        is_done = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        acm_pk = extract_field_text(get_field_local(fields, 'Acm Name (PK)')).strip()
        acm_in = extract_field_text(get_field_local(fields, 'Acm Name (IN)')).strip()
        acm = acm_in if region == "in" else acm_pk
        if not acm: acm = extract_field_text(get_field_local(fields, 'Acm', 'Assigned Member')).strip()

        if region_filter not in ['all', ''] and region != region_filter: continue
        acm_filter = request.args.get('acm', 'All').strip().lower()
        if acm_filter not in ['all', 'all acms', ''] and acm_filter != acm.lower(): continue
        type_filter = request.args.get('type', 'All').strip().lower()
        if type_filter not in ['all', 'all types', ''] and type_filter != agency_type.lower(): continue

        if is_done and record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            if date_str in stats["daily_trend"]:
                stats["daily_trend"][date_str] += 1

        # 🎯 EXACT KPI CLASSIFICATION
        # Previously: anything that wasn't "clos.." or "bd.." got dumped into "Creations".
        # That caught Agency Target Privilege, Point Requests, Host Sign changes, App Rating
        # Rewards, Short ID changes, Room Pinup, Welcome Package, name changes, etc. and
        # inflated the Creation KPI by roughly 2.5x on real data. We now match the exact
        # Feishu "Request Type" labels for each KPI, and put everything else into
        # "other_request_types" instead of silently miscounting it.
        is_creation_kpi = req_type in RT_CREATION_SET
        is_bd_kpi = req_type == RT_BD
        is_closing_kpi = req_type == RT_CLOSING

        if is_closing_kpi:
            stats["kpis"]["closings"] += 1
            if is_done: stats["closing_status"]["Done"] += 1
            elif is_rejected: stats["closing_status"]["Rejected"] += 1
            else: stats["closing_status"]["Under Investigation"] += 1

            if closing_reason:
                stats["closing_reasons_pie"][closing_reason] = stats["closing_reasons_pie"].get(closing_reason, 0) + 1
                if acm:
                    clean_acm = acm.title()
                    # Build the reason bucket dynamically so we never silently drop a reason
                    # (the old hardcoded {"User Request":0, "Duplicated Hosting":0} dict was
                    # dropping "Acm Request" and "Opened by mistake" records).
                    if clean_acm not in stats["acm_closing_reasons"]:
                        stats["acm_closing_reasons"][clean_acm] = {}
                    bucket = stats["acm_closing_reasons"][clean_acm]
                    bucket[closing_reason] = bucket.get(closing_reason, 0) + 1

        elif is_bd_kpi:
            stats["kpis"]["bds"] += 1
            if is_done: stats["bd_status"]["Done"] += 1
            elif is_rejected: stats["bd_status"]["Rejected"] += 1
            else: stats["bd_status"]["Under Investigation"] += 1

        elif is_creation_kpi:
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
            if reject_reason:
                stats["reject_reasons"][reject_reason] = stats["reject_reasons"].get(reject_reason, 0) + 1

        elif req_type:
            # Anything else (Agency Target Privilege, Point Request, Host Sign, Short ID,
            # Room Pinup, Welcome Package, name change, etc.) is tracked for visibility
            # only - it is NOT one of the three KPI cards.
            label = req_type.title()
            stats["other_request_types"][label] = stats["other_request_types"].get(label, 0) + 1

    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend"] = dict(sorted(stats["daily_trend"].items()))
    stats["creation_types"] = dict(sorted(stats["creation_types"].items(), key=lambda x: x[1], reverse=True))
    stats["agency_types"] = dict(sorted(stats["agency_types"].items(), key=lambda x: x[1], reverse=True))
    stats["other_request_types"] = dict(sorted(stats["other_request_types"].items(), key=lambda x: x[1], reverse=True))

    return jsonify(stats)
