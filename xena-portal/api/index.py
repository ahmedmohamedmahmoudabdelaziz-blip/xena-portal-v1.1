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

# =========================================================================
# GROUND TRUTH — extracted directly from the real exported data.
# Request Type has EXACTLY these values. Everything NOT in this map is
# ignored for KPI purposes (it is NOT a creation/bd/closing request).
# =========================================================================
REQUEST_TYPE_AGENCY_CREATION = "agency creation"
REQUEST_TYPE_BD_CREATION = "bd creation"
REQUEST_TYPE_CLOSING_AGENCY = "closing agency"

# Status has EXACTLY these 3 values, but Lark exports "Done" with a
# TRAILING SPACE ("Done "). This is the real reason KPIs were showing 0 -
# not a column-name mismatch. Always strip() before comparing.
STATUS_DONE = "done"
STATUS_REJECTED = "rejected"
STATUS_UNDER_INVESTIGATION = "under investigation"


def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, json=payload, timeout=10).json()
    return response.get("tenant_access_token")


def get_field(fields, name):
    """Exact field lookup. Column names are known and fixed from ground truth."""
    if not fields:
        return None
    return fields.get(name)


def extract_field_text(field_data):
    """Flatten any Feishu field shape (string / number / dict / list-of-dict) to plain text."""
    if field_data is None:
        return ""
    if isinstance(field_data, (str, int, float)):
        return str(field_data)
    if isinstance(field_data, dict):
        for key in ('text', 'name', 'en_name', 'value', 'label'):
            if key in field_data:
                return str(field_data[key])
        if 'id' in field_data:
            return str(field_data['id'])
        return str(field_data)
    if isinstance(field_data, list):
        if not field_data:
            return ""
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                found = False
                for key in ('text', 'name', 'en_name', 'value'):
                    if key in item:
                        texts.append(str(item[key]))
                        found = True
                        break
                if not found and 'id' in item:
                    texts.append(str(item['id']))
            else:
                texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)


def clean(field_data):
    """extract_field_text + strip whitespace. Use this everywhere before comparisons."""
    return extract_field_text(field_data).strip()


def parse_feishu_date(date_val):
    """Handles both millisecond timestamps and 'YYYY/MM/DD HH:MM' style strings."""
    if not date_val:
        return None
    if isinstance(date_val, list) and date_val:
        date_val = date_val[0]
    if isinstance(date_val, dict):
        date_val = date_val.get('value', date_val.get('text', ''))

    dt = None
    if isinstance(date_val, (int, float)):
        dt = datetime.fromtimestamp(date_val / 1000.0)
    elif isinstance(date_val, str):
        s = date_val.strip()
        if s.isdigit():
            dt = datetime.fromtimestamp(int(s) / 1000.0)
        else:
            for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(s[:16], fmt)
                    break
                except ValueError:
                    continue
    if dt:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
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
    if not code:
        return "SSO Authorization Failed.", 400

    tat = get_tenant_access_token()
    token_url = "https://open.feishu.cn/open-apis/authen/v1/oidc/access_token"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    payload = {"grant_type": "authorization_code", "code": code}
    token_resp = requests.post(token_url, headers=headers, json=payload, timeout=10).json()

    user_access_token = token_resp.get("data", {}).get("access_token")
    if not user_access_token:
        return "SSO Error: Could not verify user token.", 500

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
    if not uat:
        return jsonify({"error": "Unauthorized session."}), 401
    if not agency_code:
        return jsonify({"error": "No agency code provided"}), 400

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
    if points_response.get("code") != 0:
        return jsonify({"error": f"Feishu API Blocked: {points_response.get('msg')}"}), 403

    items = points_response.get('data', {}).get('items', [])
    if not items:
        return jsonify({"error": f"⚠️ Notice: Access Denied: Agency {agency_code} is not related to your team."}), 403

    fields = items[0].get('fields', {})
    sheet_acm_name = clean(get_field(fields, 'Acm Name (PK)')) or clean(get_field(fields, 'Acm Name (IN)')) or clean(get_field(fields, 'Assigned Member'))
    try:
        base_points = float(clean(get_field(fields, 'Base Points')).replace(',', '') or 0)
    except ValueError:
        base_points = 0

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    req_response = requests.post(req_url, headers=headers, json=points_payload, timeout=10).json()

    valid_requests = []
    if req_response.get("code") == 0:
        cm, cy = datetime.now().month, datetime.now().year
        for item in req_response.get('data', {}).get('items', []):
            r_fields = item.get('fields', {})
            ts = parse_feishu_date(get_field(r_fields, 'Submitted on'))
            if ts and ts.month == cm and ts.year == cy:
                valid_requests.append(r_fields)

    return jsonify({"base_points": base_points, "requests": valid_requests, "acm": sheet_acm_name.title(), "role": "Verified by Feishu"})


# =========================================================================
# ANALYTICS — rebuilt against the ground-truth chart specification.
# Every filter below mirrors, field for field, the exact Lark chart config.
# =========================================================================
@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    username = request.args.get('user', '').lower()
    uat = request.args.get('uat', '')
    if not uat:
        return jsonify({"error": "Unauthorized session. Please log in again."}), 401
    if not any(admin in username for admin in ADMIN_USERS):
        return jsonify({"error": "Unauthorized. Analytics are restricted to Administrators."}), 403

    tat = get_tenant_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})

    region_filter = request.args.get('region', 'ALL').strip().lower()
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')

    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1) if date_to else None

    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"

    # ---- Pull every record (with loop protection) -----------------------
    # We fetch everything and filter in Python, because "Submitted on" in
    # this base is a TEXT column ("2026/04/07 13:10"), not a real Date
    # field, so Feishu's native isGreaterEqual/isLess date filters don't
    # reliably apply to it. Filtering here guarantees correctness.
    all_items = []
    seen_ids = set()
    page_token = ""
    error_msg = None
    payload = {"page_size": 500}

    for _ in range(50):
        if page_token:
            payload["page_token"] = page_token
        try:
            res = session.post(req_url, json=payload, timeout=15).json()
        except Exception as e:
            error_msg = str(e)
            break
        if res.get("code") != 0:
            error_msg = res.get("msg")
            break

        items = res.get("data", {}).get("items", [])
        new_count = 0
        for item in items:
            rid = item.get("record_id")
            if rid not in seen_ids:
                seen_ids.add(rid)
                all_items.append(item)
                new_count += 1

        # Loop-destroyer: Feishu occasionally repeats a page. If a whole
        # page came back with nothing new, stop instead of looping.
        if new_count == 0:
            break

        page_token = res.get("data", {}).get("page_token")
        if not page_token:
            break

    # ---- Aggregate ---------------------------------------------------
    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {}, "creation_types": {}, "agency_types": {},
        "other_apps": {}, "reject_reasons": {}, "closing_reasons_pie": {},
        "acm_closing_reasons": {}, "daily_trend": {},
        "scanned_rows": len(all_items), "error_debug": error_msg,
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

        record_dt = parse_feishu_date(get_field(fields, 'Submitted on'))
        if from_dt or to_dt:
            if not record_dt or (from_dt and record_dt < from_dt) or (to_dt and record_dt >= to_dt):
                continue

        region = clean(get_field(fields, 'Region')).lower()
        if region_filter not in ('all', '') and region != region_filter:
            continue

        req_type = clean(get_field(fields, 'Request Type')).lower()
        status = clean(get_field(fields, 'Status')).lower()  # .strip() fixes the "Done " bug
        is_done = status == STATUS_DONE
        is_rejected = status == STATUS_REJECTED

        acm_pk = clean(get_field(fields, 'Acm Name (PK)'))
        acm_in = clean(get_field(fields, 'Acm Name (IN)'))
        acm = acm_in if region == "in" else acm_pk
        if not acm:
            acm = clean(get_field(fields, 'Assigned Member'))

        if acm_filter not in ('all', 'all acms', '') and acm_filter != acm.lower():
            continue

        agency_type = clean(get_field(fields, 'Agency Type'))
        # Normalize casing variants: "Acm hunting" vs "ACM hunting" -> same bucket
        agency_type_norm = agency_type.title() if agency_type else ""
        if type_filter not in ('all', 'all types', '') and type_filter != agency_type.lower():
            continue

        # ---- KPI #12 Daily trend: ANY request type, Status Done, in-range ----
        if is_done and record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            if date_str in stats["daily_trend"]:
                stats["daily_trend"][date_str] += 1

        # ---- Exact-match KPI routing (ground truth values only) --------
        if req_type == REQUEST_TYPE_CLOSING_AGENCY:
            stats["kpis"]["closings"] += 1
            if is_done:
                stats["closing_status"]["Done"] += 1
            elif is_rejected:
                stats["closing_status"]["Rejected"] += 1
            else:
                stats["closing_status"]["Under Investigation"] += 1

            closing_reason = clean(get_field(fields, 'Closing Reason'))
            if closing_reason:
                stats["closing_reasons_pie"][closing_reason] = stats["closing_reasons_pie"].get(closing_reason, 0) + 1
                if acm:
                    clean_acm = acm.title()
                    bucket = stats["acm_closing_reasons"].setdefault(clean_acm, {})
                    bucket[closing_reason] = bucket.get(closing_reason, 0) + 1

        elif req_type == REQUEST_TYPE_BD_CREATION:
            stats["kpis"]["bds"] += 1
            if is_done:
                stats["bd_status"]["Done"] += 1
            elif is_rejected:
                stats["bd_status"]["Rejected"] += 1
            else:
                stats["bd_status"]["Under Investigation"] += 1

        elif req_type == REQUEST_TYPE_AGENCY_CREATION:
            stats["kpis"]["creations"] += 1
            if is_done:
                stats["creation_status"]["Done"] += 1
            elif is_rejected:
                stats["creation_status"]["Rejected"] += 1
            else:
                stats["creation_status"]["Under Investigation"] += 1

            if is_done and acm:
                clean_acm = acm.title()
                stats["acm_performance"][clean_acm] = stats["acm_performance"].get(clean_acm, 0) + 1

            other_app = clean(get_field(fields, 'Otherapp Name'))
            if is_done and other_app:
                stats["other_apps"][other_app] = stats["other_apps"].get(other_app, 0) + 1

            creation_type = clean(get_field(fields, 'Create Way'))
            if creation_type:
                stats["creation_types"][creation_type] = stats["creation_types"].get(creation_type, 0) + 1

            if agency_type_norm:
                stats["agency_types"][agency_type_norm] = stats["agency_types"].get(agency_type_norm, 0) + 1

            if is_rejected:
                reject_reason = clean(get_field(fields, 'Reject Reason'))
                if reject_reason:
                    stats["reject_reasons"][reject_reason] = stats["reject_reasons"].get(reject_reason, 0) + 1

        # NOTE: everything else (Agency Target Privilege, Agency Point
        # Request, Add & Remove Host Sign, App Rating Reward, Add & Change
        # Short ID, Room Pinup, Welcome Package, Change agency name,
        # Add & Remove games, Remove BD, Change Agency Ownership,
        # Change Agency Manger ID, Remove short ID, "Agency applied
        # already by ACM or BD link") is intentionally NOT counted toward
        # any KPI here — it never was Agency/BD/Closing traffic.

    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend"] = dict(sorted(stats["daily_trend"].items()))
    stats["creation_types"] = dict(sorted(stats["creation_types"].items(), key=lambda x: x[1], reverse=True))
    stats["agency_types"] = dict(sorted(stats["agency_types"].items(), key=lambda x: x[1], reverse=True))

    return jsonify(stats)


if __name__ == '__main__':
    app.run(debug=True)
