import os
import logging
from flask import Flask, request, jsonify
import requests
from datetime import datetime

app = Flask(__name__)

# ... (keep your existing APP_ID, APP_SECRET, etc.)

# Configure logging
logging.basicConfig(level=logging.INFO)

# ------------------------------------------------------------
# 1. Robust field extractor for Feishu Bitable fields
# ------------------------------------------------------------
def extract_field_text(field_data):
    """Extract human‑readable text from a Feishu field value."""
    if not field_data:
        return ""
    if isinstance(field_data, (str, int, float)):
        return str(field_data)
    if isinstance(field_data, dict):
        for key in ['text', 'name', 'en_name', 'value', 'label']:
            if key in field_data:
                return str(field_data[key])
        return str(field_data)
    if isinstance(field_data, list):
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                for key in ['text', 'name', 'en_name', 'value']:
                    if key in item:
                        texts.append(str(item[key]))
                        break
                else:
                    texts.append(str(item))
            else:
                texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)

# ------------------------------------------------------------
# 2. Improved date parser (never raises exception)
# ------------------------------------------------------------
def parse_feishu_date(date_val):
    """Convert Feishu date field to datetime or None."""
    if not date_val:
        return None
    if isinstance(date_val, dict) and 'value' in date_val:
        date_val = date_val['value']
    if isinstance(date_val, list) and len(date_val) > 0:
        if isinstance(date_val[0], dict) and 'text' in date_val[0]:
            date_val = date_val[0]['text']
    if isinstance(date_val, (int, float)):
        return datetime.fromtimestamp(date_val / 1000.0)
    if isinstance(date_val, str):
        if date_val.isdigit():
            return datetime.fromtimestamp(int(date_val) / 1000.0)
        try:
            return datetime.strptime(date_val[:10], "%Y-%m-%d")
        except ValueError:
            pass
    return None

# ------------------------------------------------------------
# 3. Analytics endpoint – CORRECTED with Region restored
# ------------------------------------------------------------
@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    # --- Get tenant token ---
    tat = get_tenant_access_token()
    if not tat:
        logging.error("Failed to obtain Tenant Access Token")
        return jsonify({"error": "Authentication failed"}), 500
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}

    # --- Parse filters from URL ---
    region_filter = request.args.get('region', 'PK').strip().upper()
    acm_filter = request.args.get('acm', 'All').strip().lower()
    type_filter = request.args.get('type', 'All').strip().lower()
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')

    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") if date_to else None

    # --- Fetch data from Feishu (pagination up to 2000 records) ---
    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search"
    all_items = []
    page_token = ""
    total_pages = 4

    for _ in range(total_pages):
        payload = {"page_size": 500}
        if page_token:
            payload["page_token"] = page_token
        try:
            resp = requests.post(req_url, headers=headers, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.error(f"Feishu API error: {e}")
            break

        if data.get("code") != 0:
            logging.error(f"Feishu API returned error: {data}")
            break

        items = data.get("data", {}).get("items", [])
        all_items.extend(items)
        logging.info(f"Fetched {len(items)} records, total so far: {len(all_items)}")

        page_token = data.get("data", {}).get("page_token")
        if not data.get("data", {}).get("has_more"):
            break

    logging.info(f"Total records retrieved: {len(all_items)}")

    # --- Statistics accumulators ---
    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {},
        "creation_types": {},
        "agency_types": {},
        "other_apps": {},
        "reject_reasons": {},
        "closing_reasons_pie": {},
        "acm_closing_reasons": {},
        "daily_trend": {},
        "scanned_rows": len(all_items)
    }

    # --- Process each record ---
    for item in all_items:
        fields = item.get("fields", {})

        # 1. Date parsing
        record_dt = parse_feishu_date(fields.get("Submitted on"))

        if record_dt:
            if from_dt and record_dt.date() < from_dt.date():
                continue
            if to_dt and record_dt.date() > to_dt.date():
                continue

        # 2. Extract fields (with fallback keys)
        req_type = extract_field_text(fields.get("Request Type"))
        status = extract_field_text(fields.get("Status") or fields.get("Request Status"))
        acm = extract_field_text(fields.get("Acm Name") or fields.get("Acm") or fields.get("Assigned Member"))
        agency_type = extract_field_text(fields.get("Agency Type") or fields.get("Agencies Type"))
        creation_type = extract_field_text(fields.get("Creation Type") or fields.get("Agencies Creation Type"))
        region = extract_field_text(fields.get("Region")).strip().upper()   # <-- RESTORED
        reject_reason = extract_field_text(fields.get("Reject Reason") or fields.get("Rejection Reason") or fields.get("Agencies Rejection reason"))
        closing_reason = extract_field_text(fields.get("Closing Reason") or fields.get("Closing Agencies Reason"))
        other_app = extract_field_text(fields.get("Otherapp Name"))

        # 3. Apply UI filters
        # --- Region filter (strict equality) ---
        if region_filter not in ['ALL', ''] and region != region_filter:
            continue

        # --- ACM filter ---
        # Note: acm_filter is lowercased, acm is title-cased in original code but here we compare lowercased
        if acm_filter not in ['all', 'all acms', ''] and acm_filter not in acm.lower():
            continue

        # --- Type filter ---
        if type_filter not in ['all', 'all types', ''] and type_filter not in agency_type.lower():
            continue

        # 4. Populate statistics
        if record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            stats["daily_trend"][date_str] = stats["daily_trend"].get(date_str, 0) + 1

        if agency_type:
            stats["agency_types"][agency_type] = stats["agency_types"].get(agency_type, 0) + 1
        if creation_type:
            stats["creation_types"][creation_type] = stats["creation_types"].get(creation_type, 0) + 1

        # Request type classification
        if "Agency Creation" in req_type:
            stats["kpis"]["creations"] += 1
            if "Done" in status:
                stats["creation_status"]["Done"] += 1
            elif "Rejected" in status:
                stats["creation_status"]["Rejected"] += 1
            elif status:
                stats["creation_status"]["Under Investigation"] += 1

            if "Done" in status and acm:
                stats["acm_performance"][acm] = stats["acm_performance"].get(acm, 0) + 1
            if other_app:
                stats["other_apps"][other_app] = stats["other_apps"].get(other_app, 0) + 1

        elif "BD Creation" in req_type:
            stats["kpis"]["bds"] += 1
            if "Done" in status:
                stats["bd_status"]["Done"] += 1
            elif "Rejected" in status:
                stats["bd_status"]["Rejected"] += 1
            elif status:
                stats["bd_status"]["Under Investigation"] += 1

        elif "Closing Agency" in req_type:
            stats["kpis"]["closings"] += 1
            if "Done" in status:
                stats["closing_status"]["Done"] += 1
            elif "Rejected" in status:
                stats["closing_status"]["Rejected"] += 1
            elif status:
                stats["closing_status"]["Under Investigation"] += 1

            if closing_reason:
                stats["closing_reasons_pie"][closing_reason] = stats["closing_reasons_pie"].get(closing_reason, 0) + 1
                if acm:
                    if acm not in stats["acm_closing_reasons"]:
                        stats["acm_closing_reasons"][acm] = {"User Request": 0, "Duplicated Hosting": 0}
                    if "User Request" in closing_reason:
                        stats["acm_closing_reasons"][acm]["User Request"] += 1
                    elif "Duplicated" in closing_reason:
                        stats["acm_closing_reasons"][acm]["Duplicated Hosting"] += 1

        if "Rejected" in status and reject_reason:
            stats["reject_reasons"][reject_reason] = stats["reject_reasons"].get(reject_reason, 0) + 1

    logging.info(f"Processed {len(all_items)} records, stats: {stats['kpis']}")
    return jsonify(stats)
