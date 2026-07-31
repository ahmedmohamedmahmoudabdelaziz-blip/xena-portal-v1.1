import json, os, sys, time, traceback, requests
from datetime import datetime, timezone

# ── PATH SETUP ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PUBLIC_DIR = os.path.join(SCRIPT_DIR, "public")
DATA_DIR = os.path.join(PUBLIC_DIR, "data")

def log(msg):
    print(msg, flush=True)

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    log(f"📁 Created: {DATA_DIR}")

def copy_index_html():
    src = os.path.join(SCRIPT_DIR, "index.html")
    dst = os.path.join(PUBLIC_DIR, "index.html")
    
    if os.path.exists(src):
        with open(src, "r", encoding="utf-8") as f:
            content = f.read()
        with open(dst, "w", encoding="utf-8") as f:
            f.write(content)
        log(f"✅ Copied index.html ({len(content)} bytes)")
        return True
    else:
        fallback = "<!DOCTYPE html><html><body><h1>Xena Portal Loading...</h1></body></html>"
        with open(dst, "w", encoding="utf-8") as f:
            f.write(fallback)
        return False

def save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    
    size_mb = os.path.getsize(path) / (1024 * 1024)
    log(f"✅ Saved {filename} ({len(data)} real records) - {size_mb:.2f} MB")

def get_table_schema(table_id, token, base_id):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/fields"
    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15).json()
        if resp.get("code") == 0:
            return [f.get("field_name") for f in resp.get("data", {}).get("items", [])]
    except Exception as e:
        log(f"⚠️ Failed to fetch schema for {table_id}: {e}")
    return []

def extract_field_text(field_data):
    """Safely pull string values out of complex Feishu JSON cells."""
    if not field_data: return ""
    if isinstance(field_data, (str, int, float)): return str(field_data)
    if isinstance(field_data, dict):
        for key in ['text', 'name', 'en_name', 'email', 'value', 'label', 'id']:
            if key in field_data: return str(field_data[key])
        return str(field_data)
    if isinstance(field_data, list):
        if len(field_data) == 0: return ""
        texts = []
        for item in field_data:
            if isinstance(item, dict):
                extracted = False
                for key in ['text', 'name', 'en_name', 'email', 'value', 'id']:
                    if key in item:
                        texts.append(str(item[key]))
                        extracted = True
                        break
                if not extracted: texts.append(str(item))
            else: texts.append(str(item))
        return " ".join(texts).strip()
    return str(field_data)

def is_ghost_row(fields, human_keys):
    """
    The Impenetrable Ghost Row detector.
    We define a strict whitelist of fields that a human MUST have typed 
    for a row to be considered real (e.g., Agency Code, User ID, ACM).
    If NONE of these specific fields exist in the row, it's a ghost.
    """
    human_keys_lower = set(k.lower() for k in human_keys)
    
    for f_key, f_val in fields.items():
        if f_key.lower().strip() in human_keys_lower:
            val = extract_field_text(f_val).strip()
            # If we found even ONE human identifier (like an Agency Code), it's a real row!
            if val and val.lower() not in ('none', 'null', '[]', '{}', '0', '0.0', ''):
                return False 
                
    # If we checked every column and found NO human identifiers, it's a ghost.
    return True

def fetch_all_records(table_id, config, app_id, app_secret, base_id):
    label = config["label"]
    
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=15).json()
    token = resp.get("tenant_access_token")
    if not token: raise Exception(f"Token error: {resp}")
        
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    
    actual_fields = get_table_schema(table_id, token, base_id)
    valid_fields = [f for f in config["aliases"] if f in actual_fields][:100]
    
    api_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/search"
    
    # automatic_fields: False drops all the bloat (creator avatars, emails, metadata)
    base_payload = {"page_size": 500, "automatic_fields": False}
    
    if valid_fields:
        base_payload["field_names"] = valid_fields
        log(f"   ... Projection active: limiting to {len(valid_fields)} columns.")
    
    items = []
    page_token = None
    start = time.time()
    page_num = 1
    consecutive_empty_pages = 0
    
    for _ in range(250):
        if time.time() - start > 120:
            log(f"   ⏱️ {label}: timeout reached (120s)")
            break
            
        payload = dict(base_payload)
        if page_token:
            payload["page_token"] = page_token
            
        resp = session.post(api_url, json=payload, timeout=20)
        data = resp.json()
        
        if data.get("code") != 0:
            log(f"   ❌ {label}: Feishu error {data.get('code')}: {data.get('msg')}")
            break
            
        block = data.get("data", {})
        page_items = block.get("items", [])
        
        real_records_on_page = 0
        for it in page_items:
            fields = it.get("fields", {})
            if not is_ghost_row(fields, config["human_keys"]):
                real_records_on_page += 1
                items.append({"record_id": it.get("record_id"), "fields": fields})
                
        ghosts_on_page = len(page_items) - real_records_on_page
        log(f"   ... Fetched page {page_num} ({real_records_on_page} real records, {ghosts_on_page} ghost rows)")
        
        if real_records_on_page == 0 and len(page_items) > 0:
            consecutive_empty_pages += 1
        else:
            consecutive_empty_pages = 0
            
        if consecutive_empty_pages >= 2:
            log(f"   🛑 Detected {consecutive_empty_pages} pages of pure ghost rows. Spreadsheet bottom reached! Stopping.")
            break
            
        page_token = block.get("page_token")
        if not page_token or not block.get("has_more"):
            break
            
        page_num += 1

    seen = set()
    unique = []
    for it in items:
        rid = it.get("record_id")
        if rid and rid not in seen:
            seen.add(rid)
            unique.append(it)
            
    return unique

TABLES = [
    {
        "id": "tblFMYa3dP3Ciu0V",
        "filename": "requests.json",
        "label": "Requests",
        # If none of these exist, it's IMPOSSIBLE for it to be a real request.
        "human_keys": ["Agency Code", "User ID", "Bd Code", "BD Code", "NID Number", "NID", "Respondents", "Otherapp ID", "Otherapp Name", "Other App Name", "Acm Name (PK)", "Acm Name (IN)", "Acm", "Assigned Member"],
        "aliases": [
            "Numbering", "Submitted on Copy", "Submitted on", "Created Time", "Date",
            "Request Type", "Request type", "Type", "Category", 
            "Status", "Request Status", "Agency Status", "State",
            "Region", "Agency Region", 
            "Acm Name (PK)", "Acm Name (IN)", "Acm", "Assigned Member",
            "Agency Type", "Type of Agency", 
            "Closing Reason", "Closing Agencies Reason",
            "Otherapp Name", "Other App Name", "Other Apps", 
            "Reject Reason", "Rejection Reason",
            "Create Way", "Creation Type", "Target Type", "Agency Code", "Point Balance",
            "Latest Usage Tracker", "Agency Point Privilege", "Privilege", "Agency Privilege",
            "Counter", "Qty", "Quantities Input", "Respondents", "User ID", "Otherapp ID",
            "Bd Code", "BD Code", "NID Number", "NID", "Audition note", "Audition Note", "Duplicated Check"
        ]
    },
    {
        "id": "tbl6LYUxGi8tlkJH",
        "filename": "points.json",
        "label": "Points",
        "human_keys": ["Agency Code", "Agency Name", "Name"],
        "aliases": [
            "Agency Code", "Agency Name", "Name", "Region", "Agency Region", 
            "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member", 
            "Base Points", "base_points", "Bonus Points", "Total Points", "# Total Points", "Total",
            "Used Points", "Used", "Point Balance", "Balance"
        ]
    },
    {
        "id": "tbl3wweYCpmDmDSx",
        "filename": "access.json",
        "label": "Access",
        "human_keys": ["Email", "Person"],
        "aliases": ["Email", "Person", "Modules", "ACMs", "Regions"]
    },
    {
        "id": os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB"),
        "filename": "audit.json",
        "label": "Audit",
        "human_keys": ["Action", "Agent", "Target"],
        "aliases": ["Timestamp", "Agent", "Action", "Target", "IP Address", "Severity"]
    }
]

def main():
    log("=" * 50)
    log("🚀 Xena Portal Build Script (Infallible Strict Whitelist)")
    log(f"🕒 Time: {datetime.now(timezone.utc).isoformat()}")
    log("=" * 50)
    
    ensure_dirs()
    copy_index_html()
    
    APP_ID = os.environ.get("LARK_APP_ID", "")
    APP_SECRET = os.environ.get("LARK_APP_SECRET", "")
    BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
    
    if not APP_ID or not APP_SECRET:
        log("⚠️ LARK_APP_ID or LARK_APP_SECRET not set. Writing empty data.")
        for table in TABLES: save_json(table["filename"], [])
    else:
        for table in TABLES:
            log(f"\n⏳ Fetching {table['filename']}...")
            try:
                data = fetch_all_records(table["id"], table, APP_ID, APP_SECRET, BASE_ID)
                save_json(table["filename"], data)
            except Exception as e:
                log(f"❌ Failed {table['label']}: {e}")
                traceback.print_exc()
                save_json(table["filename"], [])
    
    log("=" * 50)
    log("🎉 Build complete!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"💥 CRITICAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(0)
