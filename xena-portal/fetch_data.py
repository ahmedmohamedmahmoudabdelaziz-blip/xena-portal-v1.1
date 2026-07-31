import json, os, sys, time, traceback, requests
from datetime import datetime, timezone
import concurrent.futures

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
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=20).json()
        if resp.get("code") == 0:
            return [f.get("field_name") for f in resp.get("data", {}).get("items", [])]
    except Exception as e:
        log(f"⚠️ Failed to fetch schema for {table_id}: {e}")
    return []

def extract_field_text(field_data):
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
    human_keys_lower = set(k.lower() for k in human_keys)
    
    for f_key, f_val in fields.items():
        if f_key.lower().strip() in human_keys_lower:
            val = extract_field_text(f_val).strip()
            if val and val.lower() not in ('none', 'null', '[]', '{}', '0', '0.0', ''):
                return False 
    return True

def fetch_all_records(table_id, config, app_id, app_secret, base_id):
    label = config["label"]
    
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=20).json()
    token = resp.get("tenant_access_token")
    if not token: raise Exception(f"Token error: {resp}")
        
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    
    actual_fields = get_table_schema(table_id, token, base_id)
    valid_fields = set([f for f in config["aliases"] if f in actual_fields])
    
    if valid_fields:
        log(f"   ... [{label}] Python-side Projection active: limiting to {len(valid_fields)} columns to save RAM.")
    else:
        log(f"   ⚠️ [{label}] Schema fetch failed or no matching columns. Falling back to downloading all columns.")
    
    api_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
    
    items = []
    page_token = None
    start = time.time()
    page_num = 1
    consecutive_empty_pages = 0
    
    for _ in range(500):
        # We increase total allowed time to 360s
        if time.time() - start > 360:
            log(f"   ⏱️ [{label}]: total timeout reached (360s)")
            break
            
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
            
        # Retry mechanism for individual pages
        success = False
        data = {}
        for attempt in range(4):
            try:
                # Increased timeout to 45 seconds to let Feishu compile the large page
                resp = session.get(api_url, params=params, timeout=45)
                data = resp.json()
                
                if data.get("code") == 0:
                    success = True
                    break
                else:
                    log(f"   ⚠️ [{label}] Feishu error {data.get('code')}: {data.get('msg')} (Attempt {attempt+1})")
                    time.sleep(2)
            except requests.exceptions.RequestException as e:
                log(f"   ⚠️ [{label}] Network timeout/error: {e} (Attempt {attempt+1})")
                time.sleep(3)
                
        if not success:
            log(f"   ❌ [{label}] Failed to fetch page {page_num} after 4 attempts. Stopping here.")
            break
            
        block = data.get("data", {})
        page_items = block.get("items", [])
        
        real_records_on_page = 0
        for it in page_items:
            fields = it.get("fields", {})
            
            if not is_ghost_row(fields, config["human_keys"]):
                real_records_on_page += 1
                
                if valid_fields:
                    projected = {k: v for k, v in fields.items() if k in valid_fields}
                else:
                    projected = fields
                    
                items.append({"record_id": it.get("record_id"), "fields": projected})
                
        ghosts_on_page = len(page_items) - real_records_on_page
        log(f"   ... [{label}] Fetched page {page_num} ({real_records_on_page} real records, {ghosts_on_page} ghost rows)")
        
        if real_records_on_page == 0 and len(page_items) > 0:
            consecutive_empty_pages += 1
        else:
            consecutive_empty_pages = 0
            
        if consecutive_empty_pages >= 2:
            log(f"   🛑 [{label}] Detected {consecutive_empty_pages} pages of pure ghost rows. Spreadsheet bottom reached! Stopping.")
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

def fetch_and_save_table(table, app_id, app_secret, base_id):
    log(f"\n⏳ Starting fetch for {table['label']}...")
    try:
        data = fetch_all_records(table["id"], table, app_id, app_secret, base_id)
        save_json(table["filename"], data)
    except Exception as e:
        log(f"❌ Failed {table['label']}: {e}")
        traceback.print_exc()
        save_json(table["filename"], [])

def main():
    log("=" * 50)
    log("🚀 Xena Portal Build Script (Multi-Threaded Ghost-Buster with Robust Retry)")
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(fetch_and_save_table, table, APP_ID, APP_SECRET, BASE_ID) for table in TABLES]
            concurrent.futures.wait(futures)
    
    log("=" * 50)
    log("🎉 Build complete!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"💥 CRITICAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(0)
