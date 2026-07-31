import os
import json
import requests
from datetime import datetime, timezone

APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"

REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx"
AUDIT_TABLE_ID = os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB")

# Sets of aliases to guarantee we catch the columns regardless of slight naming variations
REQUESTS_ALIASES = {
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
}

POINTS_ALIASES = {
    "Agency Code", "Agency Name", "Name", "Region", "Agency Region", 
    "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member", 
    "Base Points", "base_points", "Bonus Points", "Total Points", "# Total Points", "Total",
    "Used Points", "Used", "Point Balance", "Balance"
}

ACCESS_ALIASES = {"Email", "Person", "Modules", "ACMs", "Regions"}
AUDIT_ALIASES = {"Timestamp", "Agent", "Action", "Target", "IP Address", "Severity"}

def get_tenant_access_token():
    if not APP_ID or not APP_SECRET:
        print("⚠️ Missing LARK_APP_ID or LARK_APP_SECRET. Aborting fetch.", flush=True)
        return None
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        resp = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=15).json()
        return resp.get("tenant_access_token")
    except Exception as e:
        print(f"❌ Failed to get access token: {e}", flush=True)
        return None

def get_valid_field_names(table_id, tat, desired_aliases):
    """Fetches the actual table schema from Feishu and intersections it with our desired aliases."""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {tat}"}
    try:
        resp = requests.get(url, headers=headers, timeout=15).json()
        if resp.get("code") == 0:
            actual_fields = [f.get("field_name") for f in resp.get("data", {}).get("items", [])]
            valid_projection = [f for f in desired_aliases if f in actual_fields]
            return valid_projection[:100]  # Feishu limits to 100 fields max
    except Exception as e:
        print(f"⚠️ Could not fetch schema for {table_id}: {e}", flush=True)
    return None

def is_real_record(fields):
    """
    Bulletproof Ghost Row Detector.
    Table-agnostic: Ignores columns that are known to be auto-generated formulas/timestamps.
    If ANY other column has actual human text, it counts the row as real.
    """
    auto_keywords = [
        "numbering", "submitted on", "created time", "point balance", 
        "total points", "used points", "tracker"
    ]
    
    for key, val in fields.items():
        key_lower = key.lower().strip()
        
        # Ignore columns that are known to auto-populate on blank rows
        if any(kw in key_lower for kw in auto_keywords):
            continue
            
        # Safely extract text from complex Feishu JSON cell objects
        text_val = ""
        if isinstance(val, list) and len(val) > 0:
            item = val[0]
            if isinstance(item, dict):
                text_val = str(item.get('text', item.get('name', item.get('value', item.get('id', '')))))
            else:
                text_val = str(item)
        elif isinstance(val, dict):
            text_val = str(val.get('text', val.get('name', val.get('value', val.get('id', '')))))
        else:
            text_val = str(val)
            
        text_val = text_val.strip().lower()
        
        # If we find actual human text in ANY non-auto column, this row is real!
        if text_val and text_val not in ("[]", "{}", "none", "", "0", "0.0", "null", "error"):
            return True
            
    return False

def fetch_all_records(table_id, tat, desired_aliases, filename):
    valid_fields = get_valid_field_names(table_id, tat, desired_aliases)
    
    base_payload = {"page_size": 500, "automatic_fields": True}
    if valid_fields:
        base_payload["field_names"] = valid_fields
        print(f"  ... Projection active: extracting {len(valid_fields)} essential columns.", flush=True)
        
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    all_records = []
    page_token = None
    has_more = True
    page_num = 1
    
    consecutive_ghost_pages = 0
    
    while has_more:
        payload = dict(base_payload)
        if page_token:
            payload["page_token"] = page_token
            
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            data = resp.json()
                
            if data.get("code") != 0:
                print(f"❌ Error fetching {table_id}: {data.get('msg')}", flush=True)
                break
                
            block = data.get("data", {})
            items = block.get("items", [])
            
            real_records_on_page = 0
            
            for item in items:
                fields = item.get("fields", {})
                
                if is_real_record(fields):
                    real_records_on_page += 1
                    record = {"record_id": item.get("record_id"), "fields": {}}
                    
                    if desired_aliases:
                        for f in desired_aliases:
                            if f in fields and fields[f] is not None:
                                record["fields"][f] = fields[f]
                    else:
                        record["fields"] = fields
                        
                    all_records.append(record)
            
            ghosts_on_page = len(items) - real_records_on_page
            print(f"  ... Fetched page {page_num} ({real_records_on_page} real records, {ghosts_on_page} ghost rows)", flush=True)
            
            if real_records_on_page == 0 and len(items) > 0:
                consecutive_ghost_pages += 1
            else:
                consecutive_ghost_pages = 0
                
            # If we hit 2 full pages of ghost rows, stop downloading
            if consecutive_ghost_pages >= 2:
                print(f"  🛑 Detected {consecutive_ghost_pages} pages of pure ghost rows. Bottom of spreadsheet reached! Stopping early.", flush=True)
                break
                
            has_more = block.get("has_more", False)
            page_token = block.get("page_token")
            page_num += 1
            
        except Exception as e:
            print(f"❌ Exception fetching {table_id} on page {page_num}: {e}", flush=True)
            break
            
    return all_records

def main():
    print("==================================================", flush=True)
    print("🚀 Xena Portal Build Script (Universal Ghost-Buster)", flush=True)
    print(f"🕒 Time: {datetime.now(timezone.utc).isoformat()}", flush=True)
    
    tat = get_tenant_access_token()
    if not tat:
        return
        
    output_dir = os.path.join(os.getcwd(), "public", "data")
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Target Output Dir: {output_dir}", flush=True)
    
    tables = [
        ("requests.json", REQUESTS_TABLE_ID, REQUESTS_ALIASES),
        ("points.json", POINTS_TABLE_ID, POINTS_ALIASES),
        ("access.json", ACCESS_TABLE_ID, ACCESS_ALIASES),
        ("audit.json", AUDIT_TABLE_ID, AUDIT_ALIASES)
    ]
    
    for filename, table_id, aliases in tables:
        print(f"⏳ Fetching {filename}...", flush=True)
        records = fetch_all_records(table_id, tat, desired_aliases=aliases, filename=filename)
        
        if records:
            file_path = os.path.join(output_dir, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"✅ Saved {filename} ({len(records)} real records) - {size_mb:.2f} MB\n", flush=True)
        else:
            print(f"⚠️ No records found for {filename}.\n", flush=True)
            
    print("==================================================", flush=True)
    print("🎉 Build Script Complete!", flush=True)

if __name__ == "__main__":
    main()
```eof
