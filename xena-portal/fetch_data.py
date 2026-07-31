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

def fetch_all_records(table_id, tat, desired_aliases, filename):
    valid_fields = get_valid_field_names(table_id, tat, desired_aliases)
    
    base_payload = {"page_size": 500}
    
    if valid_fields:
        base_payload["field_names"] = valid_fields
        print(f"  ... Projection active: extracting {len(valid_fields)} essential columns.", flush=True)
        
        # SMART FILTER: Tell Feishu to ignore the 100,000 completely blank rows
        filter_field = None
        if "requests" in filename.lower():
            # REMOVED "Numbering" because Feishu auto-fills it on blank rows!
            # Instead, we look for Request Type or Submitted Date.
            for f in ["Request Type", "Submitted on Copy", "Region"]:
                if f in valid_fields: 
                    filter_field = f
                    break
        elif "points" in filename.lower():
            # Use Agency Name or Region instead of Agency Code (just in case Code is also auto-generated)
            for f in ["Agency Name", "Region", "Acm"]:
                if f in valid_fields: 
                    filter_field = f
                    break
                    
        if filter_field:
            base_payload["filter"] = {
                "conjunction": "and",
                "conditions": [
                    {
                        "field_name": filter_field,
                        "operator": "isNotEmpty",
                        "value": []
                    }
                ]
            }
            print(f"  ... Anti-Blank Filter ON: Feishu will drop rows where '{filter_field}' is empty.", flush=True)
            
    else:
        print(f"  ... Schema fetch failed. Will download full fat records.", flush=True)
        
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    all_records = []
    page_token = None
    has_more = True
    page_num = 1
    
    while has_more:
        # Create a fresh copy of the payload for this specific page
        payload = dict(base_payload)
        if page_token:
            payload["page_token"] = page_token
            
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            data = resp.json()
                
            if data.get("code") != 0:
                print(f"❌ Error fetching {table_id} (Page {page_num}): {data.get('msg')}", flush=True)
                break
                
            block = data.get("data", {})
            items = block.get("items", [])
            
            # Minimize record footprint (strip out all internal Feishu metadata)
            for item in items:
                record = {"record_id": item.get("record_id"), "fields": {}}
                fields = item.get("fields", {})
                
                # Keep only fields we explicitly asked for, ignoring nulls
                if desired_aliases:
                    for f in desired_aliases:
                        if f in fields and fields[f] is not None:
                            record["fields"][f] = fields[f]
                else:
                    record["fields"] = fields
                    
                all_records.append(record)
                
            has_more = block.get("has_more", False)
            page_token = block.get("page_token")
            
            print(f"  ... Fetched page {page_num} ({len(all_records)} records total)", flush=True)
            page_num += 1
            
        except Exception as e:
            print(f"❌ Exception fetching {table_id} on page {page_num}: {e}", flush=True)
            break
            
    return all_records

def main():
    print("==================================================", flush=True)
    print("🚀 Xena Portal Build Script (Optimized)", flush=True)
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
        # Notice we are passing the filename here now so the script knows which filter to apply!
        records = fetch_all_records(table_id, tat, desired_aliases=aliases, filename=filename)
        if records:
            file_path = os.path.join(output_dir, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"✅ Saved {filename} ({len(records)} records) - {size_mb:.2f} MB\n", flush=True)
        else:
            print(f"⚠️ No records found for {filename}.\n", flush=True)
            
    print("==================================================", flush=True)
    print("🎉 Build Script Complete!", flush=True)

if __name__ == "__main__":
    main()
