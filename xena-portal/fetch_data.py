import os
import json
import requests
from datetime import datetime

APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")
BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"

REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx"
AUDIT_TABLE_ID = os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB")

REQUESTS_FIELDS = [
    "Numbering", "Submitted on Copy", "Submitted on", "Created Time", "Date",
    "Request Type", "Type", "Category", "Status", "Request Status", "Agency Status", "State",
    "Region", "Agency Region", "Acm Name (PK)", "Acm Name (IN)", "Acm", "Assigned Member",
    "Agency Type", "Type of Agency", "Closing Reason", "Closing Agencies Reason",
    "Otherapp Name", "Other App Name", "Other Apps", "Reject Reason", "Rejection Reason",
    "Create Way", "Creation Type", "Target Type", "Agency Code", "Point Balance",
    "Latest Usage Tracker", "Agency Point Privilege", "Privilege", "Agency Privilege",
    "Counter", "Qty", "Quantities Input", "Respondents", "User ID", "Otherapp ID",
    "Bd Code", "BD Code", "NID Number", "NID", "Audition note", "Audition Note", "Duplicated Check"
]

POINTS_FIELDS = [
    "Agency Code", "Agency Name", "Name", "Region", "Agency Region", 
    "Acm", "Acm Name (PK)", "Acm Name (IN)", "Assigned Member", 
    "Base Points", "base_points", "Bonus Points", "Total Points", "# Total Points", "Total",
    "Used Points", "Used", "Point Balance", "Balance"
]

ACCESS_FIELDS = ["Email", "Person", "Modules", "ACMs", "Regions"]
AUDIT_FIELDS = ["Timestamp", "Agent", "Action", "Target", "IP Address", "Severity"]

def get_tenant_access_token():
    if not APP_ID or not APP_SECRET:
        print("⚠️ Missing LARK_APP_ID or LARK_APP_SECRET. Aborting fetch.")
        return None
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        resp = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=15).json()
        return resp.get("tenant_access_token")
    except Exception as e:
        print(f"❌ Failed to get access token: {e}")
        return None

def fetch_all_records(table_id, tat, essential_fields=None):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records/search?automatic_fields=true"
    headers = {"Authorization": f"Bearer {tat}", "Content-Type": "application/json"}
    
    all_records = []
    page_token = None
    has_more = True
    
    while has_more:
        payload = {"page_size": 500}
        if essential_fields:
            payload["field_names"] = essential_fields
        if page_token:
            payload["page_token"] = page_token
            
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            data = resp.json()
            
            # If projection fails due to missing columns, retry without projection
            if data.get("code") == 1254045:
                payload.pop("field_names", None)
                resp = requests.post(url, headers=headers, json=payload, timeout=30)
                data = resp.json()
                
            if data.get("code") != 0:
                print(f"❌ Error fetching {table_id}: {data.get('msg')}")
                break
                
            block = data.get("data", {})
            items = block.get("items", [])
            
            # Minimize record footprint
            for item in items:
                record = {"record_id": item.get("record_id"), "fields": {}}
                fields = item.get("fields", {})
                if essential_fields:
                    for f in essential_fields:
                        if f in fields and fields[f] is not None:
                            record["fields"][f] = fields[f]
                else:
                    record["fields"] = fields
                all_records.append(record)
                
            has_more = block.get("has_more", False)
            page_token = block.get("page_token")
        except Exception as e:
            print(f"❌ Exception fetching {table_id}: {e}")
            break
            
    return all_records

def main():
    print("==================================================")
    print("🚀 Xena Portal Build Script")
    print(f"🕒 Time: {datetime.utcnow().isoformat()}")
    
    tat = get_tenant_access_token()
    if not tat:
        return
        
    output_dir = os.path.join(os.getcwd(), "public", "data")
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Target Output Dir: {output_dir}")
    
    tables = [
        ("requests.json", REQUESTS_TABLE_ID, REQUESTS_FIELDS),
        ("points.json", POINTS_TABLE_ID, POINTS_FIELDS),
        ("access.json", ACCESS_TABLE_ID, ACCESS_FIELDS),
        ("audit.json", AUDIT_TABLE_ID, AUDIT_FIELDS)
    ]
    
    for filename, table_id, fields in tables:
        print(f"⏳ Fetching {filename}...")
        records = fetch_all_records(table_id, tat, essential_fields=fields)
        if records:
            file_path = os.path.join(output_dir, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"✅ Saved {filename} ({len(records)} records) - {size_mb:.2f} MB")
        else:
            print(f"⚠️ No records found for {filename}.")
            
    print("==================================================")
    print("🎉 Build Script Complete!")

if __name__ == "__main__":
    main()
