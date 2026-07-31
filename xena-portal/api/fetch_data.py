import json, os, time, requests

# ── ENVIRONMENT ──
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")

BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx"
AUDIT_TABLE_ID = os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB")

DATA_DIR = "public/data"
os.makedirs(DATA_DIR, exist_ok=True)

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=15).json()
    token = resp.get("tenant_access_token")
    if not token:
        raise Exception(f"Failed to get token: {resp}")
    return token

def fetch_all_records(table_id, label):
    tat = get_tenant_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}"})
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{table_id}/records"

    items = []
    page_token = None
    start_time = time.time()
    max_time = 120  # 2 minutes safety cap

    # Sort by Numbering DESC for requests table
    sort_payload = '["Numbering DESC"]' if table_id == REQUESTS_TABLE_ID else None

    for _ in range(200):  # Hard cap at 100k rows
        if time.time() - start_time > max_time:
            print(f"⚠️ {label}: safety timeout reached")
            break

        params = {"page_size": 500, "automatic_fields": "true"}
        if sort_payload:
            params["sort"] = sort_payload
        if page_token:
            params["page_token"] = page_token

        try:
            resp = session.get(url, params=params, timeout=15)
            data = resp.json()

            # Self-heal sort errors
            if data.get("code") in (1254045, 1254402) and sort_payload:
                sort_payload = None
                params.pop("sort", None)
                resp = session.get(url, params=params, timeout=15)
                data = resp.json()

            if data.get("code") != 0:
                print(f"❌ {label}: Feishu Error {data.get('code')}: {data.get('msg')}")
                break

            block = data.get("data", {})
            page_items = block.get("items", [])
            items.extend(page_items)

            page_token = block.get("page_token")
            if not page_token or not block.get("has_more", False):
                break
        except Exception as e:
            print(f"❌ {label}: Exception: {e}")
            break

    # Deduplicate
    seen, unique = set(), []
    for it in items:
        rid = it.get("record_id")
        if rid not in seen:
            seen.add(rid)
            unique.append(it)

    return unique

def save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {filename} ({len(data)} records) → {path}")

if __name__ == "__main__":
    if not APP_ID or not APP_SECRET:
        print("⚠️ LARK_APP_ID or LARK_APP_SECRET not set. Writing empty files.")
        save_json("requests.json", [])
        save_json("points.json", [])
        save_json("access.json", [])
        save_json("audit.json", [])
    else:
        print("🔌 Fetching from Feishu/Lark Base...")

        requests_data = fetch_all_records(REQUESTS_TABLE_ID, "Requests")
        save_json("requests.json", requests_data)

        points_data = fetch_all_records(POINTS_TABLE_ID, "Points")
        save_json("points.json", points_data)

        access_data = fetch_all_records(ACCESS_TABLE_ID, "Access")
        save_json("access.json", access_data)

        audit_data = fetch_all_records(AUDIT_TABLE_ID, "Audit")
        save_json("audit.json", audit_data)

    print("🎉 Build-time data fetch complete!")
