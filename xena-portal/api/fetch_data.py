import json, os, time, requests, shutil

# ── ENVIRONMENT ──
APP_ID = os.environ.get("LARK_APP_ID")
APP_SECRET = os.environ.get("LARK_APP_SECRET")

BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
REQUESTS_TABLE_ID = "tblFMYa3dP3Ciu0V"
POINTS_TABLE_ID = "tbl6LYUxGi8tlkJH"
ACCESS_TABLE_ID = "tbl3wweYCpmDmDSx"
AUDIT_TABLE_ID = os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB")

# ── PATHS ──
# Get the directory where this script lives (repo root)
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
PUBLIC_DIR = os.path.join(REPO_ROOT, "public")
DATA_DIR = os.path.join(PUBLIC_DIR, "data")

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
    max_time = 120

    sort_payload = '["Numbering DESC"]' if table_id == REQUESTS_TABLE_ID else None

    for _ in range(200):
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
    print(f"✅ Saved {filename} ({len(data)} records)")

def copy_static_files():
    """Copy index.html and any assets into public/ so Vercel can serve them."""
    # Copy index.html
    src_index = os.path.join(REPO_ROOT, "index.html")
    dst_index = os.path.join(PUBLIC_DIR, "index.html")
    
    if os.path.exists(src_index):
        shutil.copy2(src_index, dst_index)
        print(f"✅ Copied index.html → public/")
    else:
        print(f"⚠️ index.html not found at {src_index}")
        # Create a basic placeholder so the site doesn't 404
        with open(dst_index, "w", encoding="utf-8") as f:
            f.write("<!DOCTYPE html><html><body><h1>Xena Portal</h1><p>Loading...</p></body></html>")
        print(f"⚠️ Created placeholder index.html")

    # Copy common asset folders if they exist
    for folder in ["assets", "css", "js", "images", "img", "fonts"]:
        src = os.path.join(REPO_ROOT, folder)
        dst = os.path.join(PUBLIC_DIR, folder)
        if os.path.exists(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            print(f"✅ Copied {folder}/ → public/{folder}/")

if __name__ == "__main__":
    # 1. Copy static files FIRST
    copy_static_files()

    # 2. Fetch Feishu data
    if not APP_ID or not APP_SECRET:
        print("⚠️ LARK_APP_ID or LARK_APP_SECRET not set. Writing empty data files.")
        for name in ["requests.json", "points.json", "access.json", "audit.json"]:
            save_json(name, [])
    else:
        print("🔌 Fetching from Feishu/Lark Base...")
        save_json("requests.json", fetch_all_records(REQUESTS_TABLE_ID, "Requests"))
        save_json("points.json",  fetch_all_records(POINTS_TABLE_ID,  "Points"))
        save_json("access.json",  fetch_all_records(ACCESS_TABLE_ID,  "Access"))
        save_json("audit.json",   fetch_all_records(AUDIT_TABLE_ID,   "Audit"))

    print("🎉 Build complete!")
    print(f"📁 public/ contents: {os.listdir(PUBLIC_DIR)}")
