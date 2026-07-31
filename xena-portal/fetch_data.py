import json, os, sys, time, traceback

# ── PATH SETUP ──
# This script lives next to index.html
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
        fallback = """<!DOCTYPE html>
<html><head><title>Xena Portal</title></head>
<body><h1>Xena Portal</h1><p>Loading...</p></body></html>"""
        with open(dst, "w", encoding="utf-8") as f:
            f.write(fallback)
        log(f"⚠️ Created fallback index.html (original not found at {src})")
        return False

def save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"✅ Saved {filename} ({len(data)} records)")

def fetch_all_records(table_id, label, app_id, app_secret, base_id):
    import requests
    # Get token
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=15).json()
    token = resp.get("tenant_access_token")
    if not token:
        raise Exception(f"Token error: {resp}")
    
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    api_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
    
    items = []
    page_token = None
    start = time.time()
    
    for _ in range(200):
        if time.time() - start > 120:
            log(f"⏱️ {label}: timeout")
            break
        
        params = {"page_size": 500, "automatic_fields": "true"}
        if page_token:
            params["page_token"] = page_token
        
        resp = session.get(api_url, params=params, timeout=15)
        data = resp.json()
        
        if data.get("code") != 0:
            log(f"❌ {label}: Feishu error {data.get('code')}: {data.get('msg')}")
            break
        
        block = data.get("data", {})
        items.extend(block.get("items", []))
        
        page_token = block.get("page_token")
        if not page_token or not block.get("has_more"):
            break
    
    # Deduplicate
    seen = set()
    unique = []
    for it in items:
        rid = it.get("record_id")
        if rid and rid not in seen:
            seen.add(rid)
            unique.append(it)
    
    return unique

def main():
    log("=" * 50)
    log("🚀 Xena Portal Build Script")
    log(f"📂 Script dir: {SCRIPT_DIR}")
    log(f"📂 Public dir: {PUBLIC_DIR}")
    log("=" * 50)
    
    # Step 1: Directories
    ensure_dirs()
    
    # Step 2: Copy index.html
    copy_index_html()
    
    # Step 3: Fetch data
    APP_ID = os.environ.get("LARK_APP_ID", "")
    APP_SECRET = os.environ.get("LARK_APP_SECRET", "")
    BASE_ID = "C9zFb52m4abhtHsX5LjcBywbnze"
    
    if not APP_ID or not APP_SECRET:
        log("⚠️ LARK_APP_ID or LARK_APP_SECRET not set. Writing empty data files.")
        for name in ["requests.json", "points.json", "access.json", "audit.json"]:
            save_json(name, [])
    else:
        tables = [
            ("tblFMYa3dP3Ciu0V", "requests.json", "Requests"),
            ("tbl6LYUxGi8tlkJH", "points.json", "Points"),
            ("tbl3wweYCpmDmDSx", "access.json", "Access"),
            (os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB"), "audit.json", "Audit"),
        ]
        for table_id, filename, label in tables:
            try:
                data = fetch_all_records(table_id, label, APP_ID, APP_SECRET, BASE_ID)
                save_json(filename, data)
            except Exception as e:
                log(f"❌ Failed {label}: {e}")
                traceback.print_exc()
                save_json(filename, [])
    
    # Step 4: Verify
    log("=" * 50)
    log("📁 public/ contents:")
    for root, dirs, files in os.walk(PUBLIC_DIR):
        level = root.replace(PUBLIC_DIR, '').count(os.sep)
        indent = '  ' * level
        log(f"{indent}{os.path.basename(root)}/")
        subindent = '  ' * (level + 1)
        for file in files:
            fp = os.path.join(root, file)
            log(f"{subindent}{file} ({os.path.getsize(fp)} bytes)")
    log("=" * 50)
    log("🎉 Build complete!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"💥 CRITICAL ERROR: {e}")
        traceback.print_exc()
        # Ensure public/index.html exists even on total failure
        try:
            os.makedirs(PUBLIC_DIR, exist_ok=True)
            with open(os.path.join(PUBLIC_DIR, "index.html"), "w") as f:
                f.write("<!DOCTYPE html><html><body><h1>Build Error - Check Logs</h1></body></html>")
        except:
            pass
    sys.exit(0)
