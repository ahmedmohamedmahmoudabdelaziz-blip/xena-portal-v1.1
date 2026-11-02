#!/usr/bin/env python3
"""Xena Portal - Redis data refresher (replaces the 30-min Vercel rebuild cycle).

WHY THIS EXISTS
---------------
The old setup pinged a Vercel deploy hook every 30 min, forcing a FULL rebuild
(~216 MB Functions Storage each) just to refresh the data snapshot baked into
public/data/*.json. That burned 34 GB of storage in 3.5 days.

This script produces the SAME snapshot - same tables, same field projection,
same ghost-row filtering as fetch_data.py - but pushes it straight into the
Upstash Redis the portal already uses (key prefix xena:buildsnap:<name>).
index4.py's load_local_json() picks the FRESHER of disk-baked vs Redis-pushed
snapshot, so the portal keeps serving instantly with zero new deployments.

Runs from GitHub Actions (.github/workflows/refresh-data.yml) every 30 min.
Free for public repos.

SAFETY RULES
------------
- NEVER overwrite a good snapshot with an empty/failed fetch. A table that
  fails or returns zero real records is skipped; the previous snapshot stays.
- Oversized payloads (> ~8 MB) are rejected before sending to Upstash.
- Exit code 1 only if ALL tables failed (so a partial outage still shows
  green-ish but logs the failures; a total outage fails the workflow run
  loudly so you get a GitHub email).

ENV (set as GitHub repo secrets, same values as your Vercel env vars):
  LARK_APP_ID, LARK_APP_SECRET,
  UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN
"""
import base64
import gzip
import json, os, sys, time, traceback
from datetime import datetime, timezone
import concurrent.futures

import requests

REDIS_KEY_PREFIX = "xena:buildsnap:"
# Upstash REST rejects oversized commands (free tier ~10 MB/request; the
# Requests table snapshot is ~22 MB raw). Above PLAIN_JSON_LIMIT we store the
# SAME snapshot dict gzip-compressed and base64-wrapped, flagged with
# "encoding": "gzip+base64" — the backend (_redis_buildsnap) unwraps it
# transparently. MAX_VALUE_BYTES guards the FINAL encoded size.
PLAIN_JSON_LIMIT = 7_000_000
MAX_VALUE_BYTES = 9_000_000
PER_TABLE_FETCH_BUDGET_SECONDS = 360 # same as fetch_data.py


def log(msg):
    print(msg, flush=True)


# ── Feishu helpers (identical logic to fetch_data.py) ──────────────────────

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
        log(f"   ... [{label}] Projection active: limiting to {len(valid_fields)} columns.")
    else:
        log(f"   ⚠️ [{label}] Schema fetch failed or no matching columns. Downloading all columns.")

    api_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"

    items = []
    page_token = None
    start = time.time()
    page_num = 1
    consecutive_empty_pages = 0

    for _ in range(500):
        if time.time() - start > PER_TABLE_FETCH_BUDGET_SECONDS:
            log(f"   ⏱️ [{label}]: total timeout reached ({PER_TABLE_FETCH_BUDGET_SECONDS}s)")
            break

        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token

        success = False
        data = {}
        for attempt in range(4):
            try:
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
            log(f"   🛑 [{label}] Detected {consecutive_empty_pages} pages of pure ghost rows. Bottom reached! Stopping.")
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


# ── Same tables / same projections as fetch_data.py (parity requirement) ────

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
            "Agency Name",
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
            "Used Points", "Used", "Point Balance", "Balance",
            "Target whole check", "target_whole_check", "YTD Target", "Target Whole Check"
        ]
    },
    {
        "id": "tbl3wweYCpmDmDSx",
        "filename": "access.json",
        "label": "Access",
        "human_keys": ["Email", "Person"],
        # NOTE: "Active" added vs fetch_data.py - it is the Phase 51 access-toggle
        # source of truth; including it keeps access.json faithful to the sheet.
        "aliases": ["Email", "Person", "Modules", "ACMs", "Regions", "Active"]
    },
    {
        "id": os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB"),
        "filename": "audit.json",
        "label": "Audit",
        "human_keys": ["Action", "Agent", "Target"],
        "aliases": ["Timestamp", "Agent", "Action", "Target", "IP Address", "Severity"]
    }
]


# ── Upstash writer ──────────────────────────────────────────────────────────

def redis_set(key, value_str, redis_url, redis_token):
    """One Upstash REST command. Returns (ok, detail). Never raises."""
    try:
        resp = requests.post(
            redis_url,
            headers={"Authorization": f"Bearer {redis_token}"},
            json=["SET", key, value_str],
            timeout=30,
        )
        if resp.status_code != 200:
            return False, f"http {resp.status_code}: {resp.text[:200]}"
        body = resp.json()
        if "error" in body and body["error"]:
            return False, f"upstash error: {body['error']}"
        return True, str(body.get("result"))
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def refresh_table(table, app_id, app_secret, base_id, redis_url, redis_token):
    """Fetch one table from Feishu and push its snapshot to Redis.
    Returns (ok, detail). NEVER overwrites with an empty/failed fetch."""
    label = table["label"]
    name = table["filename"].rsplit(".", 1)[0]
    key = REDIS_KEY_PREFIX + name

    try:
        data = fetch_all_records(table["id"], table, app_id, app_secret, base_id)
    except Exception as e:
        log(f"❌ [{label}] fetch crashed: {e}")
        traceback.print_exc()
        return False, "fetch crashed; previous snapshot kept"

    if not data:
        log(f"❌ [{label}] fetch returned ZERO real records - NOT overwriting the existing snapshot.")
        return False, "empty fetch; previous snapshot kept"

    payload = json.dumps({
        "data": data,
        "generated_at": time.time(),
        "generated_at_iso": datetime.now(timezone.utc).isoformat(),
        "records": len(data),
    }, ensure_ascii=False, separators=(",", ":"))

    size = len(payload.encode("utf-8"))
    if size > PLAIN_JSON_LIMIT:
        raw = size
        packed = base64.b64encode(gzip.compress(payload.encode("utf-8"), compresslevel=6)).decode("ascii")
        payload = json.dumps({"encoding": "gzip+base64", "payload_b64": packed}, separators=(",", ":"))
        size = len(payload.encode("utf-8"))
        log(f"   ... [{label}] snapshot {raw/1e6:.1f} MB -> gzip+base64 {size/1e6:.1f} MB")

    if size > MAX_VALUE_BYTES:
        log(f"❌ [{label}] payload {size} bytes exceeds limit {MAX_VALUE_BYTES} even after compression - NOT writing.")
        return False, f"payload too large ({size} bytes); previous snapshot kept"

    ok, detail = redis_set(key, payload, redis_url, redis_token)
    if ok:
        log(f"✅ [{label}] pushed {len(data)} records ({size/1024:.0f} KB) -> {key}")
        return True, f"{len(data)} records"
    log(f"❌ [{label}] Redis write failed: {detail}")
    return False, f"redis write failed: {detail}"


def write_meta(statuses, redis_url, redis_token):
    meta = json.dumps({
        "generated_at": time.time(),
        "generated_at_iso": datetime.now(timezone.utc).isoformat(),
        "tables": statuses,
    }, ensure_ascii=False)
    ok, detail = redis_set(REDIS_KEY_PREFIX + "_meta", meta, redis_url, redis_token)
    log(f"🕒 meta write: {'ok' if ok else detail}")


def main():
    log("=" * 50)
    log("🚀 Xena Portal Redis Refresh (zero-deploy data update)")
    log(f"🕒 Time: {datetime.now(timezone.utc).isoformat()}")
    log("=" * 50)

    app_id = os.environ.get("LARK_APP_ID", "")
    app_secret = os.environ.get("LARK_APP_SECRET", "")
    redis_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
    redis_token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
    base_id = "C9zFb52m4abhtHsX5LjcBywbnze"

    missing = [n for n, v in [("LARK_APP_ID", app_id), ("LARK_APP_SECRET", app_secret),
                              ("UPSTASH_REDIS_REST_URL", redis_url), ("UPSTASH_REDIS_REST_TOKEN", redis_token)] if not v]
    if missing:
        log(f"💥 Missing env vars: {', '.join(missing)} - set them as GitHub repo secrets.")
        sys.exit(1)

    statuses = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(refresh_table, t, app_id, app_secret, base_id, redis_url, redis_token): t for t in TABLES}
        for fut in concurrent.futures.as_completed(futures):
            label = futures[fut]["label"]
            try:
                ok, detail = fut.result()
            except Exception as e:
                ok, detail = False, f"runner crashed: {e}"
                traceback.print_exc()
            statuses[label] = {"ok": ok, "detail": detail}

    write_meta(statuses, redis_url, redis_token)

    ok_count = sum(1 for s in statuses.values() if s["ok"])
    log("=" * 50)
    log(f"🏁 Done: {ok_count}/{len(TABLES)} tables refreshed.")
    for label, s in statuses.items():
        log(f"   {'✅' if s['ok'] else '❌'} {label}: {s['detail']}")
    log("=" * 50)

    if ok_count == 0:
        log("💥 ALL tables failed - failing the workflow run so you get notified.")
        sys.exit(1)


if __name__ == "__main__":
    main()
