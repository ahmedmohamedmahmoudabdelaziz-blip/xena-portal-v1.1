#!/usr/bin/env python3
"""Xena Portal - data snapshot refresher (Phase 53: private-GitHub-repo transport).

WHY THIS EXISTS
---------------
Phase 52 pushed the 30-min snapshot into Upstash Redis. Ahmed's free-tier
screenshot settled it: one full push is ~6.8 MB, so 48 pushes/day = ~9.6
GB/month of writes alone - essentially the whole 10 GB/month free bandwidth
budget before the portal reads a single byte. Phase 53 moves the snapshot
transport to a PRIVATE GitHub repo (`xena-data`):

  - GitHub Actions minutes are free for the PUBLIC portal repo (this
    workflow lives there; the data repo is just storage).
  - Raw file downloads are not metered - bandwidth stops being a constraint.
  - Every run force-pushes ONE root commit (parents=[]) so the repo never
    accumulates history; unchanged tables reuse their existing blob (sha256
    check), so quiet hours only rewrite the tiny meta.json heartbeat.
  - Redis stays in the portal for COMMENTS only (~7 MB, trivially free) and
    remains a read fallback during the cutover.

WHAT IT WRITES (into the data repo, branch `main`)
--------------------------------------------------
  requests.json.gz / points.json.gz / audit.json.gz / access.json.gz
      gzip of {"data": [...], "generated_at": ts, "generated_at_iso": iso,
      "records": n} - same tables, same field projection, same ghost-row
      filtering as fetch_data.py.
  meta.json
      {"generated_at": run_ts, "tables": {name: {"ok": bool, "file": ...,
      "records": n, "generated_at": ts, "sha256": <of the data json>,
      "bytes": gz_size, "detail": ..., "mode": "full"|"incremental",
      "last_full_at": ts}}} - the backend reads this first (60 s cache) and
      only downloads a .json.gz when its sha256 changes.

PHASE 55 - INCREMENTAL REFRESH (2026-11-03)
-------------------------------------------
Ahmed: "why does the refresh keep failing to download all records? why does
it take so long?" Root cause: Feishu has slow nights (45 s/page, code 1254607
"Data not ready") and Requests alone is 19k rows = 38 sequential pages, so
the 600 s per-table budget was occasionally blown -> the completeness guard
REFUSED to push a partial snapshot (by design - the 2026-11-03 Analytics
outage was caused by exactly such a partial). Fix - stop re-downloading the
whole table every 30 minutes:

  - Requests + Audit fetch INCREMENTALLY via records/search with a
    server-side `isGreater` date filter (validated live: 14-day window
    covered 1,583/1,583 snapshot rows, 0 missing). Typical run: 2-4 pages
    instead of 38 - minutes become seconds and the 600 s budget is never
    in danger. fetched rows are merged into the previous snapshot by
    record_id (new appended, changed replaced).
  - 2-day OVERLAP covers the measured filter fuzziness (~14 h early rows
    - harmless, the record_id merge dedups them).
  - Phase 55c (2026-11-03): Ahmed added the auto-updating "Last Modified
    Date" field (Feishu type 1002) to the Requests table, so its incremental
    key is now modification time - edits to ANY row, however old, are
    re-fetched within 30 min (validated live: September-submitted rows
    modified today appear; 313 modifications in one 3 h sample). The
    submitted date remains the fallback if the field is ever removed.
  - Backstop: a FULL refresh of every incremental table every 8 h
    (FULL_REFRESH_INTERVAL_S) - reconciles deletions and anything a filtered
    read could ever miss. Before Phase 55c this was also the only catch for
    old-row edits (Feishu's API has no filter/sort on the SYSTEM modified_at
    - InvalidSort/InvalidFilter, probed live; the user-added "Last Modified
    Date" FIELD is what made modified-time filtering possible).
  - Points + Access stay FULL every run: they are small (5.7k / 8 rows)
    and Points balances change via row EDITS, which a date filter can't
    see.
  - All 4 tables still fetch in PARALLEL (ThreadPoolExecutor(4)) - that
    was already true since Phase 53; the log only *looks* sequential.
  - FORCE_FULL=1 env forces every table full (manual dispatch input in
    the workflow).

SAFETY RULES (unchanged since Phase 52)
---------------------------------------
- NEVER overwrite a good snapshot with an empty/failed fetch: a table that
  crashes, times out mid-fetch, or returns zero real records keeps its
  previous blob and is marked "ok": false in meta.json.
- An incomplete INCREMENTAL fetch is equally fatal for that run's push:
  the merged result is discarded and the previous snapshot is kept.
- Oversized blobs (> MAX_BLOB_BYTES) are rejected before upload.
- Exit code 1 only if ALL tables failed (total outage fails the workflow run
  loudly so GitHub emails you; a partial outage still commits the healthy
  tables and keeps the failed ones' previous snapshots).

ENV (GitHub repo secrets on the PUBLIC portal repo):
  LARK_APP_ID, LARK_APP_SECRET     - Feishu app credentials (same as Vercel)
  DATA_REPO_TOKEN                  - fine-grained PAT, Contents: Read+Write,
                                     scoped to the PRIVATE data repo only
  DATA_REPO                        - e.g. "your-username/xena-data"
  DATA_REPO_BRANCH                 - optional, default "main"
  FORCE_FULL                       - optional "1" forces full refresh of all
"""
import base64
import gzip
import hashlib
import json, os, sys, time, traceback
from datetime import datetime, timezone
import concurrent.futures

import requests

GITHUB_API = "https://api.github.com"
# GitHub's contents/git APIs reject very large blobs; snapshots today are
# <= ~3 MB gz, so 45 MB is generous headroom while still catching runaway growth.
MAX_BLOB_BYTES = 45_000_000
PER_TABLE_FETCH_BUDGET_SECONDS = 600  # GitHub job allows it (15 min cap, 4 tables in
                                      # parallel). A budget stop or failed page marks
                                      # the fetch INCOMPLETE and the table is NOT
                                      # pushed - a partial snapshot is worse than a
                                      # stale one (live incident 2026-11-03:
                                      # 17,500/18,901 requests pushed at 360s).
INCREMENTAL_OVERLAP_MS = 2 * 86400 * 1000   # 2-day lookback past the newest row we
                                            # already have. Measured API filter fuzz
                                            # was ~14 h early - harmless, the merge
                                            # dedups by record_id.
FULL_REFRESH_INTERVAL_S = 8 * 3600         # backstop: re-read EVERY row of an
                                            # incremental table at most 8 h apart, so
                                            # edits to old rows surface within 8 h.
FILTER_FUZZ_WARN_MS = 3 * 86400 * 1000     # if filtered results contain rows older
                                            # than this below `since`, the filter is
                                            # probably not being applied -> warn (the
                                            # fetch is still complete, just slow).


def log(msg):
    print(msg, flush=True)


# ── Feishu helpers (identical logic to fetch_data.py) ──────────────────────

def get_tenant_token(app_id, app_secret):
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=20).json()
    token = resp.get("tenant_access_token")
    if not token:
        raise Exception(f"Token error: {resp}")
    return token


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


def _dedup_by_record_id(items):
    seen = set()
    unique = []
    for it in items:
        rid = it.get("record_id")
        if rid and rid not in seen:
            seen.add(rid)
            unique.append(it)
    return unique


def _project_and_filter(page_items, config, valid_fields):
    """Shared row handling for both fetch paths: ghost-row filter + projection."""
    out = []
    for it in page_items:
        fields = it.get("fields", {})
        if is_ghost_row(fields, config["human_keys"]):
            continue
        if valid_fields:
            projected = {k: v for k, v in fields.items() if k in valid_fields}
        else:
            projected = fields
        out.append({"record_id": it.get("record_id"), "fields": projected})
    return out


def fetch_all_records(table_id, config, app_id, app_secret, base_id):
    """FULL fetch via the list-records GET API (unchanged since Phase 53)."""
    label = config["label"]
    token = get_tenant_token(app_id, app_secret)

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
    complete = True

    for _ in range(500):
        if time.time() - start > PER_TABLE_FETCH_BUDGET_SECONDS:
            log(f"   ⏱️ [{label}]: total timeout reached ({PER_TABLE_FETCH_BUDGET_SECONDS}s)")
            complete = False
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
            complete = False
            break

        block = data.get("data", {})
        page_items = block.get("items", [])

        real = _project_and_filter(page_items, config, valid_fields)
        items.extend(real)
        ghosts_on_page = len(page_items) - len(real)
        log(f"   ... [{label}] Fetched page {page_num} ({len(real)} real records, {ghosts_on_page} ghost rows)")

        if len(real) == 0 and len(page_items) > 0:
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

    return _dedup_by_record_id(items), complete


def fetch_records_search_filtered(table_id, config, app_id, app_secret, base_id, since_ms):
    """INCREMENTAL fetch via records/search POST with a server-side date filter.

    Only rows whose date field > since_ms are returned, so a 30-min refresh
    reads 2-4 pages instead of 38. NOTE: page_size/page_token MUST go as QUERY
    params - the API silently drops them from the body (verified live: body
    params produced 61 identical first pages).
    Returns (unique_items, complete, pages_fetched, min_date_seen|None).
    """
    label = config["label"]
    token = get_tenant_token(app_id, app_secret)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    actual_fields = get_table_schema(table_id, token, base_id)
    date_field = next((f for f in config.get("date_fields", []) if f in actual_fields), None)
    valid_fields = set([f for f in config["aliases"] if f in actual_fields])
    if not date_field:
        # Without a confirmed date field the filter is a guess - caller
        # falls back to a full fetch instead.
        log(f"   ⚠️ [{label}] No usable date field in schema - falling back to full fetch.")
        return None, False, 0, None

    log(f"   ... [{label}] incremental fetch: rows newer than {date_field} > {datetime.fromtimestamp(since_ms/1000, tz=timezone.utc).isoformat()} (2-day overlap included); projection: {len(valid_fields)} columns")

    api_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/search"

    body = {
        "filter": {"conjunction": "and", "conditions": [
            {"field_name": date_field, "operator": "isGreater",
             "value": ["ExactDate", str(int(since_ms))]}]},
        "automatic_fields": False,
    }
    if valid_fields:
        body["field_names"] = sorted(valid_fields)

    items = []
    page_token = None
    start = time.time()
    page_num = 1
    complete = True
    min_date = None

    for _ in range(500):
        if time.time() - start > PER_TABLE_FETCH_BUDGET_SECONDS:
            log(f"   ⏱️ [{label}]: total timeout reached ({PER_TABLE_FETCH_BUDGET_SECONDS}s)")
            complete = False
            break

        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token

        success = False
        data = {}
        for attempt in range(4):
            try:
                resp = session.post(api_url, params=params, json=body, timeout=45)
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
            complete = False
            break

        block = data.get("data", {})
        page_items = block.get("items", [])

        real = _project_and_filter(page_items, config, valid_fields)
        items.extend(real)
        for it in real:
            v = it["fields"].get(date_field)
            if isinstance(v, (int, float)):
                if min_date is None or v < min_date:
                    min_date = v
        log(f"   ... [{label}] Search page {page_num} ({len(real)} real records)")

        page_token = block.get("page_token")
        if not page_token or not block.get("has_more"):
            break

        page_num += 1

    pages_fetched = page_num
    if complete and min_date is not None and min_date < since_ms - FILTER_FUZZ_WARN_MS:
        log(f"   ⚠️ [{label}] Filter fuzz larger than expected (min row {datetime.fromtimestamp(min_date/1000, tz=timezone.utc).isoformat()} vs since). Was the filter applied? Merge still correct, fetch was likely a slow full read.")

    return _dedup_by_record_id(items), complete, pages_fetched, min_date


def max_date_ms(records, date_fields):
    """Newest date value across the previous snapshot's rows (epoch ms)."""
    best = 0
    for it in records:
        f = it.get("fields", {})
        for k in date_fields:
            v = f.get(k)
            if isinstance(v, (int, float)) and v > best:
                best = v
    return best


def merge_snapshots(prev_records, fetched):
    """Merge incremental rows into the previous snapshot by record_id.
    - changed rows are replaced in place (keeps the stable ascending order)
    - new rows are appended in fetch order (search returns ascending by date)
    - rows absent from the fetch (edits/deletions of old rows) survive until
      the next full refresh.
    Returns (merged, n_new, n_updated)."""
    fetched_by_id = {}
    for it in fetched:
        rid = it.get("record_id")
        if rid:
            fetched_by_id[rid] = it
    prev_ids = set()
    updated = 0
    for it in prev_records:
        rid = it.get("record_id")
        if rid:
            prev_ids.add(rid)
            if rid in fetched_by_id and fetched_by_id[rid] != it:
                updated += 1
    merged = [fetched_by_id.get(it.get("record_id"), it) for it in prev_records]
    new_ids = [rid for rid in fetched_by_id if rid not in prev_ids]
    merged.extend(fetched_by_id[rid] for rid in new_ids)
    return merged, len(new_ids), updated


# ── Same tables / same projections as fetch_data.py (parity requirement) ────

TABLES = [
    {
        "id": "tblFMYa3dP3Ciu0V",
        "filename": "requests.json",
        "label": "Requests",
        # Phase 55: 19k rows = 38 pages is the slow pole; a server-side date
        # filter cuts the 30-min refresh to a few pages. Phase 55c made the
        # incremental key the auto-updating "Last Modified Date" field, so
        # edits to OLD rows land within 30 min; the 8-hour full-refresh
        # backstop remains for deletions and meta-clock reconciliation.
        "mode": "incremental",
        # Phase 55c (2026-11-03, field added by Ahmed): "Last Modified Date" is
        # Feishu's auto-updating last-modified-time field (type 1002) - unlike
        # the submitted date it moves on EVERY edit, so the incremental fetch
        # now catches edits to old rows within 30 min (probed live: rows
        # submitted in September modified today; 313 modifications in 3h).
        # The submitted-date fields stay as fallback if the field is removed.
        "date_fields": ["Last Modified Date", "Submitted on Copy", "Submitted on", "Created Time"],
        "human_keys": ["Agency Code", "User ID", "Bd Code", "BD Code", "NID Number", "NID", "Respondents", "Otherapp ID", "Otherapp Name", "Other App Name", "Acm Name (PK)", "Acm Name (IN)", "Acm", "Assigned Member"],
        "aliases": [
            "Numbering", "Submitted on Copy", "Submitted on", "Created Time", "Date",
            # "Last Modified Date" rides in the projection so max_date_ms can
            # compute the next run's `since` from it (prev snapshot rows must
            # carry the field, otherwise since falls back to submitted date).
            "Last Modified Date",
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
        # FULL every run: balances change via row edits, invisible to a date
        # filter; 5.7k rows = ~12 pages is cheap.
        "mode": "full",
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
        "mode": "full",
        "human_keys": ["Email", "Person"],
        # NOTE: "Active" added vs fetch_data.py - it is the Phase 51 access-toggle
        # source of truth; including it keeps access.json faithful to the sheet.
        "aliases": ["Email", "Person", "Modules", "ACMs", "Regions", "Active"]
    },
    {
        "id": os.environ.get("AUDIT_TABLE_ID", "tbldHA5AeKy55BEB"),
        "filename": "audit.json",
        "label": "Audit",
        # Phase 55: append-only log with a Timestamp column - perfect for
        # incremental (11k rows and growing).
        "mode": "incremental",
        "date_fields": ["Timestamp"],
        "human_keys": ["Action", "Agent", "Target"],
        "aliases": ["Timestamp", "Agent", "Action", "Target", "IP Address", "Severity"]
    }
]


# ── Fetch phase (safety guards live here) ───────────────────────────────────

def fetch_table_snapshot(table, app_id, app_secret, base_id,
                         prev_records=None, force_full=False, full_fallback=True):
    """Fetch one table from Feishu with all Phase 52/55 safety guards.

    Incremental tables (Requests/Audit) use a server-side date filter +
    record_id merge into prev_records. Order of attempts:
      1. incremental (unless force_full / no previous snapshot)
      2. full fetch (whenever incremental can't run or failed)
      3. incremental AGAIN as a last resort when the full fetch failed and a
         previous snapshot exists (slow Feishu nights: the 8h backstop full
         fetch may die at 5k/19k pages - the table must still refresh its
         recent rows, or the snapshot starves until one full fetch survives).
         full_fallback=False (manual FORCE_FULL) keeps the strict behavior.

    Returns (ok, detail, data_list|None, mode). A failed fetch NEVER produces
    data - the previous snapshot is kept instead."""
    label = table["label"]
    is_inc_table = table.get("mode") == "incremental"

    def incremental_attempt():
        """Returns a 4-tuple result, or None = 'cannot do incremental, try full'."""
        if not (is_inc_table and prev_records):
            return None
        since_ms = max_date_ms(prev_records, table["date_fields"]) - INCREMENTAL_OVERLAP_MS
        try:
            fetched, complete, pages, _min_date = fetch_records_search_filtered(
                table["id"], table, app_id, app_secret, base_id, since_ms)
        except Exception as e:
            log(f"⚠️ [{label}] incremental fetch crashed ({e}) - falling back to a full fetch")
            return None
        if fetched is None and pages == 0:
            # no usable date field in schema -> explicit full-fetch fallback
            log(f"ℹ️ [{label}] incremental unavailable - falling back to a full fetch")
            return None
        if not complete:
            log(f"⚠️ [{label}] incremental fetch INCOMPLETE after {pages} page(s) - falling back to a full fetch")
            return None
        if not fetched:
            # genuinely nothing new -> merged == prev, still a fresh verified pass
            log(f"✅ [{label}] incremental: 0 new rows in {pages} page(s) - snapshot unchanged")
            return True, f"incremental: 0 new ({pages}p)", list(prev_records), "incremental"
        merged, n_new, n_upd = merge_snapshots(prev_records, fetched)
        if len(merged) < len(prev_records):
            log(f"   ⚠️ [{label}] merged {len(merged)} < prev {len(prev_records)} (rows deleted in Feishu?) - keeping merged; next full refresh reconciles")
        log(f"✅ [{label}] incremental: {pages} pages, +{n_new} new, {n_upd} updated -> {len(merged)} records total")
        return True, f"incremental {pages}p: +{n_new} new, {n_upd} updated, total {len(merged)}", merged, "incremental"

    # 1) incremental first (unless forced / no previous snapshot)
    if not force_full:
        res = incremental_attempt()
        if res is not None:
            return res

    # 2) full fetch
    try:
        data, complete = fetch_all_records(table["id"], table, app_id, app_secret, base_id)
    except Exception as e:
        log(f"❌ [{label}] fetch crashed: {e}")
        traceback.print_exc()
        data, complete = None, False

    if complete and data:
        log(f"✅ [{label}] full fetch: {len(data)} records")
        return True, f"full: {len(data)} records", data, "full"

    # 3) full failed -> last-resort incremental (slow-Feishu starvation guard)
    if full_fallback and prev_records:
        log(f"⚠️ [{label}] full fetch failed - retrying incrementally so recent rows still land")
        res = incremental_attempt()
        if res is not None and res[0]:
            return res
        if res is not None and not res[0]:
            return res
        # None here = incremental impossible too; fall through to failure

    if data is not None and not complete:
        log(f"❌ [{label}] fetch INCOMPLETE ({len(data)} records before stop) - NOT overwriting; a partial snapshot is worse than a stale one.")
        return False, f"incomplete fetch ({len(data)} records before stop); previous snapshot kept", None, "full"
    if data is not None and not data:
        log(f"❌ [{label}] fetch returned ZERO real records - NOT overwriting the existing snapshot.")
        return False, "empty fetch; previous snapshot kept", None, "full"
    log(f"❌ [{label}] fetch crashed - previous snapshot kept")
    return False, "fetch crashed; previous snapshot kept", None, "full"


# ── GitHub writer (Git Data API, single root commit + force ref) ────────────

class GitHubRepo:
    """Minimal Git Data API client. Every push is ONE commit with parents=[]
    and a force ref move, so the data repo never accumulates history."""

    def __init__(self, repo, token, branch):
        self.repo, self.token, self.branch = repo, token, branch
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })

    def _req(self, method, path, **kw):
        return self.s.request(method, GITHUB_API + path, timeout=30, **kw)

    def load_state(self):
        """Return (tree_sha|None, entries {path: blob_sha}, old_meta dict).
        An empty repo returns (None, {}, {}). GitHub answers 409
        "Git Repository is empty" (not 404) for refs on a repo with no
        commits yet — both mean "start fresh"."""
        r = self._req("GET", f"/repos/{self.repo}/git/ref/heads/{self.branch}")
        if r.status_code in (404, 409):
            return None, {}, {}
        r.raise_for_status()
        commit_sha = r.json()["object"]["sha"]
        r = self._req("GET", f"/repos/{self.repo}/git/commits/{commit_sha}")
        r.raise_for_status()
        tree_sha = r.json()["tree"]["sha"]
        r = self._req("GET", f"/repos/{self.repo}/git/trees/{tree_sha}")
        r.raise_for_status()
        entries = {t["path"]: t["sha"] for t in r.json().get("tree", []) if t.get("type") == "blob"}
        old_meta = {}
        if "meta.json" in entries:
            raw = self.raw_get("meta.json")
            if raw:
                try:
                    m = json.loads(raw.decode("utf-8"))
                    if isinstance(m, dict):
                        old_meta = m
                except Exception:
                    pass
        return tree_sha, entries, old_meta

    def raw_get(self, path):
        """Download a file's bytes. Phase 54b lesson: raw.githubusercontent.com
        flakes at the wrong moment - use api.github.com/contents with retries
        instead (verified live: 200 + 1.9 MB), bail fast on auth/404."""
        url = f"{GITHUB_API}/repos/{self.repo}/contents/{path}"
        headers = {"Authorization": f"Bearer {self.token}",
                   "Accept": "application/vnd.github.raw",
                   "X-GitHub-Api-Version": "2022-11-28"}
        last_err = None
        for attempt in range(3):
            try:
                r = requests.get(url, headers=headers,
                                 params={"ref": self.branch}, timeout=30)
                if r.status_code == 200:
                    return r.content
                if 500 <= r.status_code < 600 or r.status_code == 429:
                    last_err = f"status {r.status_code}"
                else:
                    log(f"⚠️ raw_get({path}) bad status {r.status_code} - not retrying")
                    return None
            except Exception as e:
                last_err = str(e)
            if attempt < 2:
                time.sleep(1 + attempt)
        log(f"⚠️ raw_get({path}) failed after 3 attempts: {last_err}")
        return None

    def seed_repo(self):
        """Create the very first commit via the Contents API. GitHub's
        git-data endpoints (blobs/trees/commits) all answer 409
        "Git Repository is empty" on a zero-commit repo, and the Contents
        API is the only way to create that first commit."""
        seed = (b"# xena-data\n\nXena Portal 30-min data snapshots "
                b"(written by GitHub Actions). Do not edit manually.\n")
        r = self._req("PUT", f"/repos/{self.repo}/contents/README.md",
                      json={"message": "init snapshot store",
                            "content": base64.b64encode(seed).decode("ascii"),
                            "branch": self.branch})
        if r.status_code not in (200, 201):
            raise RuntimeError(f"seed failed: {r.status_code} {r.text[:200]}")

    def create_blob(self, content_bytes):
        r = self._req("POST", f"/repos/{self.repo}/git/blobs",
                      json={"content": base64.b64encode(content_bytes).decode("ascii"),
                            "encoding": "base64"})
        r.raise_for_status()
        return r.json()["sha"]

    def push(self, base_tree_sha, updates, message):
        """Create tree (on top of base_tree so untouched files carry over),
        commit with parents=[], then force-move the branch ref. Handles the
        empty-repo case by creating the ref instead of patching it."""
        tree = [{"path": p, "mode": "100644", "type": "blob", "sha": s}
                for p, s in updates.items()]
        body = {"tree": tree}
        if base_tree_sha:
            body["base_tree"] = base_tree_sha
        r = self._req("POST", f"/repos/{self.repo}/git/trees", json=body)
        r.raise_for_status()
        new_tree = r.json()["sha"]

        r = self._req("POST", f"/repos/{self.repo}/git/commits",
                      json={"message": message, "tree": new_tree, "parents": []})
        r.raise_for_status()
        commit = r.json()["sha"]

        if base_tree_sha:
            # Existing history -> move the branch ref.
            r = self._req("PATCH", f"/repos/{self.repo}/git/refs/heads/{self.branch}",
                          json={"sha": commit, "force": True})
            if r.status_code == 404:  # branch missing unexpectedly
                r = self._req("POST", f"/repos/{self.repo}/git/refs",
                              json={"ref": f"refs/heads/{self.branch}", "sha": commit})
        else:
            # Empty repo -> the branch does not exist yet; create it directly.
            r = self._req("POST", f"/repos/{self.repo}/git/refs",
                          json={"ref": f"refs/heads/{self.branch}", "sha": commit})
        r.raise_for_status()
        return commit


def load_prev_snapshot(repo, old_entry, old_file, name):
    """Download + gunzip the previous snapshot's data list (or None)."""
    if not (old_entry.get("sha256") and old_file in _ENTRIES):
        return None
    raw = repo.raw_get(old_file)
    if not raw:
        log(f"⚠️ [{name}] previous snapshot download failed - will do a full fetch")
        return None
    try:
        d = json.loads(gzip.decompress(raw).decode("utf-8"))
        data = d.get("data")
        if isinstance(data, list) and data:
            return data
    except Exception as e:
        log(f"⚠️ [{name}] previous snapshot unreadable ({e}) - will do a full fetch")
    return None


# module-level view of the repo file entries, set by main() before the
# parallel fetch; used by load_prev_snapshot for blob-existence checks.
_ENTRIES = {}


def main():
    log("=" * 50)
    log("🚀 Xena Portal Data Refresh (GitHub-repo transport, Phase 55 incremental)")
    log(f"🕒 Time: {datetime.now(timezone.utc).isoformat()}")
    log("=" * 50)

    app_id = os.environ.get("LARK_APP_ID", "")
    app_secret = os.environ.get("LARK_APP_SECRET", "")
    repo_name = os.environ.get("DATA_REPO", "").strip()
    repo_token = os.environ.get("DATA_REPO_TOKEN", "").strip()
    branch = os.environ.get("DATA_REPO_BRANCH", "main").strip() or "main"
    force_full_env = os.environ.get("FORCE_FULL", "").strip().lower() in ("1", "true", "yes")
    base_id = "C9zFb52m4abhtHsX5LjcBywbnze"

    missing = [n for n, v in [("LARK_APP_ID", app_id), ("LARK_APP_SECRET", app_secret),
                              ("DATA_REPO", repo_name), ("DATA_REPO_TOKEN", repo_token)] if not v]
    if missing:
        log(f"💥 Missing env vars: {', '.join(missing)} - set them as GitHub repo secrets.")
        sys.exit(1)

    if force_full_env:
        log("🔁 FORCE_FULL set - every table does a full fetch this run")

    # 1) Repo state FIRST: the previous snapshots + last_full_at feed the
    #    incremental decision for each table.
    repo = GitHubRepo(repo_name, repo_token, branch)
    tree_sha, entries, old_meta = repo.load_state()
    if tree_sha is None:
        # Zero-commit repo: seed the first commit via the Contents API (the
        # git-data endpoints 409 until it exists), then reload.
        log("ℹ️  Data repo has no commits yet - seeding README.md first commit via Contents API")
        repo.seed_repo()
        tree_sha, entries, old_meta = repo.load_state()
    global _ENTRIES
    _ENTRIES = entries
    old_tables = (old_meta.get("tables") or {}) if isinstance(old_meta, dict) else {}

    # 2) Per-table fetch plan: incremental needs the previous data list; the
    #    8-hour backstop (or a missing last_full_at) forces full. Even on a
    #    backstop-full run we still load the previous snapshot: if the full
    #    fetch dies mid-way on a slow Feishu night, fetch_table_snapshot can
    #    fall back to incremental so the table does not starve.
    now = time.time()
    plans = {}
    for t in TABLES:
        name = t["filename"].rsplit(".", 1)[0]
        old_entry = old_tables.get(name) if isinstance(old_tables.get(name), dict) else {}
        plan = {"prev": None, "force_full": force_full_env,
                "full_fallback": not force_full_env, "old_entry": old_entry}
        if t.get("mode") == "incremental" and not force_full_env:
            last_full_at = old_entry.get("last_full_at") or 0
            if not last_full_at:
                plan["force_full"] = True   # first run after deploy / after meta loss
                log(f"ℹ️  [{t['label']}] no last_full_at in meta - trying a full fetch this run (stamps the backstop clock; falls back to incremental if Feishu is slow)")
            elif now - last_full_at >= FULL_REFRESH_INTERVAL_S:
                plan["force_full"] = True
                log(f"ℹ️  [{t['label']}] last full refresh {int((now-last_full_at)//3600)}h ago - doing a full fetch (backstop; falls back to incremental if it fails)")
            old_file = old_entry.get("file") or f"{name}.json.gz"
            prev = load_prev_snapshot(repo, old_entry, old_file, t["label"])
            if prev is None and not plan["force_full"]:
                plan["force_full"] = True  # no usable previous snapshot
            else:
                plan["prev"] = prev
        plans[name] = plan

    # 3) Fetch all tables in parallel (network-bound, up to 10 min each).
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_table_snapshot, t, app_id, app_secret, base_id,
                                   prev_records=plans[t["filename"].rsplit(".", 1)[0]]["prev"],
                                   force_full=plans[t["filename"].rsplit(".", 1)[0]]["force_full"],
                                   full_fallback=plans[t["filename"].rsplit(".", 1)[0]]["full_fallback"]): t
                   for t in TABLES}
        for fut in concurrent.futures.as_completed(futures):
            label = futures[fut]["label"]
            try:
                results[label] = fut.result()
            except Exception as e:
                traceback.print_exc()
                results[label] = (False, f"runner crashed: {e}", None, "full")

    ok_count = sum(1 for ok, _, _, _ in results.values() if ok)
    if ok_count == 0:
        log("💥 ALL tables failed - leaving every snapshot untouched and failing the run so you get notified.")
        sys.exit(1)

    # 4) Build blobs only for CHANGED tables; reuse old blobs for the rest.
    now = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    updates = {}
    new_tables = {}
    statuses = {}

    for t in TABLES:
        name = t["filename"].rsplit(".", 1)[0]
        label = t["label"]
        ok, detail, data, mode = results[label]
        old_entry = plans[name]["old_entry"]
        old_file = old_entry.get("file") or f"{name}.json.gz"

        # last_full_at bookkeeping: stamped by a full pass, carried by an incremental one
        if ok and mode == "full":
            last_full_at = now
        else:
            last_full_at = old_entry.get("last_full_at")

        if not ok:
            # Keep the previous blob (carried over via base_tree) and say so.
            kept = dict(old_entry)
            kept.update({"ok": False, "detail": detail})
            new_tables[name] = kept
            statuses[label] = {"ok": False, "detail": detail}
            log(f"❌ [{label}] {detail}")
            continue

        data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        h = hashlib.sha256(data_json.encode("utf-8")).hexdigest()

        if old_entry.get("sha256") == h and old_file in entries:
            # Content identical - reuse the existing blob, refresh verified-at.
            new_tables[name] = {
                "ok": True, "file": old_file, "records": len(data),
                "generated_at": now, "generated_at_iso": now_iso,
                "sha256": h, "bytes": old_entry.get("bytes"),
                "mode": mode, "last_full_at": last_full_at,
                "detail": "unchanged",
            }
            statuses[label] = {"ok": True, "detail": f"{len(data)} records (unchanged)"}
            log(f"⏭️  [{label}] unchanged ({len(data)} records) - reusing existing snapshot")
            continue

        payload = json.dumps({
            "data": data,
            "generated_at": now,
            "generated_at_iso": now_iso,
            "records": len(data),
        }, ensure_ascii=False, separators=(",", ":"))
        gz = gzip.compress(payload.encode("utf-8"), compresslevel=6, mtime=0)
        if len(gz) > MAX_BLOB_BYTES:
            detail = f"gz payload {len(gz)} bytes exceeds limit {MAX_BLOB_BYTES}; previous snapshot kept"
            kept = dict(old_entry)
            kept.update({"ok": False, "detail": detail})
            new_tables[name] = kept
            statuses[label] = {"ok": False, "detail": detail}
            log(f"❌ [{label}] {detail}")
            continue

        blob_sha = repo.create_blob(gz)
        updates[f"{name}.json.gz"] = blob_sha
        new_tables[name] = {
            "ok": True, "file": f"{name}.json.gz", "records": len(data),
            "generated_at": now, "generated_at_iso": now_iso,
            "sha256": h, "bytes": len(gz),
            "mode": mode, "last_full_at": last_full_at,
        }
        statuses[label] = {"ok": True, "detail": detail}
        log(f"✅ [{label}] pushed {len(data)} records ({len(gz)/1024:.0f} KB gz, {mode}) -> {name}.json.gz")

    # 5) meta.json always gets rewritten (run heartbeat + per-table status) and
    #    everything lands in ONE atomic root commit.
    meta = {
        "generated_at": now,
        "generated_at_iso": now_iso,
        "tables": new_tables,
    }
    updates["meta.json"] = repo.create_blob(json.dumps(meta, ensure_ascii=False, indent=1).encode("utf-8"))

    commit = repo.push(tree_sha, updates,
                       f"data snapshot {now_iso} ({ok_count}/{len(TABLES)} tables ok)")

    log("=" * 50)
    log(f"🏁 Done: {ok_count}/{len(TABLES)} tables refreshed. Commit {commit[:8]} (root commit, history-free).")
    for label, s in statuses.items():
        log(f"   {'✅' if s['ok'] else '❌'} {label}: {s['detail']}")
    log("=" * 50)


if __name__ == "__main__":
    main()
