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
      "bytes": gz_size, "detail": ...}}} - the backend reads this first
      (60 s cache) and only downloads a .json.gz when its sha256 changes.

SAFETY RULES (unchanged from Phase 52)
--------------------------------------
- NEVER overwrite a good snapshot with an empty/failed fetch: a table that
  crashes, times out mid-fetch, or returns zero real records keeps its
  previous blob and is marked "ok": false in meta.json.
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

    return unique, complete


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


# ── Fetch phase (safety guards live here) ───────────────────────────────────

def fetch_table_snapshot(table, app_id, app_secret, base_id):
    """Fetch one table from Feishu with all Phase 52 safety guards.
    Returns (ok, detail, data_list|None). A failed fetch NEVER produces data."""
    label = table["label"]
    try:
        data, complete = fetch_all_records(table["id"], table, app_id, app_secret, base_id)
    except Exception as e:
        log(f"❌ [{label}] fetch crashed: {e}")
        traceback.print_exc()
        return False, "fetch crashed; previous snapshot kept", None

    if not complete:
        log(f"❌ [{label}] fetch INCOMPLETE ({len(data)} records before stop) - NOT overwriting; a partial snapshot is worse than a stale one.")
        return False, f"incomplete fetch ({len(data)} records before stop); previous snapshot kept", None

    if not data:
        log(f"❌ [{label}] fetch returned ZERO real records - NOT overwriting the existing snapshot.")
        return False, "empty fetch; previous snapshot kept", None

    log(f"✅ [{label}] fetched {len(data)} records")
    return True, f"{len(data)} records", data


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
        url = f"https://raw.githubusercontent.com/{self.repo}/{self.branch}/{path}"
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {self.token}"}, timeout=30)
            if r.status_code == 200:
                return r.content
        except Exception:
            pass
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


def main():
    log("=" * 50)
    log("🚀 Xena Portal Data Refresh (GitHub-repo transport, zero-deploy)")
    log(f"🕒 Time: {datetime.now(timezone.utc).isoformat()}")
    log("=" * 50)

    app_id = os.environ.get("LARK_APP_ID", "")
    app_secret = os.environ.get("LARK_APP_SECRET", "")
    repo_name = os.environ.get("DATA_REPO", "").strip()
    repo_token = os.environ.get("DATA_REPO_TOKEN", "").strip()
    branch = os.environ.get("DATA_REPO_BRANCH", "main").strip() or "main"
    base_id = "C9zFb52m4abhtHsX5LjcBywbnze"

    missing = [n for n, v in [("LARK_APP_ID", app_id), ("LARK_APP_SECRET", app_secret),
                              ("DATA_REPO", repo_name), ("DATA_REPO_TOKEN", repo_token)] if not v]
    if missing:
        log(f"💥 Missing env vars: {', '.join(missing)} - set them as GitHub repo secrets.")
        sys.exit(1)

    # 1) Fetch all tables in parallel (network-bound, up to 10 min each).
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_table_snapshot, t, app_id, app_secret, base_id): t
                   for t in TABLES}
        for fut in concurrent.futures.as_completed(futures):
            label = futures[fut]["label"]
            try:
                results[label] = fut.result()
            except Exception as e:
                traceback.print_exc()
                results[label] = (False, f"runner crashed: {e}", None)

    ok_count = sum(1 for ok, _, _ in results.values() if ok)
    if ok_count == 0:
        log("💥 ALL tables failed - leaving every snapshot untouched and failing the run so you get notified.")
        sys.exit(1)

    # 2) Load current repo state (old tree + old meta for hash-skip).
    repo = GitHubRepo(repo_name, repo_token, branch)
    tree_sha, entries, old_meta = repo.load_state()
    if tree_sha is None:
        # Zero-commit repo: seed the first commit via the Contents API (the
        # git-data endpoints 409 until it exists), then reload.
        log("ℹ️  Data repo has no commits yet - seeding README.md first commit via Contents API")
        repo.seed_repo()
        tree_sha, entries, old_meta = repo.load_state()
    old_tables = (old_meta.get("tables") or {}) if isinstance(old_meta, dict) else {}

    # 3) Build blobs only for CHANGED tables; reuse old blobs for the rest.
    now = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    updates = {}
    new_tables = {}
    statuses = {}

    for t in TABLES:
        name = t["filename"].rsplit(".", 1)[0]
        label = t["label"]
        ok, detail, data = results[label]
        old_entry = old_tables.get(name) if isinstance(old_tables.get(name), dict) else {}
        old_file = old_entry.get("file") or f"{name}.json.gz"

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
        }
        statuses[label] = {"ok": True, "detail": f"{len(data)} records"}
        log(f"✅ [{label}] pushed {len(data)} records ({len(gz)/1024:.0f} KB gz) -> {name}.json.gz")

    # 4) meta.json always gets rewritten (run heartbeat + per-table status) and
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
