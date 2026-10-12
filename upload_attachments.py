import os
import json
import glob
import re
import time
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dotenv import load_dotenv
import sys
import jira_credentials

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
load_dotenv()

# =============================================================
# CONFIGURATION
# =============================================================
JIRA_URL       = os.getenv("JIRA_URL")        
JIRA_EMAIL     = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

# Anchor every path to THIS script's own folder, not to the process's cwd.
# Relative paths ("data/mapping", "seperate/data/raw", etc.) only resolve
# correctly if the script happens to be launched from the exact right
# working directory. bulk_load_to_jira.py launches this script via
# subprocess.run([sys.executable, "upload_attachments.py"]) with no cwd
# argument, so it inherits whatever cwd was active at that moment — if
# that ever differs (run manually from another folder, run via an IDE
# "Run" button, etc.) the old relative paths silently point at nothing.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROJECT_KEY  = os.getenv("project_key")
DATA_ROOT    = f"{PROJECT_KEY}_data" if PROJECT_KEY else "data"

# RAW_DATA_DIR must match where the extractors actually write
# (test_refactored_shared_pool/{DATA_ROOT}/raw — see flatten.py / extract_*.py)
RAW_DATA_DIR = os.path.join(BASE_DIR, "test_refactored_shared_pool", DATA_ROOT, "raw")
# MAPPING_DIR must match bulk_load_to_jira.py's MAPPING_DIR ({DATA_ROOT}/mapping)
MAPPING_DIR  = os.path.join(BASE_DIR, DATA_ROOT, "mapping")
ID_MAPPING_FILE = os.path.join(MAPPING_DIR, "id_mapping.json")

ATTACHMENT_MAPPING_FILE = os.path.join(MAPPING_DIR, "attachment_mapping.json")

RATE_LIMIT_DELAY = 0.2   # seconds between uploads (per worker, same as before)
MAX_RETRIES      = 3
SKIP_EXISTING    = True  # skip files already in attachment_mapping

# ── Parallel + checkpoint settings ──────────────────────────────────────────
# Same idea as the comments phase in bulk_load_to_jira.py: upload several
# files at once instead of one-at-a-time, but only flush attachment_mapping
# to disk every N successful uploads instead of after every single file
# (rewriting the whole mapping file per upload is the same hidden bottleneck
# the comments phase had).
ATTACHMENT_MAX_WORKERS = int(os.getenv("ATTACHMENT_MAX_WORKERS", "6"))
MAPPING_SAVE_EVERY      = int(os.getenv("ATTACHMENT_MAPPING_SAVE_EVERY", "20"))

# ── Auto-retry-with-cooldown ────────────────────────────────────────────────
# If a pass ends with files still failed (mostly persistent 429s), wait
# COOLDOWN_SECONDS and run the whole upload pass again. SKIP_EXISTING means
# a re-run only retries files that never succeeded — nothing is re-uploaded.
MAX_UPLOAD_PASSES = int(os.getenv("ATTACHMENT_MAX_PASSES", "5"))
COOLDOWN_SECONDS   = int(os.getenv("ATTACHMENT_COOLDOWN_SECONDS", "60"))

from threading import Lock
import concurrent.futures
mapping_lock = Lock()
stats_lock   = Lock()

_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".svg", ".tiff", ".tif"}


def _guess_file_type(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    return "image" if ext in _IMAGE_EXTENSIONS else "document"

os.makedirs(MAPPING_DIR, exist_ok=True)

print("  [DEBUG] BASE_DIR         :", BASE_DIR)
print("  [DEBUG] cwd at launch    :", os.getcwd())
print("  [DEBUG] project_key env  :", repr(PROJECT_KEY))
print("  [DEBUG] MAPPING_DIR      :", MAPPING_DIR)
print("  [DEBUG] RAW_DATA_DIR     :", RAW_DATA_DIR)
print("  [DEBUG] ID_MAPPING_FILE  :", ID_MAPPING_FILE, "| exists:", os.path.exists(ID_MAPPING_FILE))

# NOTE: the old fixed AUTH = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN) is
# gone. Each upload worker thread is now pinned to its own service account
# (round-robin, from the SAME pool bulk_load_to_jira.py uses) — see
# _get_auth_for_this_thread() below and jira_credentials.py.
HEADERS = {"X-Atlassian-Token": "no-check"}   # required by Jira for multipart upload


def _get_auth_for_this_thread() -> HTTPBasicAuth:
    email, token = jira_credentials.get_auth_for_this_thread()
    return HTTPBasicAuth(email, token)

# =============================================================
# STATS
# =============================================================
stats = {
    "started_at": None,
    "completed_at": None,
    "total_files_found": 0,
    "uploaded": 0,
    "skipped": 0,
    "failed": 0,
    "errors": [],
}

# =============================================================
# LOAD / SAVE MAPPINGS
# =============================================================

def load_id_mapping() -> dict:
    """Load source_issue_id  {jira_key, jira_id} mapping."""
    mapping_file = ID_MAPPING_FILE

    if not os.path.exists(mapping_file):
        print(f"  WARNING: {mapping_file} not found.")

        # Auto-discover: search the whole project tree for any
        # id_mapping.json, in case DATA_ROOT/project_key doesn't match
        # what bulk_load_to_jira.py actually used when it ran.
        candidates = glob.glob(os.path.join(BASE_DIR, "**", "id_mapping.json"), recursive=True)
        if candidates:
            print("  Found candidate id_mapping.json file(s) instead:")
            for c in candidates:
                print(f"    - {c}")
            mapping_file = candidates[0]
            print(f"  Using: {mapping_file}")
            print(f"  (Fix: set project_key in .env to match the value used when")
            print(f"   bulk_load_to_jira.py ran, so this resolves automatically.)")
        else:
            print("  No id_mapping.json found anywhere under the project folder.")
            print("  This means bulk_load_to_jira.py (Phase 5) has not successfully")
            print("  created any issues in Jira yet. Run that first.")
            return {}

    with open(mapping_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("issues", {})


def load_attachment_mapping() -> dict:
    """Load existing attachment mapping (for resume/skip logic)."""
    if os.path.exists(ATTACHMENT_MAPPING_FILE):
        with open(ATTACHMENT_MAPPING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Older mapping files predate the by_source_issue_id index
        # (used for description-level attachments) — backfill it so
        # lookups against it never KeyError.
        data.setdefault("by_source_issue_id", {})
        data.setdefault("by_source_comment_id", {})
        data.setdefault("by_local_path", {})
        return data
    return {"by_local_path": {}, "by_source_comment_id": {}, "by_source_issue_id": {}}


def save_attachment_mapping(mapping: dict):
    """Persist attachment mapping to disk."""
    with open(ATTACHMENT_MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=4, ensure_ascii=False)


# =============================================================
# UPLOAD SINGLE FILE
# =============================================================

def upload_file(file_path: str, jira_issue_key: str) -> dict | None:
    """
    Upload one local file to a Jira issue.

    Returns:
        {attachment_id, filename, content_url} or None on failure.
    """
    url  = f"{JIRA_URL}/rest/api/3/issue/{jira_issue_key}/attachments"
    auth = _get_auth_for_this_thread()   # this thread's pinned service account

    for attempt in range(MAX_RETRIES):
        try:
            with open(file_path, "rb") as f:
                filename = os.path.basename(file_path)
                response = requests.post(
                    url,
                    headers=HEADERS,
                    auth=auth,
                    files={"file": (filename, f)},
                    timeout=60,
                )

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 5))
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue

            if response.status_code in (500, 502, 503) and attempt < MAX_RETRIES - 1:
                time.sleep(3 * (attempt + 1))
                continue

            response.raise_for_status()

            data = response.json()
            # Jira returns a list
            if isinstance(data, list):
                data = data[0]

            time.sleep(RATE_LIMIT_DELAY)
            return {
                "attachment_id": str(data["id"]),
                "filename":      data["filename"],
                "content_url":   data["content"],   # direct download URL from Jira
                "mime_type":     data.get("mimeType", ""),
                "size":          data.get("size", 0),
            }

        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                print(f"    FAILED: {file_path}  {e}")
                stats["errors"].append({"file": file_path, "error": str(e)})
                return None
            time.sleep(2 * (attempt + 1))

    return None


# =============================================================
# SCAN: COMMENT ATTACHMENTS
# =============================================================

def collect_comment_files() -> list[dict]:
    """
    Scan data/raw/comments/{project_key}/attachments/comment_{id}/images/
    and                                              /comment_{id}/documents/

    Returns list of:
        {
          local_path, source_comment_id, file_type: "image"|"document",
          project_key
        }
    """
    files = []
    comments_dir = os.path.join(RAW_DATA_DIR, "comments")

    if not os.path.isdir(comments_dir):
        return files

    for project_dir in glob.glob(os.path.join(comments_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))
        attachments_dir = os.path.join(project_dir, "attachments")

        if not os.path.isdir(attachments_dir):
            continue

        for comment_dir in glob.glob(os.path.join(attachments_dir, "comment_*/")):
            comment_id = os.path.basename(os.path.normpath(comment_dir)).replace("comment_", "")

            # images sub-folder
            images_dir = os.path.join(comment_dir, "images")
            if os.path.isdir(images_dir):
                for fname in os.listdir(images_dir):
                    fpath = os.path.join(images_dir, fname)
                    if os.path.isfile(fpath):
                        files.append({
                            "local_path":        fpath,
                            "source_comment_id": comment_id,
                            "file_type":         "image",
                            "project_key":       project_key,
                        })

            # documents sub-folder
            docs_dir = os.path.join(comment_dir, "documents")
            if os.path.isdir(docs_dir):
                for fname in os.listdir(docs_dir):
                    fpath = os.path.join(docs_dir, fname)
                    if os.path.isfile(fpath):
                        files.append({
                            "local_path":        fpath,
                            "source_comment_id": comment_id,
                            "file_type":         "document",
                            "project_key":       project_key,
                        })

    return files


# =============================================================
# SCAN: ISSUE DOCUMENT ATTACHMENTS
# =============================================================

# =============================================================
# RESOLVE: INLINE DOCUMENT ID  ISSUE ID
# =============================================================
# extract_issue_details.py downloads a SECOND, different category of
# document: inline images/files referenced inside an issue's
# description/comment HTML (data-inline-document-id="...", or
# href="/documents/123"). Those are saved FLAT as
#   documents/{project_key}/{doc_id}_{original_filename}
# — NOT under documents/{project_key}/attachments/{album}/, and with
# NO docs_{issue_id}.json metadata file (that file is only written by
# extract_documents.py's *separate* album-based document extractor).
# So we rebuild the doc_id -> issue_id link ourselves by re-scanning
# each saved issue_*.json with the exact same regexes the extractor
# used to find these ids in the first place.
_INLINE_DOC_ID_PATTERNS = [
    re.compile(r'(?:src|href)\s*=\s*["\']/?documents/(\d+)["\']', re.IGNORECASE),
    re.compile(r'data-inline-document-id\s*=\s*["\'](\d+)["\']', re.IGNORECASE),
]


def _find_inline_doc_ids(obj) -> set[str]:
    """Recursively scan an issue JSON payload for inline document ids."""
    found: set[str] = set()
    if isinstance(obj, dict):
        for v in obj.values():
            found |= _find_inline_doc_ids(v)
    elif isinstance(obj, list):
        for item in obj:
            found |= _find_inline_doc_ids(item)
    elif isinstance(obj, str) and ("/documents/" in obj or "data-inline-document-id" in obj):
        for pattern in _INLINE_DOC_ID_PATTERNS:
            found.update(pattern.findall(obj))
    return found


def build_inline_doc_id_to_issue_map(project_key: str) -> dict[str, str]:
    """
    Scan RAW_DATA_DIR/issues/{project_key}/issue_*.json and return
    {doc_id: source_issue_id} for every inline document id referenced
    in any issue's description/comments.
    """
    mapping: dict[str, str] = {}
    issues_dir = os.path.join(RAW_DATA_DIR, "issues", project_key)

    if not os.path.isdir(issues_dir):
        return mapping

    for json_file in glob.glob(os.path.join(issues_dir, "issue_*.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            issue_id = str(
                data.get("issue", {}).get("id")
                or data.get("id")
                or os.path.basename(json_file).replace("issue_", "").replace(".json", "")
            )
            for doc_id in _find_inline_doc_ids(data):
                mapping.setdefault(doc_id, issue_id)
        except Exception:
            pass

    return mapping


def collect_issue_document_files() -> list[dict]:
    """
    Scan BOTH document layouts produced by the extraction phase:

    1) Album-based (extract_documents.py):
       documents/{project_key}/attachments/{album}/{filename}
       -- issue id resolved via docs_{issue_id}.json metadata.

    2) Flat inline documents (extract_issue_details.py):
       documents/{project_key}/{doc_id}_{filename}
       -- issue id resolved via build_inline_doc_id_to_issue_map().

    Returns list of:
        {local_path, source_issue_id, album_name, project_key}
    """
    files = []
    docs_base = os.path.join(RAW_DATA_DIR, "documents")

    if not os.path.isdir(docs_base):
        return files

    for project_dir in glob.glob(os.path.join(docs_base, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))
        attachments_dir = os.path.join(project_dir, "attachments")

        # ---- Layout 1: album-based ----
        if os.path.isdir(attachments_dir):
            # Build filename  source_issue_id lookup from docs_*.json metadata
            filename_to_issue: dict[str, str] = {}
            for json_file in glob.glob(os.path.join(project_dir, "docs_*.json")):
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                    issue_id  = str(meta.get("issue_id", ""))
                    documents = meta.get("documents") or []
                    for doc in documents:
                        fname = doc.get("attachment_file_name", "")
                        if fname and issue_id:
                            filename_to_issue[fname] = issue_id
                except Exception:
                    pass

            # Walk albums
            for album_dir in glob.glob(os.path.join(attachments_dir, "*/")):
                album_name = os.path.basename(os.path.normpath(album_dir))
                for fname in os.listdir(album_dir):
                    fpath = os.path.join(album_dir, fname)
                    if os.path.isfile(fpath):
                        source_issue_id = filename_to_issue.get(fname, "")
                        files.append({
                            "local_path":      fpath,
                            "source_issue_id": source_issue_id,
                            "album_name":      album_name,
                            "project_key":     project_key,
                            "file_type":       _guess_file_type(fname),
                        })

        # ---- Layout 2: flat inline documents ----
        doc_id_to_issue = build_inline_doc_id_to_issue_map(project_key)
        for fname in os.listdir(project_dir):
            fpath = os.path.join(project_dir, fname)
            if not os.path.isfile(fpath):
                continue
            if fname.startswith("docs_") and fname.endswith(".json"):
                continue  # metadata file, not an attachment

            # Filename format is "{doc_id}_{original_filename}"
            doc_id = fname.split("_", 1)[0]
            source_issue_id = doc_id_to_issue.get(doc_id, "")

            if not source_issue_id:
                print(f"    WARNING: could not resolve issue for inline document "
                      f"'{fname}' (doc_id={doc_id}) in project {project_key}")

            files.append({
                "local_path":      fpath,
                "source_issue_id": source_issue_id,
                "album_name":      "inline",
                "project_key":     project_key,
                "file_type":       _guess_file_type(fname),
            })

    return files


# =============================================================
# RESOLVE: COMMENT  ISSUE ID
# =============================================================

def build_comment_to_issue_map() -> dict[str, str]:
    """
    Read all comments_*.json files and return
    {source_comment_id: source_issue_id}
    """
    mapping: dict[str, str] = {}
    comments_dir = os.path.join(RAW_DATA_DIR, "comments")

    if not os.path.isdir(comments_dir):
        return mapping

    for json_file in glob.glob(os.path.join(comments_dir, "**", "comments_*.json"), recursive=True):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for c in data.get("comments", []):
                cid     = str(c.get("id", ""))
                iid     = str(c.get("commentable_id", ""))
                if cid and iid:
                    mapping[cid] = iid
        except Exception:
            pass

    return mapping


def reindex_attachment_mapping(attachment_mapping: dict) -> dict:
    """
    Rebuild by_source_comment_id / by_source_issue_id purely from
    by_local_path, which is the authoritative record of every file
    already uploaded.

    Why this exists: entries only get indexed into those two dicts at
    upload time (in the block below). If attachment_mapping.json was
    created by an OLDER version of this script (before the
    by_source_issue_id index existed), or a previous run's files were
    skipped as already-uploaded, they'd never get indexed. Running
    this unconditionally on every startup is cheap (no API calls, no
    re-uploading) and guarantees the indices always match reality.
    """
    by_comment: dict = {}
    by_issue:   dict = {}

    for local_path, entry in attachment_mapping.get("by_local_path", {}).items():
        # Older runs defaulted every issue-document to file_type
        # "document" (the collector didn't detect images yet). Fix
        # that up now so the ADF link renders as "[Image] ..." not
        # "[Attachment] ..." for images.
        entry["file_type"] = _guess_file_type(os.path.basename(local_path))

        comment_id = entry.get("source_comment_id", "")
        issue_id   = entry.get("source_issue_id", "")

        if comment_id:
            by_comment.setdefault(comment_id, []).append(entry)
        elif issue_id:
            by_issue.setdefault(issue_id, []).append(entry)

    attachment_mapping["by_source_comment_id"] = by_comment
    attachment_mapping["by_source_issue_id"]   = by_issue
    save_attachment_mapping(attachment_mapping)
    return attachment_mapping


# =============================================================
# MAIN UPLOAD LOOP
# =============================================================

def run_uploads():
    stats["started_at"] = datetime.now().isoformat()

    print("=" * 60)
    print("  UPLOAD ATTACHMENTS TO JIRA")
    print(f"  Target: {JIRA_URL}")
    print("=" * 60)
    jira_credentials.print_pool_summary()
    pool_n = jira_credentials.pool_size()
    if ATTACHMENT_MAX_WORKERS > pool_n:
        print(f"  ⚠ ATTACHMENT_MAX_WORKERS ({ATTACHMENT_MAX_WORKERS}) > accounts configured "
              f"({pool_n}) — some upload threads will share an account's rate limit.")

    # Load mappings
    id_mapping         = load_id_mapping()          # source_issue_id  {jira_key}
    attachment_mapping = load_attachment_mapping()  # existing uploads
    attachment_mapping = reindex_attachment_mapping(attachment_mapping)
    comment_to_issue   = build_comment_to_issue_map()  # comment_id  issue_id

    if not id_mapping:
        print("\n  No id_mapping found. Aborting.")
        sys.exit(1)  

    print(f"\n  Issues in id_mapping:    {len(id_mapping)}")
    print(f"  CommentsIssues map:     {len(comment_to_issue)}")

    # Collect all files
    print("\n  Scanning local files...")
    print(f"  Looking under: {RAW_DATA_DIR}  (exists: {os.path.isdir(RAW_DATA_DIR)})")
    if not os.path.isdir(RAW_DATA_DIR):
        print(f"  WARNING: RAW_DATA_DIR does not exist — 0 files will be found.")
        print(f"  Check that 'project_key' in .env matches the value used during")
        print(f"  extraction/flatten (Phases 1-2), so the folder name lines up.")
    comment_files = collect_comment_files()
    issue_files   = collect_issue_document_files()
    all_files     = comment_files + issue_files
    stats["total_files_found"] = len(all_files)

    print(f"  Comment attachments:     {len(comment_files)}")
    print(f"  Issue attachments:       {len(issue_files)}")
    print(f"  Total:                   {len(all_files)}")

    # Resolve issue key / skip-existing BEFORE spinning up workers, so the
    # thread pool only ever sees files that actually need uploading.
    pending = []
    for i, file_info in enumerate(all_files):
        local_path         = file_info["local_path"]
        source_issue_id    = file_info.get("source_issue_id", "")
        source_comment_id  = file_info.get("source_comment_id", "")

        if not source_issue_id and source_comment_id:
            source_issue_id = comment_to_issue.get(source_comment_id, "")

        issue_info = id_mapping.get(source_issue_id, {})
        jira_key   = issue_info.get("jira_key", "")

        if not jira_key:
            print(f"  [{i+1}/{len(all_files)}] SKIP (no Jira key for issue {source_issue_id}): {os.path.basename(local_path)}")
            stats["skipped"] += 1
            continue

        if SKIP_EXISTING and local_path in attachment_mapping["by_local_path"]:
            stats["skipped"] += 1
            continue

        pending.append({**file_info, "source_issue_id": source_issue_id,
                         "source_comment_id": source_comment_id, "jira_key": jira_key})

    if not pending:
        print("\n  Nothing left to upload (all files already uploaded or skipped).")
    else:
        workers = min(ATTACHMENT_MAX_WORKERS, len(pending))
        print(f"\n  Uploading {len(pending)} file(s)  |  Workers: {workers}\n")

        processed = 0  # successes + failures this pass, for checkpoint timing

        def upload_one(idx, file_info):
            nonlocal processed
            local_path         = file_info["local_path"]
            jira_key           = file_info["jira_key"]
            source_issue_id    = file_info["source_issue_id"]
            source_comment_id  = file_info["source_comment_id"]

            result = upload_file(local_path, jira_key)

            if result:
                entry = {
                    **result,
                    "jira_issue_key":    jira_key,
                    "source_issue_id":   source_issue_id,
                    "source_comment_id": source_comment_id,
                    "local_path":        local_path,
                    "file_type":         file_info.get("file_type", "document"),
                }
                do_checkpoint = False
                with mapping_lock:
                    attachment_mapping["by_local_path"][local_path] = entry
                    if source_comment_id:
                        attachment_mapping["by_source_comment_id"].setdefault(source_comment_id, []).append(entry)
                    elif source_issue_id:
                        attachment_mapping["by_source_issue_id"].setdefault(source_issue_id, []).append(entry)
                with stats_lock:
                    stats["uploaded"] += 1
                    processed += 1
                    if processed % MAPPING_SAVE_EVERY == 0:
                        do_checkpoint = True
                if do_checkpoint:
                    with mapping_lock:
                        save_attachment_mapping(attachment_mapping)
                print(f"  [{idx+1}/{len(pending)}] {jira_key} ← {os.path.basename(local_path)} (ID: {result['attachment_id']})")
            else:
                with stats_lock:
                    stats["failed"] += 1
                    processed += 1
                print(f"  [{idx+1}/{len(pending)}] {jira_key} ← {os.path.basename(local_path)} FAILED")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(upload_one, idx, fi) for idx, fi in enumerate(pending)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"  ✖ Upload worker exception: {e}")

        # Final flush — guarantees everything uploaded this pass is on disk
        # even if the last MAPPING_SAVE_EVERY checkpoint wasn't reached.
        with mapping_lock:
            save_attachment_mapping(attachment_mapping)

    # Save final stats
    stats["completed_at"] = datetime.now().isoformat()
    stats_path = os.path.join(MAPPING_DIR, "upload_stats.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=4)

    print(f"\n{'=' * 60}")
    print("  UPLOAD COMPLETE (this pass)")
    print(f"  Uploaded:  {stats['uploaded']}")
    print(f"  Skipped:   {stats['skipped']}")
    print(f"  Failed:    {stats['failed']}")
    print(f"  Mapping:   {ATTACHMENT_MAPPING_FILE}")
    print(f"{'=' * 60}")

    return stats["failed"]


def run_uploads_with_cooldown_retries(max_passes=MAX_UPLOAD_PASSES, cooldown_seconds=COOLDOWN_SECONDS):
    """
    Run run_uploads() up to max_passes times. If a pass ends with files
    still failed (mostly persistent 429s), wait cooldown_seconds so Jira's
    rate-limit window can reset, then run the whole thing again —
    SKIP_EXISTING means only the still-missing files get retried, never
    files that already succeeded.
    """
    for attempt in range(1, max_passes + 1):
        print(f"\n  ── Attachment upload: pass {attempt}/{max_passes} ──")
        stats["uploaded"] = 0
        stats["skipped"]  = 0
        stats["failed"]   = 0
        stats["errors"]   = []
        failed = run_uploads()

        if failed == 0:
            if attempt > 1:
                print(f"  ✔ All remaining attachments uploaded successfully on pass {attempt}.")
            return
        if attempt == max_passes:
            print(f"  ⚠ {failed} attachment(s) still failing after {max_passes} passes. "
                  f"Check upload_stats.json / errors above.")
            return
        print(f"  ⚠ {failed} attachment(s) failed this pass. "
              f"Cooling down {cooldown_seconds}s before retrying...")
        time.sleep(cooldown_seconds)


if __name__ == "__main__":
    run_uploads_with_cooldown_retries()