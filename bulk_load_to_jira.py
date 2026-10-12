''' looger file '''
''' python bulk_load_to_jira.py --retry-failed '''
'''python bulk_load_to_jira.py --patch-comments'''
import os
import json
import time
import random
import requests
from datetime import datetime
from dotenv import load_dotenv
from collections import Counter, defaultdict, deque
import concurrent.futures
from threading import Lock, Thread, Event
import pandas as pd
import subprocess
import sys
import jira_credentials

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

load_dotenv()

JIRA_URL        = os.getenv("JIRA_URL")
JIRA_EMAIL      = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN  = os.getenv("JIRA_API_TOKEN")

# Use the SAME source-key field that prepare_payload.py actually writes onto
# every created issue (resolved from mappings.xlsx). The old hardcoded
# "customfield_10142" below was never used anywhere and did not match the
# real field (defaults to customfield_11501) — that mismatch is why a live
# Jira lookup for missing link targets never worked.
try:
    from mappings import SOURCE_KEY_CUSTOM_FIELD as _REAL_SOURCE_KEY_FIELD
except Exception:
    _REAL_SOURCE_KEY_FIELD = None

try:
    from mappings import PROJECT_MAP as _PROJECT_MAP, DEFAULT_PROJECT as _DEFAULT_PROJECT
    _ALL_JIRA_PROJECT_KEYS = sorted({
        v for v in list(_PROJECT_MAP.values()) + [_DEFAULT_PROJECT]
        if v and str(v).strip()
    })
except Exception:
    _ALL_JIRA_PROJECT_KEYS = []

PROJECT_KEY_ENV = os.getenv("project_key")
DATA_ROOT       = f"{PROJECT_KEY_ENV}_data" if PROJECT_KEY_ENV else "data"

PAYLOAD_DIR = f"{DATA_ROOT}/payload"
MAPPING_DIR = f"{DATA_ROOT}/mapping"
os.makedirs(MAPPING_DIR, exist_ok=True)

# ── Rate limiting ──────────────────────────────────────────────────────────────
RATE_LIMIT_DELAY = 0.5
RETRY_DELAY      = 5
MAX_RETRIES      = 1

# ── Bulk settings ──────────────────────────────────────────────────────────────
BULK_BATCH_SIZE = 30
BULK_TIMEOUT    = 120
SINGLE_TIMEOUT  = 60

# ── Parallel settings ──────────────────────────────────────────────────────────
# NOTE: previously hardcoded to 6. Now reads MAX_ISSUE_WORKERS from .env so
# it can be tuned without touching code — set to however many service
# accounts you have available (see jira_credentials.py / .env.sample).
MAX_ISSUE_WORKERS   = int(os.getenv("MAX_ISSUE_WORKERS", "3"))
MAX_RELATED_WORKERS = 10

# Versions and sprints were previously posted one-at-a-time in a plain for
# loop. Parallelized the same way as everything else in this script — a
# ThreadPoolExecutor with a small worker count, since these lists are
# usually short and Jira's version/sprint endpoints aren't bulk endpoints.
MAX_VERSION_WORKERS = int(os.getenv("VERSION_MAX_WORKERS", "6"))
MAX_SPRINT_WORKERS  = int(os.getenv("SPRINT_MAX_WORKERS", "6"))

# Phase 3A/3B — sprint & version assignment workers. Both jira_post/jira_put
# already resolve auth per-thread via jira_credentials.get_auth_for_this_thread(),
# so each worker here automatically gets pinned to its own service account —
# no extra plumbing needed beyond just running these in a pool.
SPRINT_ASSIGN_MAX_WORKERS  = int(os.getenv("SPRINT_ASSIGN_MAX_WORKERS", "6"))
VERSION_ASSIGN_MAX_WORKERS = int(os.getenv("VERSION_ASSIGN_MAX_WORKERS", "6"))

# Comments phase: parallelized ACROSS issues (each issue's own comments are
# still posted strictly in order on a single thread, so comment order within
# an issue is never scrambled). Override with env var if you want to tune it
# per Jira site without editing code.
MAX_COMMENT_WORKERS = int(os.getenv("COMMENT_MAX_WORKERS", "6"))

# Phase 5 (remaining links) & Phase 6 (dependencies) — these were previously
# plain sequential `for` loops with one blocking jira_post() at a time, which
# is why they crawled compared to every other phase. Each link/dependency is
# independent of the others (order doesn't matter the way it does for
# comments), so it's safe to fan them out across a worker pool the same way
# issues/comments already are.
MAX_LINK_WORKERS       = int(os.getenv("LINK_MAX_WORKERS", "20"))
MAX_DEPENDENCY_WORKERS = int(os.getenv("DEPENDENCY_MAX_WORKERS", "20"))

# Issue types that are always children of another issue, sharing its
# project. For these, the payload's "project" key must match the PARENT's
# project (taken from the parent's jira_key, e.g. "EX-30" -> "EX"), not
# whatever project the source row happened to carry — a sub-task can never
# live in a different Jira project than its parent.
SUBTASK_TYPES = {
    "Review Sub-task", "PR Intake Bot", "Deployment Ticket",
    "DS POC Sub-task", "Sub-Task", "Iteration Bug", "VQA",
    "Ticket Intake Bot","Sub-task","Copy Review","Testcase Review Sub-task"
}

# When Jira rejects a create because the assignee/reporter accountId is not
# a valid/assignable user in the target project (e.g. deactivated user,
# user without project access), swap just that field to this fallback
# account and retry once — in the SAME worker, immediately — instead of
# deferring to a separate retry pass. This matters because a later wave's
# children look this issue up by source_id in id_mapping; deferring it
# would leave those children unable to resolve their parent this run.
FALLBACK_USER_ACCOUNT_ID = "712020:3b3abe92-a909-431c-b678-76cf3a521f10"


def _is_user_field_error(error_msg):
    """
    True only for the specific 'invalid assignee/reporter user' errors, e.g.:
      {"assignee": "User '...' cannot be assigned issues."}
      {"reporter": "The reporter specified is not a user."}
    Returns (is_match, fix_assignee, fix_reporter) so the caller knows
    exactly which field(s) to patch — never touches a field Jira didn't
    actually complain about.
    """
    msg = error_msg.lower()
    fix_assignee = "assignee" in msg and (
        "cannot be assigned" in msg or "not a user" in msg or "does not exist" in msg
    )
    fix_reporter = "reporter" in msg and (
        "not a user" in msg or "does not exist" in msg or "cannot be assigned" in msg
    )
    return (fix_assignee or fix_reporter), fix_assignee, fix_reporter

# How many successfully-posted comments to batch up before flushing
# id_mapping.json to disk. Writing the whole mapping file after every single
# comment (the old behaviour) is a major hidden bottleneck once you have
# thousands of comments — this trades a slightly larger "redo on crash"
# window for a big speedup. A final flush always happens at the end of the
# phase regardless of this interval.
MAPPING_SAVE_EVERY = 25

# Same idea, applied to patching already-posted comments with attachment
# links (Phase "--patch-comments").
MAX_PATCH_WORKERS = int(os.getenv("PATCH_MAX_WORKERS", "6"))

# ── Auto-retry-with-cooldown ────────────────────────────────────────────────
# If a phase (comments, comment-patch) ends a pass with items still failed
# (mostly persistent 429s), wait COOLDOWN_SECONDS and run the SAME phase
# again. RESUME_MODE / already-patched checks inside each phase mean a
# re-run only retries what's still missing — nothing already successful
# gets redone.
MAX_PHASE_PASSES = int(os.getenv("MAX_PHASE_PASSES", "5"))
COOLDOWN_SECONDS  = int(os.getenv("PHASE_COOLDOWN_SECONDS", "60"))

# ── Sprint assignment chunk size (Jira hard limit = 50) ────────────────────────
SPRINT_CHUNK_SIZE = 50

# ── Feature flags ──────────────────────────────────────────────────────────────
LOAD_VERSIONS              = True
LOAD_SPRINTS               = True
LOAD_ISSUES                = True
LOAD_COMMENTS              = True
LOAD_LINKS                 = True
LOAD_DEPENDENCIES          = True
LOAD_HISTORY               = False
APPLY_STATUS_TRANSITIONS   = True
UPLOAD_ATTACHMENTS         = True

# Comments posted AFTER attachments — never inline during issue creation
INLINE_COMMENTS_DURING_ISSUE_CREATION = False

# ── Resume mode ────────────────────────────────────────────────────────────────
RESUME_MODE = True

JIRA_HEADERS = {
    "Accept":       "application/json",
    "Content-Type": "application/json",
}
# NOTE: the old fixed JIRA_AUTH = (JIRA_EMAIL, JIRA_API_TOKEN) tuple is gone.
# Auth is now resolved PER-THREAD inside jira_request() via
# jira_credentials.get_auth_for_this_thread() — see the multi-account pool
# in jira_credentials.py. Each worker thread gets pinned to its own service
# account the first time it makes a call, and keeps using it for the rest
# of the run.

SOURCE_KEY_CUSTOM_FIELD = "customfield_10142"
START_DATE_FIELD        = "customfield_10015"


# =============================================================================
# THREAD-SAFE STATE
# =============================================================================

mapping_lock = Lock()
report_lock  = Lock()
print_lock   = Lock()
failed_lock  = Lock()

id_mapping = {
    "issues":   {},
    "versions": {},
    "sprints":  {},
    "comments": {},
    "links":    {},
}

MAPPING_FILE        = os.path.join(MAPPING_DIR, "id_mapping.json")
FAILED_ISSUES_FILE  = os.path.join(MAPPING_DIR, "failed_issues.json")
FAILED_SINGLES_FILE = os.path.join(MAPPING_DIR, "failed_singles.json")

# ── Load existing mapping for resume ──────────────────────────────────────────
if RESUME_MODE and os.path.exists(MAPPING_FILE):
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        existing = json.load(f)
    for key in id_mapping:
        id_mapping[key].update(existing.get(key, {}))
    total_existing = sum(len(v) for v in id_mapping.values())
    if total_existing > 0:
        print(f"✔ Loaded existing mapping: {total_existing} entries")

report = {
    "started_at":   None,
    "completed_at": None,
    "versions":     {"success": 0, "failed": 0, "skipped": 0},
    "sprints":      {"success": 0, "failed": 0, "skipped": 0},
    "issues":       {"success": 0, "failed": 0, "skipped": 0, "deferred": 0},
    "comments":     {"success": 0, "failed": 0, "skipped": 0},
    "links":        {"success": 0, "failed": 0, "skipped": 0},
    "dependencies": {"success": 0, "failed": 0, "skipped": 0},
    "attachments":  {"success": 0, "failed": 0, "skipped": 0},
    "history":      {"success": 0, "failed": 0, "skipped": 0},
    "batches":      {"total": 0, "success": 0, "failed": 0, "fallback": 0},
    "waves":        {"total": 0, "completed": 0},
    "parent_mapping_missing": {"count": 0, "issue_ids": []},
    "errors":       [],
}

# ── In-memory failed lists (also persisted to disk immediately on failure) ─────
# failed_issues      : bulk-path failures (deferred/batch errors)
# failed_single_items: fallback single-creation failures
failed_issues       = []
failed_single_items = []   # FIX 1 — separate list for single-path failures
failed_comments     = []
failed_links        = []

# Global issue counter for per-issue terminal output
_issue_counter      = 0
_issue_total        = 0
_issue_counter_lock = Lock()


# =============================================================================
# NEW: API CALL / RATE TRACKING (requirement #1 and #3)
# =============================================================================
# rate_limit_hits — was referenced in jira_request() but never defined
# anywhere, so every 429 silently crashed into the bare `except Exception:
# return None, 0` at the bottom of jira_request(), which is why the
# Retry-After backoff never actually ran. Defining it here fixes that.
rate_limit_hits = {"count": 0}

# Every actual HTTP call (GET/POST/PUT, any status code) gets a timestamp
# appended here so we can report "API calls in the last 1s / last 60s" on
# demand. Bounded to the last 60s of timestamps to avoid unbounded growth
# on long runs.
_api_call_lock  = Lock()
_api_call_times = deque()
_api_call_total = {"count": 0}
_api_status_counts = Counter()   # cumulative counts by HTTP status code

RATE_MONITOR_INTERVAL_SECONDS = int(os.getenv("RATE_MONITOR_INTERVAL_SECONDS", "10"))
_rate_monitor_stop_event = Event()
_rate_monitor_thread     = None


def _record_api_call(status_code):
    """Thread-safe: call once per real HTTP request/response."""
    now = time.time()
    with _api_call_lock:
        _api_call_times.append(now)
        _api_call_total["count"] += 1
        _api_status_counts[status_code] += 1
        cutoff = now - 60
        while _api_call_times and _api_call_times[0] < cutoff:
            _api_call_times.popleft()


def _api_rate_snapshot():
    """Returns (calls_last_1s, calls_last_60s, total_calls_this_run)."""
    now = time.time()
    with _api_call_lock:
        last_1s  = sum(1 for t in _api_call_times if t >= now - 1)
        last_60s = len(_api_call_times)
        total    = _api_call_total["count"]
    return last_1s, last_60s, total


def _rate_monitor_loop():
    """
    Background daemon thread — prints a cumulative API-usage snapshot every
    RATE_MONITOR_INTERVAL_SECONDS while a run is in progress. Answers
    requirement #3: "for 1 second how many api hit, for a min how many api hit".
    """
    while not _rate_monitor_stop_event.wait(RATE_MONITOR_INTERVAL_SECONDS):
        last_1s, last_60s, total = _api_rate_snapshot()
        with _api_call_lock:
            status_summary = ", ".join(
                f"{code}:{count}" for code, count in sorted(_api_status_counts.items())
            )
        thread_safe_print(
            f"  [API RATE] last 1s: {last_1s} req/s  |  last 60s: {last_60s} req/min  |  "
            f"total this run: {total}  |  by status: {status_summary or 'n/a'}  |  "
            f"429 hits: {rate_limit_hits['count']}"
        )


def start_rate_monitor():
    global _rate_monitor_thread
    _rate_monitor_stop_event.clear()
    _rate_monitor_thread = Thread(target=_rate_monitor_loop, daemon=True)
    _rate_monitor_thread.start()


def stop_rate_monitor():
    _rate_monitor_stop_event.set()
    if _rate_monitor_thread is not None:
        _rate_monitor_thread.join(timeout=2)
    last_1s, last_60s, total = _api_rate_snapshot()
    with _api_call_lock:
        status_summary = ", ".join(
            f"{code}:{count}" for code, count in sorted(_api_status_counts.items())
        )
    print(
        f"\n  [API RATE — FINAL] total calls this run: {total}  |  "
        f"by status: {status_summary or 'n/a'}  |  429 hits: {rate_limit_hits['count']}"
    )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def load_json(filename):
    filepath = os.path.join(PAYLOAD_DIR, filename)
    if not os.path.exists(filepath):
        print(f"   ⚠ {filename} not found")
        return []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"   ✔ Loaded {filename}: {len(data)} records")
        return data
    except json.JSONDecodeError as e:
        print(f"   ✖ Invalid JSON in {filename}: {e}")
        return []


# =============================================================================
# LEAN ISSUES LOADING (avoids loading the whole multi-GB issues.json)
# =============================================================================
ISSUES_FULL_DIR = os.path.join(PAYLOAD_DIR, "issues_full")


def load_issues_meta():
    """
    Lightweight issues list — every issue, but WITHOUT the heavy "payload"
    (fields/description) content. Use this everywhere that only needs
    source_issue_id / source_parent_id / source_epic_id / jira_issue_type /
    etc. Falls back to the legacy full issues.json if issues_meta.json
    doesn't exist yet (older payload directories), with a warning since
    that path is the one that can OOM on a large migration.
    """
    meta = load_json("issues_meta.json")
    if meta:
        return meta
    print("   ⚠ issues_meta.json not found — falling back to legacy issues.json")
    print("     (this loads the FULL payload into memory; re-run prepare_payload.py")
    print("     to generate issues_meta.json + issues_full/ and avoid that.)")
    return load_json("issues.json")


def hydrate_issue(item):
    """
    Given a lightweight item from load_issues_meta(), returns the FULL
    record (including "payload") by reading its per-issue file on demand.
    If `item` already has "payload" (e.g. the legacy issues.json fallback
    path was used), it's returned unchanged — no-op.
    """
    if "payload" in item:
        return item
    source_id = str(item.get("source_issue_id", ""))
    filepath = os.path.join(ISSUES_FULL_DIR, f"{source_id}.json")
    if not os.path.exists(filepath):
        return item  # nothing to hydrate with — caller's .get("payload", {}) still works safely
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"   ⚠ Failed to hydrate issue {source_id}: {e}")
        return item


def save_mapping():
    """Write id_mapping to disk. Must be called inside mapping_lock."""
    try:
        with open(MAPPING_FILE, "w", encoding="utf-8") as f:
            json.dump(id_mapping, f, indent=2)
    except Exception as e:
        print(f"   ✖ Failed to save mapping: {e}")


def _persist_failed_single(item):
    """
    FIX 1 — Immediately persist a single-path failure to disk so it survives
    crashes and can be targeted by --retry-failed later.
    Thread-safe: acquires failed_lock internally.
    """
    with failed_lock:
        failed_single_items.append(item)
        # Merge with any existing file so partial runs accumulate correctly
        existing = []
        if os.path.exists(FAILED_SINGLES_FILE):
            try:
                with open(FAILED_SINGLES_FILE, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                existing = []
        # De-duplicate by source_issue_id
        seen_ids = {str(e.get("source_issue_id")) for e in existing}
        new_id   = str(item.get("source_issue_id", ""))
        if new_id not in seen_ids:
            existing.append(item)
        try:
            with open(FAILED_SINGLES_FILE, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
        except Exception as e:
            print(f"   ✖ Failed to persist failed single: {e}")

def _is_parent_mapping_error(error_msg) -> bool:
    msg = str(error_msg).lower()
    return "parent" in msg and (
        "not specified" in msg or "issuetype" in msg or "sub-task" in msg
    )


def _record_parent_mapping_missing(source_id):
    with report_lock:
        report["parent_mapping_missing"]["count"] += 1
        report["parent_mapping_missing"]["issue_ids"].append(str(source_id))

def log_error(entity_type, source_id, error_msg):
    with report_lock:
        report["errors"].append({
            "entity_type": entity_type,
            "source_id":   str(source_id),
            "error":       error_msg[:500],
            "timestamp":   datetime.now().isoformat(),
        })


def thread_safe_print(message):
    with print_lock:
        print(message)


def save_failed(entity_type, items):
    if not items:
        return
    filepath = os.path.join(MAPPING_DIR, f"failed_{entity_type}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)
    print(f"   ✔ {len(items)} failed items saved → failed_{entity_type}.json")


PENDING_DEPS_FILE = os.path.join(MAPPING_DIR, "pending_dependencies.json")
_pending_deps_lock = Lock()


def save_pending_dependency(dep):
    """
    Append one unresolved dependency (cross-project or otherwise not yet
    resolvable) to data/mapping/pending_dependencies.json, so it can be
    retried later by retry_dependencies.py once more projects are migrated,
    without needing to re-run this project's full load.
    """
    with _pending_deps_lock:
        pending = []
        if os.path.exists(PENDING_DEPS_FILE):
            with open(PENDING_DEPS_FILE, "r", encoding="utf-8") as f:
                try:
                    pending = json.load(f)
                except json.JSONDecodeError:
                    pending = []

        dep_record = dict(dep)
        dep_record["first_seen"] = datetime.now().isoformat()
        pending.append(dep_record)

        with open(PENDING_DEPS_FILE, "w", encoding="utf-8") as f:
            json.dump(pending, f, indent=2)


def jira_request(method, endpoint, payload=None, retries=MAX_RETRIES, is_bulk=False):
    url     = f"{JIRA_URL}/rest/{endpoint.lstrip('/')}"
    timeout = BULK_TIMEOUT if is_bulk else SINGLE_TIMEOUT

    # Per-thread account: this worker thread is pinned to one service
    # account (round-robin assigned on its first call) for its whole life.
    thread_auth = jira_credentials.get_auth_for_this_thread()

    for attempt in range(retries):
        try:
            if method == "GET":
                response = requests.get(
                    url, headers=JIRA_HEADERS, auth=thread_auth, timeout=timeout
                )
            elif method == "POST":
                response = requests.post(
                    url, json=payload, headers=JIRA_HEADERS, auth=thread_auth, timeout=timeout
                )
            elif method == "PUT":
                response = requests.put(
                    url, json=payload, headers=JIRA_HEADERS, auth=thread_auth, timeout=timeout
                )
            else:
                raise ValueError(f"Unsupported method: {method}")

            # Every real HTTP response (any status) counts toward the
            # cumulative API-hit tracker (requirement #3).
            _record_api_call(response.status_code)

            # if response.status_code == 429:
            #     retry_after = int(response.headers.get("Retry-After", RETRY_DELAY))
            #     time.sleep(retry_after)
            #     continue
            if response.status_code == 429:

                retry_after = int(response.headers.get("Retry-After", RETRY_DELAY))

                # requirement #1 — full response headers (not just Retry-After),
                # since the user can't see the raw HTTP headers otherwise.
                headers_dump = dict(response.headers)

                # Also capture the JSON response body, if any, since Jira
                # sometimes puts extra rate-limit context there too.
                try:
                    body_dump = response.json() if response.content else {}
                except Exception:
                    body_dump = {"_raw_text": response.text[:500]}

                print(f"  [RATE LIMIT] 429 on {method} {endpoint} — "
                      f"attempt {attempt+1}/{retries}, sleeping {retry_after}s "
                      f"(Retry-After header)")
                print(f"  [RATE LIMIT] Response headers: {json.dumps(headers_dump, indent=2)}")
                print(f"  [RATE LIMIT] Response body: {json.dumps(body_dump, indent=2)[:1000]}")
                rate_limit_hits["count"] += 1
                time.sleep(retry_after)
                continue
 

            if response.status_code in (503, 502, 500) and attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            time.sleep(RATE_LIMIT_DELAY)
            return response.json() if response.content else {}, response.status_code

        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
            continue
        except requests.exceptions.ConnectionError:
            time.sleep(RETRY_DELAY)
        except Exception:
            return None, 0

    return None, 0


def jira_get(endpoint):
    return jira_request("GET", endpoint)


def jira_post(endpoint, payload, is_bulk=False):
    return jira_request("POST", endpoint, payload, is_bulk=is_bulk)


def jira_put(endpoint, payload, is_bulk=False):
    return jira_request("PUT", endpoint, payload, is_bulk=is_bulk)


# =============================================================================
# LIVE JIRA FALLBACK LOOKUP FOR LINK TARGETS
# =============================================================================
# A link fails when its target isn't in the LOCAL id_mapping.json — but the
# target issue may already exist in Jira (created in an earlier run, or as
# part of a different project's migration that this run didn't extract).
# Before giving up on a link, search Jira directly for an issue carrying the
# same source ID in SOURCE_KEY_CUSTOM_FIELD. If found, cache it into
# id_mapping so we never need to search for it again.

_source_lookup_cache = {}   # source_id -> jira_key or None (already tried, not found)
_source_lookup_lock  = Lock()


def resolve_missing_target(source_id: str):
    """
    Look up a source issue ID directly in Jira via JQL, using the source-key
    custom field written onto every migrated issue. Returns the jira_key if
    found, else None. Caches both hits and misses so we never repeat a
    lookup for the same ID within one run.
    """
    if not _REAL_SOURCE_KEY_FIELD:
        return None

    with _source_lookup_lock:
        if source_id in _source_lookup_cache:
            return _source_lookup_cache[source_id]

    field_num = _REAL_SOURCE_KEY_FIELD.replace("customfield_", "")

    if _ALL_JIRA_PROJECT_KEYS:
        project_clause = "project in (" + ",".join(f'"{p}"' for p in _ALL_JIRA_PROJECT_KEYS) + ")"
        jql = f'{project_clause} AND cf[{field_num}] = "{source_id}"'
    else:
        # No project list available — fall back to bounding by the field itself
        # being non-empty plus equality, which some instances still accept.
        jql = f'cf[{field_num}] = "{source_id}"'

    endpoint = f"api/3/search/jql?jql={requests.utils.quote(jql)}&fields=key&maxResults=1"

    data, status = jira_get(endpoint)
    jira_key = None
    if status == 200 and data:
        issues = data.get("issues", [])
        if issues:
            jira_key = issues[0].get("key")

    with _source_lookup_lock:
        _source_lookup_cache[source_id] = jira_key

    if jira_key:
        with mapping_lock:
            id_mapping["issues"][str(source_id)] = {
                "jira_key":    jira_key,
                "jira_id":     None,
                "source_id":   str(source_id),
                "issue_type":  None,
                "title":       None,
                "created_at":  None,
                "resolved_via": "live_jira_lookup",
            }
            save_mapping()
        print(f"   ↳ [LINK RESOLVE] Found existing Jira issue for source {source_id} → {jira_key} (via live lookup)")

    return jira_key


def apply_status_transition(jira_key, target_status):
    if not target_status or target_status in ("", "nan", "None", "NaT"):
        return True

    data, status = jira_get(f"api/3/issue/{jira_key}/transitions")
    if status != 200 or not data:
        return False

    transitions   = data.get("transitions", [])
    transition_id = None
    available     = []

    for trans in transitions:
        to_status_name = trans.get("to", {}).get("name", "")
        available.append(to_status_name)
        if to_status_name.lower() == target_status.lower():
            transition_id = trans.get("id")
            break

    if not transition_id:
        thread_safe_print(
            f"     ⚠ No transition to '{target_status}' for {jira_key}. "
            f"Available: {available}"
        )
        return False

    payload = {"transition": {"id": transition_id}}
    _, t_status = jira_post(f"api/3/issue/{jira_key}/transitions", payload)
    return t_status == 204


# =============================================================================
# PHASE 1A: LOAD VERSIONS
# =============================================================================

def load_versions():
    """
    Parallelized version creation (requirement #4 — was a plain for-loop
    posting one version at a time). All original behavior is preserved:
    RESUME_MODE skip, "already exists" lookup + mapping, error logging.
    Only the "one HTTP call at a time" part changed — everything that
    touches shared state (id_mapping, report, the project-versions cache)
    is now guarded by a lock since multiple worker threads touch it.
    """
    if not LOAD_VERSIONS:
        print("\n⏭ Versions: SKIPPED")
        return
 
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 1A: Loading Versions (parallel)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
 
    versions = load_json("versions.json")
    if not versions:
        return
 
    # Cache of project_key -> {version_name_lower: version_data}
    # so we don't re-fetch the same project's versions repeatedly.
    # Now shared across worker threads, so it's guarded by a lock.
    _project_versions_cache = {}
    _versions_cache_lock    = Lock()
 
    def _find_existing_version(project_key, version_name):
        with _versions_cache_lock:
            cached = _project_versions_cache.get(project_key)
        if cached is None:
            data, status = jira_get(f"api/3/project/{project_key}/versions")
            lookup = {}
            if status == 200 and isinstance(data, list):
                for v in data:
                    lookup[v.get("name", "").strip().lower()] = v
            with _versions_cache_lock:
                _project_versions_cache[project_key] = lookup
            thread_safe_print(f"     ↳ Fetched {len(lookup)} existing versions from project '{project_key}'")
            cached = lookup
        return cached.get(version_name.strip().lower())

    def _process_version(indexed_item):
        i, item = indexed_item
        _item_start = time.time()   # requirement #2 — per-item timing
        source_id = str(item.get("source_release_id", ""))

        if RESUME_MODE and source_id in id_mapping["versions"]:
            with report_lock:
                report["versions"]["skipped"] += 1
            return

        payload = item.get("payload", {})
        data, status = jira_post("api/3/version", payload)
        _elapsed = time.time() - _item_start

        if status == 201:
            with mapping_lock:
                id_mapping["versions"][source_id] = {
                    "jira_version_id":   data.get("id"),
                    "jira_version_name": data.get("name"),
                    "source_release_id": source_id,
                }
                save_mapping()
            with report_lock:
                report["versions"]["success"] += 1
            thread_safe_print(f"  [{i}/{len(versions)}] ✔ {data.get('name')}  (took {_elapsed:.2f}s)")
        else:
            error_msg = str(data.get("errors", data.get("errorMessages", data)))

            # ── Version already exists — look it up and map it anyway ──
            if "already exists" in error_msg.lower():
                project_key  = payload.get("project", "")
                version_name = payload.get("name", "")
                existing = _find_existing_version(project_key, version_name)

                if existing:
                    with mapping_lock:
                        id_mapping["versions"][source_id] = {
                            "jira_version_id":   existing.get("id"),
                            "jira_version_name": existing.get("name"),
                            "source_release_id": source_id,
                            "resolved_via":      "already_exists_lookup",
                        }
                        save_mapping()
                    with report_lock:
                        report["versions"]["success"] += 1
                    thread_safe_print(
                        f"  [{i}/{len(versions)}] ✔ {version_name} (already existed — mapped)  "
                        f"(took {_elapsed:.2f}s)"
                    )
                    return
                else:
                    thread_safe_print(
                        f"  [{i}/{len(versions)}] ⚠ '{version_name}' reported as existing "
                        f"but no exact name match found in project '{project_key}' — check formatting"
                    )

            log_error("version", source_id, error_msg)
            with report_lock:
                report["versions"]["failed"] += 1
            thread_safe_print(f"  [{i}/{len(versions)}] ✖ {source_id}: {error_msg[:80]}  (took {_elapsed:.2f}s)")

    _phase_start = time.time()
    workers = min(MAX_VERSION_WORKERS, max(len(versions), 1))
    print(f"  {len(versions)} version(s)  |  Workers: {workers}\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_process_version, (i, item)) for i, item in enumerate(versions, 1)]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                thread_safe_print(f"   ✖ Version worker exception: {e}")
    _phase_elapsed = time.time() - _phase_start
 
    print(
        f"  Summary: {report['versions']['success']} ✔ | "
        f"{report['versions']['failed']} ✖ | "
        f"{report['versions']['skipped']} skipped  |  phase took {_phase_elapsed:.2f}s"
    )
def export_versions_to_excel():
    rows = []
    for source_id, data in id_mapping["versions"].items():
        rows.append({
            "Version Name":      data.get("jira_version_name"),
            "Source Release ID": source_id,
            "Jira Version ID":   data.get("jira_version_id"),
        })
    if not rows:
        print("  No version data to export")
        return
    df = pd.DataFrame(rows)
    fp = os.path.join(MAPPING_DIR, "versions_mapping.xlsx")
    df.to_excel(fp, index=False)
    print(f"   ✔ Versions Excel exported → {fp}")
 
 
# =============================================================================
# PHASE 1B: LOAD SPRINTS
# =============================================================================

def load_sprints():
    """
    Parallelized sprint creation (requirement #4 — was a plain for-loop
    posting one sprint at a time). All original behavior preserved
    (RESUME_MODE skip, missing-originBoardId failure, board-name lookup,
    error logging); shared state (id_mapping, report) is now guarded by
    locks since multiple worker threads touch it concurrently.
    """
    if not LOAD_SPRINTS:
        print("\n⏭ Sprints: SKIPPED")
        return

    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 1B: Loading Sprints (parallel)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    sprints = load_json("sprints.json")
    if not sprints:
        return

    def _process_sprint(indexed_item):
        i, item = indexed_item
        _item_start = time.time()   # requirement #2 — per-item timing
        source_id = str(item.get("source_sprint_id", ""))

        if RESUME_MODE and source_id in id_mapping["sprints"]:
            with report_lock:
                report["sprints"]["skipped"] += 1
            return

        payload = item.get("payload", {})
        if not payload.get("originBoardId"):
            log_error("sprint", source_id, "Missing originBoardId")
            with report_lock:
                report["sprints"]["failed"] += 1
            return

        data, status = jira_post("agile/1.0/sprint", payload)

        if status == 201:
            board_id = payload.get("originBoardId")
            board_data, board_status = jira_get(f"agile/1.0/board/{board_id}")
            _elapsed = time.time() - _item_start

            with mapping_lock:
                id_mapping["sprints"][source_id] = {
                    "jira_sprint_id":   data.get("id"),
                    "jira_sprint_name": data.get("name"),
                    "jira_board_id":    board_id,
                    "jira_board_name":  board_data.get("name", "") if board_status == 200 else "",
                }
                save_mapping()
            with report_lock:
                report["sprints"]["success"] += 1
            thread_safe_print(f"  [{i}/{len(sprints)}] ✔ {data.get('name')}  (took {_elapsed:.2f}s)")
        else:
            _elapsed = time.time() - _item_start
            error_msg = str(data.get("errors", data.get("errorMessages", data)))
            log_error("sprint", source_id, error_msg)
            with report_lock:
                report["sprints"]["failed"] += 1
            thread_safe_print(f"  [{i}/{len(sprints)}] ✖ {source_id}: {error_msg[:80]}  (took {_elapsed:.2f}s)")

    _phase_start = time.time()
    workers = min(MAX_SPRINT_WORKERS, max(len(sprints), 1))
    print(f"  {len(sprints)} sprint(s)  |  Workers: {workers}\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_process_sprint, (i, item)) for i, item in enumerate(sprints, 1)]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                thread_safe_print(f"   ✖ Sprint worker exception: {e}")
    _phase_elapsed = time.time() - _phase_start

    print(
        f"  Summary: {report['sprints']['success']} ✔ | "
        f"{report['sprints']['failed']} ✖ | "
        f"{report['sprints']['skipped']} skipped  |  phase took {_phase_elapsed:.2f}s"
    )


def export_sprints_to_excel():
    rows = []
    for source_id, data in id_mapping["sprints"].items():
        rows.append({
            "Sprint Name":      data.get("jira_sprint_name"),
            "Source Sprint ID": source_id,
            "Jira Sprint ID":   data.get("jira_sprint_id"),
            "Jira Board ID":    data.get("jira_board_id"),
            "Jira Board Name":  data.get("jira_board_name"),
        })
    if not rows:
        print("  No sprint data to export")
        return
    df = pd.DataFrame(rows)
    fp = os.path.join(MAPPING_DIR, "sprints_mapping.xlsx")
    df.to_excel(fp, index=False)
    print(f"   ✔ Sprints Excel exported → {fp}")


# =============================================================================
# PHASE 2: WAVE-BASED ISSUE GROUPING
# =============================================================================

def group_issues_by_wave(issues):
    waves = defaultdict(list)
    for item in issues:
        issue_type       = item.get("jira_issue_type", "Task")
        source_parent_id = str(item.get("source_parent_id", "")).split(".")[0]
        source_epic_id   = str(item.get("source_epic_id",   "")).split(".")[0]

        if issue_type == "Epic":
            waves[1].append(item)
        elif issue_type == "Sub-task":
            waves[4].append(item)
        elif source_epic_id and source_epic_id not in ("", "None", "nan"):
            waves[2].append(item)
        elif source_parent_id and source_parent_id not in ("", "None", "nan"):
            waves[3].append(item)
        else:
            waves[2].append(item)
    return waves


# =============================================================================
# GROUPED PAYLOAD LOADER
# =============================================================================

class LazyGroupedData:
    """
    Drop-in replacement for the old grouped_data dict, backed by per-issue
    files on disk instead of one giant in-memory dict. Supports the same
    .get(key) / .get(key, {}) / `if grouped_data:` usage every call site
    already relies on. Memory stays bounded by whatever's currently being
    processed, since each .get() reads exactly one small JSON file.
    """

    def __init__(self, related_dir, issue_ids):
        self._related_dir = related_dir
        self._issue_ids = set(str(i) for i in issue_ids)

    def __bool__(self):
        return len(self._issue_ids) > 0

    def __len__(self):
        return len(self._issue_ids)

    def __contains__(self, key):
        return str(key) in self._issue_ids

    def get(self, key, default=None):
        key = str(key)
        if key not in self._issue_ids:
            return default
        filepath = os.path.join(self._related_dir, f"{key}.json")
        if not os.path.exists(filepath):
            return default
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"   ⚠ Failed to load related data for issue {key}: {e}")
            return default


def load_grouped_related_data():
    """
    Returns a LazyGroupedData that reads per-issue files on demand, instead
    of loading the entire issues_with_related payload into memory at once
    (which is what caused the OOM kill during Phase 2 on a 4GB payload file).

    Falls back to the legacy single-file format if the new index isn't found.
    """
    index_file = os.path.join(PAYLOAD_DIR, "issues_with_related_index.json")
    related_dir = os.path.join(PAYLOAD_DIR, "issues_with_related")

    if os.path.exists(index_file):
        try:
            with open(index_file, "r", encoding="utf-8") as f:
                index = json.load(f)
            issue_ids = index.get("issue_ids", [])
            print(f"   ✔ Loaded grouped payload index: {len(issue_ids)} issues (lazy, per-issue files)")
            return LazyGroupedData(related_dir, issue_ids)
        except Exception as e:
            print(f"   ✖ Failed to load grouped payload index: {e}")
            return None

    legacy_file = os.path.join(PAYLOAD_DIR, "issues_with_related.json")
    if os.path.exists(legacy_file):
        print("   ⚠ issues_with_related_index.json not found — falling back to legacy")
        print("     issues_with_related.json (single large file). Re-run prepare_payload.py")
        print("     to regenerate per-issue files and avoid loading the whole file into memory.")
        try:
            with open(legacy_file, "r", encoding="utf-8") as f:
                grouped_list = json.load(f)
            grouped_dict = {}
            for item in grouped_list:
                source_id = item.get("source_issue_id")
                if source_id:
                    grouped_dict[str(source_id)] = item
            print(f"   ✔ Loaded legacy grouped payload: {len(grouped_dict)} issues")
            return grouped_dict
        except Exception as e:
            print(f"   ✖ Failed to load legacy grouped payload: {e}")
            return None

    print("   ⚠ No grouped payload found (index or legacy file) — falling back to separate files")
    return None


# =============================================================================
# RELATED DATA WORKERS
# =============================================================================

def load_comments_for_issue(jira_key, source_issue_id, comments_data):
    """Post all comments for one issue. Called from Phase 4C only."""
    success = 0
    failed  = 0

    for comment_item in comments_data:
        source_comment_id = str(comment_item.get("source_comment_id", ""))

        with mapping_lock:
            if source_comment_id in id_mapping.get("comments", {}):
                continue

        payload = comment_item.get("payload", {})
        data, status = jira_post(f"api/3/issue/{jira_key}/comment", payload)

        if status == 201:
            with mapping_lock:
                id_mapping["comments"][source_comment_id] = {
                    "jira_comment_id": data.get("id"),
                    "jira_issue_key":  jira_key,
                }
            success += 1
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            log_error("comment", source_comment_id, error_msg)
            failed += 1

    return success, failed


def load_links_for_issue(jira_key, source_issue_id, links_data):
    """
    Post all links for one issue.
    Called inline during Phase 2 batch processing.
    Reads direction from link_item to correctly assign inward/outward.
    Returns (success, failed).
    """
    success = 0
    failed  = 0

    for link_item in links_data:
        target_issue_id = str(link_item.get("target_issue_id", "")).strip()

        if not target_issue_id or target_issue_id in ("", "nan", "None"):
            log_error(
                "link",
                f"{source_issue_id}_EMPTY_TARGET",
                f"Skipped: target_issue_id is empty for source {source_issue_id}",
            )
            failed += 1
            continue

        link_id = f"{source_issue_id}_{target_issue_id}"

        with mapping_lock:
            if link_id in id_mapping.get("links", {}):
                continue

        with mapping_lock:
            target_info = id_mapping["issues"].get(target_issue_id)

        if not target_info:
            # Not in local mapping yet — try a live Jira lookup before giving up.
            found_key = resolve_missing_target(target_issue_id)
            if found_key:
                with mapping_lock:
                    target_info = id_mapping["issues"].get(target_issue_id)
            else:
                log_error(
                    "link",
                    f"{source_issue_id}_{target_issue_id}",
                    f"Target issue {target_issue_id} not found in id_mapping and not found in Jira — saved for retry",
                )
                with failed_lock:
                    failed_links.append(link_item)
                failed += 1
                continue

        target_key = target_info["jira_key"]
        link_type  = link_item.get("jira_link_type", "Relates")
        direction  = str(link_item.get("direction", "outward")).lower().strip()

        if direction == "outward":
            inward_key  = target_key
            outward_key = jira_key
        else:
            inward_key  = jira_key
            outward_key = target_key

        payload = {
            "type":         {"name": link_type},
            "inwardIssue":  {"key": inward_key},
            "outwardIssue": {"key": outward_key},
        }

        data, status = jira_post("api/3/issueLink", payload)

        if status == 201:
            with mapping_lock:
                id_mapping["links"][link_id] = {
                    "jira_source_key": jira_key,
                    "jira_target_key": target_key,
                    "link_type":       link_type,
                    "direction":       direction,
                }
            success += 1
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            log_error("link", link_id, error_msg)
            with failed_lock:
                failed_links.append(link_item)
            failed += 1

    return success, failed


def load_related_data_for_batch(created_issues_batch, grouped_data):
    """Load only LINKS inline. Comments are posted in Phase 4C."""
    if not grouped_data:
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_RELATED_WORKERS) as executor:
        futures = []

        for source_issue_id, jira_key in created_issues_batch:
            issue_data = grouped_data.get(str(source_issue_id))
            if not issue_data:
                continue

            links_data = issue_data.get("links", [])
            if links_data and LOAD_LINKS:
                futures.append(("link", executor.submit(
                    load_links_for_issue, jira_key, source_issue_id, links_data
                )))

        for entity_type, future in futures:
            try:
                success, failed = future.result(timeout=60)
                with report_lock:
                    if entity_type == "link":
                        report["links"]["success"] += success
                        report["links"]["failed"]  += failed
            except Exception as e:
                thread_safe_print(f"   ✖ {entity_type} worker error: {e}")

    with mapping_lock:
        save_mapping()


def _align_bulk_response(bulk_items, batch_metadata, created_issues, errors):
    """
    FIX 3 — Correctly pair each created Jira issue with its source metadata
    by accounting for gaps introduced by failed elements.

    Returns:
        aligned   : list of (meta, jira_key, jira_id)         — successful creations
        failed_meta: list of (meta, error_msg, sent_fields)   — failed elements
    """
    # Build a map of failed element index → error message
    failed_index_map = {}
    for error_entry in errors:
        idx        = error_entry.get("failedElementNumber", -1)
        elem_errs  = error_entry.get("elementErrors", {})
        error_msg  = str(
            elem_errs.get("errors", elem_errs.get("errorMessages", elem_errs))
        )
        failed_index_map[idx] = error_msg

    aligned      = []
    failed_meta  = []
    created_iter = iter(created_issues)   # consume in order

    for idx in range(len(bulk_items)):
        meta = batch_metadata[idx]

        if idx in failed_index_map:
            failed_meta.append((meta, failed_index_map[idx], bulk_items[idx]["fields"]))
        else:
            try:
                created = next(created_iter)
                aligned.append((meta, created.get("key"), created.get("id")))
            except StopIteration:
                # Should never happen if Jira response is consistent,
                # but guard defensively.
                failed_meta.append((meta, "Jira response exhausted unexpectedly"))

    return aligned, failed_meta


def process_batch_worker(batch_data, grouped_data=None):
    global _issue_counter

    batch, batch_num, total_batches, wave_num = batch_data
    _batch_start_time = time.time()   # requirement #2 — per-batch timing

    bulk_items     = []
    batch_metadata = []
    skipped_count  = 0
    deferred_count = 0
    created_batch  = []

    for item in batch:
        source_id = str(item.get("source_issue_id", ""))

        if RESUME_MODE and source_id in id_mapping["issues"]:
            skipped_count += 1
            jira_key = id_mapping["issues"][source_id]["jira_key"]
            created_batch.append((source_id, jira_key))
            continue

        source_parent_id = str(item.get("source_parent_id", "")).split(".")[0]
        source_epic_id   = str(item.get("source_epic_id",   "")).split(".")[0]
        issue_type       = item.get("jira_issue_type", "Task")
        jira_status      = item.get("jira_status", "")
        item             = hydrate_issue(item)  # pull in "payload" (fields/description) on demand
        payload          = item.get("payload", {})
        fields           = payload.get("fields", {}).copy()

        metadata = {
            "source_id":   source_id,
            "issue_type":  issue_type,
            "jira_status": jira_status,
            "title":       fields.get("summary", "Unknown")[:40],
            # Keep the original item so we can persist it on failure
            "_raw_item":   item,
        }
        
        defer_reason = None
        should_defer = False

        if issue_type == "Epic":
            pass
        elif issue_type == "Sub-task":
            if source_parent_id and source_parent_id not in ("None", "", "nan"):
                parent_info = id_mapping["issues"].get(source_parent_id)
                if parent_info:
                    fields["parent"] = {"key": parent_info["jira_key"]}
                else:
                    should_defer = True
                    defer_reason = (
                        f"Sub-task parent {source_parent_id!r} not yet in id_mapping "
                        f"(likely skipped/missing in extraction)"
                    )
            else:
                log_error("issue", source_id, "Sub-task missing parent_id")
                _record_parent_mapping_missing(source_id)
                with report_lock:
                    report["issues"]["failed"] += 1
                continue
        else:
            if source_parent_id and source_parent_id not in ("None", "", "nan", source_epic_id):
                parent_info = id_mapping["issues"].get(source_parent_id)
                if parent_info:
                    fields["parent"] = {"key": parent_info["jira_key"]}
                else:
                    should_defer = True
                    defer_reason = (
                        f"Parent issue {source_parent_id!r} not yet in id_mapping "
                        f"(likely skipped/missing in extraction)"
                    )
            elif source_epic_id and source_epic_id not in ("None", "", "nan"):
                epic_info = id_mapping["issues"].get(source_epic_id)
                if epic_info:
                    fields["parent"] = {"key": epic_info["jira_key"]}

        # If this issue type is one of the sub-task-like types AND a parent
        # was resolved above, it must be created in the SAME project as its
        # parent — derive that project key from the parent's own jira_key
        # (e.g. "EX-30" -> "EX") and overwrite fields["project"] with it.
        # Nothing else about the logic above is touched.
        if issue_type in SUBTASK_TYPES and fields.get("parent", {}).get("key"):
            parent_project_key = fields["parent"]["key"].split("-")[0]
            fields["project"] = {"key": parent_project_key}

        if should_defer:
            reason_msg = defer_reason or "Parent/epic not yet in id_mapping"
            thread_safe_print(
                f"  ⏸ DEFERRED [{source_id}] {issue_type}: "
                f"{fields.get('summary', 'Unknown')[:50]} — {reason_msg}"
            )
            # log_error("issue", source_id, f"DEFERRED: {reason_msg}")
            with failed_lock:
                failed_issues.append(item)
            deferred_count += 1
            continue

        bulk_items.append({"fields": fields, "update": {}})
        batch_metadata.append(metadata)

    # ── Nothing new to create ─────────────────────────────────────────────────
    if not bulk_items:
        with report_lock:
            report["issues"]["skipped"]  += skipped_count
            report["issues"]["deferred"] += deferred_count
        if skipped_count or deferred_count:
            parts = []
            if skipped_count:  parts.append(f"{skipped_count} skipped")
            if deferred_count: parts.append(f"{deferred_count} deferred")
            _elapsed = time.time() - _batch_start_time
            thread_safe_print(
                f"  Wave {wave_num} - Batch {batch_num}/{total_batches}: "
                + ", ".join(parts)
                + f"  (took {_elapsed:.2f}s)"
            )
        if grouped_data and created_batch:
            load_related_data_for_batch(created_batch, grouped_data)
        return 0, 0, deferred_count, batch_num, None

    # ── Bulk create ───────────────────────────────────────────────────────────
    bulk_payload = {"issueUpdates": bulk_items}
    data, status = jira_post("api/3/issue/bulk", bulk_payload, is_bulk=True)

    # ── Bulk failed entirely — fall back to single creation ───────────────────
    if status != 201:
        error_msg = str(
            data.get("errors", data.get("errorMessages", data))
            if data else "Timeout/Network error"
        )
        thread_safe_print(
            f"  Wave {wave_num} - Batch {batch_num}/{total_batches}: "
            f"✖ FAILED (HTTP {status}) → fallback"
        )
        with report_lock:
            report["batches"]["fallback"] += 1

        success_count = 0
        failed_count  = 0
        for meta in batch_metadata:
            item = meta["_raw_item"]
            sid  = meta["source_id"]
            if sid in id_mapping["issues"]:
                continue
            if load_single_issue(item):
                success_count += 1
                created_batch.append((sid, id_mapping["issues"][sid]["jira_key"]))
            else:
                failed_count += 1
                # FIX 1 — single-path failure is persisted inside load_single_issue

        if grouped_data and created_batch:
            load_related_data_for_batch(created_batch, grouped_data)

        _elapsed = time.time() - _batch_start_time
        thread_safe_print(
            f"  Wave {wave_num} - Batch {batch_num}/{total_batches}: "
            f"fallback path took {_elapsed:.2f}s total"
        )
        return success_count, failed_count, deferred_count, batch_num, error_msg

    # ── FIX 3 — Index-safe alignment of bulk response ─────────────────────────
    created_issues = data.get("issues", [])
    raw_errors     = data.get("errors", [])

    aligned, failed_meta = _align_bulk_response(
        bulk_items, batch_metadata, created_issues, raw_errors
    )

    success_count        = 0
    failed_count         = 0
    issues_to_transition = []

    # ── Register successful creations ─────────────────────────────────────────
    with mapping_lock:
        for meta, jira_key, jira_id in aligned:
            id_mapping["issues"][meta["source_id"]] = {
                "jira_key":   jira_key,
                "jira_id":    jira_id,
                "source_id":  meta["source_id"],
                "issue_type": meta["issue_type"],
                "title":      meta["title"],
                "created_at": datetime.now().isoformat(),
            }
            success_count += 1
            created_batch.append((meta["source_id"], jira_key))

            if APPLY_STATUS_TRANSITIONS and meta["jira_status"]:
                issues_to_transition.append((jira_key, meta["jira_status"], meta))

        save_mapping()

    # ── Apply status transitions ──────────────────────────────────────────────
    for jira_key, target_status, meta in issues_to_transition:
        apply_status_transition(jira_key, target_status)

    # ── Print per-issue lines ─────────────────────────────────────────────────
    for meta, jira_key, _ in aligned:
        with _issue_counter_lock:
            _issue_counter += 1
            counter = _issue_counter
        status_label = f"[{meta['jira_status']}]" if meta["jira_status"] else ""
        thread_safe_print(
            f"  [{counter}/{_issue_total}] {meta['issue_type']}: "
            f"{meta['title'][:45]}...  {jira_key}  ✔  {status_label}"
        )

    # ── Handle bulk element errors ────────────────────────────────────────────
    permanently_failed_count = 0
    for meta, error_msg, failed_fields in failed_meta:
        # ── Assignee/reporter invalid-user auto-fix ─────────────────────────
        # Only for THIS specific error class. Patch just the field Jira
        # complained about, keep everything else in the payload identical
        # (parent/project/sprint/etc. already resolved above), and retry
        # right here in the same worker — no separate queue, no next wave.
        is_user_err, fix_assignee, fix_reporter = _is_user_field_error(error_msg)
        if is_user_err:
            retry_fields = dict(failed_fields)
            if fix_assignee:
                retry_fields["assignee"] = {"accountId": FALLBACK_USER_ACCOUNT_ID}
            if fix_reporter:
                retry_fields["reporter"] = {"accountId": FALLBACK_USER_ACCOUNT_ID}

            thread_safe_print(
                f"  ↻ [{meta['source_id']}] {meta['issue_type']}: invalid "
                f"{'assignee' if fix_assignee else ''}"
                f"{'/' if fix_assignee and fix_reporter else ''}"
                f"{'reporter' if fix_reporter else ''} user — "
                f"retrying with fallback account (same worker)"
            )
            retry_data, retry_status = jira_post("api/3/issue", {"fields": retry_fields})
            if retry_status == 201 and retry_data.get("key"):
                jira_key = retry_data["key"]
                jira_id  = retry_data.get("id")
                with mapping_lock:
                    id_mapping["issues"][meta["source_id"]] = {
                        "jira_key":   jira_key,
                        "jira_id":    jira_id,
                        "source_id":  meta["source_id"],
                        "issue_type": meta["issue_type"],
                        "title":      meta["title"],
                        "created_at": datetime.now().isoformat(),
                    }
                    save_mapping()
                success_count += 1
                created_batch.append((meta["source_id"], jira_key))
                if APPLY_STATUS_TRANSITIONS and meta["jira_status"]:
                    apply_status_transition(jira_key, meta["jira_status"])
                with _issue_counter_lock:
                    _issue_counter += 1
                    counter = _issue_counter
                status_label = f"[{meta['jira_status']}]" if meta["jira_status"] else ""
                thread_safe_print(
                    f"  [{counter}/{_issue_total}] {meta['issue_type']}: "
                    f"{meta['title'][:45]}...  {jira_key}  ✔ (fallback user)  {status_label}"
                )
                continue   # fixed & created — skip normal failure handling below
            else:
                thread_safe_print(
                    f"     ✖ Fallback-user retry also failed for [{meta['source_id']}]: "
                    f"HTTP {retry_status} — {str(retry_data)[:120]}"
                )
                # fall through to normal failure handling below

        failed_count += 1   # per-batch progress line only — unchanged UI

        with _issue_counter_lock:
            _issue_counter += 1
            counter = _issue_counter

        # raw_item = meta.get("_raw_item")

        # if raw_item:
        #     # Queued for individual retry below — do NOT log_error() /
        #     # _record_parent_mapping_missing() here; load_single_issue()
        #     # will do exactly one of those based on the retry's real outcome.
        #     if not _is_parent_mapping_error(error_msg):
        #         thread_safe_print(
        #             f"  [{counter}/{_issue_total}] {meta['issue_type']}: "
        #             f"{meta['title'][:45]}...  ✖  {error_msg[:80]}"
        #         )
        #     with failed_lock:
        #         failed_issues.append(raw_item)
        #     _persist_failed_single(raw_item)   # also write to failed_singles.json
        # else:
        #     # No raw item -> can't be retried -> this genuinely IS the
        #     # final outcome, so record it right here, exactly once.
        #     log_error("issue", meta["source_id"], error_msg)
        #     permanently_failed_count += 1
        #     if _is_parent_mapping_error(error_msg):
        #         _record_parent_mapping_missing(meta["source_id"])
        #     else:
        #         thread_safe_print(
        #             f"  [{counter}/{_issue_total}] {meta['issue_type']}: "
        #             f"{meta['title'][:45]}...  ✖  {error_msg[:80]}"
        #         )
        raw_item = meta.get("_raw_item")
 
        # Log the failure right now, in this same batch/thread — do NOT wait
        # for a later retry pass. This is what puts an "issue" entry
        # (id + reason) into errors.json in THIS run, without needing the
        # slow end-of-phase retry_failed_issues() call.
        log_error("issue", meta["source_id"], error_msg)
        permanently_failed_count += 1
 
        if _is_parent_mapping_error(error_msg):
            _record_parent_mapping_missing(meta["source_id"])
        else:
            thread_safe_print(
                f"  [{counter}/{_issue_total}] {meta['issue_type']}: "
                f"{meta['title'][:45]}...  ✖  {error_msg[:80]}"
            )
 
        if raw_item:
            # Still keep it available for an OPTIONAL manual retry later
            # (python bulk_load_to_jira.py --retry-failed) — this no longer
            # blocks or slows down the current run to do it automatically.
            with failed_lock:
                failed_issues.append(raw_item)
            _persist_failed_single(raw_item)   # also write to failed_singles.json

    # ── Load only links inline (comments in Phase 4C) ─────────────────────────
    if grouped_data and created_batch:
        load_related_data_for_batch(created_batch, grouped_data)

    with report_lock:
        report["issues"]["success"]  += success_count
        report["issues"]["failed"]   += permanently_failed_count
        report["issues"]["skipped"]  += skipped_count
        report["issues"]["deferred"] += deferred_count

    _elapsed = time.time() - _batch_start_time   # requirement #2
    status_msg = (
        f"  Wave {wave_num} - Batch {batch_num}/{total_batches}: "
        f"✔ {success_count} created"
    )
    if failed_count:   status_msg += f", {failed_count} ✖ failed"
    if skipped_count:  status_msg += f", {skipped_count} skipped"
    if deferred_count: status_msg += f", {deferred_count} deferred"
    status_msg += f"  (took {_elapsed:.2f}s)"
    thread_safe_print(status_msg)

    return success_count, failed_count, deferred_count, batch_num, None


# =============================================================================
# PHASE 2: WAVE-BASED PARALLEL LOAD
# =============================================================================

def load_issues_wave_parallel():
    global _issue_total, _issue_counter

    if not LOAD_ISSUES:
        print("\n⏭ Issues: SKIPPED")
        return

    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 2: Wave-Based Parallel Issue Creation (HYBRID)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  NOTE: Comments will be posted in Phase 4C after attachments are uploaded.")

    grouped_data = load_grouped_related_data()
    issues       = load_issues_meta()
    if not issues:
        return

    _issue_total   = len(issues)
    _issue_counter = 0

    print(f"\n  Configuration:")
    print(f"    Total issues:       {_issue_total:,}")
    print(f"    Batch size:         {BULK_BATCH_SIZE}")
    print(f"    Max workers/wave:   {MAX_ISSUE_WORKERS}")
    print(f"    Grouped payload:    {'✔ Enabled' if grouped_data else '⚠ Fallback mode'}")

    waves = group_issues_by_wave(issues)

    print(f"\n  Wave structure:")
    for wave_num in sorted(waves.keys()):
        wc = len(waves[wave_num])
        tc = Counter(i.get("jira_issue_type") for i in waves[wave_num])
        print(
            f"    Wave {wave_num}: {wc:4d} issues "
            f"({', '.join(f'{t}: {c}' for t, c in sorted(tc.items()))})"
        )

    with report_lock:
        report["waves"]["total"] = len(waves)

    print(f"\n  Starting wave-based parallel creation...\n")

    for wave_num in sorted(waves.keys()):
        wave_issues = waves[wave_num]
        wave_count  = len(wave_issues)

        print(f"  ╔{'═' * 66}╗")
        print(f"  ║ WAVE {wave_num}: {wave_count} issues{' ' * (55 - len(str(wave_count)))}║")
        print(f"  ╚{'═' * 66}╝")

        batches          = [
            wave_issues[i:i+BULK_BATCH_SIZE]
            for i in range(0, wave_count, BULK_BATCH_SIZE)
        ]
        total_batches    = len(batches)
        workers_for_wave = min(MAX_ISSUE_WORKERS, total_batches)

        with report_lock:
            report["batches"]["total"] += total_batches

        print(f"    Batches: {total_batches}  |  Workers: {workers_for_wave}\n")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers_for_wave) as executor:
            futures = [
                executor.submit(
                    process_batch_worker,
                    (batch, bn, total_batches, wave_num),
                    grouped_data,
                )
                for bn, batch in enumerate(batches, 1)
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    _, _, _, _, error = future.result()
                    with report_lock:
                        if error is None:
                            report["batches"]["success"] += 1
                        else:
                            report["batches"]["failed"] += 1
                except Exception as e:
                    thread_safe_print(f"   ✖ Worker exception: {e}")
                    with report_lock:
                        report["batches"]["failed"] += 1

        with report_lock:
            report["waves"]["completed"] += 1

        print(f"\n   ✔ Wave {wave_num} complete\n")

    # ── Deferred issues are NOT retried automatically here anymore ─────────────
    # They remain in `failed_issues` (in-memory, this run) and were already
    # persisted per-item to failed_singles.json wherever that happens upstream.
    # Retry them later on purpose via:  python bulk_load_to_jira.py --retry-failed
    with failed_lock:
        retry_count = len(failed_issues)

    if retry_count > 0:
        print(f"\n    {retry_count} deferred issues were NOT retried automatically "
              f"(auto-retry disabled). Run --retry-failed when ready.")

    print(f"\n  " + "━" * 70)
    print(f"  Summary:")
    print(f"     Issues:          {report['issues']['success']:6,d} ✔")
    print(f"     Issues failed:   {report['issues']['failed']:6,d} ✖")
    print(f"     Skipped:         {report['issues']['skipped']:6,d}")
    print(f"     Links :        {report['links']['success']:6,d} ✔")
    print(f"    Batches:           {report['batches']['success']:6,d} / {report['batches']['total']}")
    print(f"    Waves:             {report['waves']['completed']:6d} / {report['waves']['total']}")
    print(f"  " + "━" * 70)


def retry_failed_issues():
    """Retry in-memory deferred/bulk-failed issues after all waves complete."""
    with failed_lock:
        issues_to_retry = failed_issues.copy()

    still_failed = []
    for item in issues_to_retry:
        source_id = str(item.get("source_issue_id", ""))
        if source_id in id_mapping["issues"]:
            continue
        if not load_single_issue(item):
            still_failed.append(item)

    if still_failed:
        fp = os.path.join(MAPPING_DIR, "failed_issues.json")
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(still_failed, f, indent=2)
        print(f"     ✖ {len(still_failed)} issues still failed → {fp}")
    else:
        print(f"     ✔ All retried successfully")


def load_single_issue(item):
    """
    Create one issue individually.

    FIX 1 — On failure, the item is persisted to failed_singles.json immediately
    via _persist_failed_single(), making it targetable by --retry-failed later.
    """
    source_id        = str(item.get("source_issue_id", ""))
    source_parent_id = str(item.get("source_parent_id", "")).split(".")[0]
    source_epic_id   = str(item.get("source_epic_id",   "")).split(".")[0]
    issue_type       = item.get("jira_issue_type", "Task")
    jira_status      = item.get("jira_status", "")
    payload          = item.get("payload", {}).copy()
    fields           = payload.get("fields", {}).copy()

    if RESUME_MODE and source_id in id_mapping["issues"]:
        return True

    if issue_type == "Epic":
        pass
    elif issue_type == "Sub-task":
        if source_parent_id and source_parent_id not in ("None", "", "nan"):
            parent_info = id_mapping["issues"].get(source_parent_id)
            if parent_info:
                fields["parent"] = {"key": parent_info["jira_key"]}
            else:
                reason = (
                    f"Sub-task parent {source_parent_id!r} still not in id_mapping "
                    f"after retry — parent was likely skipped or missing in extraction"
                )
                thread_safe_print(f"     ✖ DEFERRED→FAILED [{source_id}]: {reason}")
                log_error("issue", source_id, f"DEFERRED→FAILED: {reason}")
                _record_parent_mapping_missing(source_id)
                # Parent still not available — persist and bail
                _persist_failed_single(item)
                with report_lock:
                    report["issues"]["failed"] += 1
                return False
    else:
        if source_parent_id and source_parent_id not in ("None", "", "nan", source_epic_id):
            parent_info = id_mapping["issues"].get(source_parent_id)
            if parent_info:
                fields["parent"] = {"key": parent_info["jira_key"]}
            else:
                reason = (
                    f"Parent {source_parent_id!r} still not in id_mapping "
                    f"after retry — parent was likely skipped or missing in extraction"
                )
                thread_safe_print(f"     ✖ DEFERRED→FAILED [{source_id}]: {reason}")
                log_error("issue", source_id, f"DEFERRED→FAILED: {reason}")
                _record_parent_mapping_missing(source_id)
                _persist_failed_single(item)
                with report_lock:
                    report["issues"]["failed"] += 1
                return False
        elif source_epic_id and source_epic_id not in ("None", "", "nan"):
            epic_info = id_mapping["issues"].get(source_epic_id)
            if epic_info:
                fields["parent"] = {"key": epic_info["jira_key"]}

    # If this issue type is one of the sub-task-like types AND a parent was
    # resolved above, it must be created in the SAME project as its parent —
    # derive that project key from the parent's own jira_key (e.g.
    # "EX-30" -> "EX") and overwrite fields["project"] with it. Nothing else
    # about the logic above is touched.
    if issue_type in SUBTASK_TYPES and fields.get("parent", {}).get("key"):
        parent_project_key = fields["parent"]["key"].split("-")[0]
        fields["project"] = {"key": parent_project_key}

    payload["fields"] = fields
    data, status = jira_post("api/3/issue", payload)

    if status != 201:
        first_error_msg = str(
            data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
        )
        is_user_err, fix_assignee, fix_reporter = _is_user_field_error(first_error_msg)
        if is_user_err:
            thread_safe_print(
                f"     ↻ [{source_id}] invalid "
                f"{'assignee' if fix_assignee else ''}"
                f"{'/' if fix_assignee and fix_reporter else ''}"
                f"{'reporter' if fix_reporter else ''} user — "
                f"retrying with fallback account (same worker)"
            )
            if fix_assignee:
                fields["assignee"] = {"accountId": FALLBACK_USER_ACCOUNT_ID}
            if fix_reporter:
                fields["reporter"] = {"accountId": FALLBACK_USER_ACCOUNT_ID}
            payload["fields"] = fields
            data, status = jira_post("api/3/issue", payload)

    if status == 201:
        jira_key = data.get("key")
        with mapping_lock:
            id_mapping["issues"][source_id] = {
                "jira_key":   jira_key,
                "jira_id":    data.get("id"),
                "source_id":  source_id,
                "issue_type": issue_type,
                "title":      fields.get("summary", ""),
                "created_at": datetime.now().isoformat(),
            }
            save_mapping()
        with report_lock:
            report["issues"]["success"] += 1
        if APPLY_STATUS_TRANSITIONS and jira_status:
            apply_status_transition(jira_key, jira_status)
        thread_safe_print(f"     ✔ {jira_key} (single fallback)")
        return True
    else:
        error_msg = str(
            data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
        )
        log_error("issue", source_id, error_msg)
        # FIX 1 — Persist to disk immediately so --retry-failed can find it
        _persist_failed_single(item)
        with report_lock:
            report["issues"]["failed"] += 1

        if _is_parent_mapping_error(error_msg):
            _record_parent_mapping_missing(source_id)
        else:
            thread_safe_print(
                f"     ✖ {source_id} single creation failed: {error_msg[:80]}"
            )
        return False


# =============================================================================
# FIX 2 — RETRY FAILED CLI MODE
#
# Reads failed_singles.json (all single-path failures from current + prior runs),
# skips anything already in id_mapping (RESUME_MODE safe),
# and re-attempts creation one by one.
#
# Usage:  python bulk_load_to_jira.py --retry-failed
# =============================================================================

def run_retry_failed_mode():
    """
    Standalone retry pass for all issues that failed single creation.
    Safe to run multiple times — skips already-mapped issues automatically.
    """
    print("=" * 70)
    print("  RETRY-FAILED MODE")
    print("=" * 70)

    if not os.path.exists(FAILED_SINGLES_FILE):
        print(f"  ✔ No {FAILED_SINGLES_FILE} found — nothing to retry.")
        return

    with open(FAILED_SINGLES_FILE, "r", encoding="utf-8") as f:
        items = json.load(f)

    print(f"  Loaded {len(items)} failed items from {FAILED_SINGLES_FILE}")

    still_failed  = []
    success_count = 0
    skip_count    = 0

    for item in items:
        source_id = str(item.get("source_issue_id", ""))
        if source_id in id_mapping["issues"]:
            skip_count += 1
            continue
        if load_single_issue(item):
            success_count += 1
        else:
            still_failed.append(item)

    # Overwrite file with only the ones that are still failing
    with open(FAILED_SINGLES_FILE, "w", encoding="utf-8") as f:
        json.dump(still_failed, f, indent=2)

    print(f"\n  Results:")
    print(f"    ✔ Recovered:   {success_count}")
    print(f"    ⏭ Skipped:    {skip_count}  (already in Jira)")
    print(f"    ✖ Still failed: {len(still_failed)}")
    if still_failed:
        print(f"    Remaining failures saved → {FAILED_SINGLES_FILE}")


# =============================================================================
# PHASE 3A: ASSIGN SPRINTS (chunked to 50 — Jira hard limit)
# =============================================================================

def assign_issues_to_sprints():
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 3A: Assigning Issues to Sprints (parallel)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    issues           = load_issues_meta()
    sprint_issue_map = {}

    for item in issues:
        source_issue_id  = str(item.get("source_issue_id"))
        source_sprint_id = str(item.get("source_sprint_id", "")).split(".")[0]

        if source_sprint_id in ("", "None", "nan"):
            continue

        sprint_info = id_mapping["sprints"].get(source_sprint_id)
        issue_info  = id_mapping["issues"].get(source_issue_id)

        if not sprint_info or not issue_info:
            continue

        sprint_issue_map.setdefault(sprint_info["jira_sprint_id"], []).append(
            issue_info["jira_key"]
        )

    # Flatten to a list of (sprint_id, chunk_idx, total_chunks, chunk) work
    # items so every chunk — across every sprint — can run on its own thread.
    # Order within a sprint doesn't matter to Jira (each chunk is an
    # independent "add these issues" call), so this is safe to parallelize
    # even across chunks of the same sprint.
    work_items = []
    for sprint_id, issue_keys in sprint_issue_map.items():
        chunks = [
            issue_keys[i:i+SPRINT_CHUNK_SIZE]
            for i in range(0, len(issue_keys), SPRINT_CHUNK_SIZE)
        ]
        for chunk_idx, chunk in enumerate(chunks, 1):
            work_items.append((sprint_id, chunk_idx, len(chunks), chunk))

    total_assigned_lock = Lock()
    total_assigned = {"count": 0}

    def _assign_chunk(work_item):
        sprint_id, chunk_idx, total_chunks, chunk = work_item
        data, status = jira_post(
            f"agile/1.0/sprint/{sprint_id}/issue", {"issues": chunk}
        )

        if status == 204:
            with total_assigned_lock:
                total_assigned["count"] += len(chunk)
            thread_safe_print(
                f"  Sprint {sprint_id}: chunk {chunk_idx}/{total_chunks} "
                f"({len(chunk)} issues)... ✔"
            )
        else:
            err = str(data.get("errorMessages", data)) if data else "Unknown"
            thread_safe_print(
                f"  Sprint {sprint_id}: chunk {chunk_idx}/{total_chunks} "
                f"({len(chunk)} issues)... ✖ {err}"
            )
            log_error("sprint_assign", sprint_id, err)

    if work_items:
        workers = min(SPRINT_ASSIGN_MAX_WORKERS, len(work_items))
        print(f"  {len(work_items)} chunk(s) across {len(sprint_issue_map)} sprint(s)  |  Workers: {workers}\n")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_assign_chunk, wi) for wi in work_items]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    thread_safe_print(f"   ✖ Sprint-assign worker exception: {e}")

    print(f"   ✔ Assigned {total_assigned['count']} issues across {len(sprint_issue_map)} sprints")


# =============================================================================
# PHASE 3B: ASSIGN VERSIONS
# =============================================================================

def assign_versions_to_issues():
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 3B: Assigning Versions to Issues (parallel)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    issues = load_issues_meta()

    # Build the work list up front — one PUT per issue that actually has a
    # resolvable version + issue mapping — so the pool only ever touches
    # real work items.
    work_items = []
    for item in issues:
        source_issue_id   = str(item.get("source_issue_id"))
        source_release_id = str(item.get("source_release_id", "")).split(".")[0]

        if source_release_id in ("", "None", "nan"):
            continue

        version_info = id_mapping["versions"].get(source_release_id)
        issue_info   = id_mapping["issues"].get(source_issue_id)

        if not version_info or not issue_info:
            continue

        work_items.append((issue_info["jira_key"], version_info["jira_version_id"]))

    assigned_lock = Lock()
    assigned_count = {"count": 0}

    def _assign_version(work_item):
        jira_key, version_id = work_item
        payload = {
            "update": {
                "fixVersions": [{"add": {"id": version_id}}]
            }
        }
        _, status = jira_put(f"api/3/issue/{jira_key}", payload)
        if status == 204:
            with assigned_lock:
                assigned_count["count"] += 1
                current = assigned_count["count"]
            if current % 50 == 0:
                thread_safe_print(f"  Assigned {current} versions...")
        else:
            thread_safe_print(f"   ✖ Failed to assign version to {jira_key} (HTTP {status})")

    if work_items:
        workers = min(VERSION_ASSIGN_MAX_WORKERS, len(work_items))
        print(f"  {len(work_items)} issue(s) to assign  |  Workers: {workers}\n")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_assign_version, wi) for wi in work_items]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    thread_safe_print(f"   ✖ Version-assign worker exception: {e}")

    print(f"   ✔ Assigned fixVersions to {assigned_count['count']} issues")


# =============================================================================
# PHASE 4A: UPLOAD ATTACHMENTS
# =============================================================================

def upload_attachments_phase():
    if not UPLOAD_ATTACHMENTS:
        print("\n⏭ Attachments: SKIPPED")
        return False

    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 4A: Uploading Attachments to Jira")
    print("  (Documents + Comment images/attachments)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    result = subprocess.run([sys.executable, "upload_attachments.py"], capture_output=False)

    if result.returncode != 0:
        print("   ✖ upload_attachments.py exited with errors.")
        print("   Comments may be missing attachment links.")
        return False

    print("   ✔ Attachments uploaded successfully.")
    return True


# =============================================================================
# PHASE 4B: REBUILD COMMENT PAYLOADS WITH ATTACHMENT LINKS
# =============================================================================

def rebuild_comment_payloads_with_attachments():
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 4B: Rebuilding Comment Payloads with Attachment Links")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    try:
        import prepare_payload as pp
        attachment_mapping = pp.load_attachment_mapping()
        comment_payloads   = pp.prepare_comments(attachment_mapping)
        print(f"   ✔ {len(comment_payloads)} comment payloads rebuilt with attachment links.")
        return attachment_mapping
    except Exception as e:
        print(f"   ✖ Could not rebuild comment payloads: {e}")
        return {"by_local_path": {}, "by_source_comment_id": {}}


# =============================================================================
# PHASE 4C: POST ALL COMMENTS (with attachment links embedded)
# =============================================================================

def run_with_cooldown_retries(phase_fn, label, max_passes=MAX_PHASE_PASSES, cooldown_seconds=COOLDOWN_SECONDS):
    """
    Run phase_fn() (must return a dict containing at least "failed") up to
    max_passes times. If a pass ends with failed > 0, wait cooldown_seconds
    (letting Jira's rate-limit window reset) and run phase_fn() again —
    each phase already skips anything that already succeeded, so a re-run
    only retries what's still missing, never redoes finished work.
    """
    result = None
    for attempt in range(1, max_passes + 1):
        thread_safe_print(f"\n  ── {label}: pass {attempt}/{max_passes} ──")
        result = phase_fn() or {}
        failed = result.get("failed", 0)

        if failed == 0:
            if attempt > 1:
                thread_safe_print(f"  ✔ {label}: all remaining items succeeded on pass {attempt}.")
            return result

        if attempt == max_passes:
            thread_safe_print(
                f"  ⚠ {label}: {failed} item(s) still failing after {max_passes} passes. "
                f"Giving up for this run — check failed_*.json / the error log."
            )
            return result

        thread_safe_print(
            f"  ⚠ {label}: {failed} item(s) failed this pass. "
            f"Cooling down {cooldown_seconds}s before retrying..."
        )
        time.sleep(cooldown_seconds)
    return result


def load_all_comments_with_attachments():
    """
    Post all comments for all issues.

    Parallelized ACROSS issues: one worker thread handles all of a given
    issue's comments, posting them strictly in original order (so comment
    order on that issue is never scrambled). Different issues run on
    different threads at the same time. Jira has no bulk "create comments"
    endpoint, so each comment is still a single POST — the speedup comes
    from having several of those POSTs in flight at once instead of one at
    a time, while 429s are still handled per-request via jira_request()'s
    existing Retry-After backoff.
    """
    if not LOAD_COMMENTS:
        print("\n⏭ Comments: SKIPPED")
        return {"posted": 0, "skipped": 0, "failed": 0}

    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 4C: Posting All Comments (with attachment links)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    comments = load_json("comments.json")
    if not comments:
        return {"posted": 0, "skipped": 0, "failed": 0}

    total = len(comments)

    # Group by parent issue, preserving each comment's original position
    # within that issue's group (comments.json is already in chronological
    # order per issue from prepare_comments()).
    groups = defaultdict(list)
    for item in comments:
        groups[str(item.get("source_issue_id", ""))].append(item)

    posted_count  = 0
    skipped_count = 0
    failed_count  = 0
    processed     = 0  # successes + failures, for periodic mapping checkpoints

    def post_comment_group(source_issue_id, group_items):
        nonlocal posted_count, skipped_count, failed_count, processed
        _group_start = time.time()   # requirement #2 — per-issue comment group timing
        _group_posted_before = posted_count
        for item in group_items:
            source_comment_id = str(item.get("source_comment_id", ""))

            if RESUME_MODE and source_comment_id in id_mapping.get("comments", {}):
                with report_lock:
                    skipped_count += 1
                continue

            issue_info = id_mapping["issues"].get(source_issue_id)
            if not issue_info:
                log_error(
                    "comment", source_comment_id,
                    f"Parent issue {source_issue_id} not in id_mapping",
                )
                with report_lock:
                    failed_count += 1
                    report["comments"]["failed"] += 1
                continue

            jira_key = issue_info["jira_key"]
            payload  = item.get("payload", {})

            data, status = jira_post(f"api/3/issue/{jira_key}/comment", payload)

            if status == 201:
                do_checkpoint = False
                with mapping_lock:
                    id_mapping["comments"][source_comment_id] = {
                        "jira_comment_id": data.get("id"),
                        "jira_issue_key":  jira_key,
                    }
                with report_lock:
                    posted_count += 1
                    processed += 1
                    report["comments"]["success"] += 1
                    current_processed = processed
                    current_posted    = posted_count
                if current_processed % MAPPING_SAVE_EVERY == 0:
                    with mapping_lock:
                        save_mapping()
                if current_posted % 50 == 0:
                    thread_safe_print(
                        f"  [{current_posted + failed_count}/{total}] "
                        f"{current_posted} comments posted so far..."
                    )
            else:
                error_msg = str(
                    data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
                )
                log_error("comment", source_comment_id, error_msg)
                with report_lock:
                    failed_count += 1
                    processed += 1
                    report["comments"]["failed"] += 1

        _group_elapsed = time.time() - _group_start
        thread_safe_print(
            f"     ↳ issue {source_issue_id}: {len(group_items)} comment(s) "
            f"processed in {_group_elapsed:.2f}s"
        )

    workers = min(MAX_COMMENT_WORKERS, max(len(groups), 1))
    print(f"  {len(groups)} issues have comments  |  Workers: {workers}\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(post_comment_group, source_issue_id, items)
            for source_issue_id, items in groups.items()
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                thread_safe_print(f"   ✖ Comment worker exception: {e}")

    # Final flush — guarantees everything posted this run is on disk even if
    # the last MAPPING_SAVE_EVERY checkpoint wasn't reached.
    with mapping_lock:
        save_mapping()

    if failed_count > 0:
        save_failed("comments", [
            item for item in comments
            if str(item.get("source_comment_id", ""))
            not in id_mapping.get("comments", {})
        ])

    print(
        f"\n  Summary: {posted_count} ✔ posted | "
        f"{skipped_count} skipped | {failed_count} ✖ failed"
    )
    print(f"  Total: {total} comments processed")

    return {"posted": posted_count, "skipped": skipped_count, "failed": failed_count}


# =============================================================================
# FIX 4: UPDATE ALREADY-POSTED COMMENTS WITH ATTACHMENT LINKS
# =============================================================================

def update_posted_comments_with_attachments():
    """
    Patch already-posted comments via PUT to embed attachment links.
    Run via:  python bulk_load_to_jira.py --patch-comments

    Parallelized the same way as the comment-posting phase (worker pool,
    same MAX_PATCH_WORKERS knob), and resumable: a comment already patched
    successfully in an earlier pass is marked in id_mapping.json
    ("attachments_patched": true) and skipped on the next run, so retrying
    after a cooldown only touches the comments still missing their links.
    """
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PATCH: Updating Already-Posted Comments with Attachment Links")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    comments = load_json("comments.json")
    if not comments:
        print("  No comments.json found")
        return {"updated": 0, "skipped": 0, "failed": 0}

    attachment_mapping_path = os.path.join(MAPPING_DIR, "attachment_mapping.json")
    if not os.path.exists(attachment_mapping_path):
        print("   ✖ attachment_mapping.json not found — run upload_attachments.py first")
        return {"updated": 0, "skipped": 0, "failed": 0}

    with open(attachment_mapping_path, "r", encoding="utf-8") as f:
        attachment_mapping = json.load(f)

    by_comment = attachment_mapping.get("by_source_comment_id", {})

    # Build the work list up front (same filtering as before), so the
    # worker pool only ever sees comments that actually need a PATCH.
    pending       = []
    skipped_count = 0
    for item in comments:
        source_comment_id = str(item.get("source_comment_id", ""))
        posted_info = id_mapping.get("comments", {}).get(source_comment_id)

        if not posted_info or source_comment_id not in by_comment:
            skipped_count += 1
            continue
        if RESUME_MODE and posted_info.get("attachments_patched"):
            skipped_count += 1
            continue

        pending.append((source_comment_id, posted_info, item))

    updated_count = 0
    failed_count  = 0
    processed     = 0

    def patch_one(source_comment_id, posted_info, item):
        nonlocal updated_count, failed_count, processed
        _patch_start     = time.time()   # requirement #2 — per-patch timing
        jira_key        = posted_info["jira_issue_key"]
        jira_comment_id = posted_info["jira_comment_id"]
        payload         = item.get("payload", {})

        data, status = jira_put(
            f"api/3/issue/{jira_key}/comment/{jira_comment_id}", payload
        )
        _patch_elapsed = time.time() - _patch_start

        if status == 200:
            do_checkpoint = False
            with mapping_lock:
                id_mapping["comments"][source_comment_id]["attachments_patched"] = True
            with report_lock:
                updated_count += 1
                processed += 1
                if processed % MAPPING_SAVE_EVERY == 0:
                    do_checkpoint = True
            if do_checkpoint:
                with mapping_lock:
                    save_mapping()
            thread_safe_print(
                f"   ✔ Updated comment {jira_comment_id} on {jira_key} "
                f"with {len(by_comment[source_comment_id])} attachment(s)  (took {_patch_elapsed:.2f}s)"
            )
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            with report_lock:
                failed_count += 1
                processed += 1
            log_error("comment_update", source_comment_id, error_msg)
            thread_safe_print(
                f"   ✖ Failed to update comment {jira_comment_id}: {error_msg[:80]}  (took {_patch_elapsed:.2f}s)"
            )

    if pending:
        workers = min(MAX_PATCH_WORKERS, len(pending))
        print(f"  {len(pending)} comment(s) need patching  |  Workers: {workers}\n")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(patch_one, *args) for args in pending]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    thread_safe_print(f"   ✖ Patch worker exception: {e}")

        with mapping_lock:
            save_mapping()
    else:
        print("  Nothing left to patch (all comments already patched or not eligible).")

    print(
        f"\n  Summary: {updated_count} ✔ updated | "
        f"{skipped_count} skipped (no attachments, not yet posted, or already patched) | "
        f"{failed_count} ✖ failed"
    )

    return {"updated": updated_count, "skipped": skipped_count, "failed": failed_count}


# =============================================================================
# FIX 5: REBUILD + PATCH ISSUE DESCRIPTIONS WITH ATTACHMENT LINKS
# =============================================================================

def rebuild_issue_payloads_with_attachments():
    """
    Regenerates issues.json with attachment links embedded in each
    description ADF — mirrors rebuild_comment_payloads_with_attachments().
    """
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 4D: Rebuilding Issue Payloads with Attachment Links")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    try:
        import prepare_payload as pp
        attachment_mapping = pp.load_attachment_mapping()
        issue_payloads      = pp.prepare_issues(attachment_mapping)
        print(f"   ✔ {len(issue_payloads)} issue payloads rebuilt with attachment links.")
        return attachment_mapping
    except Exception as e:
        print(f"   ✖ Could not rebuild issue payloads: {e}")
        return {"by_local_path": {}, "by_source_comment_id": {}, "by_source_issue_id": {}}


def update_posted_issues_with_attachments():
    """
    Patch already-created issues via PUT to embed attachment links
    (images/documents that were referenced inline in the description)
    that were uploaded to Jira AFTER the issue was originally created.

    Only touches the 'description' field — every other field on the
    issue is left exactly as-is, so this is safe to run repeatedly and
    never creates a duplicate issue or duplicate attachment.

    Run via:  python bulk_load_to_jira.py --patch-descriptions
    """
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PATCH: Updating Already-Posted Issue Descriptions with Attachment Links")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    issues = load_issues_meta()
    if not issues:
        print("  No issues found")
        return

    attachment_mapping_path = os.path.join(MAPPING_DIR, "attachment_mapping.json")
    if not os.path.exists(attachment_mapping_path):
        print("   ✖ attachment_mapping.json not found — run upload_attachments.py first")
        return

    with open(attachment_mapping_path, "r", encoding="utf-8") as f:
        attachment_mapping = json.load(f)

    by_issue      = attachment_mapping.get("by_source_issue_id", {})
    updated_count = 0
    skipped_count = 0
    failed_count  = 0

    for item in issues:
        source_issue_id = str(item.get("source_issue_id", ""))

        posted_info = id_mapping.get("issues", {}).get(source_issue_id)
        if not posted_info:
            skipped_count += 1
            continue

        if source_issue_id not in by_issue:
            skipped_count += 1
            continue

        jira_key    = posted_info["jira_key"]
        item        = hydrate_issue(item)  # only reached for issues that have attachments
        description = item.get("payload", {}).get("fields", {}).get("description")
        if not description:
            skipped_count += 1
            continue

        # Only send the description field — never re-send the full
        # payload (project/issuetype etc. must not be touched on update).
        patch_payload = {"fields": {"description": description}}

        data, status = jira_put(f"api/3/issue/{jira_key}", patch_payload)

        if status == 204:
            updated_count += 1
            thread_safe_print(
                f"   ✔ Updated description on {jira_key} "
                f"with {len(by_issue[source_issue_id])} attachment(s)"
            )
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            failed_count += 1
            log_error("issue_description_update", source_issue_id, error_msg)
            thread_safe_print(
                f"   ✖ Failed to update description on {jira_key}: {error_msg[:80]}"
            )

    print(
        f"\n  Summary: {updated_count} ✔ updated | "
        f"{skipped_count} skipped (no attachments or not yet posted) | "
        f"{failed_count} ✖ failed"
    )


# =============================================================================
# PHASE 5: REMAINING LINKS
# =============================================================================

def load_remaining_links():
    if not LOAD_LINKS:
        print("\n Links: SKIPPED")
        return

    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 5: Loading Remaining Issue Links")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    links = load_json("links.json")
    if not links:
        return

    remaining     = 0
    skipped_count = 0
    failed_count  = 0
    success_count = 0
    processed     = 0   # successes + failures, for periodic mapping checkpoints

    # Build the work list up front (same filtering as before) so the worker
    # pool only ever sees links that actually still need posting.
    pending = []
    for item in links:
        source_issue_id = str(item.get("source_issue_id", "")).split(".")[0].strip()
        target_issue_id = str(item.get("target_issue_id", "")).split(".")[0].strip()

        if not target_issue_id or target_issue_id in ("", "nan", "None"):
            log_error("link", source_issue_id, "Empty target_issue_id — skipped")
            with report_lock:
                report["links"]["failed"] += 1
            failed_count += 1
            continue

        link_id = f"{source_issue_id}_{target_issue_id}"

        with mapping_lock:
            already_loaded = link_id in id_mapping.get("links", {})

        if already_loaded:
            skipped_count += 1
            continue

        pending.append((source_issue_id, target_issue_id, link_id, item))

    remaining = len(pending)

    def post_one_link(source_issue_id, target_issue_id, link_id, item):
        nonlocal success_count, failed_count, processed

        source_info = id_mapping["issues"].get(source_issue_id)
        target_info = id_mapping["issues"].get(target_issue_id)

        if not source_info:
            found_key = resolve_missing_target(source_issue_id)
            if found_key:
                source_info = id_mapping["issues"].get(source_issue_id)
            else:
                log_error("link", link_id, f"Source issue {source_issue_id} not in id_mapping and not found in Jira")
                with failed_lock:
                    failed_links.append(item)
                with report_lock:
                    report["links"]["failed"] += 1
                    failed_count += 1
                    processed += 1
                return

        if not target_info:
            found_key = resolve_missing_target(target_issue_id)
            if found_key:
                target_info = id_mapping["issues"].get(target_issue_id)
            else:
                log_error("link", link_id, f"Target issue {target_issue_id} not in id_mapping and not found in Jira — saved for retry")
                with failed_lock:
                    failed_links.append(item)
                with report_lock:
                    report["links"]["failed"] += 1
                    failed_count += 1
                    processed += 1
                return

        source_key = source_info["jira_key"]
        target_key = target_info["jira_key"]
        link_type  = item.get("jira_link_type", "Relates")
        direction  = str(item.get("direction", "outward")).lower().strip()

        if direction == "outward":
            inward_key  = target_key
            outward_key = source_key
        else:
            inward_key  = source_key
            outward_key = target_key

        payload = {
            "type":         {"name": link_type},
            "inwardIssue":  {"key": inward_key},
            "outwardIssue": {"key": outward_key},
        }

        data, status = jira_post("api/3/issueLink", payload)

        if status == 201:
            do_checkpoint = False
            with mapping_lock:
                id_mapping["links"][link_id] = {
                    "jira_source_key": source_key,
                    "jira_target_key": target_key,
                    "link_type":       link_type,
                    "direction":       direction,
                }
            with report_lock:
                report["links"]["success"] += 1
                success_count += 1
                processed += 1
                current_processed = processed
            if current_processed % MAPPING_SAVE_EVERY == 0:
                with mapping_lock:
                    save_mapping()
            if success_count % 50 == 0:
                thread_safe_print(
                    f"  [{success_count + failed_count}/{remaining}] "
                    f"{success_count} links posted so far..."
                )
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            log_error("link", link_id, error_msg)
            with failed_lock:
                failed_links.append(item)
            with report_lock:
                report["links"]["failed"] += 1
                failed_count += 1
                processed += 1

    workers = min(MAX_LINK_WORKERS, max(len(pending), 1))
    print(f"  {remaining} links to post  |  Workers: {workers}\n")

    if pending:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(post_one_link, sid, tid, lid, item)
                for sid, tid, lid, item in pending
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    thread_safe_print(f"   ✖ Link worker exception: {e}")

    # Final flush — guarantees everything posted this run is on disk even if
    # the last MAPPING_SAVE_EVERY checkpoint wasn't reached.
    with mapping_lock:
        save_mapping()

    # Save unresolved links for retry after full migration
    if failed_links:
        retry_path = os.path.join(MAPPING_DIR, "links_pending_retry.json")
        with open(retry_path, "w") as f:
            json.dump(failed_links, f, indent=2)
        print(f"  {len(failed_links)} links saved for retry → links_pending_retry.json")
        print(f"  Re-run after full migration to resolve cross-issue links")

    print(f"  Summary: {success_count}  | {failed_count}  | {skipped_count} already loaded")


# =============================================================================
# PHASE 6: DEPENDENCIES
# =============================================================================

def load_dependencies():
    if not LOAD_DEPENDENCIES:
        print("\n⏭ Dependencies: SKIPPED")
        return

    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  PHASE 6: Loading Dependencies (Predecessor/Successor)")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    deps = load_json("dependencies.json")
    if not deps:
        print("  No dependencies.json found")
        return

    success_count = 0
    failed_count  = 0
    skipped_count = 0
    processed     = 0   # successes + failures, for periodic mapping checkpoints

    # Build the work list up front so the worker pool only sees dependencies
    # that actually still need posting.
    pending = []
    for dep in deps:
        source_id    = str(dep.get("source_issue_id", ""))
        linked_id    = str(dep.get("linked_source_id", ""))
        relationship = dep.get("relationship", "blocks")
        link_key     = f"{source_id}_{linked_id}_{relationship}"

        if RESUME_MODE and link_key in id_mapping.get("links", {}):
            skipped_count += 1
            continue

        pending.append((source_id, linked_id, relationship, link_key, dep))

    def post_one_dependency(source_id, linked_id, relationship, link_key, dep):
        nonlocal success_count, failed_count, processed

        source_info = id_mapping["issues"].get(source_id)
        linked_info = id_mapping["issues"].get(linked_id)

        if not source_info:
            resolve_missing_target(source_id)
            source_info = id_mapping["issues"].get(source_id)

        if not linked_info:
            resolve_missing_target(linked_id)
            linked_info = id_mapping["issues"].get(linked_id)

        if not source_info or not linked_info:
            missing_side = "source" if not source_info else "linked"
            missing_id   = source_id if not source_info else linked_id
            proj         = dep.get("linked_issue_project_id", "unknown")
            log_error(
                "dependency", link_key,
                f"{missing_side.title()} {missing_id} not found — likely belongs to "
                f"unmigrated project '{proj}' (not in PROJECT_MAP)",
            )
            save_pending_dependency(dep)
            with report_lock:
                failed_count += 1
                processed += 1
            return

        source_key = source_info["jira_key"]
        linked_key = linked_info["jira_key"]

        if relationship == "blocks":
            payload = {
                "type":         {"name": "Blocks"},
                "inwardIssue":  {"key": source_key},
                "outwardIssue": {"key": linked_key},
            }
        else:
            payload = {
                "type":         {"name": "Blocks"},
                "inwardIssue":  {"key": linked_key},
                "outwardIssue": {"key": source_key},
            }

        data, status = jira_post("api/3/issueLink", payload)

        if status == 201:
            with mapping_lock:
                id_mapping["links"][link_key] = {
                    "source_key":   source_key,
                    "linked_key":   linked_key,
                    "relationship": relationship,
                }
            with report_lock:
                success_count += 1
                processed += 1
                current_processed = processed
            if current_processed % MAPPING_SAVE_EVERY == 0:
                with mapping_lock:
                    save_mapping()
            if success_count % 50 == 0:
                thread_safe_print(
                    f"  [{success_count + failed_count}/{len(pending)}] "
                    f"{success_count} dependencies posted so far..."
                )
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            log_error("dependency", link_key, error_msg)
            with report_lock:
                failed_count += 1
                processed += 1

    workers = min(MAX_DEPENDENCY_WORKERS, max(len(pending), 1))
    print(f"  {len(pending)} dependencies to post  |  Workers: {workers}\n")

    if pending:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(post_one_dependency, sid, lid, rel, lk, dep)
                for sid, lid, rel, lk, dep in pending
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    thread_safe_print(f"   ✖ Dependency worker exception: {e}")

    # Final flush — guarantees everything posted this run is on disk even if
    # the last MAPPING_SAVE_EVERY checkpoint wasn't reached.
    with mapping_lock:
        save_mapping()

    report["dependencies"]["success"] = success_count
    report["dependencies"]["failed"]  = failed_count
    report["dependencies"]["skipped"] = skipped_count
    print(
        f"  Summary: {success_count} ✔ | {failed_count} ✖ | {skipped_count} skipped"
    )


# =============================================================================
# PHASE 7: VALIDATE
# =============================================================================

def validate_migration():
    print(f"\n{'━' * 70}")
    print("  PHASE 7: VALIDATION")
    print(f"{'━' * 70}")

    validation_report = {
        "validated_at":    datetime.now().isoformat(),
        "source_count":    0,
        "jira_count":      0,
        "count_match":     False,
        "missing_in_jira": [],
        "spot_check":      [],
    }

    issues_payload = load_issues_meta()
    source_count   = len(issues_payload)
    jira_count     = len(id_mapping["issues"])

    validation_report["source_count"] = source_count
    validation_report["jira_count"]   = jira_count
    validation_report["count_match"]  = (source_count == jira_count)

    print(f"\n  Count Check:")
    print(f"    Source issues:   {source_count:,}")
    print(f"    Jira issues:     {jira_count:,}")

    if source_count == jira_count:
        print(f"     ✔ Counts match!")
    else:
        diff = source_count - jira_count
        print(f"     ✖ Mismatch: {diff} issues not created")
        source_ids = {str(i.get("source_issue_id")) for i in issues_payload}
        missing    = source_ids - set(id_mapping["issues"].keys())
        validation_report["missing_in_jira"] = list(missing)
        for mid in list(missing)[:10]:
            print(f"      • Source ID: {mid}")

    print(f"\n  Spot Check (5 random issues):")
    mapped_ids = list(id_mapping["issues"].keys())
    sample_ids = random.sample(mapped_ids, min(5, len(mapped_ids)))

    for source_id in sample_ids:
        jira_key = id_mapping["issues"][source_id].get("jira_key")
        data, status = jira_get(f"api/3/issue/{jira_key}")
        if status == 200:
            summary = data.get("fields", {}).get("summary", "")
            st      = data.get("fields", {}).get("status", {}).get("name", "")
            itype   = data.get("fields", {}).get("issuetype", {}).get("name", "")
            print(f"     {jira_key} | {itype} | {st} | {summary[:40]}")
            validation_report["spot_check"].append({
                "source_id": source_id,
                "jira_key":  jira_key,
                "summary":   summary[:50],
                "status":    st,
                "type":      itype,
            })
        else:
            print(f"     {jira_key} | HTTP {status}")

    val_path = os.path.join(MAPPING_DIR, "validation_report.json")
    with open(val_path, "w") as f:
        json.dump(validation_report, f, indent=2)
    print(f"\n   ✔ Validation report saved: {val_path}")
    print(f"{'━' * 70}")


# =============================================================================
# SAVE ERROR LOG
# =============================================================================

def save_error_log():
    errors = report.get("errors", [])
    if not errors:
        print("   ✔ No errors to log")
        return

    grouped   = defaultdict(list)
    for e in errors:
        grouped[e["entity_type"]].append(e)

    error_path = os.path.join(MAPPING_DIR, "errors.json")
    with open(error_path, "w", encoding="utf-8") as f:
        json.dump(
            {"total": len(errors), "by_type": dict(grouped), "all": errors},
            f, indent=2,
        )

    print(f"   ✔ Error log saved: {error_path}  ({len(errors)} errors)")
    for etype, errs in grouped.items():
        print(f"    {etype}: {len(errs)} errors")

def export_failed_items_excel():
    """Export every failed item (issue/comment/link/version/sprint/etc.) to an
    Excel file with its ID and the reason it failed, for easy review."""
    errors = report.get("errors", [])
    if not errors:
        print("   ✔ No failures to export — skipping failed_items.xlsx")
        return

    rows = [{
        "Entity Type":        e.get("entity_type", ""),
        "ID":                 e.get("source_id", ""),
        "Reason for Failure": e.get("error", ""),
        "Timestamp":          e.get("timestamp", ""),
    } for e in errors]

    df = pd.DataFrame(rows)
    file_path = os.path.join(MAPPING_DIR, "failed_items.xlsx")
    df.to_excel(file_path, index=False)
    print(f"   ✔ Failed items Excel exported: {file_path}  ({len(rows)} rows)")

# =============================================================================
# TRANSFER SUMMARY REPORT
# =============================================================================

def export_transfer_summary(grouped_data):
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  EXPORT: Transfer Summary Report")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    issues = load_issues_meta()
    if not issues:
        return

    sprint_lookup = {}
    for item in issues:
        sid              = str(item.get("source_issue_id", ""))
        source_sprint_id = str(item.get("source_sprint_id", "")).split(".")[0]
        if source_sprint_id not in ("", "None", "nan"):
            sprint_info = id_mapping["sprints"].get(source_sprint_id)
            if sprint_info:
                sprint_lookup[sid] = sprint_info.get("jira_sprint_name", "")

    deps_data   = load_json("dependencies.json")
    deps_lookup = defaultdict(int)
    for dep in deps_data:
        deps_lookup[str(dep.get("source_issue_id", ""))] += 1

    rows = []

    for item in issues:
        source_id  = str(item.get("source_issue_id", ""))
        issue_type = item.get("jira_issue_type", "Task")
        item       = hydrate_issue(item)  # one small file at a time, for the title only
        title      = item.get("payload", {}).get("fields", {}).get("summary", "")[:80]

        mapped = id_mapping["issues"].get(source_id)
        posted = mapped is not None

        jira_key   = mapped.get("jira_key", "")   if mapped else ""
        created_at = mapped.get("created_at", "") if mapped else ""

        gdata         = grouped_data.get(source_id, {}) if grouped_data else {}
        comment_count = len(gdata.get("comments", []))
        link_count    = len(gdata.get("links", []))
        attach_count  = len(gdata.get("attachments", []))

        dep_count   = deps_lookup.get(source_id, 0)
        sprint_name = sprint_lookup.get(source_id, "")

        comment_posted = sum(
            1 for c in gdata.get("comments", [])
            if str(c.get("source_comment_id", "")) in id_mapping.get("comments", {})
        )

        rows.append({
            "Source ID":         source_id,
            "Jira Key":          jira_key,
            "Issue Type":        issue_type,
            "Title":             title,
            "Posted":            "✔" if posted else "✖",
            "Created At":        created_at,
            "Sprint":            sprint_name,
            "Comments (total)":  comment_count,
            "Comments (posted)": comment_posted,
            "Issue Attachments": attach_count,
            "Links":             link_count,
            "Dependencies":      dep_count,
        })

    df = pd.DataFrame(rows)

    output_path = os.path.join(MAPPING_DIR, "transfer_summary.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Transfer Summary", index=False)
        ws = writer.sheets["Transfer Summary"]

        for col_idx, col in enumerate(df.columns, 1):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 4
            from openpyxl.utils import get_column_letter
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len, 60)

        from openpyxl.styles import PatternFill, Font, Alignment
        header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        for cell in ws[1]:
            cell.fill      = header_fill
            cell.font      = header_font
            cell.alignment = Alignment(horizontal="center")

        green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        red   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        posted_col_idx = df.columns.get_loc("Posted") + 1
        from openpyxl.utils import get_column_letter as gcl
        for row_idx in range(2, len(df) + 2):
            cell = ws[f"{gcl(posted_col_idx)}{row_idx}"]
            cell.fill = green if cell.value == "✔" else red

    print(f"   ✔ Transfer summary exported: {output_path}")
    print(
        f"    {len(df)} issues | "
        f"{df['Comments (total)'].sum()} comments | "
        f"{df['Links'].sum()} links | "
        f"{df['Dependencies'].sum()} dependencies"
    )


# =============================================================================
# SAVE REPORT
# =============================================================================

def save_report():
    report["completed_at"] = datetime.now().isoformat()
    report_path = os.path.join(MAPPING_DIR, "load_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"\n   ✔ Load report saved: {report_path}")


# =============================================================================
# MAIN
# =============================================================================

def validate_all_accounts():
    """
    NEW: hits /rest/api/3/myself once per configured service account BEFORE
    the real run starts, so a bad email/token on account #4 (say) is caught
    immediately instead of surfacing as a wall of mysterious 401s halfway
    through a wave. Prints which display name/email each account resolves
    to, so you can visually confirm account N really is the account you
    think it is.
    """
    print("\n  Validating all configured Jira service accounts...")
    # jira_credentials.print_pool_summary()

    accounts = jira_credentials.get_account_pool()
    all_ok = True
    for i, acc in enumerate(accounts, 1):
        resp = requests.get(
            f"{JIRA_URL}/rest/api/3/myself",
            headers=JIRA_HEADERS,
            auth=(acc["email"], acc["token"]),
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"    [{i}] ✔ {acc['email']}  → {data.get('displayName')} ({data.get('emailAddress')})")
        else:
            all_ok = False
            print(f"    [{i}] ✖ {acc['email']}  → HTTP {resp.status_code}: {resp.text[:150]}")

    if not all_ok:
        print("\n   ✖ One or more service accounts failed to authenticate. Fix your .env before continuing.")
    return all_ok


def main():
    report["started_at"] = datetime.now().isoformat()
    start_rate_monitor()   # requirement #3 — cumulative API-hit logging every N seconds

    print("=" * 70)
    print("  BULK PARALLEL JIRA MIGRATION (HYBRID MODE)")
    print("=" * 70)
    print(f"  Target:            {JIRA_URL}")
    print(f"  MAX_ISSUE_WORKERS: {MAX_ISSUE_WORKERS}")
    print(f"  Resume:            {RESUME_MODE}")
    print(f"  Time:              {report['started_at']}")
    print("=" * 70)

    if not validate_all_accounts():
        stop_rate_monitor()
        return 1

    pool_n = jira_credentials.pool_size()
    if MAX_ISSUE_WORKERS > pool_n:
        print(f"\n  ⚠ MAX_ISSUE_WORKERS ({MAX_ISSUE_WORKERS}) is higher than the number "
              f"of service accounts configured ({pool_n}).")
        print(f"    Some worker threads will share an account's Jira rate limit. "
              f"Add more JIRA_EMAIL_n/JIRA_API_TOKEN_n pairs to give every worker its own.")

    # ── PHASE 1: Versions + Sprints ───────────────────────────────────────────
    load_versions()
    export_versions_to_excel()
    load_sprints()
    export_sprints_to_excel()

    # ── PHASE 2: Issues (bulk+parallel) + inline links only ───────────────────
    load_issues_wave_parallel()

    # ── PHASE 3: Sprint + version assignment ──────────────────────────────────
    assign_issues_to_sprints()
    assign_versions_to_issues()

    # ── PHASE 4A: Upload ALL attachments FIRST ────────────────────────────────
    upload_attachments_phase()

    # ── PHASE 4B: Rebuild comment payloads with attachment links ──────────────
    rebuild_comment_payloads_with_attachments()

    # ── PHASE 4C: Post ALL comments (with attachment links) ───────────────────
    # Wrapped so that if a pass ends with comments still failing (mostly
    # persistent 429s), it cools down and automatically retries — only the
    # still-missing comments, thanks to RESUME_MODE — until everything is
    # posted or MAX_PHASE_PASSES is reached.
    run_with_cooldown_retries(load_all_comments_with_attachments, "Comments")

    # ── PHASE 4D: Patch issue descriptions with attachment links ──────────────
    # (issues were created in Phase 2, before their inline attachments existed
    #  in Jira — this rebuilds & PUTs the description now that they do)
    rebuild_issue_payloads_with_attachments()
    update_posted_issues_with_attachments()

    # ── PHASE 5: Remaining links ──────────────────────────────────────────────
    load_remaining_links()

    # ── PHASE 6: Dependencies ─────────────────────────────────────────────────
    load_dependencies()

    # ── PHASE 7: Validate ─────────────────────────────────────────────────────
    validate_migration()

    # ── Save reports ──────────────────────────────────────────────────────────
    save_report()
    save_error_log()
    export_failed_items_excel()
    grouped_data = load_grouped_related_data()
    export_transfer_summary(grouped_data)

    # ── Final summary ──────────────────────────────────────────────────────────
    start    = datetime.fromisoformat(report["started_at"])
    end      = datetime.fromisoformat(report["completed_at"])
    duration = end - start

    print(f"\n{'=' * 70}")
    print("  MIGRATION COMPLETE")
    print(f"{'=' * 70}")
    print(f"\n  Results:")
    print(f"    Versions:          {report['versions']['success']:6,d} ✔ | {report['versions']['failed']} ✖")
    print(f"    Sprints:           {report['sprints']['success']:6,d} ✔ | {report['sprints']['failed']} ✖")
    print(f"    Issues:            {report['issues']['success']:6,d} ✔ | {report['issues']['failed']} ✖")
    print(f"    Comments:          {report['comments']['success']:6,d} ✔ | {report['comments']['failed']} ✖")
    print(f"    Links:             {report['links']['success']:6,d} ✔ | {report['links']['failed']} ✖")
    print(f"    Dependencies:      {report['dependencies']['success']:6,d} ✔ | {report['dependencies']['failed']} ✖")
    print(f"    Batches:           {report['batches']['success']:6,d} / {report['batches']['total']}")
    print(f"    Waves:             {report['waves']['completed']:6d} / {report['waves']['total']}")
    print(f"    Parent Mapping Missing issues: {report['parent_mapping_missing']['count']:6,d}")
    print(f"\n  Performance:")
    print(f"    Time elapsed:      {duration}")

    if report["issues"]["success"] > 0:
        avg  = duration.total_seconds() / report["issues"]["success"]
        tput = report["issues"]["success"] / (duration.total_seconds() / 60)
        print(f"    Avg/issue:         {avg:.2f}s")
        print(f"    Throughput:        {tput:.0f} issues/min")

    print(f"\n  Output files:")
    print(f"    ID Mapping:        {MAPPING_FILE}")
    print(f"    Failed Singles:    {FAILED_SINGLES_FILE}")
    print(f"    Load Report:       {os.path.join(MAPPING_DIR, 'load_report.json')}")
    print(f"    Error Log:         {os.path.join(MAPPING_DIR, 'errors.json')}")
    print(f"    Transfer Summary:  {os.path.join(MAPPING_DIR, 'transfer_summary.xlsx')}")
    print(f"{'=' * 70}")
    if report["parent_mapping_missing"]["issue_ids"]:
        print(f"\nIssues not posted due to missing of parent id")
        for iid in report["parent_mapping_missing"]["issue_ids"]:
            print(f"    {iid}")

    stop_rate_monitor()
    return 0 if report["issues"]["failed"] == 0 else 1


# =============================================================================
# CLI ENTRY POINTS
#
#   python bulk_load_to_jira.py                     → full migration
#   python bulk_load_to_jira.py --retry-failed       → retry failed_singles.json only
#   python bulk_load_to_jira.py --patch-comments     → patch comments with attachments
#   python bulk_load_to_jira.py --patch-descriptions → patch issue descriptions with attachments
# =============================================================================

if __name__ == "__main__":
    from run_logger import RunLogger

    with RunLogger("bulk_load_to_jira") as run_log:
        if len(sys.argv) > 1 and sys.argv[1] == "--patch-comments":
            start_rate_monitor()
            print("=" * 70)
            print("  PATCH MODE: Updating existing comments with attachment links")
            print("=" * 70)
            rebuild_comment_payloads_with_attachments()
            run_with_cooldown_retries(update_posted_comments_with_attachments, "Comment patch")
            stop_rate_monitor()

        elif len(sys.argv) > 1 and sys.argv[1] == "--patch-descriptions":
            start_rate_monitor()
            print("=" * 70)
            print("  PATCH MODE: Updating existing issue descriptions with attachment links")
            print("=" * 70)
            rebuild_issue_payloads_with_attachments()
            update_posted_issues_with_attachments()
            stop_rate_monitor()

        elif len(sys.argv) > 1 and sys.argv[1] == "--retry-failed":
            # FIX 2 — Dedicated retry-failed CLI mode
            start_rate_monitor()
            run_retry_failed_mode()
            stop_rate_monitor()

        elif len(sys.argv) > 1 and sys.argv[1] == "--from-4a":
            # Resume tail: skip versions/sprints/issues/sprint-version-assignment
            # entirely. Starts at Phase 4A and runs through Phase 7 + reports.
            # Use this when versions/sprints/issues are already fully loaded
            # and you don't want to re-scan known-failed tickets.
            report["started_at"] = datetime.now().isoformat()
            start_rate_monitor()

            print("=" * 70)
            print("  RESUME MODE: Starting from Phase 4A (Attachments)")
            print("=" * 70)

            # ── PHASE 4A: Upload ALL attachments FIRST ────────────────────
            upload_attachments_phase()

            # ── PHASE 4B: Rebuild comment payloads with attachment links ──
            rebuild_comment_payloads_with_attachments()

            # ── PHASE 4C: Post ALL comments (with attachment links) ───────
            run_with_cooldown_retries(load_all_comments_with_attachments, "Comments")

            # ── PHASE 4D: Patch issue descriptions with attachment links ──
            rebuild_issue_payloads_with_attachments()
            update_posted_issues_with_attachments()

            # ── PHASE 5: Remaining links ────────────────────────────────────
            load_remaining_links()

            # ── PHASE 6: Dependencies ───────────────────────────────────────
            load_dependencies()

            # ── PHASE 7: Validate ───────────────────────────────────────────
            validate_migration()

            # ── Save reports ─────────────────────────────────────────────
            save_report()
            save_error_log()

            grouped_data = load_grouped_related_data()
            export_transfer_summary(grouped_data)

            stop_rate_monitor()
            print("\n" + "=" * 70)
            print("  RESUME RUN COMPLETE (4A → 7)")
            print("=" * 70)

        else:
            exit_code = main()
            run_log.set_exit_code(exit_code)
            exit(exit_code)