"""
Standalone reconciliation script for cross-project dependencies.

Run this AFTER all 65 projects have been extracted + loaded (issues, comments,
links, and dependencies all done via bulk_load_to_jira.py). It picks up every
dependency that failed to resolve during each project's own Phase 6 run
(saved to data/mapping/pending_dependencies.json), and retries them against
the single COMBINED id_mapping.json that now spans all migrated projects.

Safe to re-run any number of times — already-posted links are skipped via the
same id_mapping["links"] check used in the main loader, and dependencies still
unresolved are simply written back to pending_dependencies.json for the next
attempt (e.g. after more projects get migrated later).

Usage:
    python retry_dependencies.py
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Reuse everything already built and tested in the main loader: jira_get,
# jira_post, resolve_missing_target, id_mapping, mapping_lock, save_mapping,
# log_error, MAPPING_DIR, RESUME_MODE flags etc.
from bulk_load_to_jira import (
    id_mapping,
    mapping_lock,
    save_mapping,
    resolve_missing_target,
    jira_post,
    MAPPING_DIR,
)

load_dotenv()

PENDING_DEPS_FILE = os.path.join(MAPPING_DIR, "pending_dependencies.json")
RETRY_LOG_FILE     = os.path.join(MAPPING_DIR, "dependency_retry_log.json")
RETRY_ERRORS_FILE  = os.path.join(MAPPING_DIR, "dependency_retry_errors.json")


def load_pending():
    if not os.path.exists(PENDING_DEPS_FILE):
        print(f"  No pending dependencies file found at {PENDING_DEPS_FILE}")
        return []
    with open(PENDING_DEPS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_pending(remaining):
    with open(PENDING_DEPS_FILE, "w", encoding="utf-8") as f:
        json.dump(remaining, f, indent=2)


def log_retry_error(link_key, dep, error_msg):
    """
    Dedicated, CUMULATIVE error log for this script only — appends across
    runs instead of overwriting, and is kept completely separate from the
    main bulk_load_to_jira.py errors.json so nothing gets clobbered.
    Use this for real Jira POST failures (both issues resolved, but the
    link itself was rejected) — distinct from "still not found" entries,
    which live in pending_dependencies.json instead.
    """
    existing = []
    if os.path.exists(RETRY_ERRORS_FILE):
        with open(RETRY_ERRORS_FILE, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []

    existing.append({
        "link_key":  link_key,
        "source_id": dep.get("source_issue_id"),
        "linked_id": dep.get("linked_source_id"),
        "linked_issue_key": dep.get("linked_issue_key"),
        "relationship": dep.get("relationship"),
        "error":     str(error_msg)[:500],
        "timestamp": datetime.now().isoformat(),
    })

    with open(RETRY_ERRORS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)


def post_dependency(source_key, linked_key, relationship):
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
    return jira_post("api/3/issueLink", payload)


def retry_pending_dependencies():
    pending = load_pending()
    if not pending:
        print("  Nothing to retry.")
        return

    print(f"  Loaded {len(pending)} pending dependencies")

    still_pending  = []
    resolved_now   = []
    resolved_count = 0
    failed_count   = 0
    skipped_count  = 0

    for dep in pending:
        source_id    = str(dep.get("source_issue_id", ""))
        linked_id    = str(dep.get("linked_source_id", ""))
        relationship = dep.get("relationship", "blocks")
        link_key     = f"{source_id}_{linked_id}_{relationship}"

        # already resolved by a previous retry pass or the main loader
        if link_key in id_mapping.get("links", {}):
            skipped_count += 1
            continue

        source_info = id_mapping["issues"].get(source_id)
        linked_info = id_mapping["issues"].get(linked_id)

        # try local mapping first (covers issues migrated in later projects
        # since this dependency was first marked pending), then live lookup
        if not source_info:
            resolve_missing_target(source_id)
            source_info = id_mapping["issues"].get(source_id)

        if not linked_info:
            resolve_missing_target(linked_id)
            linked_info = id_mapping["issues"].get(linked_id)

        if not source_info or not linked_info:
            dep["last_checked"] = datetime.now().isoformat()
            dep["status"]       = "unresolved"
            missing_side        = "source" if not source_info else "linked"
            missing_id          = source_id if not source_info else linked_id
            dep["last_error"]   = f"{missing_side.title()} {missing_id} still not in id_mapping / not found in Jira"
            still_pending.append(dep)
            failed_count += 1
            continue

        source_key = source_info["jira_key"]
        linked_key = linked_info["jira_key"]

        data, status = post_dependency(source_key, linked_key, relationship)

        if status == 201:
            with mapping_lock:
                id_mapping["links"][link_key] = {
                    "source_key":   source_key,
                    "linked_key":   linked_key,
                    "relationship": relationship,
                    "resolved_via": "retry_dependencies",
                }
                save_mapping()
            resolved_count += 1
            resolved_now.append(link_key)
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            log_retry_error(link_key, dep, error_msg)
            dep["last_checked"] = datetime.now().isoformat()
            dep["status"]       = "post_failed"
            dep["last_error"]   = error_msg
            still_pending.append(dep)
            failed_count += 1

    save_pending(still_pending)

    unresolved_count  = sum(1 for d in still_pending if d.get("status") == "unresolved")
    post_failed_count = sum(1 for d in still_pending if d.get("status") == "post_failed")

    with open(RETRY_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "run_at":            datetime.now().isoformat(),
            "resolved":          resolved_count,
            "resolved_links":    resolved_now,
            "still_unresolved":  unresolved_count,   # mapping still missing — likely unmigrated project
            "still_post_failed": post_failed_count,  # mapping found, Jira rejected the link — needs investigation
            "skipped":           skipped_count,
        }, f, indent=2)

    print("\n  ─────────────────────────────")
    print(f"  Resolved this run   : {resolved_count}")
    print(f"  Still unresolved    : {unresolved_count}   (mapping missing — check pending_dependencies.json)")
    print(f"  Post failed         : {post_failed_count}   (see dependency_retry_errors.json)")
    print(f"  Already done        : {skipped_count}")
    print(f"  Remaining saved to  : {PENDING_DEPS_FILE}")
    print(f"  Retry errors saved  : {RETRY_ERRORS_FILE}")
    print(f"  Run summary saved   : {RETRY_LOG_FILE}")
    print("  ─────────────────────────────")


if __name__ == "__main__":
    print("=" * 60)
    print("  DEPENDENCY RECONCILIATION — retrying pending cross-project links")
    print("=" * 60)
    retry_pending_dependencies()
    print("\nDone.\n")