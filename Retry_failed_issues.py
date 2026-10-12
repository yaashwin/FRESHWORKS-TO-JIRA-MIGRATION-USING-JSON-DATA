"""
=====================================================================
  RETRY FAILED — Re-extract or Re-load only failed items
=====================================================================
Checks all log/error files and retries only what failed.

Handles:
  - Failed issue extractions  (comments, details, links, deps, docs, resources)
  - Failed Jira loads         (issues, comments, links, dependencies)

Run:
    python Retry_failed_issues.py

Options:
    python3 Retry_failed_issues.py --phase extraction
    python3 Retry_failed_issues.py --phase load
    python3 Retry_failed_issues.py --phase both        (default)
    python3 Retry_failed_issues.py --dry-run           (show what would retry, don't run)
=====================================================================
"""

import os
import json
import subprocess
import time
import argparse
from datetime import datetime
import sys
from dotenv import load_dotenv
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
# =============================================================
# CONFIGURATION
# =============================================================
load_dotenv()
# RAW_DATA_DIR      = "seperate/data/raw"
# MAPPING_DIR       = "data/mapping"
# PAYLOAD_DIR       = "data/payload"
# TRANSFORM_DIR     = "data/transformed"

# ID_MAPPING_FILE   = os.path.join(MAPPING_DIR, "id_mapping.json")
# LOGS_DIR          = os.path.join(RAW_DATA_DIR, "logs")
PROJECT_KEY   = os.getenv("project_key")
DATA_ROOT     = f"{PROJECT_KEY}_data" if PROJECT_KEY else "data"

RAW_DATA_DIR  = f"test_refactored_shared_pool/{DATA_ROOT}/raw"
MAPPING_DIR   = f"{DATA_ROOT}/mapping"
PAYLOAD_DIR   = f"{DATA_ROOT}/payload"
TRANSFORM_DIR = f"{DATA_ROOT}/transformed"
LOGS_DIR      = os.path.join(RAW_DATA_DIR, "logs")
ID_MAPPING_FILE = os.path.join(MAPPING_DIR, "id_mapping.json")
# Extraction log files  tells us what failed during extraction
EXTRACTION_LOG_FILES = {
    "comments":             os.path.join(LOGS_DIR, "comments_extraction.json"),
    "issue_details":        os.path.join(LOGS_DIR, "issue_details_extraction.json"),
    "issue_links":          os.path.join(LOGS_DIR, "issue_links_extraction.json"),
    "dependencies":         os.path.join(LOGS_DIR, "dependencies_extraction.json"),
    "documents":            os.path.join(LOGS_DIR, "documents_extraction.json"),
    "integrated_resources": os.path.join(LOGS_DIR, "integrated_resources_extraction.json"),
}

# Extraction scripts  maps entity to script
EXTRACTION_SCRIPTS = {
    "comments":             "extract_comments.py",
    "issue_details":        "extract_issue_details.py",
    "issue_links":          "extract_issue_links.py",
    "dependencies":         "extract_dependencies.py",
    "documents":            "extract_documents.py",
    "integrated_resources": "extract_integrated_resources.py",
}

# Load failure files  written by load_to_jira.py
LOAD_FAILED_FILES = {
    "issues":   os.path.join(MAPPING_DIR, "failed_issues.json"),
    "comments": os.path.join(MAPPING_DIR, "failed_comments.json"),
    "links":    os.path.join(MAPPING_DIR, "links_pending_retry.json"),
}

# =============================================================
# HELPERS
# =============================================================

def log(message, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {message}")


def separator(title=""):
    line = "=" * 65
    if title:
        print(f"\n{line}\n  {title}\n{line}")
    else:
        print(line)


def load_json_file(filepath):
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log(f"Could not read {filepath}: {e}", level="WARN")
        return None


def save_json_file(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def run_script_with_env(script, env_vars=None, label=""):
    """Run a script as subprocess with optional env vars. Returns success bool."""
    if not os.path.exists(script):
        log(f"Script not found: {script}", level="ERROR")
        return False

    env = os.environ.copy()
    if env_vars:
        env.update(env_vars)

    log(f"Running: {script} {label}")
    start = time.time()

    process = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env
    )

    for line in process.stdout:
        line = line.rstrip()
        if line:
            print(f"  {line}")

    return_code = process.wait()
    duration = time.time() - start

    if return_code == 0:
        log(f"Completed in {duration:.1f}s")
        return True
    else:
        log(f"Failed (exit code {return_code}) after {duration:.1f}s", level="ERROR")
        return False


# =============================================================
# PHASE 1 — CHECK EXTRACTION FAILURES
# =============================================================

def check_extraction_failures():
    """
    Read all extraction log files and find which issue IDs failed.
    Returns dict: { entity_type: [issue_id, ...] }
    """
    failures = {}

    for entity, log_file in EXTRACTION_LOG_FILES.items():
        data = load_json_file(log_file)
        if not data:
            continue

        errors = data.get("errors", [])
        failed_ids = []

        for err in errors:
            issue_id = err.get("issue_id")
            if issue_id:
                failed_ids.append(str(issue_id))

        if failed_ids:
            failures[entity] = failed_ids
            log(f"[EXTRACTION] {entity}: {len(failed_ids)} failed issue(s)")

    return failures


def build_retry_issues_file(issue_ids, output_path):
    """
    Build a temporary issues list JSON file for only the failed issue IDs.
    This file is passed to extraction scripts via EXTRACTION_ISSUES_FILE env var.
    """
    # We need to find the full issue objects from the combined issues list
    # Try to find from any existing issues list file
    issues_list_path = None

    # Look for combined issues list
    import glob
    patterns = [
        os.path.join(RAW_DATA_DIR, "issues", "*_issues_list.json"),
        os.path.join(RAW_DATA_DIR, "issues", "*_issues_list_page_*.json"),
    ]

    all_issues = []
    found_files = []
    for pattern in patterns:
        found_files.extend(glob.glob(pattern))

    for fpath in found_files:
        data = load_json_file(fpath)
        if data:
            issues = data.get("issues", [])
            all_issues.extend(issues)

    if not all_issues:
        log("Could not find any issues list file to build retry list", level="ERROR")
        return False

    # Filter to only the failed IDs
    id_set = set(str(i) for i in issue_ids)
    retry_issues = [
        issue for issue in all_issues
        if str(issue.get("id")) in id_set
    ]

    if not retry_issues:
        log(f"None of the failed IDs found in issues list. IDs: {issue_ids[:5]}...", level="WARN")
        return False

    save_json_file(
        {"issues": retry_issues, "total_count": len(retry_issues)},
        output_path
    )
    log(f"Retry issues file built: {len(retry_issues)} issues  {output_path}")
    return True


def retry_extraction_failures(failures, dry_run=False):
    """
    For each entity type with failures, re-run the extraction script
    with only the failed issue IDs.
    """
    if not failures:
        log("No extraction failures found.")
        return

    separator("RETRYING EXTRACTION FAILURES")

    retry_file = os.path.join(RAW_DATA_DIR, "issues", "_retry_issues_list.json")
    results = {}

    for entity, failed_ids in failures.items():
        log(f"\n--- {entity.upper()}: {len(failed_ids)} failed issues ---")

        if dry_run:
            log(f"  [DRY RUN] Would retry: {failed_ids[:10]}")
            continue

        script = EXTRACTION_SCRIPTS.get(entity)
        if not script:
            log(f"No script found for entity: {entity}", level="WARN")
            continue

        # Build retry issues file for this batch
        # First, disable SKIP_EXISTING for these specific IDs by deleting their output files
        deleted = delete_failed_extraction_outputs(entity, failed_ids)
        log(f"  Cleared {deleted} existing (failed) output files")

        # Build a temp issues list with just the failed IDs
        ok = build_retry_issues_file(failed_ids, retry_file)
        if not ok:
            log(f"  Skipping {entity} — could not build retry list", level="WARN")
            continue

        # Run the extraction script with the retry file
        success = run_script_with_env(
            script,
            env_vars={"EXTRACTION_ISSUES_FILE": retry_file},
            label=f"[retry {len(failed_ids)} {entity}]"
        )
        results[entity] = success

    # Clean up temp retry file
    if os.path.exists(retry_file):
        os.remove(retry_file)

    return results


def delete_failed_extraction_outputs(entity, issue_ids):
    """
    Delete existing output files for failed issue IDs so SKIP_EXISTING
    doesn't skip them on retry.
    """
    import glob

    # Map entity  output directory and file pattern
    entity_paths = {
        "comments":             (os.path.join(RAW_DATA_DIR, "comments", "*"), "comments_{id}.json"),
        "issue_details":        (os.path.join(RAW_DATA_DIR, "issues", "*"), "issue_{id}.json"),
        "issue_links":          (os.path.join(RAW_DATA_DIR, "links", "*"), "links_{id}.json"),
        "dependencies":         (os.path.join(RAW_DATA_DIR, "dependencies", "*"), "deps_{id}.json"),
        "documents":            (os.path.join(RAW_DATA_DIR, "documents", "*"), "docs_{id}.json"),
        "integrated_resources": (os.path.join(RAW_DATA_DIR, "integrated_resources"), "ir_{id}.json"),
    }

    if entity not in entity_paths:
        return 0

    dir_pattern, file_pattern = entity_paths[entity]
    deleted = 0

    for issue_id in issue_ids:
        filename = file_pattern.replace("{id}", str(issue_id))

        # Search in all project subdirs
        search_paths = glob.glob(os.path.join(dir_pattern, filename))

        # Also search directly in the dir (for integrated_resources which has no subdir)
        direct_path = os.path.join(dir_pattern.rstrip("*"), filename)
        if os.path.exists(direct_path):
            search_paths.append(direct_path)

        for fpath in search_paths:
            if os.path.exists(fpath):
                os.remove(fpath)
                deleted += 1

    return deleted


# =============================================================
# PHASE 5 — CHECK LOAD FAILURES
# =============================================================

def check_load_failures():
    """
    Check failed_*.json files written by load_to_jira.py.
    Returns dict: { entity_type: [items...] }
    """
    failures = {}

    for entity, failed_file in LOAD_FAILED_FILES.items():
        data = load_json_file(failed_file)
        if data and len(data) > 0:
            failures[entity] = data
            log(f"[LOAD] {entity}: {len(data)} failed item(s)")

    return failures


def retry_load_failures(failures, dry_run=False):
    """
    For load failures, patch the failed files back and re-run load_to_jira.py.
    load_to_jira.py already has RESUME_MODE which skips already-loaded items,
    so re-running it will only process the ones not yet in id_mapping.
    """
    if not failures:
        log("No load failures found.")
        return

    separator("RETRYING LOAD FAILURES")

    for entity, items in failures.items():
        log(f"\n--- {entity.upper()}: {len(items)} failed items ---")

        if dry_run:
            for item in items[:3]:
                source_id = item.get("source_issue_id") or item.get("source_comment_id") or "unknown"
                log(f"  [DRY RUN] Would retry: {source_id}")
            if len(items) > 3:
                log(f"  [DRY RUN] ... and {len(items) - 3} more")
            continue

        # Re-inject failed items back into the payload file so load_to_jira picks them up
        reinject_failed_to_payload(entity, items)

    if dry_run:
        return

    # Re-run load_to_jira.py — RESUME_MODE will skip already loaded,
    # and pick up the newly re-injected failed items
    log("\nRe-running load_to_jira.py (RESUME_MODE will skip already-loaded items)...")
    success = run_script_with_env("load_to_jira.py", label="[retry failed loads]")

    if success:
        log("Load retry completed.")
        # Clean up failed files after successful retry
        for entity in failures:
            failed_file = LOAD_FAILED_FILES[entity]
            if os.path.exists(failed_file):
                # Check if still has items
                remaining = load_json_file(failed_file)
                if not remaining:
                    os.remove(failed_file)
                    log(f"Cleared {failed_file} (all retried successfully)")
    else:
        log("Load retry had errors. Check logs.", level="ERROR")


def reinject_failed_to_payload(entity, failed_items):
    """
    Merge failed items back into the payload JSON so load_to_jira.py
    can attempt them again. Avoids duplicates by checking source IDs.
    """
    payload_files = {
        "issues":   os.path.join(PAYLOAD_DIR, "issues.json"),
        "comments": os.path.join(PAYLOAD_DIR, "comments.json"),
        "links":    os.path.join(PAYLOAD_DIR, "links.json"),
    }

    payload_file = payload_files.get(entity)
    if not payload_file or not os.path.exists(payload_file):
        log(f"Payload file not found for {entity}: {payload_file}", level="WARN")
        return

    existing = load_json_file(payload_file) or []

    # Build set of existing IDs to avoid duplicates
    id_field = {
        "issues":   "source_issue_id",
        "comments": "source_comment_id",
        "links":    "source_issue_id",
    }.get(entity, "source_issue_id")

    existing_ids = {str(item.get(id_field, "")) for item in existing}

    added = 0
    for item in failed_items:
        item_id = str(item.get(id_field, ""))
        if item_id not in existing_ids:
            existing.append(item)
            existing_ids.add(item_id)
            added += 1

    if added > 0:
        save_json_file(existing, payload_file)
        log(f"  Re-injected {added} failed {entity} back into {payload_file}")
    else:
        log(f"  All failed {entity} already in payload file (will be retried by RESUME_MODE)")


# =============================================================
# ALSO CHECK: ID MAPPING vs PAYLOAD (find truly missing issues)
# =============================================================

def find_missing_from_jira():
    """
    Compare payload/issues.json vs id_mapping.json to find
    issues that were never loaded to Jira at all (no entry in id_mapping).
    These are issues that didn't even fail — they were just never attempted
    or silently skipped.
    """
    separator("CHECKING FOR MISSING ISSUES IN JIRA")

    payload_issues = load_json_file(os.path.join(PAYLOAD_DIR, "issues.json")) or []
    id_mapping     = load_json_file(ID_MAPPING_FILE) or {}
    mapped_issues  = id_mapping.get("issues", {})

    payload_ids = {str(item.get("source_issue_id", "")) for item in payload_issues}
    mapped_ids  = set(mapped_issues.keys())

    missing = payload_ids - mapped_ids
    missing.discard("")  # remove empty string

    if not missing:
        log("All payload issues are present in id_mapping. Nothing missing.")
        return []

    log(f"Found {len(missing)} issue(s) in payload but NOT in id_mapping (never loaded):", level="WARN")
    for mid in sorted(list(missing))[:20]:
        log(f"  Source ID: {mid}", level="WARN")
    if len(missing) > 20:
        log(f"  ... and {len(missing) - 20} more", level="WARN")

    # Save to a file for reference
    missing_file = os.path.join(MAPPING_DIR, "missing_from_jira.json")
    save_json_file({"missing_issue_ids": sorted(list(missing)), "count": len(missing)}, missing_file)
    log(f"Saved to: {missing_file}")

    return list(missing)


def retry_missing_from_jira(missing_ids, dry_run=False):
    """
    Re-run load_to_jira.py for issues that are in payload but not in id_mapping.
    Since RESUME_MODE skips mapped ones, re-running will pick up the missing ones.
    """
    if not missing_ids:
        return

    separator("RETRYING MISSING ISSUES")
    log(f"{len(missing_ids)} issues will be retried")

    if dry_run:
        log("[DRY RUN] Would re-run load_to_jira.py to pick up missing issues")
        return

    log("Re-running load_to_jira.py...")
    run_script_with_env("load_to_jira.py", label="[retry missing issues]")


# =============================================================
# FULL STATUS REPORT
# =============================================================

def print_status_report():
    """Print a full status of what's failed/missing without running anything."""
    separator("FAILURE STATUS REPORT")

    # Extraction failures
    print("\n[EXTRACTION FAILURES]")
    extraction_failures = check_extraction_failures()
    if not extraction_failures:
        print("  No extraction failures found.")
    else:
        total_failed = sum(len(v) for v in extraction_failures.values())
        print(f"  Total failed issue extractions: {total_failed}")
        for entity, ids in extraction_failures.items():
            print(f"  {entity:<25}: {len(ids)} failed")

    # Load failures
    print("\n[LOAD FAILURES]")
    load_failures = check_load_failures()
    if not load_failures:
        print("  No load failures found.")
    else:
        for entity, items in load_failures.items():
            print(f"  {entity:<25}: {len(items)} failed")

    # Missing from Jira
    print("\n[MISSING FROM JIRA]")
    find_missing_from_jira()

    print()


# =============================================================
# ARGUMENT PARSING
# =============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Retry failed extractions and/or Jira loads"
    )
    parser.add_argument(
        "--phase",
        choices=["extraction", "load", "both"],
        default="both",
        help="Which phase to retry (default: both)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be retried without actually running"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show failure status report only, then exit"
    )
    return parser.parse_args()


# =============================================================
# MAIN
# =============================================================

def main():
    args = parse_args()

    separator("RETRY FAILED — SMART RESUME")
    log(f"Started  : {datetime.now().isoformat()}")
    log(f"Phase    : {args.phase}")
    log(f"Dry run  : {args.dry_run}")

    # Status only
    if args.status:
        print_status_report()
        return

    # ── EXTRACTION RETRY ──────────────────────────────────────
    if args.phase in ("extraction", "both"):
        separator("PHASE 1 — EXTRACTION RETRY CHECK")
        extraction_failures = check_extraction_failures()

        if extraction_failures:
            retry_extraction_failures(extraction_failures, dry_run=args.dry_run)
        else:
            log("No extraction failures. Skipping extraction retry.")

    # ── LOAD RETRY ────────────────────────────────────────────
    if args.phase in ("load", "both"):
        separator("PHASE 5 — LOAD RETRY CHECK")

        # Check explicit failed files
        load_failures = check_load_failures()

        # Also check for issues silently missing from Jira
        missing_ids = find_missing_from_jira()

        if load_failures or missing_ids:
            if load_failures:
                retry_load_failures(load_failures, dry_run=args.dry_run)
            if missing_ids:
                retry_missing_from_jira(missing_ids, dry_run=args.dry_run)
        else:
            log("No load failures or missing issues. Everything looks good!")

    # ── FINAL STATUS ─────────────────────────────────────────
    separator("DONE")
    log("Retry complete. Run --status to check remaining failures.")
    log("If issues still fail, check logs/ and data/mapping/ for details.")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(1)