"""
============================================================
STEP 5: POST TO JIRA + STORE MAPPING IDs + VALIDATION
============================================================
Reads payload JSON files from data/payload/
POSTs data to Jira Cloud REST API
Stores source_id  jira_key mapping in data/mapping/
Runs validation after each entity type

Load Order:
  1. Versions    POST /rest/api/3/version
  2. Sprints     POST /rest/agile/1.0/sprint
  3. Issues      POST /rest/api/3/issue
  4. Comments    POST /rest/api/3/issue/{key}/comment
  5. Links       POST /rest/api/3/issueLink
  6. History     PUT /rest/api/3/issue/{key} (optional, slow)

Input:  data/payload/*.json
Output: data/mapping/id_mapping.json
        data/mapping/load_report.json
        data/mapping/failed_*.json
        data/mapping/validation_report.json
============================================================
"""

import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# =============================================================
# CONFIGURATION
# =============================================================
load_dotenv()

JIRA_URL = os.getenv("JIRA_URL")            # https://your-domain.atlassian.net
JIRA_EMAIL = os.getenv("JIRA_EMAIL")        # your-email@company.com
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")  # your API token

PROJECT_KEY_ENV = os.getenv("project_key")
DATA_ROOT       = f"{PROJECT_KEY_ENV}_data" if PROJECT_KEY_ENV else "data"

PAYLOAD_DIR = f"{DATA_ROOT}/payload"
MAPPING_DIR = f"{DATA_ROOT}/mapping"
os.makedirs(MAPPING_DIR, exist_ok=True)

# Rate limiting
RATE_LIMIT_DELAY = 0.5     # seconds between requests
RETRY_DELAY = 5            # seconds before retry on 429/503
MAX_RETRIES = 3            # max retries per request

# Feature flags
LOAD_VERSIONS = True       # create versions/releases
LOAD_SPRINTS = True        # create sprints (needs Jira Software + board)
LOAD_ISSUES = True         # create issues
LOAD_COMMENTS = True       # add comments to issues
LOAD_LINKS = True          # create issue links
LOAD_HISTORY = False       #   SLOW - recreate change history (not recommended)

# Resume from where we left off (skips already created issues)
RESUME_MODE = True

JIRA_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

JIRA_AUTH = (JIRA_EMAIL, JIRA_API_TOKEN)


# =============================================================
# ID MAPPING STORE
# source_issue_id  {jira_key, jira_id, created_at}
# =============================================================

id_mapping = {
    "issues": {},     # source_id   {jira_key, jira_id}
    "versions": {},   # source_id   {jira_version_id, jira_version_name}
    "sprints": {},    # source_id   {jira_sprint_id, jira_sprint_name}
    "comments": {},   # source_comment_id  jira_comment_id
    "links": {},      # source_link  jira_link_id
}

# Load existing mapping (for RESUME_MODE)
MAPPING_FILE = os.path.join(MAPPING_DIR, "id_mapping.json")

if RESUME_MODE and os.path.exists(MAPPING_FILE):
    with open(MAPPING_FILE, "r") as f:
        existing = json.load(f)
    for key in id_mapping:
        id_mapping[key].update(existing.get(key, {}))
    print(f" Loaded existing mapping: {sum(len(v) for v in id_mapping.values())} entries")


# =============================================================
# LOAD REPORT
# =============================================================
report = {
    "started_at": None,
    "completed_at": None,
    "versions": {"success": 0, "failed": 0, "skipped": 0},
    "sprints": {"success": 0, "failed": 0, "skipped": 0},
    "issues": {"success": 0, "failed": 0, "skipped": 0},
    "comments": {"success": 0, "failed": 0, "skipped": 0},
    "links": {"success": 0, "failed": 0, "skipped": 0},
    "history": {"success": 0, "failed": 0, "skipped": 0},
    "errors": [],
}

# Track failed items for retry
failed_issues = []
failed_comments = []
failed_links = []


# =============================================================
# HELPER FUNCTIONS
# =============================================================

def load_json(filename):
    """Load payload JSON file."""
    filepath = os.path.join(PAYLOAD_DIR, filename)
    if not os.path.exists(filepath):
        print(f"      {filename} not found, skipping")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_mapping():
    """Save ID mapping to disk after each successful creation."""
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(id_mapping, f, indent=4)


def export_sprints_to_excel():
    rows = []

    for source_id, data in id_mapping["sprints"].items():
        rows.append({
            "Sprint Name": data.get("jira_sprint_name"),
            "Source Sprint ID": source_id,
            "Jira Sprint ID": data.get("jira_sprint_id"),
            "Jira Board ID": data.get("jira_board_id"),
            "Jira Board Name": data.get("jira_board_name"),
        })

    if not rows:
        print("No sprint data to export")
        return

    df = pd.DataFrame(rows)
    file_path = os.path.join(MAPPING_DIR, "sprints_mapping.xlsx")
    df.to_excel(file_path, index=False)

    print(f" Sprint Excel exported  {file_path}")


def export_versions_to_excel():
    rows = []

    for source_id, data in id_mapping["versions"].items():
        rows.append({
            "Version Name": data.get("jira_version_name"),
            "Source Release ID": source_id,
            "Jira Version ID": data.get("jira_version_id"),
        })

    if not rows:
        print("No version data to export")
        return

    df = pd.DataFrame(rows)
    file_path = os.path.join(MAPPING_DIR, "versions_mapping.xlsx")
    df.to_excel(file_path, index=False)

    print(f" Versions Excel exported  {file_path}")

def save_failed(entity_type, items):
    """Save failed items for manual retry."""
    if not items:
        return
    filepath = os.path.join(MAPPING_DIR, f"failed_{entity_type}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=4)
    print(f"      {len(items)} failed items saved to failed_{entity_type}.json")


def jira_request(method, endpoint, payload=None, retries=MAX_RETRIES):
    """
    Generic Jira API request with retry logic.
    
    Args:
        method: "GET", "POST", "PUT"
        endpoint: API endpoint (e.g., "api/3/issue")
        payload: JSON payload for POST/PUT
        retries: Max retry attempts
    
    Returns:
        (response_json, status_code) or (None, status_code) on failure
    """
    url = f"{JIRA_URL}/rest/{endpoint.lstrip('/')}"

    for attempt in range(retries):
        try:
            if method == "GET":
                response = requests.get(
                    url,
                    headers=JIRA_HEADERS,
                    auth=JIRA_AUTH,
                    timeout=30
                )
            elif method == "POST":
                response = requests.post(
                    url,
                    json=payload,
                    headers=JIRA_HEADERS,
                    auth=JIRA_AUTH,
                    timeout=30
                )
            elif method == "PUT":
                response = requests.put(
                    url,
                    json=payload,
                    headers=JIRA_HEADERS,
                    auth=JIRA_AUTH,
                    timeout=30
                )
            else:
                raise ValueError(f"Unsupported method: {method}")

            # Rate limited
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RETRY_DELAY))
                print(f"     Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            # Server error — retry
            if response.status_code in (503, 502, 500) and attempt < retries - 1:
                print(f"     Server error {response.status_code}. Retry {attempt + 1}/{retries}...")
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            time.sleep(RATE_LIMIT_DELAY)
            return response.json() if response.content else {}, response.status_code

        except requests.exceptions.ConnectionError:
            print(f"     Connection error. Retry {attempt + 1}/{retries}...")
            time.sleep(RETRY_DELAY)
        except requests.exceptions.Timeout:
            print(f"     Timeout. Retry {attempt + 1}/{retries}...")
            time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"     Unexpected error: {e}")
            return None, 0

    return None, 0


def jira_get(endpoint):
    """GET request to Jira API."""
    return jira_request("GET", endpoint)


def jira_post(endpoint, payload):
    """POST request to Jira API."""
    return jira_request("POST", endpoint, payload)


def jira_put(endpoint, payload):
    """PUT request to Jira API."""
    return jira_request("PUT", endpoint, payload)


def log_error(entity_type, source_id, error_msg, payload=None):
    """Log an error to the report."""
    error = {
        "entity_type": entity_type,
        "source_id": str(source_id),
        "error": error_msg,
        "timestamp": datetime.now().isoformat(),
    }
    if payload:
        error["payload_summary"] = str(payload)[:500]

    report["errors"].append(error)
    print(f"     [{entity_type}] {source_id}: {error_msg[:100]}")


# =============================================================
# STEP 1: LOAD VERSIONS
# =============================================================

def load_versions():
    """
    POST /rest/api/3/version
    Creates releases/versions in Jira project.
    Must be done BEFORE issues (if using fixVersions field).
    """
    if not LOAD_VERSIONS:
        print("\n Versions: SKIPPED (LOAD_VERSIONS=False)")
        return

    print("\n Loading versions...")
    versions = load_json("versions.json")

    if not versions:
        return

    for i, item in enumerate(versions):
        source_id = str(item.get("source_release_id", ""))

        # Skip if already mapped
        if RESUME_MODE and source_id in id_mapping["versions"]:
            report["versions"]["skipped"] += 1
            continue

        payload = item.get("payload", {})
        summary = payload.get("name", "Unknown")

        data, status = jira_post("api/3/version", payload)

        if status == 201:
            jira_version_id = data.get("id")
            jira_version_name = data.get("name")
            id_mapping["versions"][source_id] = {
                "jira_version_id": jira_version_id,
                "jira_version_name": jira_version_name,
                "source_release_id": source_id,
            }
            save_mapping()
            report["versions"]["success"] += 1
            print(f"     [{i+1}/{len(versions)}] Version: {jira_version_name}")

        else:
            error_msg = str(data.get("errors", data.get("errorMessages", data)))
            log_error("version", source_id, error_msg, payload)
            report["versions"]["failed"] += 1

    print(f"\n   Versions: {report['versions']['success']} success | "
          f"{report['versions']['failed']} failed | "
          f"{report['versions']['skipped']} skipped")


# =============================================================
# STEP 2: LOAD SPRINTS
# =============================================================

def load_sprints():
    """
    POST /rest/agile/1.0/sprint
    Creates sprints in Jira board.
    Requires Jira Software + board configured.
    """
    if not LOAD_SPRINTS:
        print("\n Sprints: SKIPPED (LOAD_SPRINTS=False)")
        return

    print("\n Loading sprints...")
    sprints = load_json("sprints.json")

    if not sprints:
        return

    for i, item in enumerate(sprints):
        source_id = str(item.get("source_sprint_id", ""))

        if RESUME_MODE and source_id in id_mapping["sprints"]:
            report["sprints"]["skipped"] += 1
            continue

        payload = item.get("payload", {})

        if not payload.get("originBoardId"):
            log_error("sprint", source_id, "Missing originBoardId", payload)
            report["sprints"]["failed"] += 1
            continue

        data, status = jira_post("agile/1.0/sprint", payload)

        if status == 201:
            jira_sprint_id = data.get("id")
            board_id = payload.get("originBoardId")
            print(f"   Fetching board for sprint  Board ID: {board_id}")

            board_name = ""
            board_data, board_status = jira_get(f"agile/1.0/board/{board_id}")

            print(f"   Board API Status: {board_status}")
            if board_status == 200:
               board_name = board_data.get("name", "")
            else:
              print(f"    Could not fetch board name for {board_id}")
  
    #  SAVE EVERYTHING
            id_mapping["sprints"][source_id] = {
               "jira_sprint_id": jira_sprint_id,
               "jira_sprint_name": data.get("name"),
               "jira_board_id": board_id,
               "jira_board_name": board_name
            }

            save_mapping()
            report["sprints"]["success"] += 1
            print(f"     [{i+1}/{len(sprints)}] Sprint: {data.get('name')}")

        else:
            error_msg = str(data.get("errors", data.get("errorMessages", data)))
            log_error("sprint", source_id, error_msg, payload)
            report["sprints"]["failed"] += 1

    print(f"\n   Sprints: {report['sprints']['success']} success | "
          f"{report['sprints']['failed']} failed | "
          f"{report['sprints']['skipped']} skipped")


# =============================================================
# STEP 3: LOAD ISSUES
# =============================================================

# def load_issues():
#     """
#     POST /rest/api/3/issue
#     Creates issues in Jira.

#     Key behaviors:
#     - Epics first (migration_sequence sorting handles this)
#     - Parent epic ID is set after epic is created
#     - Sub-tasks require parent issue key
#     - Tracks source_id  jira_key mapping for all issues
#     """
#     if not LOAD_ISSUES:
#         print("\n Issues: SKIPPED (LOAD_ISSUES=False)")
#         return

#     print("\n Loading issues...")
#     issues = load_json("issues.json")

#     if not issues:
#         return

#     total = len(issues)

#     for i, item in enumerate(issues):
#         source_id = str(item.get("source_issue_id", ""))
#         source_parent_id = str(item.get("source_parent_id", ""))
#         source_epic_id = str(item.get("source_epic_id", ""))
#         issue_type = item.get("jira_issue_type", "Task")
#         payload = item.get("payload", {}).copy()
#         fields = payload.get("fields", {}).copy()

#         # Skip if already created
#         if RESUME_MODE and source_id in id_mapping["issues"]:
#             report["issues"]["skipped"] += 1
#             continue

#         title = fields.get("summary", "Unknown")[:40]
#         print(f"  [{i+1}/{total}] {issue_type}: {title}...", end=" ")

#         # ── Resolve parent for Sub-tasks ──
#         if issue_type == "Sub-task":
#             parent_key = None

#             if source_parent_id and source_parent_id != "None":
#                 parent_info = id_mapping["issues"].get(source_parent_id)
#                 if parent_info:
#                     parent_key = parent_info["jira_key"]

#             if not parent_key:
#                 print(f"  (parent {source_parent_id} not created yet)")
#                 log_error("issue", source_id, f"Parent {source_parent_id} not in id_mapping")
#                 failed_issues.append(item)
#                 report["issues"]["failed"] += 1
#                 continue

#             fields["parent"] = {"key": parent_key}

#         # ── Resolve Epic Link (for Stories under Epics) ──
#         if issue_type in ("Story", "Task", "Bug") and source_epic_id and source_epic_id != "None":
#             epic_info = id_mapping["issues"].get(source_epic_id)
#             if epic_info:
#                 epic_key = epic_info["jira_key"]
#                 # Jira Cloud: use "parent" for epic link
#                 fields["parent"] = {"key": epic_key}

#         payload["fields"] = fields
#         data, status = jira_post("api/3/issue", payload)

#         if status == 201:
#             jira_key = data.get("key")
#             jira_id = data.get("id")

#             id_mapping["issues"][source_id] = {
#                 "jira_key": jira_key,
#                 "jira_id": jira_id,
#                 "source_id": source_id,
#                 "issue_type": issue_type,
#                 "title": fields.get("summary", ""),
#                 "created_at": datetime.now().isoformat(),
#             }
#             save_mapping()
#             report["issues"]["success"] += 1
#             print(f" {jira_key}")

#         else:
#             errors = data.get("errors", {}) if data else {}
#             error_msgs = data.get("errorMessages", []) if data else []
#             error_str = f"{errors} {error_msgs}".strip()
#             log_error("issue", source_id, error_str, payload)
#             failed_issues.append(item)
#             report["issues"]["failed"] += 1
#             print(f"")

#     # Retry failed sub-tasks (their parents may have been created later)
#     if failed_issues:
#         print(f"\n   Retrying {len(failed_issues)} failed issues...")
#         retry_failed_issues()

#     print(f"\n   Issues: {report['issues']['success']} success | "
#           f"{report['issues']['failed']} failed | "
#           f"{report['issues']['skipped']} skipped")

def apply_status_transition(jira_key: str, target_status: str) -> bool:
    """
    Transition a Jira issue to the target status name.
 
    Jira does not allow setting status directly in fields on creation —
    it must be done via a POST to /rest/api/3/issue/{key}/transitions.
 
    Steps:
      1. GET available transitions for the issue
      2. Find the transition whose "to" status name matches target_status
      3. POST that transition ID
 
    Returns True on success, False on failure.
    """
    if not target_status or target_status in ("", "nan", "None"):
        return True   # nothing to do
 
    # ── Step 1: Get available transitions ────────────────────
    data, status = jira_get(f"api/3/issue/{jira_key}/transitions")
    if status != 200 or not data:
        print(f"      Could not fetch transitions for {jira_key}")
        return False
 
    transitions = data.get("transitions", [])
 
    # ── Step 2: Find matching transition ─────────────────────
    transition_id = None
    available = []
 
    for trans in transitions:
        to_status_name = trans.get("to", {}).get("name", "")
        available.append(to_status_name)
        if to_status_name.lower() == target_status.lower():
            transition_id = trans.get("id")
            break
 
    if not transition_id:
        print(f"      No transition to '{target_status}' for {jira_key}. "
              f"Available: {available}")
        return False
 
    # ── Step 3: Execute transition ────────────────────────────
    payload = {"transition": {"id": transition_id}}
    _, t_status = jira_post(f"api/3/issue/{jira_key}/transitions", payload)
 
    if t_status == 204:
        return True
    else:
        print(f"      Transition failed for {jira_key}  '{target_status}'")
        return False
 

# def load_issues():
#     """
#     POST /rest/api/3/issue
#     Creates issues in Jira with proper parent-child linking.
#     """
#     if not LOAD_ISSUES:
#         print("\n Issues: SKIPPED (LOAD_ISSUES=False)")
#         return

#     print("\n Loading issues...")
#     issues = load_json("issues.json")

#     if not issues:
#         return

#     total = len(issues)

#     for i, item in enumerate(issues):
#         source_id = str(item.get("source_issue_id", ""))
#         # source_parent_id = str(item.get("source_parent_id", ""))
#         # source_epic_id = str(item.get("source_epic_id", ""))
#         source_parent_id = str(item.get("source_parent_id", "")).split(".")[0]
#         source_epic_id   = str(item.get("source_epic_id",   "")).split(".")[0]
#         issue_type = item.get("jira_issue_type", "Task")
#         payload = item.get("payload", {}).copy()
#         fields = payload.get("fields", {}).copy()

#         # Skip if already created
#         if RESUME_MODE and source_id in id_mapping["issues"]:
#             report["issues"]["skipped"] += 1
#             continue

#         title = fields.get("summary", "Unknown")[:40]
#         print(f"  [{i+1}/{total}] {issue_type}: {title}...", end=" ")

#         # ═══════════════════════════════════════════════════════════
#         # PARENT-CHILD LINKING
#         # ═══════════════════════════════════════════════════════════
        
#         # ── Case 1: Sub-task linking to parent ──
#         if issue_type == "Sub-task":
#             parent_key = None

#             if source_parent_id and source_parent_id not in ("None", "", "nan"):
#                 parent_info = id_mapping["issues"].get(source_parent_id)
#                 if parent_info:
#                     parent_key = parent_info["jira_key"]

#             if not parent_key:
#                 print(f"  (parent {source_parent_id} not found)")
#                 log_error("issue", source_id, f"Parent {source_parent_id} not in id_mapping")
#                 failed_issues.append(item)
#                 report["issues"]["failed"] += 1
#                 continue

#             fields["parent"] = {"key": parent_key}
#             print(f"(parent: {parent_key}) ", end="")

#         # ── Case 2: Regular issue with parent (not Sub-task) ──
#         elif source_parent_id and source_parent_id not in ("None", "", "nan", source_epic_id):
#             # Only link if parent is NOT the same as epic (avoid duplicate linking)
#             parent_info = id_mapping["issues"].get(source_parent_id)
#             if parent_info:
#                 parent_key = parent_info["jira_key"]
#                 fields["parent"] = {"key": parent_key}
#                 print(f"(parent: {parent_key}) ", end="")

#         # ── Case 3: Story/Task/Bug linking to Epic ──
#         elif source_epic_id and source_epic_id not in ("None", "", "nan"):
#             epic_info = id_mapping["issues"].get(source_epic_id)
#             if epic_info:
#                 epic_key = epic_info["jira_key"]
#                 fields["parent"] = {"key": epic_key}
#                 print(f"(epic: {epic_key}) ", end="")

#         # ═══════════════════════════════════════════════════════════
#         # CREATE ISSUE
#         # ═══════════════════════════════════════════════════════════
        
#         payload["fields"] = fields
#         data, status = jira_post("api/3/issue", payload)

#         if status == 201:
#             jira_key = data.get("key")
#             jira_id = data.get("id")

#             id_mapping["issues"][source_id] = {
#                 "jira_key": jira_key,
#                 "jira_id": jira_id,
#                 "source_id": source_id,
#                 "issue_type": issue_type,
#                 "title": fields.get("summary", ""),
#                 "created_at": datetime.now().isoformat(),
#             }
#             save_mapping()
#             report["issues"]["success"] += 1
#             print(f" {jira_key}")

#         else:
#             errors = data.get("errors", {}) if data else {}
#             error_msgs = data.get("errorMessages", []) if data else []
#             error_str = f"{errors} {error_msgs}".strip()
#             log_error("issue", source_id, error_str, payload)
#             failed_issues.append(item)
#             report["issues"]["failed"] += 1
#             print(f"")

#     # ═══════════════════════════════════════════════════════════
#     # RETRY FAILED ISSUES
#     # ═══════════════════════════════════════════════════════════
    
#     if failed_issues:
#         print(f"\n   Retrying {len(failed_issues)} failed issues...")
#         retry_failed_issues()

#     print(f"\n   Issues: {report['issues']['success']} success | "
#           f"{report['issues']['failed']} failed | "
#           f"{report['issues']['skipped']} skipped")

def load_issues():
    """
    POST /rest/api/3/issue
    Creates issues in Jira then applies status transition.
    """
    if not LOAD_ISSUES:
        print("\n Issues: SKIPPED (LOAD_ISSUES=False)")
        return
 
    print("\n Loading issues...")
    issues = load_json("issues.json")
 
    if not issues:
        return
 
    total = len(issues)
 
    for i, item in enumerate(issues):
        source_id        = str(item.get("source_issue_id", ""))
        source_parent_id = str(item.get("source_parent_id", "")).split(".")[0]  # ← fix .0
        source_epic_id   = str(item.get("source_epic_id",   "")).split(".")[0]  # ← fix .0
        issue_type       = item.get("jira_issue_type", "Task")
        jira_status      = item.get("jira_status", "")                           # ← NEW
        payload          = item.get("payload", {}).copy()
        fields           = payload.get("fields", {}).copy()
 
        # Skip if already created
        if RESUME_MODE and source_id in id_mapping["issues"]:
            report["issues"]["skipped"] += 1
            continue
 
        title = fields.get("summary", "Unknown")[:40]
        print(f"  [{i+1}/{total}] {issue_type}: {title}...", end=" ")
 
        # ── Resolve parent / epic ─────────────────────────────
        if issue_type == "Sub-task":
            parent_key = None
            if source_parent_id and source_parent_id not in ("None", "", "nan"):
                parent_info = id_mapping["issues"].get(source_parent_id)
                if parent_info:
                    parent_key = parent_info["jira_key"]
            if not parent_key:
                print(f"  (parent {source_parent_id} not found)")
                log_error("issue", source_id, f"Parent {source_parent_id} not in id_mapping")
                failed_issues.append(item)
                report["issues"]["failed"] += 1
                continue
            fields["parent"] = {"key": parent_key}
 
        elif source_parent_id and source_parent_id not in ("None", "", "nan", source_epic_id):
            parent_info = id_mapping["issues"].get(source_parent_id)
            if parent_info:
                fields["parent"] = {"key": parent_info["jira_key"]}
 
        elif source_epic_id and source_epic_id not in ("None", "", "nan"):
            epic_info = id_mapping["issues"].get(source_epic_id)
            if epic_info:
                fields["parent"] = {"key": epic_info["jira_key"]}
 
        # ── Create issue ──────────────────────────────────────
        payload["fields"] = fields
        data, status = jira_post("api/3/issue", payload)
 
        if status == 201:
            jira_key = data.get("key")
            jira_id  = data.get("id")
 
            id_mapping["issues"][source_id] = {
                "jira_key":   jira_key,
                "jira_id":    jira_id,
                "source_id":  source_id,
                "issue_type": issue_type,
                "title":      fields.get("summary", ""),
                "created_at": datetime.now().isoformat(),
            }
            save_mapping()
            report["issues"]["success"] += 1
            print(f"  {jira_key}", end=" ")
 
            # ── Apply status transition ───────────────────────  ← NEW
            if jira_status:
                ok = apply_status_transition(jira_key, jira_status)
                if ok:
                    print(f"  [{jira_status}]")
                else:
                    print(f"  [status '{jira_status}' not applied]")
            else:
                print()
 
        else:
            errors     = data.get("errors", {}) if data else {}
            error_msgs = data.get("errorMessages", []) if data else []
            error_str  = f"{errors} {error_msgs}".strip()
            log_error("issue", source_id, error_str, payload)
            failed_issues.append(item)
            report["issues"]["failed"] += 1
            print("  FAILED")
 
    if failed_issues:
        print(f"\n  Retrying {len(failed_issues)} failed issues...")
        retry_failed_issues()
 
    print(f"\n  Issues: {report['issues']['success']} success | "
          f"{report['issues']['failed']} failed | "
          f"{report['issues']['skipped']} skipped")
 

def retry_failed_issues():
    """Retry failed issues once (handles ordering issues)."""
    still_failed = []

    for item in failed_issues:
        source_id = str(item.get("source_issue_id", ""))
        # source_parent_id = str(item.get("source_parent_id", ""))
        source_parent_id = str(item.get("source_parent_id", "")).split(".")[0]
        issue_type = item.get("jira_issue_type", "Task")
        payload = item.get("payload", {}).copy()
        fields = payload.get("fields", {}).copy()

        if source_id in id_mapping["issues"]:
            continue  # already created in first pass

        # Resolve parent again
        if issue_type == "Sub-task" and source_parent_id != "None":
            parent_info = id_mapping["issues"].get(source_parent_id)
            if parent_info:
                fields["parent"] = {"key": parent_info["jira_key"]}
            else:
                still_failed.append(item)
                continue

        payload["fields"] = fields
        data, status = jira_post("api/3/issue", payload)

        if status == 201:
            jira_key = data.get("key")
            id_mapping["issues"][source_id] = {
                "jira_key": jira_key,
                "jira_id": data.get("id"),
                "source_id": source_id,
            }
            save_mapping()
            report["issues"]["success"] += 1
            report["issues"]["failed"] -= 1
            print(f"     Retry: {jira_key}")
        else:
            still_failed.append(item)

    if still_failed:
        save_failed("issues", still_failed)
# def load_dependencies():
#     """
#     POST /rest/api/3/issueLink for all predecessor/successor dependencies.
 
#     Rules:
#       1. Both source and related issue must exist in id_mapping
#       2. If either is missing  export to unresolved_dependencies.xlsx
#       3. Always runs AFTER all issues are created (called last in main)
    
#     Jira link format:
#       Blocks:        inwardIssue=blocker, outwardIssue=blocked
#       Is blocked by: inwardIssue=blocked, outwardIssue=blocker
#     """
#     print("\n Loading dependencies (predecessor/successor links)...")
 
#     deps = load_json("dependencies.json")
#     if not deps:
#         print("      No dependencies.json found")
#         return
 
#     # Load issues reference CSV for names
#     issues_ref = {}
#     issues_csv = os.path.join("data", "transformed", "issues.csv")
#     if os.path.exists(issues_csv):
#         df_issues = pd.read_csv(issues_csv, encoding="utf-8-sig", low_memory=False)
#         df_issues = df_issues.where(pd.notna(df_issues), None)
#         for _, row in df_issues.iterrows():
#             iid = str(row.get("source_issue_id") or row.get("issue_id") or "")
#             if iid:
#                 issues_ref[iid] = {
#                     "title":      str(row.get("title") or ""),
#                     "issue_key":  str(row.get("issue_key") or ""),
#                     "project_key": str(row.get("project_key") or ""),
#                 }
 
#     success_count  = 0
#     failed_count   = 0
#     skipped_count  = 0
#     unresolved     = []   # links where one side is not in id_mapping
 
#     for dep in deps:
#         source_id  = str(dep.get("source_issue_id", ""))
#         related_id = str(dep.get("related_source_id", ""))
#         link_type  = dep.get("jira_link_type", "Blocks")
#         dep_type   = dep.get("dep_type", "successor")
 
#         link_key = f"{source_id}_{related_id}_{dep_type}"
 
#         # Skip if already loaded
#         if RESUME_MODE and link_key in id_mapping.get("links", {}):
#             skipped_count += 1
#             continue
 
#         # Resolve source issue
#         source_info = id_mapping["issues"].get(source_id)
 
#         # Resolve related issue
#         related_info = id_mapping["issues"].get(related_id)
 
#         # ── If either side is missing  unresolved export ──────
#         if not source_info or not related_info:
#             source_ref  = issues_ref.get(source_id, {})
#             related_ref = issues_ref.get(related_id, {})
 
#             unresolved.append({
#                 "source_issue_id":      source_id,
#                 "source_issue_key":     source_ref.get("issue_key", ""),
#                 "source_title":         source_ref.get("title", ""),
#                 "source_project":       source_ref.get("project_key", ""),
#                 "source_jira_key":      source_info.get("jira_key", "") if source_info else "NOT CREATED",
#                 "dependency_type":      dep_type,
#                 "jira_link_type":       link_type,
#                 "related_source_id":    related_id,
#                 "related_issue_key":    related_ref.get("issue_key", ""),
#                 "related_title":        related_ref.get("title", ""),
#                 "related_project":      related_ref.get("project_key", ""),
#                 "related_jira_key":     related_info.get("jira_key", "") if related_info else "NOT CREATED",
#                 "reason":               "Source not in id_mapping" if not source_info
#                                         else "Related issue not in id_mapping (different project or not migrated)",
#             })
#             failed_count += 1
#             continue
 
#         source_key  = source_info["jira_key"]
#         related_key = related_info["jira_key"]
 
#         # ── Build Jira link payload ────────────────────────────
#         # Blocks:        source BLOCKS related
#         #   inward  = "is blocked by"  related
#         #   outward = "blocks"         source
#         # We use inwardIssue=source, outwardIssue=related for "Blocks"
#         if dep_type == "successor":
#             # source issue BLOCKS related issue
#             payload = {
#                 "type": {"name": "Blocks"},
#                 "inwardIssue":  {"key": source_key},   # source blocks
#                 "outwardIssue": {"key": related_key},  # related is blocked
#             }
#         else:
#             # source issue IS BLOCKED BY related issue
#             payload = {
#                 "type": {"name": "Blocks"},
#                 "inwardIssue":  {"key": related_key},  # related blocks
#                 "outwardIssue": {"key": source_key},   # source is blocked
#             }
 
#         data, status = jira_post("api/3/issueLink", payload)
 
#         if status == 201:
#             if "links" not in id_mapping:
#                 id_mapping["links"] = {}
#             id_mapping["links"][link_key] = {
#                 "source_key":  source_key,
#                 "related_key": related_key,
#                 "dep_type":    dep_type,
#                 "link_type":   link_type,
#             }
#             save_mapping()
#             success_count += 1
#             print(f"      {source_key} {'BLOCKS' if dep_type == 'successor' else 'BLOCKED BY'} {related_key}")
 
#         else:
#             error_msg = str(data.get("errors", data.get("errorMessages", data)) if data else "Unknown")
#             print(f"      FAILED: {source_key}  {related_key}: {error_msg}")
#             failed_count += 1
 
#             source_ref  = issues_ref.get(source_id, {})
#             related_ref = issues_ref.get(related_id, {})
#             unresolved.append({
#                 "source_issue_id":   source_id,
#                 "source_issue_key":  source_ref.get("issue_key", ""),
#                 "source_title":      source_ref.get("title", ""),
#                 "source_project":    source_ref.get("project_key", ""),
#                 "source_jira_key":   source_key,
#                 "dependency_type":   dep_type,
#                 "jira_link_type":    link_type,
#                 "related_source_id": related_id,
#                 "related_issue_key": related_ref.get("issue_key", ""),
#                 "related_title":     related_ref.get("title", ""),
#                 "related_project":   related_ref.get("project_key", ""),
#                 "related_jira_key":  related_key,
#                 "reason":            f"API error: {error_msg}",
#             })
 
#     # ── Export unresolved to Excel ─────────────────────────────
#     if unresolved:
#         export_unresolved_dependencies(unresolved)
 
#     print(f"\n   Dependencies: {success_count} success | "
#           f"{failed_count} failed/unresolved | "
#           f"{skipped_count} skipped")
 
def load_dependencies():
    """
    POST /rest/api/3/issueLink for all predecessor/successor dependencies.
 
    Uses 'Blocks' link type with direction controlled by inward/outward:
 
        relationship = "blocks":
            inwardIssue  = this issue  (it blocks)
            outwardIssue = linked issue (it is blocked)
 
        relationship = "is_blocked_by":
            inwardIssue  = linked issue (it blocks)
            outwardIssue = this issue   (it is blocked)
 
    Cross-project detection:
        If linked_issue_project_id does not match any project in id_mapping,
        the link is added to unresolved for manual linking in Jira.
 
    Always runs LAST — all issues must exist before linking.
    """
    print("\n Loading dependencies (predecessor/successor links)...")
 
    deps = load_json("dependencies.json")
    if not deps:
        print("      No dependencies.json found")
        return
 
    # Load issues CSV for name/key reference in unresolved export
    issues_ref = {}
    issues_csv = os.path.join(DATA_ROOT, "transformed", "issues.csv")
    if os.path.exists(issues_csv):
        df_issues = pd.read_csv(issues_csv, encoding="utf-8-sig", low_memory=False)
        df_issues = df_issues.where(pd.notna(df_issues), None)
        for _, row in df_issues.iterrows():
            iid = str(row.get("source_issue_id") or row.get("issue_id") or "")
            if iid:
                issues_ref[iid] = {
                    "title":       str(row.get("title") or ""),
                    "issue_key":   str(row.get("issue_key") or ""),
                    "project_key": str(row.get("project_key") or ""),
                }
 
    success_count = 0
    failed_count  = 0
    skipped_count = 0
    unresolved    = []
 
    for dep in deps:
        source_id        = str(dep.get("source_issue_id", ""))
        linked_id        = str(dep.get("linked_source_id", ""))
        relationship     = dep.get("relationship", "blocks")
        linked_issue_key = dep.get("linked_issue_key", "")
        linked_proj_id   = str(dep.get("linked_issue_project_id", ""))
 
        link_key = f"{source_id}_{linked_id}_{relationship}"
 
        # Skip if already loaded
        if RESUME_MODE and link_key in id_mapping.get("links", {}):
            skipped_count += 1
            continue
 
        # Resolve this issue
        source_info = id_mapping["issues"].get(source_id)
 
        # Resolve linked issue
        linked_info = id_mapping["issues"].get(linked_id)
 
        # ── If either side is missing  unresolved ────────────
        if not source_info or not linked_info:
            source_ref  = issues_ref.get(source_id, {})
            linked_ref  = issues_ref.get(linked_id, {})
 
            # Determine reason
            if not source_info:
                reason = f"Source issue {source_id} not in id_mapping (not migrated)"
            else:
                # Check if it's a cross-project issue
                # Cross-project = linked_issue_key has a different prefix
                source_project = source_ref.get("project_key", "")
                linked_prefix  = linked_issue_key.split("-")[0] if "-" in linked_issue_key else ""
                if linked_prefix and linked_prefix != source_project:
                    reason = f"Cross-project link: linked issue is in project '{linked_prefix}' (not migrated in this run)"
                else:
                    reason = f"Linked issue {linked_id} (key: {linked_issue_key}) not in id_mapping"
 
            unresolved.append({
                "source_issue_id":         source_id,
                "source_issue_key":        source_ref.get("issue_key", ""),
                "source_title":            source_ref.get("title", ""),
                "source_project":          source_ref.get("project_key", ""),
                "source_jira_key":         source_info.get("jira_key", "") if source_info else "NOT CREATED",
                "relationship":            relationship,
                "jira_link_type":          "Blocks",
                "linked_source_id":        linked_id,
                "linked_issue_key":        linked_issue_key,
                "linked_title":            linked_ref.get("title", ""),
                "linked_project":          linked_ref.get("project_key", ""),
                "linked_jira_key":         linked_info.get("jira_key", "") if linked_info else "NOT CREATED",
                "reason":                  reason,
            })
            failed_count += 1
            continue
 
        source_key = source_info["jira_key"]
        linked_key = linked_info["jira_key"]
 
        # ── Build Jira link payload ────────────────────────────
        #
        # Jira "Blocks" link type semantics:
        #   inwardIssue  = the issue that IS BLOCKING
        #   outwardIssue = the issue that IS BLOCKED
        #
        # relationship = "blocks":
        #   source blocks linked  source=inward, linked=outward
        #
        # relationship = "is_blocked_by":
        #   linked blocks source  linked=inward, source=outward
 
        if relationship == "blocks":
            payload = {
                "type":          {"name": "Blocks"},
                "inwardIssue":   {"key": source_key},  # source BLOCKS
                "outwardIssue":  {"key": linked_key},  # linked IS BLOCKED
            }
        else:
            # is_blocked_by
            payload = {
                "type":          {"name": "Blocks"},
                "inwardIssue":   {"key": linked_key},  # linked BLOCKS
                "outwardIssue":  {"key": source_key},  # source IS BLOCKED
            }
 
        data, status = jira_post("api/3/issueLink", payload)
 
        if status == 201:
            if "links" not in id_mapping:
                id_mapping["links"] = {}
            id_mapping["links"][link_key] = {
                "source_key":  source_key,
                "linked_key":  linked_key,
                "relationship":relationship,
            }
            save_mapping()
            success_count += 1
 
            arrow = "BLOCKS" if relationship == "blocks" else "BLOCKED BY"
            print(f"      {source_key} {arrow} {linked_key}")
 
        else:
            error_msg = str(
                data.get("errors", data.get("errorMessages", data)) if data else "Unknown"
            )
            print(f"      FAILED: {source_key}  {linked_key}: {error_msg}")
            failed_count += 1
 
            source_ref = issues_ref.get(source_id, {})
            linked_ref = issues_ref.get(linked_id, {})
            unresolved.append({
                "source_issue_id":  source_id,
                "source_issue_key": source_ref.get("issue_key", ""),
                "source_title":     source_ref.get("title", ""),
                "source_project":   source_ref.get("project_key", ""),
                "source_jira_key":  source_key,
                "relationship":     relationship,
                "jira_link_type":   "Blocks",
                "linked_source_id": linked_id,
                "linked_issue_key": linked_issue_key,
                "linked_title":     linked_ref.get("title", ""),
                "linked_project":   linked_ref.get("project_key", ""),
                "linked_jira_key":  linked_key,
                "reason":           f"API error: {error_msg}",
            })
 
    # ── Export unresolved ──────────────────────────────────────
    if unresolved:
        export_unresolved_dependencies(unresolved)
 
    print(f"\n   Dependencies: {success_count} success | "
          f"{failed_count} failed/unresolved | "
          f"{skipped_count} skipped")
 


def export_unresolved_dependencies(unresolved: list):
    """
    Export unresolved dependencies to Excel for manual linking.
    
    Saved to: data/mapping/unresolved_dependencies.xlsx
    
    Columns:
        source_issue_id    | source_issue_key | source_title   | source_project
        source_jira_key    | dependency_type  | jira_link_type
        related_source_id  | related_issue_key| related_title  | related_project
        related_jira_key   | reason
    """
    output_path = os.path.join(MAPPING_DIR, "unresolved_dependencies.xlsx")
 
    df = pd.DataFrame(unresolved, columns=[
        "source_issue_id",
        "source_issue_key",
        "source_title",
        "source_project",
        "source_jira_key",
        "dependency_type",
        "jira_link_type",
        "related_source_id",
        "related_issue_key",
        "related_title",
        "related_project",
        "related_jira_key",
        "reason",
    ])
 
    # ── Format and save ────────────────────────────────────────
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Unresolved Dependencies", index=False)
 
        # Auto-width columns
        worksheet = writer.sheets["Unresolved Dependencies"]
        for col_idx, col in enumerate(df.columns, 1):
            max_len = max(
                df[col].astype(str).map(len).max(),
                len(col)
            ) + 4
            from openpyxl.utils import get_column_letter
            worksheet.column_dimensions[get_column_letter(col_idx)].width = min(max_len, 50)
 
        # Header styling
        from openpyxl.styles import PatternFill, Font
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        for cell in worksheet[1]:
            cell.fill = header_fill
            cell.font = header_font
 
    print(f"\n   Unresolved dependencies exported: {output_path}")
    print(f"   {len(df)} links need manual attention in Jira")
    print(f"\n   How to use this file:")
    print(f"     - 'source_jira_key'  = the Jira issue that exists")
    print(f"     - 'related_jira_key' = 'NOT CREATED' means it was not migrated")
    print(f"     - 'related_issue_key'= original source key for cross-project lookup")
    print(f"     - 'dependency_type'  = successor(blocks) / predecessor(blocked by)")

def assign_issues_to_sprints():
    """
    Assign issues to Jira sprints
    """
    print("\n Assigning issues to sprints...")

    issues = load_json("issues.json")

    sprint_issue_map = {}

    # ── Group issues by sprint ──
    for item in issues:
        source_issue_id = str(item.get("source_issue_id"))
        source_sprint_id = str(item.get("source_sprint_id", "")).split(".")[0]

        if source_sprint_id in ("", "None", "nan"):
            continue

        sprint_info = id_mapping["sprints"].get(source_sprint_id)
        if not sprint_info:
            continue

        jira_sprint_id = sprint_info["jira_sprint_id"]

        issue_info = id_mapping["issues"].get(source_issue_id)
        if not issue_info:
            continue

        jira_key = issue_info["jira_key"]

        sprint_issue_map.setdefault(jira_sprint_id, []).append(jira_key)

    # ── Call Jira API ──
    for sprint_id, issue_keys in sprint_issue_map.items():
        print(f"  Sprint {sprint_id}  {len(issue_keys)} issues")

        payload = {
            "issues": issue_keys
        }

        data, status = jira_post(f"agile/1.0/sprint/{sprint_id}/issue", payload)

        if status == 204:
            print(f"     Assigned successfully")
        else:
            print(f"     Failed: {data}")



def assign_versions_to_issues():
    """
    Assign Jira releases (fixVersions) to issues AFTER creation
    Similar to sprint assignment
    """
    print("\n Assigning versions to issues...")

    issues = load_json("issues.json")

    for item in issues:
        source_issue_id = str(item.get("source_issue_id"))
        source_release_id = str(item.get("source_release_id", "")).split(".")[0]

        if source_release_id in ("", "None", "nan"):
            continue

        version_info = id_mapping["versions"].get(source_release_id)
        issue_info = id_mapping["issues"].get(source_issue_id)

        if not version_info:
            print(f"   Version not found for {source_release_id}")
            continue

        if not issue_info:
            continue

        jira_key = issue_info["jira_key"]

        payload = {
            "update": {
                "fixVersions": [
                    {
                        "add": {"id": version_info["jira_version_id"]}
                    }
                ]
            }
        }

        _, status = jira_put(f"api/3/issue/{jira_key}", payload)

        if status == 204:
            print(f"   {jira_key}  Version assigned")
        else:
            print(f"   Failed for {jira_key}")
# =============================================================
# STEP 4: LOAD COMMENTS
# =============================================================

def load_comments():
    """
    POST /rest/api/3/issue/{issueKey}/comment
    Adds comments to created issues.
    Requires issues to be created first (STEP 3).
    """
    if not LOAD_COMMENTS:
        print("\n Comments: SKIPPED (LOAD_COMMENTS=False)")
        return

    print("\n Loading comments...")
    comments = load_json("comments.json")

    if not comments:
        return

    total = len(comments)

    for i, item in enumerate(comments):
        source_issue_id = str(item.get("source_issue_id", ""))
        source_comment_id = str(item.get("source_comment_id", ""))

        # Skip if already loaded
        if RESUME_MODE and source_comment_id in id_mapping["comments"]:
            report["comments"]["skipped"] += 1
            continue

        # Resolve source issue  Jira key
        issue_info = id_mapping["issues"].get(source_issue_id)
        if not issue_info:
            log_error("comment", source_comment_id,
                      f"Parent issue {source_issue_id} not in id_mapping")
            report["comments"]["failed"] += 1
            failed_comments.append(item)
            continue

        jira_key = issue_info["jira_key"]
        payload = item.get("payload", {})

        data, status = jira_post(f"api/3/issue/{jira_key}/comment", payload)

        if status == 201:
            jira_comment_id = data.get("id")
            id_mapping["comments"][source_comment_id] = {
                "jira_comment_id": jira_comment_id,
                "jira_issue_key": jira_key,
            }
            save_mapping()
            report["comments"]["success"] += 1

            if (i + 1) % 20 == 0 or i == total - 1:
                print(f"     [{i+1}/{total}] Comments loaded")

        else:
            error_msg = str(data.get("errors", data.get("errorMessages", data)) if data else "Unknown error")
            log_error("comment", source_comment_id, error_msg)
            report["comments"]["failed"] += 1
            failed_comments.append(item)

    if failed_comments:
        save_failed("comments", failed_comments)

    print(f"\n   Comments: {report['comments']['success']} success | "
          f"{report['comments']['failed']} failed | "
          f"{report['comments']['skipped']} skipped")


# =============================================================
# STEP 5: LOAD ISSUE LINKS
# =============================================================

def load_links():
    """
    POST /rest/api/3/issueLink
    Creates relationships between issues.
    Requires BOTH source and target issues to exist in Jira.
    """
    if not LOAD_LINKS:
        print("\n Links: SKIPPED (LOAD_LINKS=False)")
        return

    print("\n Loading issue links...")
    links = load_json("links.json")

    if not links:
        return

    total = len(links)

    for i, item in enumerate(links):
        source_issue_id = str(item.get("source_issue_id", ""))
        target_issue_id = str(item.get("target_issue_id", ""))
        link_id = f"{source_issue_id}_{target_issue_id}"

        # Skip if already loaded
        if RESUME_MODE and link_id in id_mapping["links"]:
            report["links"]["skipped"] += 1
            continue

        # Resolve source issue
        source_info = id_mapping["issues"].get(source_issue_id)
        if not source_info:
            log_error("link", link_id, f"Source issue {source_issue_id} not in id_mapping")
            report["links"]["failed"] += 1
            continue

        # Resolve target issue
        target_info = id_mapping["issues"].get(target_issue_id)
        if not target_info:
            log_error("link", link_id, f"Target issue {target_issue_id} not in id_mapping")
            report["links"]["failed"] += 1
            failed_links.append(item)
            continue

        source_key = source_info["jira_key"]
        target_key = target_info["jira_key"]
        link_type = item.get("jira_link_type", "Relates")

        payload = {
            "type": {"name": link_type},
            "inwardIssue": {"key": source_key},
            "outwardIssue": {"key": target_key}
        }

        data, status = jira_post("api/3/issueLink", payload)

        if status == 201:
            id_mapping["links"][link_id] = {
                "jira_source_key": source_key,
                "jira_target_key": target_key,
                "link_type": link_type,
            }
            save_mapping()
            report["links"]["success"] += 1

            if (i + 1) % 10 == 0 or i == total - 1:
                print(f"     [{i+1}/{total}] Links created")

        else:
            error_msg = str(data.get("errors", data.get("errorMessages", data)) if data else "Unknown")
            log_error("link", link_id, error_msg, payload)
            report["links"]["failed"] += 1
            failed_links.append(item)

    if failed_links:
        save_failed("links", failed_links)

    print(f"\n   Links: {report['links']['success']} success | "
          f"{report['links']['failed']} failed | "
          f"{report['links']['skipped']} skipped")


# =============================================================
# STEP 6: RECREATE HISTORY (OPTIONAL - NOT RECOMMENDED)
# =============================================================

def recreate_history():
    """
      WARNING: This is VERY SLOW and may not be worth it.
    
    Recreates issue history by replaying changes as updates.
    Requires one PUT request per change per issue.
    
    Limitations:
    - Cannot backdate changes (uses current timestamp)
    - Cannot preserve original user (uses API user)
    - Status transitions require special handling
    - Some fields cannot be updated (e.g., created date)
    
    Alternative: Keep activities.json as reference documentation.
    """
    if not LOAD_HISTORY:
        print("\n History: SKIPPED (LOAD_HISTORY=False)")
        print("     Keep activities.json as reference instead")
        return

    print("\n Recreating issue history...")
    print("      WARNING: This is SLOW and has limitations")
    
    activities = load_json("activities.json")
    
    if not activities:
        print("      No activities to replay")
        return
    
    # Group activities by issue
    activities_by_issue = {}
    for activity in activities:
        issue_id = activity.get("source_issue_id")
        if issue_id not in activities_by_issue:
            activities_by_issue[issue_id] = []
        activities_by_issue[issue_id].append(activity)
    
    # Sort by timestamp
    for issue_id in activities_by_issue:
        activities_by_issue[issue_id].sort(
            key=lambda x: x.get("created_at", "")
        )
    
    total_changes = sum(len(v) for v in activities_by_issue.values())
    
    print(f"      This will make {total_changes} update requests")
    print(f"      Estimated time: {total_changes * 0.5 / 60:.1f} minutes")
    print(f"      Changes will use current timestamp (not original)")
    print(f"      Changes will use API user (not original user)")
    
    confirm = input("\n    Continue? (yes/no): ")
    if confirm.lower() != "yes":
        print("    Skipped")
        return
    
    processed = 0
    
    for source_issue_id, changes in activities_by_issue.items():
        # Get Jira key
        issue_info = id_mapping["issues"].get(source_issue_id)
        if not issue_info:
            continue
        
        jira_key = issue_info["jira_key"]
        
        for change in changes:
            processed += 1
            changes_dict = change.get("changes", {})
            
            if not changes_dict:
                continue
            
            # Build update payload
            update_fields = {}
            
            for field_name, change_value in changes_dict.items():
                new_value = change_value.get("to")
                
                # Map field names to Jira field format
                if field_name == "priority":
                    if new_value:
                        update_fields["priority"] = {"name": new_value}
                
                elif field_name == "summary":
                    if new_value:
                        update_fields["summary"] = new_value
                
                elif field_name.startswith("cf_") or field_name.startswith("customfield_"):
                    # Custom fields
                    if new_value:
                        update_fields[field_name] = new_value
                
                # Skip status (requires transitions)
                # Skip assignee (requires accountId mapping)
                # Skip other complex fields
            
            if not update_fields:
                continue
            
            # Update issue
            payload = {"fields": update_fields}
            data, status = jira_put(f"api/3/issue/{jira_key}", payload)
            
            if status == 204:
                report["history"]["success"] += 1
                if processed % 10 == 0:
                    print(f"     [{processed}/{total_changes}] Updated {jira_key}")
            else:
                report["history"]["failed"] += 1
                log_error("history", f"{jira_key}_{field_name}", 
                         str(data.get("errors", data.get("errorMessages", data))))
    
    print(f"\n   History: {report['history']['success']} success | "
          f"{report['history']['failed']} failed")
    print(f"     Note: Timestamps and users are not preserved")


# =============================================================
# VALIDATION
# =============================================================

def validate_migration():
    """
    Validate migration results:
      1. Count check: source vs Jira
      2. Missing issues: in payload but not in Jira
      3. Sample spot check: compare fields for 5 random issues
    """
    print(f"\n{'━'*60}")
    print("  VALIDATION")
    print(f"{'━'*60}")

    validation_report = {
        "validated_at": datetime.now().isoformat(),
        "source_count": 0,
        "jira_count": 0,
        "count_match": False,
        "missing_in_jira": [],
        "spot_check": [],
        "errors": [],
    }

    # ── Count check ──
    issues_payload = load_json("issues.json")
    source_count = len(issues_payload)
    jira_count = len(id_mapping["issues"])

    validation_report["source_count"] = source_count
    validation_report["jira_count"] = jira_count
    validation_report["count_match"] = (source_count == jira_count)

    print(f"\n  Count Check:")
    print(f"    Source issues:   {source_count}")
    print(f"    Jira issues:     {jira_count}")

    if source_count == jira_count:
        print(f"     Counts match!")
    else:
        diff = source_count - jira_count
        print(f"      Mismatch: {diff} issues not created")

    # ── Missing issues ──
    source_ids = {str(item.get("source_issue_id")) for item in issues_payload}
    mapped_ids = set(id_mapping["issues"].keys())
    missing = source_ids - mapped_ids

    validation_report["missing_in_jira"] = list(missing)

    if missing:
        print(f"\n    Missing in Jira ({len(missing)}):")
        for mid in list(missing)[:10]:
            print(f"      • Source ID: {mid}")
        if len(missing) > 10:
            print(f"      ... and {len(missing) - 10} more (see validation_report.json)")
    else:
        print(f"\n   All issues present in id_mapping")

    # ── Spot check: verify 5 random issues in Jira ──
    print(f"\n  Spot Check (5 random issues):")
    import random
    sample_ids = random.sample(list(mapped_ids), min(5, len(mapped_ids)))

    for source_id in sample_ids:
        mapping_info = id_mapping["issues"][source_id]
        jira_key = mapping_info.get("jira_key")

        if not jira_key:
            continue

        # Fetch from Jira
        data, status = jira_get(f"api/3/issue/{jira_key}")

        if status == 200:
            jira_summary = data.get("fields", {}).get("summary", "")
            jira_status = data.get("fields", {}).get("status", {}).get("name", "")
            jira_type = data.get("fields", {}).get("issuetype", {}).get("name", "")

            spot = {
                "source_id": source_id,
                "jira_key": jira_key,
                "jira_summary": jira_summary[:50],
                "jira_status": jira_status,
                "jira_type": jira_type,
                "exists_in_jira": True,
            }
            validation_report["spot_check"].append(spot)

            print(f"     {jira_key} | {jira_type} | {jira_status} | {jira_summary[:40]}")

        else:
            spot = {
                "source_id": source_id,
                "jira_key": jira_key,
                "exists_in_jira": False,
                "error": f"HTTP {status}"
            }
            validation_report["spot_check"].append(spot)
            validation_report["errors"].append(spot)
            print(f"     {jira_key} | HTTP {status}")

    # ── Comment count check ──
    print(f"\n  Comment Check:")
    print(f"    Payload:   {len(load_json('comments.json'))}")
    print(f"    Loaded:    {len(id_mapping['comments'])}")
    print(f"    Failed:    {report['comments']['failed']}")

    # ── Link count check ──
    print(f"\n  Link Check:")
    print(f"    Payload:   {len(load_json('links.json'))}")
    print(f"    Loaded:    {len(id_mapping['links'])}")
    print(f"    Failed:    {report['links']['failed']}")

    # ── Save validation report ──
    val_path = os.path.join(MAPPING_DIR, "validation_report.json")
    with open(val_path, "w") as f:
        json.dump(validation_report, f, indent=4)

    print(f"\n   Validation report: {val_path}")
    print(f"{'━'*60}")

    return validation_report


# =============================================================
# SAVE LOAD REPORT
# =============================================================

def save_report():
    """Save final load report."""
    report["completed_at"] = datetime.now().isoformat()

    report_path = os.path.join(MAPPING_DIR, "load_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    print(f"\n   Load report: {report_path}")

def retry_from_failed_file():
    """
    Reload and retry issues from failed_issues.json.
    Run this after fixing the root cause.
    """
    failed_path = os.path.join(MAPPING_DIR, "failed_issues.json")
    if not os.path.exists(failed_path):
        print("No failed_issues.json found")
        return

    with open(failed_path, "r") as f:
        failed_items = json.load(f)

    print(f"\n Retrying {len(failed_items)} failed issues...")
    still_failed = []

    for item in failed_items:
        source_id      = str(item.get("source_issue_id", ""))
        source_parent_id = str(item.get("source_parent_id", "")).split(".")[0]
        source_epic_id   = str(item.get("source_epic_id", "")).split(".")[0]
        issue_type     = item.get("jira_issue_type", "Task")
        payload        = item.get("payload", {}).copy()
        fields         = payload.get("fields", {}).copy()

        # Skip if already created in a previous run
        if source_id in id_mapping["issues"]:
            print(f"  {source_id} already in id_mapping, skipping")
            continue

        # Clean summary again just in case
        if "summary" in fields:
            s = fields["summary"].replace("\n", " ").replace("\r", " ").strip()
            fields["summary"] = " ".join(s.split())[:255]

        # Resolve parent
        if source_parent_id and source_parent_id not in ("None", "", "nan"):
            parent_info = id_mapping["issues"].get(source_parent_id)
            if parent_info:
                fields["parent"] = {"key": parent_info["jira_key"]}

        # Resolve epic
        elif source_epic_id and source_epic_id not in ("None", "", "nan"):
            epic_info = id_mapping["issues"].get(source_epic_id)
            if epic_info:
                fields["parent"] = {"key": epic_info["jira_key"]}

        payload["fields"] = fields
        data, status = jira_post("api/3/issue", payload)

        if status == 201:
            jira_key = data.get("key")
            id_mapping["issues"][source_id] = {
                "jira_key":   jira_key,
                "jira_id":    data.get("id"),
                "source_id":  source_id,
                "issue_type": issue_type,
            }
            save_mapping()
            print(f"   {source_id}  {jira_key}")
        else:
            print(f"   {source_id}: {data}")
            still_failed.append(item)

    if still_failed:
        with open(failed_path, "w") as f:
            json.dump(still_failed, f, indent=4)
        print(f"  {len(still_failed)} still failing — check failed_issues.json")
    else:
        # Clear the file
        os.remove(failed_path)
        print("  All retried successfully")
        

def main():
    report["started_at"] = datetime.now().isoformat()
 
    print("=" * 60)
    print("  LOAD TO JIRA")
    print(f"  Target:  {JIRA_URL}")
    print(f"  Email:   {JIRA_EMAIL}")
    print(f"  Resume:  {RESUME_MODE}")
    print(f"  Time:    {report['started_at']}")
    print("=" * 60)
 
    # ── Verify Jira connection ─────────────────────────────────
    print("\n Testing Jira connection...")
    data, status = jira_get("api/3/myself")
    if status == 200:
        print(f"  Connected as: {data.get('displayName')} ({data.get('emailAddress')})")
    else:
        print(f"  Cannot connect to Jira (HTTP {status})")
        print(f"  Check JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN in .env")
        return
 
    # ── STEP 1-2: Versions + Sprints ──────────────────────────
    load_versions()
    export_versions_to_excel()
    load_sprints()

    export_sprints_to_excel()
 
    # ── STEP 3: Issues (must be done before attachments) ──────
    load_issues()

    assign_issues_to_sprints() 
    assign_versions_to_issues() 
    
 
    # ── STEP 4b: Upload all attachments to Jira ───────────────
    # Runs upload_attachments.py as a subprocess so it uses the
    # freshly created issues in id_mapping.json.
    print("\n Uploading attachments...")
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "upload_attachments.py"],
        capture_output=False,   # let it print to console
    )
    if result.returncode != 0:
        print("  WARNING: upload_attachments.py exited with errors.")
        print("  Check the output above. Comments may be missing attachments.")
 
    # ── STEP 4c: Re-build comment payloads WITH attachment links ──
    # Import here to avoid circular issues
    print("\n Rebuilding comment payloads with attachment links...")
    import importlib
    import prepare_payload as pp
 
    attachment_mapping = pp.load_attachment_mapping()
    comment_payloads   = pp.prepare_comments(attachment_mapping)
    print(f"  {len(comment_payloads)} comment payloads rebuilt.")
 
    load_comments()
 
    load_links()

    load_dependencies()
    
    if LOAD_HISTORY:
        recreate_history()
 
    validate_migration()
 
    save_report()
 
    print(f"\n{'='*60}")
    print("  LOAD COMPLETE")
    print(f"{'='*60}")
    print(f"  Versions:    {report['versions']['success']} success | {report['versions']['failed']} failed")
    print(f"  Sprints:     {report['sprints']['success']} success | {report['sprints']['failed']} failed")
    print(f"  Issues:      {report['issues']['success']} success | {report['issues']['failed']} failed")
    print(f"  Comments:    {report['comments']['success']} success | {report['comments']['failed']} failed")
    print(f"  Links:       {report['links']['success']} success | {report['links']['failed']} failed")
    if LOAD_HISTORY:
        print(f"  History:     {report['history']['success']} success | {report['history']['failed']} failed")
    print(f"\n  Total errors: {len(report['errors'])}")
    print(f"  ID Mapping:   {MAPPING_FILE}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()