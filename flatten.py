# flatten.py
"""
============================================================
STEP 2: FLATTEN RAW JSON → DataFrames → CSV / Excel
============================================================
Reads raw JSON files from data/raw/
Flattens nested structures into flat rows
Creates pandas DataFrames
Saves as CSV files in data/flattened/
Optionally saves combined Excel workbook

Input:  data/raw/  (from extract.py)
Output: data/flattened/*.csv
        data/flattened/migration_data.xlsx (optional)
============================================================
"""

import os
import json
import glob
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
load_dotenv()
# =============================================================
# CONFIGURATION
# =============================================================
PROJECT_KEY = os.getenv("project_key")
DATA_ROOT   = f"{PROJECT_KEY}_data" if PROJECT_KEY else "data"

RAW_DATA_DIR = f"test_refactored_shared_pool/{DATA_ROOT}/raw"
FLAT_DATA_DIR = f"{DATA_ROOT}/flattened"
SAVE_EXCEL = True                   # set False to skip Excel
EXCEL_FILENAME = "migration_data.xlsx"

os.makedirs(FLAT_DATA_DIR, exist_ok=True)


# =============================================================
# HELPER FUNCTIONS
# =============================================================

def load_json(filepath):
    """Load a JSON file safely."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Cannot read {filepath}: {e}")
        return None


def load_all_json_in_dir(directory, pattern="*.json"):
    """Load all JSON files matching pattern in a directory."""
    results = []
    search = os.path.join(directory, pattern)
    for filepath in sorted(glob.glob(search)):
        data = load_json(filepath)
        if data:
            results.append({"file": filepath, "data": data})
    return results


def load_all_json_recursive(directory, pattern="*.json"):
    """Load all JSON files recursively in subdirectories."""
    results = []
    search = os.path.join(directory, "**", pattern)
    for filepath in sorted(glob.glob(search, recursive=True)):
        data = load_json(filepath)
        if data:
            results.append({"file": filepath, "data": data})
    return results


def safe_get(d, *keys, default=None):
    """Safely get nested dict values."""
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d


def save_csv(df, filename):
    """Save DataFrame as CSV."""
    if df.empty:
        print(f"{filename} — empty, skipped")
        return

    filepath = os.path.join(FLAT_DATA_DIR, filename)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f" {filename} — {len(df)} rows, {len(df.columns)} columns")
    return filepath


def load_reference_data():
    print("\nLoading reference data...")
 
    ref_dir = os.path.join(RAW_DATA_DIR, "reference")
    refs = {
        "users": {},
        "issue_types": {},
        "priorities": {},
        "statuses": {},
    }
 
    # Users
    users_file = os.path.join(ref_dir, "users.json")
    if os.path.exists(users_file):
        users = load_json(users_file) or []
        for u in users:
            uid = u["id"]
            entry = {
                "name": u.get("name", ""),
                "email": u.get("email", ""),
                "status": u.get("status", ""),
                "user_type": u.get("user_type", ""),
                "time_zone": u.get("time_zone", ""),
            }
            # Store under both int and str key
            refs["users"][uid] = entry
            refs["users"][str(uid)] = entry
        print(f"    Users:       {len(users)}")
 
    # Issue Types — KEY FIX IS HERE
    it_file = os.path.join(ref_dir, "issue_types.json")
    if os.path.exists(it_file):
        types = load_json(it_file) or []
        for t in types:
            tid = t["id"]
            entry = {
                "label": t.get("label", ""),
                "name":  t.get("name", ""),
                "level": t.get("level", ""),
            }
            refs["issue_types"][tid]       = entry   # int key  → 14
            refs["issue_types"][str(tid)]  = entry   # str key  → "14"
        print(f"    Issue Types: {len(types)}")
 
    # Priorities
    pr_file = os.path.join(ref_dir, "priorities.json")
    if os.path.exists(pr_file):
        pris = load_json(pr_file) or []
        for p in pris:
            pid = p["id"]
            entry = {
                "label": p.get("label", ""),
                "name":  p.get("name", ""),
                "color": p.get("color", ""),
            }
            refs["priorities"][pid]      = entry
            refs["priorities"][str(pid)] = entry
        print(f"    Priorities:  {len(pris)}")
 
    # Statuses
    st_file = os.path.join(ref_dir, "statuses.json")
    if os.path.exists(st_file):
        stats_data = load_json(st_file) or []
        for s in stats_data:
            sid = s["id"]
            entry = {
                "label":       s.get("label", ""),
                "name":        s.get("name", ""),
                "category_id": s.get("status_category_id", ""),
            }
            refs["statuses"][sid]      = entry
            refs["statuses"][str(sid)] = entry
        print(f"    Statuses:    {len(stats_data)}")
 
    return refs

def flatten_projects():
    """
    Source: data/raw/projects.json           (if exists — combined list)
           data/raw/projects/*_details.json  (individual project files)

    Handles both cases:
      - Combined projects.json from paginated extraction
      - Individual detail files from single-project extraction
    """
    print("\n Flattening projects...")

    rows = []
    seen_ids = set()  # avoid duplicates

    # ── SOURCE 1: Combined projects.json (may not exist) ──
    combined_path = os.path.join(RAW_DATA_DIR, "projects.json")
    if os.path.exists(combined_path):
        combined = load_json(combined_path)
        if combined:
            for p in combined.get("projects", []):
                if p.get("id") not in seen_ids:
                    rows.append(build_project_row(p))
                    seen_ids.add(p["id"])

    # ── SOURCE 2: Individual *_details.json files ──
    details_dir = os.path.join(RAW_DATA_DIR, "projects")
    if os.path.isdir(details_dir):
        for f in load_all_json_in_dir(details_dir, "*_details.json"):
            # Detail files have {"project": {...}, "users": [...]}
            project = f["data"].get("project", {})

            if not project or not project.get("id"):
                continue

            if project["id"] in seen_ids:
                # Update existing with more detailed version
                for i, row in enumerate(rows):
                    if row["project_id"] == project["id"]:
                        rows[i] = build_project_row(project)
                        break
            else:
                rows.append(build_project_row(project))
                seen_ids.add(project["id"])

    if not rows:
        print("  No project data found")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    save_csv(df, "projects.csv")
    return df


def build_project_row(p):
    """Build a flat row dict from a project dict."""
    return {
        "project_id": p.get("id"),
        "project_name": p.get("name"),
        "project_key": p.get("key"),
        "project_type": p.get("type"),
        "parent_id": p.get("parent_id"),
        "description": p.get("description"),
        "state": p.get("state"),
        "status_id": p.get("status_id"),
        "priority_id": p.get("priority_id"),
        "board_id": p.get("board_id"),
        "owner_id": p.get("owner_id"),
        "time_zone": p.get("time_zone"),
        "sprint_duration": p.get("sprint_duration"),
        "average_velocity": p.get("average_velocity"),
        "public_workspace": p.get("public_workspace"),
        "template_flag": p.get("template_flag"),

        # Flatten progress
        "progress_todo": safe_get(p, "progress", "todo", default=0),
        "progress_in_progress": safe_get(p, "progress", "in_progress", default=0),
        "progress_done": safe_get(p, "progress", "done", default=0),

        # Flatten story points progress
        "sp_todo": safe_get(p, "story_points_progress", "todo", default=0),
        "sp_in_progress": safe_get(p, "story_points_progress", "in_progress", default=0),
        "sp_done": safe_get(p, "story_points_progress", "done", default=0),
        "sp_unestimated": safe_get(p, "story_points_progress", "unestimated", default=0),

        # Flatten estimation to comma-separated
        "estimation_values": ",".join(str(x) for x in p.get("estimation", [])),

        # Feature flags
        "sprint_enabled": p.get("sprint_enabled"),
        "tcm_enabled": p.get("tcm_enabled"),
        "release_enabled": p.get("release_enabled"),
        "time_tracking_enabled": p.get("time_tracking_enabled", p.get("time_tracking")),
        "workflow_enabled": p.get("workflow_enabled"),

        # Dates
        "start_date": p.get("start_date"),
        "end_date": p.get("end_date"),
        "created_at": p.get("created_at"),
        "archived_at": p.get("archived_at"),
        "actual_start_date": p.get("actual_start_date"),
        "actual_end_date": p.get("actual_end_date"),

        # Flatten working hours
        "working_hours_day1_start": safe_get(p, "working_hours", "1", "beginning_of_workday"),
        "working_hours_day1_end": safe_get(p, "working_hours", "1", "end_of_workday"),
        "working_hours_day2_start": safe_get(p, "working_hours", "2", "beginning_of_workday"),
        "working_hours_day2_end": safe_get(p, "working_hours", "2", "end_of_workday"),

        # Roadmap columns
        "roadmap_columns": ",".join(p.get("roadmap_card_columns", [])),

        "project_category": p.get("project_category"),
    }

# =============================================================
# FLATTEN: USERS
# =============================================================

def flatten_users():
    """
    Source: data/raw/reference/users.json
            data/raw/projects/*_details.json (embedded users)

    Collects all unique users from all sources.
    """
    print("\nFlattening users...")

    users_map = {}  # id → row (deduplicate)

    # From reference
    ref_file = os.path.join(RAW_DATA_DIR, "reference", "users.json")
    if os.path.exists(ref_file):
        for u in (load_json(ref_file) or []):
            users_map[u["id"]] = u

    # From project details (embedded users)
    details_dir = os.path.join(RAW_DATA_DIR, "projects")
    if os.path.isdir(details_dir):
        for f in load_all_json_in_dir(details_dir, "*_details.json"):
            for u in f["data"].get("users", []):
                users_map[u["id"]] = u

    # From comments (embedded users)
    # comments_dir = os.path.join(RAW_DATA_DIR, "comments")
    # if os.path.isdir(comments_dir):
    #     for f in load_all_json_recursive(comments_dir):
    #         for u in f["data"].get("users", []):
    #             users_map[u["id"]] = u
    comments_dir = os.path.join(RAW_DATA_DIR, "comments")
    if os.path.isdir(comments_dir):
        for f in load_all_json_recursive(comments_dir, pattern="comments_*.json"):
            data = f["data"]
            if not isinstance(data, dict):
                continue
            for u in data.get("users", []):
                users_map[u["id"]] = u

    rows = []
    for u in users_map.values():
        row = {
            "user_id": u.get("id"),
            "user_name": u.get("name"),
            "email": u.get("email"),
            "account_admin": u.get("account_admin"),
            "confirmed": u.get("confirmed"),
            "status": u.get("status"),
            "user_type": u.get("user_type"),
            "time_zone": u.get("time_zone"),
            "default_workspace_id": u.get("default_workspace_id"),
            "language": u.get("language"),
            "uuid": u.get("uuid"),
            "access_level": u.get("access_level"),
            "project_admin": u.get("project_admin"),
            "removable": u.get("removable"),
            "group_ids": ",".join(str(g) for g in u.get("group_ids", [])),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "users.csv")
    return df


# =============================================================
# FLATTEN: ISSUE TYPES
# =============================================================

def flatten_issue_types():
    """
    Source: data/raw/reference/issue_types.json
    """
    print("\nFlattening issue types...")

    ref_file = os.path.join(RAW_DATA_DIR, "reference", "issue_types.json")
    types = load_json(ref_file) or []

    rows = []
    for t in types:
        row = {
            "issue_type_id": t.get("id"),
            "issue_type_label": t.get("label"),
            "issue_type_name": t.get("name"),
            "description": t.get("description"),
            "type_icon": t.get("type_icon"),
            "default": t.get("default"),
            "level": t.get("level"),
            "editable": t.get("editable"),
            "removable": t.get("removable"),
            "state": t.get("state"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "issue_types.csv")
    return df


# =============================================================
# FLATTEN: PRIORITIES
# =============================================================

def flatten_priorities():
    """
    Source: data/raw/reference/priorities.json
    """
    print("\nFlattening priorities...")

    ref_file = os.path.join(RAW_DATA_DIR, "reference", "priorities.json")
    pris = load_json(ref_file) or []

    rows = []
    for p in pris:
        row = {
            "priority_id": p.get("id"),
            "priority_label": p.get("label"),
            "priority_name": p.get("name"),
            "description": p.get("description"),
            "color": p.get("color"),
            "editable": p.get("editable"),
            "default": p.get("default"),
            "removable": p.get("removable"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "priorities.csv")
    return df


# =============================================================
# FLATTEN: STATUSES
# =============================================================

def flatten_statuses():
    """
    Source: data/raw/reference/statuses.json
    """
    print("\nFlattening statuses...")

    ref_file = os.path.join(RAW_DATA_DIR, "reference", "statuses.json")
    stats_data = load_json(ref_file) or []

    rows = []
    for s in stats_data:
        row = {
            "status_id": s.get("id"),
            "status_label": s.get("label"),
            "status_name": s.get("name"),
            "description": s.get("description"),
            "status_category_id": s.get("status_category_id"),
            "estimatable": s.get("estimatable"),
            "default": s.get("default"),
            "removable": s.get("removable"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "statuses.csv")
    return df


# =============================================================
# FLATTEN: SPRINTS
# =============================================================

def flatten_sprints():
    """
    Source: data/raw/sprints/*_sprints.json

    Flattens:
      - burndown_metric.burndown_data → separate CSV
      - Main sprint fields → sprints.csv
    """
    print("\nFlattening sprints...")

    sprint_rows = []
    burndown_rows = []

    sprints_dir = os.path.join(RAW_DATA_DIR, "sprints")
    if not os.path.isdir(sprints_dir):
        print(" No sprints directory found")
        return pd.DataFrame()

    for f in load_all_json_in_dir(sprints_dir):
        sprints = f["data"].get("sprints", [])
        # Extract project key from filename
        filename = os.path.basename(f["file"])
        project_key = filename.replace("_sprints.json", "")

        for s in sprints:
            row = {
                "sprint_id": s.get("id"),
                "sprint_name": s.get("name"),
                "project_key": project_key,
                "description": s.get("description"),
                "state": s.get("state"),
                "board_id": s.get("board_id"),
                "primary_workspace_id": s.get("primary_workspace_id"),
                "start_date": s.get("start_date"),
                "end_date": s.get("end_date"),
                "planned_start_date": s.get("planned_start_date"),
                "planned_end_date": s.get("planned_end_date"),
                "created_at": s.get("created_at"),
                "planned_work_items": s.get("planned_work_items"),
                "planned_story_points": s.get("planned_story_points"),
                "completed_work_items": s.get("completed_work_items"),
                "completed_story_points": s.get("completed_story_points"),
                "planned_effort": s.get("planned_effort"),
                "completed_effort": s.get("completed_effort"),
                "average_velocity": s.get("average_velocity"),
                "label": s.get("label"),
            }
            sprint_rows.append(row)

            # Flatten burndown data
            burndown = safe_get(s, "burndown_metric", "burndown_data", default=[])
            for bd in burndown:
                bd_row = {
                    "sprint_id": s.get("id"),
                    "sprint_name": s.get("name"),
                    "project_key": project_key,
                    "date": bd.get("date"),
                    "tasks": bd.get("tasks"),
                    "story_points": bd.get("story_points"),
                    "effort": bd.get("effort"),
                }
                burndown_rows.append(bd_row)

    df_sprints = pd.DataFrame(sprint_rows)
    save_csv(df_sprints, "sprints.csv")

    if burndown_rows:
        df_burndown = pd.DataFrame(burndown_rows)
        save_csv(df_burndown, "sprint_burndown.csv")

    return df_sprints


# =============================================================
# FLATTEN: RELEASES
# =============================================================

def flatten_releases():
    """
    Source: data/raw/releases/*_releases.json

    Flattens:
      - meta → meta_todo, meta_in_progress, meta_done
    """
    print("\n Flattening releases...")

    rows = []
    releases_dir = os.path.join(RAW_DATA_DIR, "releases")
    if not os.path.isdir(releases_dir):
        print("  No releases directory found")
        return pd.DataFrame()

    for f in load_all_json_in_dir(releases_dir):
        releases = f["data"].get("releases", [])
        filename = os.path.basename(f["file"])
        project_key = filename.replace("_releases.json", "")

        for r in releases:
            row = {
                "release_id": r.get("id"),
                "release_name": r.get("name"),
                "project_key": project_key,
                "project_id": r.get("project_id"),
                "description": r.get("description"),
                "owner_id": r.get("owner_id"),
                "start_date": r.get("start_date"),
                "end_date": r.get("end_date"),
                "release_status_id": r.get("release_status_id"),
                "release_notes": r.get("release_notes"),
                "label": r.get("label"),

                # Flatten meta
                "meta_todo": safe_get(r, "meta", "todo", default=0),
                "meta_in_progress": safe_get(r, "meta", "in_progress", default=0),
                "meta_done": safe_get(r, "meta", "done", default=0),
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "releases.csv")
    return df


# =============================================================
# FLATTEN: FORMS
# =============================================================

# def flatten_forms(refs):
    """
    Source: data/raw/forms/*_forms_list.json
            data/raw/forms/{project_key}/form_{id}.json

    Two CSVs:
      - forms.csv         → form metadata
      - form_fields.csv   → individual field definitions per form
    """
    print("\n Flattening forms...")

    form_rows = []
    field_rows = []

    forms_dir = os.path.join(RAW_DATA_DIR, "forms")
    if not os.path.isdir(forms_dir):
        print("     No forms directory found")
        return pd.DataFrame()

    # Form list files
    for f in load_all_json_in_dir(forms_dir, "*_forms_list.json"):
        forms = f["data"].get("forms", [])
        filename = os.path.basename(f["file"])
        project_key = filename.replace("_forms_list.json", "")

        for form in forms:
            row = {
                "form_id": form.get("id"),
                "form_label": form.get("label"),
                "form_name": form.get("name"),
                "project_key": project_key,
                "description": form.get("description"),
                "editable": form.get("editable"),
                "default": form.get("default"),
                "type": form.get("type"),
                "removable": form.get("removable"),
                "active": form.get("active"),
            }
            form_rows.append(row)

    # Individual form field definitions
    for project_dir in glob.glob(os.path.join(forms_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))

        for f in load_all_json_in_dir(project_dir, "form_*.json"):
            form_data = f["data"].get("form", {})
            form_id = form_data.get("id")
            form_label = form_data.get("label")

            for field in form_data.get("fields", []):
                frow = {
                    "form_id": form_id,
                    "form_label": form_label,
                    "project_key": project_key,
                    "field_id": field.get("id"),
                    "field_name": field.get("name"),
                    "field_label": field.get("label"),
                    "field_type": field.get("type"),
                    "position": field.get("position"),
                    "required": field.get("required"),
                    "default": field.get("default"),
                    "visible": field.get("visible"),
                    "editable": field.get("editable"),
                    "placeholder": field.get("placeholder"),
                    "parent_id": field.get("parent_id"),
                    "choices": json.dumps(field.get("choices", [])),
                    "choice_ids": ",".join(str(c) for c in field.get("choice_ids", [])),
                }
                field_rows.append(frow)

    df_forms = pd.DataFrame(form_rows)
    save_csv(df_forms, "forms.csv")

    if field_rows:
        df_fields = pd.DataFrame(field_rows)
        save_csv(df_fields, "form_fields.csv")

    return df_forms


# =============================================================
# FLATTEN: ISSUES (Main — most complex)
# =============================================================

def flatten_issues(refs):
    """
    Source: data/raw/issues/{project_key}/issue_{id}.json
            data/raw/issues/{project_key}_issues_list.json

    Flattens:
      - custom_field → cf_qa_owner, cf_release_plan, cf_ask_type, ...
      - Resolves IDs → names using reference data:
          issue_type_id → issue_type_name
          status_id     → status_name
          priority_id   → priority_name
          owner_id      → owner_name, owner_email
          reporter_id   → reporter_name, reporter_email
      - tags → comma-separated string
      - links → removed (API navigation, not issue links)
      - transition_ids → comma-separated
    """
    print("\n Flattening issues...")

    rows = []

    # Collect issues from individual detail files
    issues_dir = os.path.join(RAW_DATA_DIR, "issues")
    if not os.path.isdir(issues_dir):
        print("   No issues directory found")
        return pd.DataFrame()

    # Find all project subdirectories
    for project_dir in glob.glob(os.path.join(issues_dir, "*/")): # find all the issues inside row -> issues -> project_key -> issue_id json files
        project_key = os.path.basename(os.path.normpath(project_dir))

        for f in load_all_json_in_dir(project_dir, "issue_*.json"):
            issue = f["data"].get("issue", {})
            if not issue:
                continue

            # Also collect any embedded reference data
            collect_refs_from_issue_response(refs, f["data"])

            row = flatten_single_issue(issue, project_key, refs)
            rows.append(row)

    # If no detail files, fall back to issues list
    if not rows:
        for f in load_all_json_in_dir(issues_dir, "*_issues_list.json"):
            issues = f["data"].get("issues", [])
            filename = os.path.basename(f["file"])
            project_key = filename.replace("_issues_list.json", "")

            for issue in issues:
                row = flatten_single_issue(issue, project_key, refs)
                rows.append(row)

    df = pd.DataFrame(rows)

    # Reorder columns — important fields first
    priority_cols = [
        "issue_id", "issue_key", "project_key", "title",
        "issue_type_id", "issue_type_name",
        "status_id", "status_name",
        "priority_id", "priority_name",
        "owner_id", "owner_name", "owner_email",
        "reporter_id", "reporter_name", "reporter_email",
        "parent_id", "epic_id", "epic_title",
        "sprint_id", "release_id",
        "story_points", "effort", "duration",
        "start_date", "due_by",
        "created_at", "updated_at", "resolved_at",
        "description",
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[ordered_cols]

    save_csv(df, "issues.csv")
    return df


def flatten_dependencies():
    """
    Source: data/raw/dependencies/{project_key}/deps_{issue_id}.json
 
    Produces one row per dependency link with full context:
        issue_id          — the issue being described
        project_key       — its project
        relationship      — "blocks" or "is_blocked_by"
        dependency_type   — "finish_to_start" etc
        linked_issue_id   — the other side's source ID
        linked_issue_key  — the other side's source key (e.g. FD-19)
        linked_issue_title
        linked_issue_project_id
        predecessor_id    — raw from API
        successor_id      — raw from API
    """
    print("\n Flattening dependencies...")
 
    rows = []
    deps_dir = os.path.join(RAW_DATA_DIR, "dependencies")
 
    if not os.path.isdir(deps_dir):
        print("     No dependencies directory found")
        return pd.DataFrame()
 
    for project_dir in glob.glob(os.path.join(deps_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))
 
        for f in load_all_json_in_dir(project_dir, "deps_*.json"):
            data      = f["data"]
            issue_id  = data.get("issue_id")
 
            # ── successor_dependencies: this issue BLOCKS linked ──
            for dep in (data.get("successor_dependencies") or []):
                rows.append({
                    "issue_id":               issue_id,
                    "project_key":            project_key,
                    "relationship":           "blocks",
                    "dependency_type":        dep.get("dependency_type", "finish_to_start"),
                    "dep_record_id":          dep.get("id"),
                    "predecessor_id":         dep.get("predecessor_id"),   # = issue_id
                    "successor_id":           dep.get("successor_id"),     # = linked
                    "linked_issue_id":        dep.get("linked_issue_id", dep.get("successor_id")),
                    "linked_issue_key":       dep.get("linked_issue_key", ""),
                    "linked_issue_title":     dep.get("linked_issue_title", ""),
                    "linked_issue_project_id":dep.get("linked_issue_project_id", ""),
                })
 
            # ── predecessor_dependencies: this issue IS BLOCKED BY linked ──
            for dep in (data.get("predecessor_dependencies") or []):
                rows.append({
                    "issue_id":               issue_id,
                    "project_key":            project_key,
                    "relationship":           "is_blocked_by",
                    "dependency_type":        dep.get("dependency_type", "finish_to_start"),
                    "dep_record_id":          dep.get("id"),
                    "predecessor_id":         dep.get("predecessor_id"),   # = linked
                    "successor_id":           dep.get("successor_id"),     # = issue_id
                    "linked_issue_id":        dep.get("linked_issue_id", dep.get("predecessor_id")),
                    "linked_issue_key":       dep.get("linked_issue_key", ""),
                    "linked_issue_title":     dep.get("linked_issue_title", ""),
                    "linked_issue_project_id":dep.get("linked_issue_project_id", ""),
                })
 
    df = pd.DataFrame(rows)
    save_csv(df, "dependencies.csv")
    return df


def flatten_single_issue(issue, project_key, refs):
    """Flatten one issue dict into a flat row dict."""
 
    # ── Normalize all IDs to string for safe dict lookup ──────
    issue_type_id = str(issue.get("issue_type_id", "")).split(".")[0]  # "14.0" → "14"
    status_id     = str(issue.get("status_id",     "")).split(".")[0]
    priority_id   = str(issue.get("priority_id",   "")).split(".")[0]
    owner_id      = str(issue.get("owner_id",      "")).split(".")[0]
    reporter_id   = str(issue.get("reporter_id",   "")).split(".")[0]
 
    # ── Resolve label from refs using normalized string key ───
    issue_type_label = safe_get(refs["issue_types"], issue_type_id, "label", default="")
    issue_type_level = safe_get(refs["issue_types"], issue_type_id, "level", default="")
 
    # Fallback: if label still empty, try looking up by int key
    if not issue_type_label:
        try:
            issue_type_label = safe_get(
                refs["issue_types"], int(issue_type_id), "label", default=""
            )
        except (ValueError, TypeError):
            pass
 
    row = {
        # Core fields
        "issue_id":      issue.get("id"),
        "issue_key":     issue.get("key"),
        "project_key":   project_key,
        "project_id":    issue.get("project_id"),
        "title":         issue.get("title") or issue.get("name"),
        "description":   issue.get("description"),
 
        # IDs — keep original int value in CSV for traceability
        "issue_type_id":    issue.get("issue_type_id"),
        "status_id":        issue.get("status_id"),
        "priority_id":      issue.get("priority_id"),
        "owner_id":         issue.get("owner_id"),
        "reporter_id":      issue.get("reporter_id"),
        "parent_id":        issue.get("parent_id"),
        "epic_id":          issue.get("epic_id"),
        "sub_project_id":   issue.get("sub_project_id"),
        "sprint_id":        issue.get("sprint_id"),
        "release_id":       issue.get("release_id"),
        "form_id":          issue.get("form_id"),
        "workflow_id":      issue.get("workflow_id"),
 
        # Resolved names ← using normalized lookup
        "issue_type_name":  issue_type_label,
        "issue_type_level": issue_type_level,   # NEW: "standard" | "epic" | "subtask"
        "status_name":      safe_get(refs["statuses"],   status_id,   "label", default=""),
        "priority_name":    safe_get(refs["priorities"], priority_id, "label", default=""),
        "owner_name":       safe_get(refs["users"],      owner_id,    "name",  default=""),
        "owner_email":      safe_get(refs["users"],      owner_id,    "email", default=""),
        "reporter_name":    safe_get(refs["users"],      reporter_id, "name",  default=""),
        "reporter_email":   safe_get(refs["users"],      reporter_id, "email", default=""),
 
        # Hierarchy
        "parent_title":          issue.get("parent_title"),
        "parent_issue_type_id":  issue.get("parent_issue_type_id"),
        "epic_title":            issue.get("epic_title"),
 
        # Metrics
        "story_points":     issue.get("story_points"),
        "effort":           issue.get("effort"),
        "duration":         issue.get("duration"),
        "position":         issue.get("position"),
        "progress_pulse":   issue.get("progress_pulse"),
 
        # Time tracking
        "total_billable_time":     issue.get("total_billable_time"),
        "total_non_billable_time": issue.get("total_non_billable_time"),
        "total_time_spent":        issue.get("total_time_spent"),
 
        # Dates
        "start_date":         issue.get("start_date"),
        "due_by":             issue.get("due_by"),
        "created_at":         issue.get("created_at"),
        "updated_at":         issue.get("updated_at"),
        "resolved_at":        issue.get("resolved_at"),
        "started_at":         issue.get("started_at"),
        "status_changed_at":  issue.get("status_changed_at"),
        "predicted_end_date": issue.get("predicted_end_date"),
 
        # Flags
        "resolved":       issue.get("resolved"),
        "blocked":        issue.get("blocked"),
        "blocked_at":     issue.get("blocked_at"),
        "blocked_reason": issue.get("blocked_reason"),
        "blocked_days":   issue.get("blocked_days"),
        "deleted":        issue.get("deleted"),
        "following":      issue.get("following"),
        "eta_flag":       issue.get("eta_flag"),
 
        # Tags + transitions
        "tags":           ",".join(str(t) for t in issue.get("tags", [])),
        "transition_ids": ",".join(str(t) for t in issue.get("transition_ids", [])),
    }
 
    # Flatten custom fields
    custom_fields = issue.get("custom_field", {})
    if isinstance(custom_fields, dict):
        for cf_key, cf_value in custom_fields.items():
            row[cf_key] = cf_value
 
    return row

def flatten_integrated_resources():
    """
    Source: data/raw/integrated_resources/ir_{issue_id}.json
    
    Flattens integrated resource metadata into a CSV.
    """
    print("\n Flattening integrated resources...")

    rows = []
    ir_dir = os.path.join(RAW_DATA_DIR, "integrated_resources")
    
    if not os.path.isdir(ir_dir):
        print("    No integrated_resources directory found")
        return pd.DataFrame()

    for f in load_all_json_in_dir(ir_dir, "ir_*.json"):
        data = f["data"]
        filename = os.path.basename(f["file"])
        issue_id = filename.replace("ir_", "").replace(".json", "")

        integrated_resources = data.get("integrated_resources") or []
        
        for ir in integrated_resources:
            row = {
                "issue_id": issue_id,
                "integrated_resource_id": ir.get("id"),
                "display_id": ir.get("display_id"),
                "integration_account_id": ir.get("integration_account_id"),
                "resource_id": ir.get("resource_id"),
                "resource_type": ir.get("resource_type"),
                "remote_resource_id": ir.get("remote_resource_id"),
                "remote_resource": ir.get("remote_resource"),
                "remote_resource_type": ir.get("remote_resource_type"),
                "meta": ir.get("meta"),
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "integrated_resources.csv")
    return df


def collect_refs_from_issue_response(refs, response_data):
    """Collect reference data embedded in issue detail responses."""
 
    for u in response_data.get("users", []):
        uid = u["id"]
        entry = {
            "name":  u.get("name", ""),
            "email": u.get("email", ""),
        }
        refs["users"][uid]      = entry
        refs["users"][str(uid)] = entry
 
    for t in response_data.get("issue_types", []):
        tid = t["id"]
        entry = {
            "label": t.get("label", ""),
            "name":  t.get("name", ""),
            "level": t.get("level", ""),
        }
        refs["issue_types"][tid]      = entry
        refs["issue_types"][str(tid)] = entry
 
    for p in response_data.get("priorities", []):
        pid = p["id"]
        entry = {
            "label": p.get("label", ""),
            "name":  p.get("name", ""),
        }
        refs["priorities"][pid]      = entry
        refs["priorities"][str(pid)] = entry
 
    for s in response_data.get("statuses", []):
        sid = s["id"]
        entry = {
            "label": s.get("label", ""),
            "name":  s.get("name", ""),
        }
        refs["statuses"][sid]      = entry
        refs["statuses"][str(sid)] = entry


def flatten_comments(refs):
    """
    Source: data/raw/comments/{project_key}/comments_{issue_id}.json

    Flattens:
      - commentable → commentable_id, commentable_type
      - links → documents_link
      - Resolves user_id → user_name, user_email
    """
    print("\n Flattening comments...")

    rows = []
    comments_dir = os.path.join(RAW_DATA_DIR, "comments")
    if not os.path.isdir(comments_dir):
        print("     No comments directory found")
        return pd.DataFrame()

    for project_dir in glob.glob(os.path.join(comments_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))

        for f in load_all_json_in_dir(project_dir, "comments_*.json"):
            comments = f["data"].get("comments", [])

            # Collect embedded users
            for u in f["data"].get("users", []):
                refs["users"][u["id"]] = {
                    "name": u.get("name", ""),
                    "email": u.get("email", ""),
                }

            for c in comments:
                user_id = c.get("user_id")
                row = {
                    "comment_id": c.get("id"),
                    "project_key": project_key,

                    # issue_id from commentable_id directly
                    "issue_id": c.get("commentable_id"),

                    # Also keep commentable fields separately
                    "commentable_id": c.get("commentable_id"),
                    "commentable_type": c.get("commentable_type"),
                    "commentable_obj_id": safe_get(c, "commentable", "id"),
                    "commentable_obj_type": safe_get(c, "commentable", "type"),

                    "content": c.get("content"),
                    "user_id": user_id,
                    "user_name": safe_get(refs["users"], user_id, "name", default=""),
                    "user_email": safe_get(refs["users"], user_id, "email", default=""),
                    "created_at": c.get("created_at"),

                    # Flatten links
                    "documents_link": safe_get(c, "links", "documents"),
                }
                rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "comments.csv")
    return df
def flatten_comment_images():
    """
    Source: data/raw/comments/{project_key}/comments_{issue_id}.json
    
    Extracts images found in comment HTML content (<img> tags).
    
    Output CSV columns:
        - comment_id
        - user_id
        - project_key
        - issue_id
        - image_url (original URL from HTML)
        - local_file_path (where file was downloaded)
        - filename
        - file_size_kb
    
    Creates: comment_images.csv
    """
    print("\n Flattening comment images...")

    rows = []
    comments_dir = os.path.join(RAW_DATA_DIR, "comments")
    
    if not os.path.isdir(comments_dir):
        print("     No comments directory found")
        return pd.DataFrame()

    for project_dir in glob.glob(os.path.join(comments_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))

        # Look for images in attachments/comment_{id}/images/
        attachments_dir = os.path.join(project_dir, "attachments")
        if not os.path.isdir(attachments_dir):
            continue

        for comment_dir in glob.glob(os.path.join(attachments_dir, "comment_*/")):
            # Extract comment_id from folder name
            comment_id = os.path.basename(os.path.normpath(comment_dir)).replace("comment_", "")
            
            images_dir = os.path.join(comment_dir, "images")
            if not os.path.isdir(images_dir):
                continue

            # List all image files
            for image_file in os.listdir(images_dir):
                file_path = os.path.join(images_dir, image_file)
                
                if not os.path.isfile(file_path):
                    continue
                
                # Get file size
                file_size_kb = os.path.getsize(file_path) / 1024
                
                # Find the parent comment to get user_id and issue_id
                user_id = ""
                issue_id = ""
                
                # Load comments JSON to get user_id and issue_id
                for f in load_all_json_in_dir(project_dir, "comments_*.json"):
                    data = f["data"]
                    comments = data.get("comments", [])
                    
                    for c in comments:
                        if str(c.get("id")) == comment_id:
                            user_id = c.get("user_id", "")
                            issue_id = c.get("commentable_id", "")
                            break
                
                row = {
                    "comment_id": comment_id,
                    "user_id": user_id,
                    "project_key": project_key,
                    "issue_id": issue_id,
                    "local_file_path": file_path,
                    "filename": image_file,
                    "file_size_kb": round(file_size_kb, 2),
                }
                rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "comment_images.csv")
    return df

def flatten_comment_attachments():
    """
    Source: data/raw/comments/{project_key}/comments_{issue_id}.json
    
    Extracts document attachments from comment links.documents.
    
    Output CSV columns:
        - comment_id
        - user_id
        - project_key
        - issue_id
        - document_id
        - attachment_file_name
        - content_type
        - file_size
        - local_file_path
        - original_link
    
    Creates: comment_attachments.csv
    """
    print("\n Flattening comment attachments...")

    rows = []
    comments_dir = os.path.join(RAW_DATA_DIR, "comments")
    
    if not os.path.isdir(comments_dir):
        print("     No comments directory found")
        return pd.DataFrame()

    for project_dir in glob.glob(os.path.join(comments_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))

        # Look for documents in attachments/comment_{id}/documents/
        attachments_dir = os.path.join(project_dir, "attachments")
        if not os.path.isdir(attachments_dir):
            continue

        for comment_dir in glob.glob(os.path.join(attachments_dir, "comment_*/")):
            # Extract comment_id from folder name
            comment_id = os.path.basename(os.path.normpath(comment_dir)).replace("comment_", "")
            
            documents_dir = os.path.join(comment_dir, "documents")
            if not os.path.isdir(documents_dir):
                continue

            # List all document files
            for doc_file in os.listdir(documents_dir):
                file_path = os.path.join(documents_dir, doc_file)
                
                if not os.path.isfile(file_path):
                    continue
                
                # Get file size
                file_size_bytes = os.path.getsize(file_path)
                file_size_kb = file_size_bytes / 1024
                
                # Determine file type from extension
                file_ext = os.path.splitext(doc_file)[1].lstrip(".")
                
                # Find the parent comment to get user_id and issue_id
                user_id = ""
                issue_id = ""
                
                for f in load_all_json_in_dir(project_dir, "comments_*.json"):
                    data = f["data"]
                    comments = data.get("comments", [])
                    
                    for c in comments:
                        if str(c.get("id")) == comment_id:
                            user_id = c.get("user_id", "")
                            issue_id = c.get("commentable_id", "")
                            break
                
                row = {
                    "comment_id": comment_id,
                    "user_id": user_id,
                    "project_key": project_key,
                    "issue_id": issue_id,
                    "attachment_file_name": doc_file,
                    "content_type": file_ext,
                    "file_size": f"{file_size_kb:.1f} KB",
                    "file_size_kb": round(file_size_kb, 2),
                    "local_file_path": file_path,
                }
                rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "comment_attachments.csv")
    return df

# =============================================================
# FLATTEN: ISSUE LINKS
# =============================================================

def flatten_issue_links():
    """
    Source: data/raw/links/{project_key}/links_{issue_id}.json
    """
    print("\n Flattening issue links...")

    rows = []
    links_dir = os.path.join(RAW_DATA_DIR, "links")
    if not os.path.isdir(links_dir):
        print("   No links directory found")
        return pd.DataFrame()

    for project_dir in glob.glob(os.path.join(links_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))

        for f in load_all_json_in_dir(project_dir, "links_*.json"):
            links = f["data"].get("issue_links", [])
            # Extract issue_id from filename
            filename = os.path.basename(f["file"])
            issue_id = filename.replace("links_", "").replace(".json", "")

            for link in links:
                row = {
                    "source_issue_id": issue_id,
                    "project_key": project_key,
                    "link_id": link.get("id"),
                    "link_type": link.get("type"),
                    # "target_issue_id": link.get("target_issue_id") or link.get("linked_issue_id"),
                    "target_issue_id": link.get("linkable_id"),
                    "link_record_id":  link.get("id"),
                    "direction": link.get("direction"),
                }
                # Flatten any extra fields
                for k, v in link.items():
                    if k not in row and not isinstance(v, (dict, list)):
                        row[k] = v

                rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "issue_links.csv")
    return df


# =============================================================
# FLATTEN: ACTIVITIES / CHANGELOG
# =============================================================
def flatten_activities(refs):
    print("\n Flattening activities/changelog...")

    activity_rows = []

    act_dir = os.path.join(RAW_DATA_DIR, "activities")
    if not os.path.isdir(act_dir):
        print("   No activities directory found")
        return pd.DataFrame()

    for project_dir in glob.glob(os.path.join(act_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))

        for f in load_all_json_in_dir(project_dir, "act_*.json"):
            activities = f["data"].get("activities", [])
            filename = os.path.basename(f["file"])
            issue_id = filename.replace("act_", "").replace(".json", "")

            for act in activities:
                user_id = act.get("user_id")
                activity_id = act.get("id")

                properties = act.get("properties", {})
                changes = act.get("changes", {})

                # Base activity row with EXPLICIT property fields first
                act_row = {
                    "activity_id": activity_id,
                    "issue_id": issue_id,
                    "project_key": project_key,
                    "action": act.get("action"),
                    "user_id": user_id,
                    "user_name": safe_get(refs["users"], user_id, "name", default=""),
                    "created_at": act.get("created_at"),
                    
                    #  EXPLICIT property fields (always present, even if None)
                    "prop_type": properties.get("type"),
                    "prop_id": properties.get("id"),
                }

                #  Add any OTHER properties dynamically
                for key, value in properties.items():
                    if key not in ["type", "id"]:  # Skip already added
                        if isinstance(value, (dict, list)):
                            act_row[f"prop_{key}"] = json.dumps(value)
                        else:
                            act_row[f"prop_{key}"] = value

                #  Capture other top-level fields
                skip_keys = {"id", "user_id", "action", "created_at", "changes", "properties"}
                for key, value in act.items():
                    if key not in skip_keys:
                        if isinstance(value, (dict, list)):
                            act_row[key] = json.dumps(value)
                        else:
                            act_row[key] = value

                #  Extract all changes into separate columns
                for field_name, change_value in changes.items():
                    if field_name == "custom_fields" and isinstance(change_value, dict):
                        for cf_name, cf_change in change_value.items():
                            old_val, new_val = parse_change_value(cf_change)
                            act_row[f"change_{cf_name}_old"] = old_val
                            act_row[f"change_{cf_name}_new"] = new_val
                    else:
                        old_val, new_val = parse_change_value(change_value)
                        act_row[f"change_{field_name}_old"] = old_val
                        act_row[f"change_{field_name}_new"] = new_val

                activity_rows.append(act_row)

    df_activities = pd.DataFrame(activity_rows)
    
    #  Verify the data is there
    print(f"\n   Columns: {list(df_activities.columns)}")
    print(f"   Sample prop_type values: {df_activities['prop_type'].value_counts()}")
    print(f"   Sample prop_id values: {df_activities['prop_id'].value_counts()}")
    
    save_csv(df_activities, "activities.csv")
    
    print(f"    Saved {len(activity_rows)} activities")
    return df_activities


def parse_change_value(change):
    """
    Parse change format: [old_value, new_value]
    Returns: (old_value, new_value) as strings
    """
    if isinstance(change, list) and len(change) == 2:
        old_val = str(change[0]) if change[0] is not None else ""
        new_val = str(change[1]) if change[1] is not None else ""
        return old_val, new_val
    else:
        return "", str(change)



# =============================================================
# FLATTEN: DOCUMENT INVENTORY
# =============================================================

def flatten_documents():
    """
    Source: data/raw/documents/{project_key}/docs_{issue_id}.json
    
    Flattens documents with local file paths and user information.
    
    Output CSV columns:
        - project_key
        - album_name
        - file_path (local path where file is stored)
        - document_id
        - issue_id (attachable_id)
        - content_type
        - file_size
        - attachment_file_name
        - link (original download URL)
        - user_id
        - user_name
        - user_uuid
        - user_email
    
    Includes blank rows for issues with no documents.
    """
    print("\n Flattening documents...")

    rows = []
    docs_dir = os.path.join(RAW_DATA_DIR, "documents")
    
    if not os.path.isdir(docs_dir):
        print("     No documents directory found")
        return pd.DataFrame()

    for project_dir in glob.glob(os.path.join(docs_dir, "*/")):
        project_key = os.path.basename(os.path.normpath(project_dir))

        for f in load_all_json_in_dir(project_dir, "docs_*.json"):
            data = f["data"]
            issue_id = data.get("issue_id")
            
            documents = data.get("documents")
            users = data.get("users", [])
            
            # ═══════════════════════════════════════════════════════════
            # Build user lookup map
            # ═══════════════════════════════════════════════════════════
            user_map = {}
            if users:
                for u in users:
                    user_map[u.get("id")] = {
                        "name": u.get("name", ""),
                        "uuid": u.get("uuid", ""),
                        "email": u.get("email", "")
                    }
            
            # ═══════════════════════════════════════════════════════════
            # If no documents, add one blank row
            # ═══════════════════════════════════════════════════════════
            if not documents:
                row = {
                    "project_key": project_key,
                    "album_name": "",
                    "file_path": "",
                    "document_id": "",
                    "issue_id": issue_id,
                    "attachable_id": "",
                    "content_type": "",
                    "file_size": "",
                    "attachment_file_name": "",
                    "link": "",
                    "user_id": "",
                    "user_name": "",
                    "user_uuid": "",
                    "user_email": "",
                }
                rows.append(row)
            else:
                # ═══════════════════════════════════════════════════════════
                # Flatten each document
                # ═══════════════════════════════════════════════════════════
                for doc in documents:
                    user_id = doc.get("user_id")
                    user_info = user_map.get(user_id, {})
                    
                    row = {
                        "project_key": project_key,
                        "album_name": doc.get("album_name", ""),
                        "file_path": doc.get("local_file_path", ""),  # Local downloaded path
                        "document_id": doc.get("id", ""),
                        "issue_id": issue_id,
                        "attachable_id": doc.get("attachable_id", ""),
                        "content_type": doc.get("content_type", ""),
                        "file_size": doc.get("file_size", ""),
                        "attachment_file_name": doc.get("attachment_file_name", ""),
                        "link": doc.get("link", ""),
                        "user_id": user_id if user_id else "",
                        "user_name": user_info.get("name", ""),
                        "user_uuid": user_info.get("uuid", ""),
                        "user_email": user_info.get("email", ""),
                    }
                    rows.append(row)

    df = pd.DataFrame(rows)
    save_csv(df, "documents.csv")
    return df

# =============================================================
# SAVE COMBINED EXCEL (optional)
# =============================================================

def save_excel(all_dataframes):
    """
    Save all DataFrames as sheets in one Excel workbook.
    """
    if not SAVE_EXCEL:
        return

    print("\n Saving combined Excel workbook...")

    filepath = os.path.join(FLAT_DATA_DIR, EXCEL_FILENAME)

    try:
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            for sheet_name, df in all_dataframes.items():
                if df is not None and not df.empty:
                    # Excel sheet name max 31 chars
                    safe_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=safe_name, index=False)
                    print(f"  Sheet: {safe_name} ({len(df)} rows)")

        print(f"\n  Saved: {filepath}")

    except ImportError:
        print("    Install openpyxl for Excel: pip install openpyxl")
    except Exception as e:
        print(f"     Excel save failed: {e}")


# =============================================================
# MAIN
# =============================================================

def main():
    print("=" * 60)
    print("  FLATTEN: Raw JSON → DataFrames → CSV / Excel")
    print(f"  Input:   {RAW_DATA_DIR}/")
    print(f"  Output:  {FLAT_DATA_DIR}/")
    print(f"  Time:    {datetime.now().isoformat()}")
    print("=" * 60)

    # Load reference data for ID → name resolution
    refs = load_reference_data()

    # Flatten each entity type
    all_dfs = {}

    all_dfs["projects"] = flatten_projects()
    all_dfs["users"] = flatten_users()
    all_dfs["issue_types"] = flatten_issue_types()
    all_dfs["priorities"] = flatten_priorities()
    all_dfs["statuses"] = flatten_statuses()
    all_dfs["sprints"] = flatten_sprints()
    all_dfs["releases"] = flatten_releases()
    # all_dfs["forms"] = flatten_forms(refs)
    all_dfs["issues"] = flatten_issues(refs)
    all_dfs["comments"] = flatten_comments(refs)
    all_dfs["issue_links"] = flatten_issue_links()
    # all_dfs["test_cases"] = flatten_test_cases()
    all_dfs["activities"] = flatten_activities(refs)
    # all_dfs["project_filters"] = flatten_project_filters()
    all_dfs["documents"] = flatten_documents()
    all_dfs["dependencies"] = flatten_dependencies()
    all_dfs["comment_images"] = flatten_comment_images()
    all_dfs["comment_attachments"] = flatten_comment_attachments() 
    # all_dfs["documents"] = flatten_documents()
    all_dfs["integrated_resources"] = flatten_integrated_resources()  
    # Save combined Excel
    save_excel(all_dfs)

    # Print summary
    print(f"\n{'=' * 60}")
    print("  FLATTEN COMPLETE")
    print(f"{'=' * 60}")
    print(f"\n  CSV files saved to: {FLAT_DATA_DIR}/\n")

    total_rows = 0
    for name, df in all_dfs.items():
        if df is not None and not df.empty:
            total_rows += len(df)
            print(f"    {name:25s} → {len(df):6d} rows  |  {len(df.columns):3d} columns")

    print(f"\n    {'TOTAL':25s} → {total_rows:6d} rows")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()