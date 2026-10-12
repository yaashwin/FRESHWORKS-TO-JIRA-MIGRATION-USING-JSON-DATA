# transform.py
"""
============================================================
STEP 3: TRANSFORM & MAP
============================================================
Reads flattened CSV files from data/flattened/
Applies mappings from mappings.py
Saves transformed data to data/transformed/

Input:  data/flattened/*.csv
Output: data/transformed/*.csv
        data/transformed/transform_report.json
============================================================
"""

import email
import os
import json
import pandas as pd
from datetime import datetime
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
from mappings import (
    PROJECT_MAP, DEFAULT_PROJECT,
    PROJECT_SUBPROJECT_MAP, get_project_key,
    ISSUE_TYPE_MAP, DEFAULT_ISSUE_TYPE,
    STATUS_MAP, DEFAULT_STATUS,
    PRIORITY_MAP, DEFAULT_PRIORITY,
    USER_MAP, MY_ACCOUNT_ID, MAP_UNMAPPED_USERS_TO_ME,
    DEFAULT_LINK_DIRECTION,
    CUSTOM_FIELD_MAP,
    LINK_TYPE_MAP, DEFAULT_LINK_TYPE,
    SPRINT_STATE_MAP,
    RELEASE_STATUS_MAP,
    FIELDS_TO_SKIP,
)
from prepare_payload import is_empty
import re
from dotenv import load_dotenv

load_dotenv()

# =============================================================
# CONFIGURATION
# =============================================================
PROJECT_KEY = os.getenv("project_key")
DATA_ROOT   = f"{PROJECT_KEY}_data" if PROJECT_KEY else "data"

FLAT_DATA_DIR = f"{DATA_ROOT}/flattened"
TRANSFORM_DATA_DIR = f"{DATA_ROOT}/transformed"

os.makedirs(TRANSFORM_DATA_DIR, exist_ok=True)

# ── sub_project_id -> sub_project name cache ─────────────────────────────
# Same file prepare_payload.py's load_subprojects_from_api() reads/writes
# (DATA_ROOT/mapping/subprojects_cache.json). transform.py only READS it
# here — it never calls the API — so this cache must already be populated
# (e.g. from a prior pipeline run) for sub_project-aware project mapping
# to work. If it's missing/empty, issues just fall back to PROJECT_MAP.
SUBPROJECT_CACHE_FILE = f"{DATA_ROOT}/mapping/subprojects_cache.json"


def _load_subproject_cache() -> dict:
    if os.path.exists(SUBPROJECT_CACHE_FILE):
        try:
            with open(SUBPROJECT_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"  WARNING: could not read {SUBPROJECT_CACHE_FILE}: {e}")
    return {}


SUBPROJECT_ID_NAME_MAP: dict = _load_subproject_cache()


def resolve_subproject_name(sub_project_id) -> str:
    """sub_project_id (from issues.csv) -> sub_project name, via the cache."""
    if pd.isna(sub_project_id) or sub_project_id is None:
        return ""

    sp_id = str(sub_project_id).strip()
    if sp_id == "" or sp_id.lower() in ("nan", "none"):
        return ""

    sp_id = sp_id.split(".")[0]  # ids sometimes arrive as "123.0"

    return (
        SUBPROJECT_ID_NAME_MAP.get(sp_id)
        or SUBPROJECT_ID_NAME_MAP.get(sp_id.upper())
        or ""
    )

# Transform report — tracks all unmapped values
report = {
    "started_at": None,
    "completed_at": None,
    "unmapped_projects": set(),
    "unmapped_issue_types": set(),
    "unmapped_statuses": set(),
    "unmapped_priorities": set(),
    "unmapped_users": set(),
    "unmapped_link_types": set(),
    "unmapped_custom_fields": set(),
    "skipped_custom_fields": set(),
    "total_issues_transformed": 0,
    "total_comments_transformed": 0,
    "total_issues_skipped": 0,
    "warnings": [],
}


# =============================================================
# HELPER FUNCTIONS
# =============================================================
#from where the data is been taken and where the transformed data is been stored
def load_csv(filename):
    """Load CSV from flattened directory."""
    filepath = os.path.join(FLAT_DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f" {filename} not found, skipping")
        return pd.DataFrame()
    df = pd.read_csv(
        filepath,
        encoding="utf-8-sig",
        keep_default_na=False,  # stop "NA"/"NULL"/etc. being read as NaN
        na_values=[],
    )
    print(f"     Loaded {filename}: {len(df)} rows")
    return df


def save_csv(df, filename):
    """Save DataFrame to transformed directory."""
    if df.empty:
        print(f" {filename} — empty, skipped")
        return
    filepath = os.path.join(TRANSFORM_DATA_DIR, filename)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f" {filename} — {len(df)} rows")

def safe_str(value):
    """Convert value to string safely."""
    if is_empty(value):
        return ""
    return str(value).strip()

#mapping the value of source to target using mapping dict and also tracking the unmapped values for reporting
def map_value(value, mapping, default, unmapped_set_name=None):
    """
    Map a source value to target value using mapping dict.
    Tracks unmapped values for reporting.
    """
    if pd.isna(value) or value is None or str(value).strip() == "":
        return default

    value_str = str(value).strip()

    # Exact match
    if value_str in mapping:
        return mapping[value_str]

    # Case-insensitive match
    for key, target in mapping.items():
        if key.lower() == value_str.lower():
            return target

    # Not found — track and return default
    if unmapped_set_name:
        report[unmapped_set_name].add(value_str)

    return default


# def map_user(email_or_id):
#     """
#     Map source user email/id  Jira accountId.
#     """
#     if pd.isna(email_or_id) or email_or_id is None or str(email_or_id).strip() == "":
#         return None

#     email_str = str(email_or_id).strip()

#     if email_str in USER_MAP:
#         return USER_MAP[email_str]

#     # Case-insensitive email match
#     for key, account_id in USER_MAP.items():
#         if key.lower() == email_str.lower():
#             return account_id

#     # Unmapped
#     report["unmapped_users"].add(email_str)

#     if MAP_UNMAPPED_USERS_TO_ME:
#         return MY_ACCOUNT_ID

#     return None

def map_user(email_or_id):
    # Empty input = no user, always return None (never fall back to default)
    if pd.isna(email_or_id) or email_or_id is None or str(email_or_id).strip() == "":
        return None   # ← was: return MY_ACCOUNT_ID if MAP_UNMAPPED_USERS_TO_ME else None

    email_str = str(email_or_id).strip()

    if email_str in USER_MAP:
        return USER_MAP[email_str]

    for key, account_id in USER_MAP.items():
        if key.lower() == email_str.lower():
            return account_id

    # Value exists but isn't in the map — THEN fall back to default
    report["unmapped_users"].add(email_str)

    if MAP_UNMAPPED_USERS_TO_ME:
        return MY_ACCOUNT_ID

    return None

# =============================================================
# TRANSFORM: PROJECTS
# =============================================================

def transform_projects():
    """
    Transform project data.
    Maps project keys to Jira project keys.
    """
    print("\n Transforming projects...")
    df = load_csv("projects.csv")
    if df.empty:
        return df

    # Map project key
    df["jira_project_key"] = df["project_key"].apply(
        lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
    )

    # Keep original for reference
    df["source_project_key"] = df["project_key"]

    save_csv(df, "projects.csv")
    return df


# =============================================================
# TRANSFORM: USERS
# =============================================================

def transform_users():
    """
    Transform user data.
    Maps source users to Jira account IDs.
    """
    print("\n Transforming users...")
    df = load_csv("users.csv")
    if df.empty:
        return df

    # Map email  Jira accountId
    df["jira_account_id"] = df["email"].apply(map_user)

    # Flag mapping status
    df["mapping_status"] = df.apply(
        lambda row: "mapped" if str(row.get("email", "")) in USER_MAP
        else ("self_mapped" if MAP_UNMAPPED_USERS_TO_ME else "unmapped"),
        axis=1
    )

    # Keep originals
    df["source_user_id"] = df["user_id"]
    df["source_email"] = df["email"]
    df["source_name"] = df["user_name"]

    save_csv(df, "users.csv")
    return df


# =============================================================
# TRANSFORM: ISSUES (Main — most important)
# =============================================================

# def transform_issues():
#     """
#     Transform issues with all mappings applied:
#       - project_key  jira_project_key
#       - issue_type_name  jira_issue_type
#       - status_name  jira_status
#       - priority_name  jira_priority
#       - owner_email  jira_assignee_id
#       - reporter_email  jira_reporter_id
#       - custom fields  jira custom field IDs
#       - Remove skipped fields
#       - Sort by hierarchy (epics first, then stories, then sub-tasks)
#     """
#     print("\n Transforming issues...")
#     df = load_csv("issues.csv")
#     if df.empty:
#         return df

#     # ── Map project ──
#     df["jira_project_key"] = df["project_key"].apply(
#         lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
#     )

#     # ── Map issue type ──
#     df["jira_issue_type"] = df["issue_type_name"].apply(
#         lambda x: map_value(x, ISSUE_TYPE_MAP, DEFAULT_ISSUE_TYPE, "unmapped_issue_types")
#     )

#     # ── Map status ──
#     df["jira_status"] = df["status_name"].apply(
#         lambda x: map_value(x, STATUS_MAP, DEFAULT_STATUS, "unmapped_statuses")
#     )

#     # ── Map priority ──
#     df["jira_priority"] = df["priority_name"].apply(
#         lambda x: map_value(x, PRIORITY_MAP, DEFAULT_PRIORITY, "unmapped_priorities")
#     )

#     # ── Map users ──
#     df["jira_assignee_id"] = df["owner_email"].apply(map_user)
#     df["jira_reporter_id"] = df["reporter_email"].apply(map_user)

#     # ── Map custom fields ──
#     cf_columns = [col for col in df.columns if col.startswith("cf_")]
#     mapped_cf = {}
#     skipped_cf = []

#     for cf_col in cf_columns:
#         if cf_col in CUSTOM_FIELD_MAP:
#             jira_field_id = CUSTOM_FIELD_MAP[cf_col]
#             if jira_field_id is not None:
#                 mapped_cf[cf_col] = jira_field_id
#             else:
#                 skipped_cf.append(cf_col)
#                 report["skipped_custom_fields"].add(cf_col)
#         else:
#             report["unmapped_custom_fields"].add(cf_col)
#             skipped_cf.append(cf_col)

#     # Rename mapped custom fields to Jira field IDs
#     if mapped_cf:
#         df = df.rename(columns=mapped_cf)
#         print(f" Mapped {len(mapped_cf)} custom fields")

#     if skipped_cf:
#         print(f" Skipping {len(skipped_cf)} unmapped custom fields")

#     # ── Keep source values for reference ──
#     df["source_issue_id"] = df["issue_id"]
#     df["source_project_key"] = df["project_key"]
#     df["source_issue_type"] = df["issue_type_name"]
#     df["source_status"] = df["status_name"]
#     df["source_priority"] = df["priority_name"]
#     df["source_assignee"] = df["owner_name"]
#     df["source_reporter"] = df["reporter_name"]

#     # ── Determine migration order ──
#     # Epics first  Stories/Tasks  Sub-tasks  Bugs
#     type_order = {
#         "Epic": 1,
#         "Story": 2,
#         "Task": 2,
#         "Bug": 3,
#         "Sub-task": 4,
#     }
#     df["_migration_order"] = df["jira_issue_type"].map(type_order).fillna(5)

#     # Issues with no parent come before issues with parent
#     df["_has_parent"] = df["parent_id"].notna().astype(int)

#     # Sort: epics first, then non-child items, then children
#     df = df.sort_values(
#         by=["_migration_order", "_has_parent", "issue_id"],
#         ascending=[True, True, True]
#     ).reset_index(drop=True)

#     # Add migration sequence number
#     df["migration_sequence"] = range(1, len(df) + 1)

#     # ── Remove helper columns ──
#     df = df.drop(columns=["_migration_order", "_has_parent"], errors="ignore")

#     # ── Remove skipped fields ──
#     cols_to_drop = [col for col in FIELDS_TO_SKIP if col in df.columns]
#     if cols_to_drop:
#         df = df.drop(columns=cols_to_drop, errors="ignore")

#     report["total_issues_transformed"] = len(df)

#     save_csv(df, "issues.csv")
#     return df

def transform_issues():
    print("\n Transforming issues...")
    df = load_csv("issues.csv")
    if df.empty:
        return df
    
    for col in ["title", "summary"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'[\n\r\t]', ' ', regex=True)
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )
 
    # ── DEBUG: show what issue type names came through ────────
    if "issue_type_name" in df.columns:
        counts = df["issue_type_name"].value_counts(dropna=False)
        print(f"\n  issue_type_name distribution:")
        for val, cnt in counts.items():
            print(f"    '{val}': {cnt} issues")
 
    # ── Map issue type ────────────────────────────────────────
    def resolve_issue_type(row):
        name  = str(row.get("issue_type_name", "")).strip()
        level = str(row.get("issue_type_level", "")).strip()
 
        # Try mapping by label first
        if name and name in ISSUE_TYPE_MAP:
            return ISSUE_TYPE_MAP[name]
 
        # Case-insensitive fallback
        for key, val in ISSUE_TYPE_MAP.items():
            if key.lower() == name.lower():
                return val
 
        # Use level as last resort
        if level == "epic":
            return "Epic"
        if level == "subtask":
            return "Sub-task"
 
        # Track unmapped and return default
        if name:
            report["unmapped_issue_types"].add(name)
        return DEFAULT_ISSUE_TYPE
 
    df["jira_issue_type"] = df.apply(resolve_issue_type, axis=1)
 
    # ── Rest of transform_issues() stays the same ─────────────
    # Resolve each issue's sub_project_id -> sub_project name via the
    # cache, then map (project_key, sub_project_name) -> Jira project key.
    # Falls back to the plain project_key mapping (PROJECT_MAP) when the
    # issue has no sub_project, or its sub_project isn't found in either
    # the cache or the excel's Sub_project column.
    df["fr_subproject_name"] = df.get("sub_project_id", "").apply(resolve_subproject_name)

    def resolve_project(row):
        source_key = safe_str(row.get("project_key"))
        subproject = safe_str(row.get("fr_subproject_name"))

        jira_key = get_project_key(source_key, subproject)

        if not jira_key:
            report["unmapped_projects"].add(
                f"{source_key} / {subproject}" if subproject else source_key
            )

        return jira_key

    df["jira_project_key"] = df.apply(resolve_project, axis=1)
    # df["jira_status"]   = df["status_name"].apply(
    #     lambda x: map_value(x, STATUS_MAP, DEFAULT_STATUS, "unmapped_statuses")
    # )

    def resolve_status(row):
        issue_type = safe_str(row.get("issue_type_name"))
        fw_status = safe_str(row.get("status_name"))

        # Get mapping for issue type
        type_map = STATUS_MAP.get(issue_type)

        if type_map:
            if fw_status in type_map:
             return type_map[fw_status]

            # Case-insensitive fallback
            for key, value in type_map.items():
                if key.lower() == fw_status.lower():
                  return value

    # Track unmapped
        report["unmapped_statuses"].add(
            f"{issue_type} -> {fw_status}"
       )

        return DEFAULT_STATUS


    df["jira_status"] = df.apply(resolve_status, axis=1)
    df["jira_priority"] = df["priority_name"].apply(
        lambda x: map_value(x, PRIORITY_MAP, DEFAULT_PRIORITY, "unmapped_priorities")
    )
    df["jira_assignee_id"] = df["owner_email"].apply(map_user)
    # df["jira_reporter_id"] = df["reporter_email"].apply(map_user)
    # def resolve_reporter(row):
    #     name  = safe_str(row.get("reporter_name", ""))
    #     email = safe_str(row.get("reporter_email", ""))
    #     # Try name first (matches mappings.xlsx user sheet source_name column)
    #     if name:
    #         from mappings import USER_MAP
    #         normalized = re.sub(r'\s+', ' ', name.strip().lower())
    #         if normalized in USER_MAP:
    #             return USER_MAP[normalized]
    #     # Fall back to email
    #     return map_user(email)

    def resolve_reporter(row):
        name  = safe_str(row.get("reporter_name", ""))
        email = safe_str(row.get("reporter_email", ""))
        if name:
            normalized = re.sub(r'\s+', ' ', name.strip().lower())
            account_id = USER_MAP.get(normalized, "")
            if account_id and account_id not in ("", "nan", "None"):
                return account_id
        return map_user(email)
    
    df["jira_reporter_id"] = df.apply(resolve_reporter, axis=1)
    
    cf_columns = [col for col in df.columns if col.startswith("cf_")]
    for idx, row in df.iterrows():
        issue_type = safe_str(row.get("issue_type_name"))
        type_cf_map = CUSTOM_FIELD_MAP.get(issue_type) or CUSTOM_FIELD_MAP.get(issue_type.lower()) or {}
        
        for cf_col in cf_columns:
            value = row.get(cf_col)
            if is_empty(value):
                continue
            jira_field_id = type_cf_map.get(cf_col)
            if jira_field_id:
                df.at[idx, jira_field_id] = value
            else:
                report["unmapped_custom_fields"].add(f"{issue_type}:{cf_col}")
 
    df["source_issue_id"]   = df["issue_id"]
    df["source_project_key"] = df["project_key"]
    df["source_issue_type"] = df["issue_type_name"]
    df["source_status"]     = df["status_name"]
    df["source_priority"]   = df["priority_name"]
    df["source_assignee"]   = df["owner_name"]
    df["source_reporter"]   = df["reporter_name"]
 
    type_order = {
        "Epic":     1,
        "Story":    2,
        "Task":     2,
        "Bug":      3,
        "Sub-task": 4,
    }
    df["_migration_order"] = df["jira_issue_type"].map(type_order).fillna(5)
    df["_has_parent"]      = df["parent_id"].notna().astype(int)
 
    df = df.sort_values(
        by=["_migration_order", "_has_parent", "issue_id"],
        ascending=[True, True, True]
    ).reset_index(drop=True)
 
    df["migration_sequence"] = range(1, len(df) + 1)
    df = df.drop(columns=["_migration_order", "_has_parent"], errors="ignore")
 
    cols_to_drop = [col for col in FIELDS_TO_SKIP if col in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop, errors="ignore")
 
    report["total_issues_transformed"] = len(df)
    save_csv(df, "issues.csv")
    return df
 


# =============================================================
# TRANSFORM: COMMENTS
# =============================================================

def transform_comments():
    """
    Transform comments:
      - Map user  Jira accountId
      - Map project key
      - Clean HTML content (optional)
    """
    print("\n Transforming comments...")
    df = load_csv("comments.csv")
    if df.empty:
        return df

    # Map project
    df["jira_project_key"] = df["project_key"].apply(
        lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
    )

    # Map comment author
    df["jira_author_id"] = df["user_email"].apply(map_user)

    # Keep originals
    df["source_user_id"] = df["user_id"]
    df["source_user_name"] = df["user_name"]
    df["source_issue_id"] = df["issue_id"]

    report["total_comments_transformed"] = len(df)

    save_csv(df, "comments.csv")
    return df


# =============================================================
# TRANSFORM: ISSUE LINKS
# =============================================================

# def transform_issue_links():
#     """
#     Transform issue links:
#       - Map link types to Jira link types
#     """
#     print("\n Transforming issue links...")
#     df = load_csv("issue_links.csv")
#     if df.empty:
#         return df

#     # Map link type
#     if "link_type" in df.columns:
#         df["jira_link_type"] = df["link_type"].apply(
#             lambda x: map_value(x, LINK_TYPE_MAP, DEFAULT_LINK_TYPE, "unmapped_link_types")
#         )

#     # Keep originals
#     df["source_link_type"] = df.get("link_type")
#     df["source_issue_id"] = df["source_issue_id"]
#     df["source_target_id"] = df.get("target_issue_id")

#     save_csv(df, "issue_links.csv")
#     return df
def transform_issue_links():
    """
    Transform issue links:
      - Map link types to Jira link types
      - Write direction (inward/outward) for payload builder
    """
    print("\n Transforming issue links...")
    df = load_csv("issue_links.csv")
    if df.empty:
        return df
    
    print(f"  issue_links columns: {df.columns.tolist()}")  # ← add this temporarily
    print(f"  sample relationship values: {df.get('relationship', df.get('link_type', pd.Series())).value_counts().to_dict()}")

    if "relationship" in df.columns:
        mask = df["relationship"].str.strip() != ""
        df.loc[mask, "link_type"] = df.loc[mask, "relationship"]

    def resolve_link(value):
        """Returns (jira_link_type, direction) tuple."""
        if pd.isna(value) or str(value).strip() == "":
            return DEFAULT_LINK_TYPE, DEFAULT_LINK_DIRECTION

        value_str = str(value).strip().lower()

        # Exact match
        if value_str in LINK_TYPE_MAP:
            entry = LINK_TYPE_MAP[value_str]
            return entry["jira_link_type"], entry["direction"]

        # Case-insensitive fallback
        for key, entry in LINK_TYPE_MAP.items():
            if key.lower() == value_str:
                return entry["jira_link_type"], entry["direction"]

        # Unmapped — track and use default
        report["unmapped_link_types"].add(value_str)
        return DEFAULT_LINK_TYPE, DEFAULT_LINK_DIRECTION

    if "link_type" in df.columns:
        resolved = df["link_type"].apply(resolve_link)
        df["jira_link_type"] = resolved.apply(lambda x: x[0])
        df["link_direction"]  = resolved.apply(lambda x: x[1])
    else:
        df["jira_link_type"] = DEFAULT_LINK_TYPE
        df["link_direction"]  = DEFAULT_LINK_DIRECTION

    # Keep originals
    # df["source_link_type"] = df.get("link_type", "")
    # df["source_issue_id"]  = df["source_issue_id"]
    # df["source_target_id"] = df.get("target_issue_id", "")
    df["source_link_type"] = df.get("link_type", "")
    df["source_issue_id"]  = df["source_issue_id"].astype(str).str.split(".").str[0].str.strip()
    df["target_issue_id"]  = df["target_issue_id"].astype(str).str.split(".").str[0].str.strip()
    df["source_target_id"] = df["target_issue_id"]
    

    save_csv(df, "issue_links.csv")
    return df


# =============================================================
# TRANSFORM: SPRINTS
# =============================================================

def transform_sprints():
    """
    Transform sprints:
      - Map state codes to names
      - Map project keys
    """
    print("\n Transforming sprints...")
    df = load_csv("sprints.csv")
    if df.empty:
        return df

    # Map project
    df["jira_project_key"] = df["project_key"].apply(
        lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
    )

    # Map sprint state
    if "state" in df.columns:
        df["sprint_state_name"] = df["state"].map(SPRINT_STATE_MAP).fillna("unknown")

    # Keep originals
    df["source_sprint_id"] = df["sprint_id"]

    save_csv(df, "sprints.csv")
    return df


# =============================================================
# TRANSFORM: RELEASES
# =============================================================

def transform_releases():
    """
    Transform releases:
      - Map release status
      - Map project keys
    """
    print("\n Transforming releases...")
    df = load_csv("releases.csv")
    if df.empty:
        return df

    # Map project
    df["jira_project_key"] = df["project_key"].apply(
        lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
    )

    # Map release status
    if "release_status_id" in df.columns:
        df["release_status_name"] = df["release_status_id"].map(RELEASE_STATUS_MAP).fillna("unknown")

    # Keep originals
    df["source_release_id"] = df["release_id"]

    save_csv(df, "releases.csv")
    return df


# =============================================================
# TRANSFORM: ACTIVITIES / CHANGELOG
# =============================================================

# def transform_activities():
#     """
#     Transform activity changelog:
#       - Map user IDs
#       - Map project keys
#       - Map field values (old/new status, priority names)
#     """
#     print("\n Transforming activities...")

#     # Activity summary
#     df = load_csv("activities.csv")
#     if not df.empty:
#         df["jira_project_key"] = df["project_key"].apply(
#             lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
#         )
#         save_csv(df, "activities.csv")

#     # Activity changes (field-level)
#     df_changes = load_csv("activity_changes.csv")
#     if not df_changes.empty:
#         df_changes["jira_project_key"] = df_changes["project_key"].apply(
#             lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
#         )

#         # Map old/new values for known field types
#         for idx, row in df_changes.iterrows():
#             field = row.get("field_name", "")

#             if field == "status":
#                 df_changes.at[idx, "jira_old_value"] = map_value(
#                     row.get("old_value"), STATUS_MAP, DEFAULT_STATUS
#                 )
#                 df_changes.at[idx, "jira_new_value"] = map_value(
#                     row.get("new_value"), STATUS_MAP, DEFAULT_STATUS
#                 )
#             elif field == "priority":
#                 df_changes.at[idx, "jira_old_value"] = map_value(
#                     row.get("old_value"), PRIORITY_MAP, DEFAULT_PRIORITY
#                 )
#                 df_changes.at[idx, "jira_new_value"] = map_value(
#                     row.get("new_value"), PRIORITY_MAP, DEFAULT_PRIORITY
#                 )
#             elif field == "issue_type":
#                 df_changes.at[idx, "jira_old_value"] = map_value(
#                     row.get("old_value"), ISSUE_TYPE_MAP, DEFAULT_ISSUE_TYPE
#                 )
#                 df_changes.at[idx, "jira_new_value"] = map_value(
#                     row.get("new_value"), ISSUE_TYPE_MAP, DEFAULT_ISSUE_TYPE
#                 )
#             else:
#                 df_changes.at[idx, "jira_old_value"] = row.get("old_value")
#                 df_changes.at[idx, "jira_new_value"] = row.get("new_value")

#         save_csv(df_changes, "activity_changes.csv")

#     return df

# =============================================================
# TRANSFORM: ACTIVITIES / CHANGELOG (Updated for new format)
# =============================================================

def transform_activities():
    """
    Transform activity changelog (new single-file format).
    
    Input: activities.csv with embedded change columns
    Output: activities.csv with mapped values
    """
    print("\n Transforming activities...")

    df = load_csv("activities.csv")
    if df.empty:
        return df

    # Map project
    df["jira_project_key"] = df["project_key"].apply(
        lambda x: map_value(x, PROJECT_MAP, DEFAULT_PROJECT, "unmapped_projects")
    )

    # Map user
    if "user_id" in df.columns:
        df["jira_user_id"] = df.apply(
            lambda row: map_user(row.get("user_name") or row.get("user_id")),
            axis=1
        )

    # Map change values for specific fields
    change_cols = [col for col in df.columns if col.startswith("change_")]
    
    for col in change_cols:
        # Extract field name from column (e.g., "change_status_old"  "status")
        parts = col.replace("change_", "").rsplit("_", 1)
        if len(parts) != 2:
            continue
            
        field_name, direction = parts  # e.g., ("status", "old")
        
        # Map status changes
        if field_name == "status":
            df[f"jira_change_{field_name}_{direction}"] = df[col].apply(
                lambda x: map_value(x, STATUS_MAP, DEFAULT_STATUS) if pd.notna(x) else None
            )
        
        # Map priority changes
        elif field_name == "priority":
            df[f"jira_change_{field_name}_{direction}"] = df[col].apply(
                lambda x: map_value(x, PRIORITY_MAP, DEFAULT_PRIORITY) if pd.notna(x) else None
            )
        
        # Map issue type changes
        elif field_name == "issue_type":
            df[f"jira_change_{field_name}_{direction}"] = df[col].apply(
                lambda x: map_value(x, ISSUE_TYPE_MAP, DEFAULT_ISSUE_TYPE) if pd.notna(x) else None
            )

    # Keep originals
    df["source_activity_id"] = df["activity_id"]
    df["source_issue_id"] = df["issue_id"]
    df["source_project_key"] = df["project_key"]

    save_csv(df, "activities.csv")
    return df

# =============================================================
# BUILD ACTIVITY/CHANGELOG PAYLOAD
# =============================================================

def build_activity_payload(row):
    """
    Convert one activity row  Jira history entry.
    
    NOTE: Jira doesn't allow POST to history - this is for documentation only.
    History is created automatically when you update issues.
    
    This payload shows what WOULD be the change if applied via issue update.
    """
    
    # Collect all changes from this activity
    changes = {}
    
    # Find all change columns
    for col in row.index:
        if col.startswith("change_") and col.endswith("_new"):
            # Extract field name: "change_status_new"  "status"
            field_name = col.replace("change_", "").replace("_new", "")
            old_col = f"change_{field_name}_old"
            
            old_value = row.get(old_col)
            new_value = row.get(col)
            
            # Skip if no actual change
            if pd.isna(new_value) or str(old_value) == str(new_value):
                continue
            
            # Use Jira-mapped values if available
            jira_old_col = f"jira_change_{field_name}_old"
            jira_new_col = f"jira_change_{field_name}_new"
            
            if jira_new_col in row.index:
                new_value = row.get(jira_new_col, new_value)
            if jira_old_col in row.index:
                old_value = row.get(jira_old_col, old_value)
            
            changes[field_name] = {
                "from": safe_str(old_value),
                "to": safe_str(new_value)
            }
    
    return {
        "source_activity_id": safe_str(row.get("source_activity_id") or row.get("activity_id")),
        "source_issue_id": safe_str(row.get("source_issue_id") or row.get("issue_id")),
        "action": safe_str(row.get("action")),
        "user_id": safe_str(row.get("jira_user_id") or row.get("user_id")),
        "user_name": safe_str(row.get("user_name")),
        "created_at": safe_str(row.get("created_at")),
        "changes": changes,
        "note": "History is auto-created by Jira. This is for reference only."
    }


def prepare_activities():
    """
    Build payload for all activities.
    
      NOTE: This is for DOCUMENTATION/REFERENCE only.
    Jira automatically creates history when you update issues.
    You cannot POST directly to /rest/api/3/issue/{key}/changelog
    """
    print("\n Preparing activity payloads (reference only)...")
    df = load_csv("activities.csv")
    if df.empty:
        return []

    # Sort by created_at to maintain chronological order
    if "created_at" in df.columns:
        df = df.sort_values("created_at")

    payloads = []
    for _, row in df.iterrows():
        payload = build_activity_payload(row)
        
        # Only include if there are actual changes
        if payload.get("changes"):
            payloads.append(payload)

    save_json(payloads, "activities.json")
    
    print(f"        Activities saved for reference only")
    print(f"        Jira creates history automatically when you update issues")
    print(f"        Use this file to verify what changed on each issue")
    
    return payloads
    
# =============================================================
# PASS-THROUGH: Copy without transformation
# =============================================================

def passthrough(filename, label):
    """Copy CSV as-is (no mapping needed)."""
    print(f"\n Copying {label}...")
    df = load_csv(filename)
    if not df.empty:
        save_csv(df, filename)
    return df


# =============================================================
# GENERATE TRANSFORM REPORT
# =============================================================

def generate_report():
    """
    Save transformation report showing:
      - What was mapped successfully
      - What was NOT mapped (needs your attention)
    """
    print(f"\n{'━' * 60}")
    print("  TRANSFORM REPORT")
    print(f"{'━' * 60}")

    # Convert sets to lists for JSON serialization
    report_json = {}
    for key, value in report.items():
        if isinstance(value, set):
            report_json[key] = sorted(list(value))
        else:
            report_json[key] = value

    # Save report
    report_path = os.path.join(TRANSFORM_DATA_DIR, "transform_report.json")
    with open(report_path, "w") as f:
        json.dump(report_json, f, indent=4)

    # Print summary
    print(f"\n  Issues transformed:    {report['total_issues_transformed']}")
    print(f"  Comments transformed:  {report['total_comments_transformed']}")

    # Unmapped warnings
    has_warnings = False

    if report["unmapped_projects"]:
        has_warnings = True
        print(f"\n   UNMAPPED PROJECTS ({len(report['unmapped_projects'])}):")
        for v in sorted(report["unmapped_projects"]):
            print(f"      • '{v}'  using default: '{DEFAULT_PROJECT}'")

    if report["unmapped_issue_types"]:
        has_warnings = True
        print(f"\n    UNMAPPED ISSUE TYPES ({len(report['unmapped_issue_types'])}):")
        for v in sorted(report["unmapped_issue_types"]):
            print(f"      • '{v}'  using default: '{DEFAULT_ISSUE_TYPE}'")

    if report["unmapped_statuses"]:
        has_warnings = True
        print(f"\n    UNMAPPED STATUSES ({len(report['unmapped_statuses'])}):")
        for v in sorted(report["unmapped_statuses"]):
            print(f"      • '{v}'  using default: '{DEFAULT_STATUS}'")

    if report["unmapped_priorities"]:
        has_warnings = True
        print(f"\n    UNMAPPED PRIORITIES ({len(report['unmapped_priorities'])}):")
        for v in sorted(report["unmapped_priorities"]):
            print(f"      • '{v}'  using default: '{DEFAULT_PRIORITY}'")

    if report["unmapped_users"]:
        has_warnings = True
        mapped_to = "YOUR account" if MAP_UNMAPPED_USERS_TO_ME else "None (unassigned)"
        print(f"\n    UNMAPPED USERS ({len(report['unmapped_users'])}):")
        for v in sorted(report["unmapped_users"]):
            print(f"      • '{v}'  {mapped_to}")

    if report["unmapped_custom_fields"]:
        has_warnings = True
        print(f"\n    UNMAPPED CUSTOM FIELDS ({len(report['unmapped_custom_fields'])}):")
        for v in sorted(report["unmapped_custom_fields"]):
            print(f"      • '{v}'  SKIPPED")

    if report["skipped_custom_fields"]:
        print(f"\n    SKIPPED CUSTOM FIELDS (set to None in mappings):")
        for v in sorted(report["skipped_custom_fields"]):
            print(f"      • '{v}'")

    if report["unmapped_link_types"]:
        has_warnings = True
        print(f"\n    UNMAPPED LINK TYPES ({len(report['unmapped_link_types'])}):")
        for v in sorted(report["unmapped_link_types"]):
            print(f"      • '{v}'  using default: '{DEFAULT_LINK_TYPE}'")

    if not has_warnings:
        print("\n   All values mapped successfully!")

    print(f"\n   Report saved: {report_path}")
    print(f"{'━' * 60}")


# =============================================================
# MAIN
# =============================================================

def main():
    report["started_at"] = datetime.now().isoformat()

    print("=" * 60)
    print("  TRANSFORM & MAP: Flattened CSV  Jira-Ready CSV")
    print(f"  Input:   {FLAT_DATA_DIR}/")
    print(f"  Output:  {TRANSFORM_DATA_DIR}/")
    print(f"  Time:    {report['started_at']}")
    print("=" * 60)

    # Transform each entity
    transform_projects()
    transform_users()
    transform_issues()
    transform_comments()
    transform_issue_links()
    transform_sprints()
    transform_releases()
    transform_activities()

    # Pass-through (no mapping needed)
    passthrough("issue_types.csv", "Issue Types")
    passthrough("priorities.csv", "Priorities")
    passthrough("statuses.csv", "Statuses")
    passthrough("forms.csv", "Forms")
    passthrough("form_fields.csv", "Form Fields")
    passthrough("test_case_summaries.csv", "Test Cases")
    passthrough("documents_inventory.csv", "Documents")
    passthrough("project_filters.csv", "Project Filters")
    passthrough("dependencies.csv", "Dependencies")
    
    report["completed_at"] = datetime.now().isoformat()

    # Generate report
    generate_report()

    print(f"\n{'=' * 60}")
    print("   TRANSFORM COMPLETE")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()