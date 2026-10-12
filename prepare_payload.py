"""
============================================================
STEP 4: PREPARE JIRA PAYLOAD
============================================================
Reads transformed CSV files from data/transformed/
Converts each row into Jira REST API v3 format
Saves payload JSON files to data/payload/

Handles:
  - Plain text  Atlassian Document Format (ADF)
  - Issue hierarchy (Epic  Story  Sub-task)
  - Custom fields
  - Labels, components, fix versions
  - Sprint assignment
  - Assignee / Reporter
  - Product / Squad cascading field (resolved via sub_project_id  →
    Subproject-to-Squad mapping sheet)
  - Jira content-size limit enforcement + inline attachment links for
    both comments and issue descriptions

Input:  data/transformed/*.csv
Output: data/payload/issues.json
        data/payload/comments.json
        data/payload/links.json
        data/payload/sprints.json
        data/payload/versions.json
        data/payload/payload_summary.json
============================================================
"""

import os
import json
import re
import html
import requests
from attr import fields
import sys
import unicodedata
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
import pandas as pd
from datetime import datetime 
from custom_field_payload import CustomFieldPayloadBuilder
from mappings import (
    DEFAULT_CUSTOM_FIELD,        
    DEFAULT_CUSTOM_FIELD_VALUE,  
    LINK_TYPE_MAP, DEFAULT_LINK_TYPE, DEFAULT_LINK_DIRECTION
)
# =============================================================
# CONFIGURATION
# =============================================================
PROJECT_KEY = os.getenv("project_key")
DATA_ROOT   = f"{PROJECT_KEY}_data" if PROJECT_KEY else "data"

TRANSFORM_DATA_DIR = f"{DATA_ROOT}/transformed"
PAYLOAD_DATA_DIR = f"{DATA_ROOT}/payload"
ATTACHMENT_MAPPING_FILE = f"{DATA_ROOT}/mapping/attachment_mapping.json"
from mappings import SOURCE_KEY_CUSTOM_FIELD, START_DATE_FIELD, FW_BUG_FIELD, BUG_CATEGORY_MAP

def get_story_points_field() -> str:
    """
    Get story points Jira field ID from CF builder mapping.
    Reads from mappings.xlsx via CustomFieldPayloadBuilder.
    """
    builder = get_cf_builder()
    field_id = builder.cf_mapping.get("story_points")
    
    if field_id:
        return field_id
    
    # Fallback warning
    print("  WARNING: 'story_points' not found in mappings.xlsx custom_field sheet")
    print("  Add this row: story_points | customfield_XXXXX")
    return "story_points"

os.makedirs(PAYLOAD_DATA_DIR, exist_ok=True)

INCLUDE_ASSIGNEE = True
INCLUDE_REPORTER = True
INCLUDE_LABELS = True
INCLUDE_DUE_DATE = True
INCLUDE_STORY_POINTS = True
INCLUDE_COMPONENTS = False
INCLUDE_FIX_VERSIONS = False
INCLUDE_CUSTOM_FIELDS = True
INCLUDE_SOURCE_KEY = True
INCLUDE_START_DATE = True
USER_MAPPING_FILE = "mappings.xlsx"

# =============================================================
# HELPERS Funtion
# =============================================================
CF_BUILDER = None

def _fix_start_before_end(start_str, end_str, date_fmt="%Y-%m-%d"):
    """Ensure start_str is strictly before end_str. If not, pull start back to end - 1 day."""
    try:
        start_dt = datetime.strptime(start_str, date_fmt)
        end_dt = datetime.strptime(end_str, date_fmt)
    except ValueError:
        return start_str  # unparsable, leave untouched

    if start_dt >= end_dt:
        new_start = (end_dt - timedelta(days=1)).strftime(date_fmt)
        print(f"  NOTE: start date ({start_str}) was not before end date ({end_str}) — start date changed to {new_start}")
        return new_start
    return start_str

def get_cf_builder():
    global CF_BUILDER
    if CF_BUILDER is None:
        CF_BUILDER = CustomFieldPayloadBuilder(
            jira_url       = os.getenv("JIRA_URL"),
            jira_email     = os.getenv("JIRA_EMAIL"),
            jira_api_token = os.getenv("JIRA_API_TOKEN"),
        )
        CF_BUILDER.load()

        # Runtime child map for the 'sub_project' cascading field, fetched
        # from the external sub-projects API/cache (kept outside Excel since
        # it changes independently of the migration mapping sheet). Only
        # takes effect if 'sub_project' is configured in the excel
        # 'cascading_field' sheet with a blank child_map_sheet.
        CF_BUILDER.set_cascading_child_map("sub_project", SUBPROJECT_MAP)
    return CF_BUILDER

def load_user_mapping():
    if not os.path.exists(USER_MAPPING_FILE):
        print("User mapping file not found")
        return {}

    df = pd.read_excel(USER_MAPPING_FILE, sheet_name="user")

    mapping = {}

    for _, row in df.iterrows():
        name = safe_str(row.get("source_name"))
        account_id = safe_str(row.get("jira_account_id"))

        if name and account_id:
            mapping[normalize_name(name)] = account_id

    print(f"Loaded {len(mapping)} user mappings from 'user' sheet")
    return mapping


def load_sprint_mapping():
    if not os.path.exists(USER_MAPPING_FILE):
        print("Sprint mapping file not found")
        return {}

    df = pd.read_excel(USER_MAPPING_FILE, sheet_name="sprint")

    mapping = {}

    for _, row in df.iterrows():
        jira_project_key = safe_str(row.get("jira_project_key"))
        board_id = safe_int(row.get("jira_board_id"))

        if jira_project_key and board_id:
            mapping[jira_project_key] = board_id

    print(f"Loaded {len(mapping)} sprint board mappings: {mapping}")
    return mapping

SUBPROJECT_CACHE_FILE = f"{DATA_ROOT}/mapping/subprojects_cache.json"

def load_subprojects_from_api():
    """
    Fetch all subprojects from API and APPEND into local cache file.
    
    - If cache file exists → load existing data first
    - Fetch from API → merge new entries into existing data
    - Save merged data back to file
    - Always returns the full merged dict
    """
    base_url    = os.getenv("BASE_URL")
    auth_token  = os.getenv("AUTH_TOKEN")
    project_key = os.getenv("project_key")

    os.makedirs(os.path.dirname(SUBPROJECT_CACHE_FILE), exist_ok=True)

    # ── Step 1: Load existing cache if it exists ─────────────
    existing = {}
    if os.path.exists(SUBPROJECT_CACHE_FILE):
        try:
            with open(SUBPROJECT_CACHE_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
            print(f"  Loaded {len(existing)} existing subprojects from cache")
        except Exception as e:
            print(f"  WARNING: Could not read cache file: {e} — starting fresh")
            existing = {}

    # ── Step 2: Fetch from API ────────────────────────────────
    if not base_url or not auth_token or not project_key:
        print("  WARNING: BASE_URL / AUTH_TOKEN / project_key not set")
        print("  Using cached subprojects only")
        return existing

    try:
        url     = f"{base_url.rstrip('/')}/{project_key}/sub_projects"
        headers = {
            "Authorization": f"Token {auth_token}",
            "Content-Type":  "application/json"
        }

        print(f"  Fetching subprojects from API: {url}")
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"  WARNING: API returned {response.status_code} — using cache only")
            return existing

        data         = response.json()
        sub_projects = data.get("sub_projects", data)

        # ── Step 3: Build new entries from API response ───────
        new_entries = {}
        for sp in sub_projects:
            sp_id   = str(sp.get("id", "")).strip().split(".")[0]  # strip .0 here too
            sp_name = str(sp.get("name", "")).strip()
            if sp_id and sp_name:
                new_entries[sp_id] = sp_name

        print(f"  Fetched {len(new_entries)} subprojects from API")

        # ── Step 4: Merge — API data wins on conflict ─────────
        before_count = len(existing)
        existing.update(new_entries)
        added = len(existing) - before_count

        print(f"  Merged: {added} new entries added, {len(existing)} total")

        # ── Step 5: Append/save merged data back to file ──────
        with open(SUBPROJECT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=4, ensure_ascii=False)

        print(f"  Saved {len(existing)} subprojects → {SUBPROJECT_CACHE_FILE}")
        print(f"  Preview: {dict(list(existing.items())[:5])}")

        return existing

    except Exception as e:
        print(f"  ERROR fetching subprojects: {e}")
        print(f"  Falling back to cached data ({len(existing)} entries)")
        return existing
def group_related_data_by_issue(issues, comments, links, attachments_mapping):
    """
    Group all related data by source_issue_id for parallel loading.
    
    Returns:
        dict: {
            source_issue_id: {
                "issue": {...},
                "comments": [...],
                "links": [...],
                "attachments": [...]
            }
        }
    """
    print("\nGrouping related data by issue...")
    
    # Initialize lookup dictionaries
    comments_by_issue = {}
    links_by_issue = {}
    attachments_by_issue = {}
    
    # Group comments
    for comment in comments:
        issue_id = comment.get("source_issue_id")
        if issue_id:
            comments_by_issue.setdefault(issue_id, []).append(comment)
    
    # Group links (where this issue is the source)
    for link in links:
        issue_id = link.get("source_issue_id")
        if issue_id:
            links_by_issue.setdefault(issue_id, []).append(link)
    
    comment_to_issue = {}
    for comment in comments:
        comment_id = comment.get("source_comment_id")
        issue_id = comment.get("source_issue_id")
        if comment_id and issue_id:
            comment_to_issue[comment_id] = issue_id
    
    # Then group attachments by issue
    attachments_by_comment = attachments_mapping.get("by_source_comment_id", {})
    for comment_id, attachment_list in attachments_by_comment.items():
        issue_id = comment_to_issue.get(comment_id)
        if issue_id:
            attachments_by_issue.setdefault(issue_id, []).extend(attachment_list)
    
    # Build final grouped structure
    grouped_data = {}
    
    for issue in issues:
        issue_id = issue.get("source_issue_id")
        if not issue_id:
            continue
        
        grouped_data[issue_id] = {
            "issue": issue,
            "comments": comments_by_issue.get(issue_id, []),
            "links": links_by_issue.get(issue_id, []),
            "attachments": attachments_by_issue.get(issue_id, []),
        }
    
    # Statistics
    total_comments = sum(len(v["comments"]) for v in grouped_data.values())
    total_links = sum(len(v["links"]) for v in grouped_data.values())
    total_attachments = sum(len(v["attachments"]) for v in grouped_data.values())
    
    print(f"     Grouped {len(grouped_data)} issues")
    print(f"      Comments:    {total_comments}")
    print(f"      Links:       {total_links}")
    print(f"      Attachments: {total_attachments}")
    
    return grouped_data
def save_grouped_payload(grouped_data):
    """
    Save grouped payload for parallel loading — ONE FILE PER ISSUE instead
    of a single giant array (avoids the OOM kill from loading a multi-GB
    issues_with_related.json into memory in one shot on the load side).

    Output:
        data/payload/issues_with_related/{source_issue_id}.json   (one per issue)
        data/payload/issues_with_related_index.json               (tiny — IDs + order)
    """
    print("\nSaving grouped payload (per-issue files)...")

    related_dir = os.path.join(PAYLOAD_DATA_DIR, "issues_with_related")
    os.makedirs(related_dir, exist_ok=True)

    ordered_issue_ids = sorted(
        grouped_data.keys(),
        key=lambda iid: grouped_data[iid]["issue"].get("migration_sequence", 999999),
    )

    total_comments = 0
    total_links = 0
    total_attachments = 0

    for issue_id in ordered_issue_ids:
        data = grouped_data[issue_id]
        record = {
            "source_issue_id": issue_id,
            "issue":           data["issue"],
            "comments":        data["comments"],
            "links":           data["links"],
            "attachments":     data["attachments"],
            "related_counts": {
                "comments":    len(data["comments"]),
                "links":       len(data["links"]),
                "attachments": len(data["attachments"]),
            },
        }
        total_comments    += len(data["comments"])
        total_links       += len(data["links"])
        total_attachments += len(data["attachments"])

        issue_filepath = os.path.join(related_dir, f"{issue_id}.json")
        with open(issue_filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)

    index_filepath = os.path.join(PAYLOAD_DATA_DIR, "issues_with_related_index.json")
    with open(index_filepath, "w", encoding="utf-8") as f:
        json.dump(
            {
                "issue_ids": ordered_issue_ids,
                "count": len(ordered_issue_ids),
                "related_dir": "issues_with_related",
            },
            f, indent=2, ensure_ascii=False,
        )

    print(f"     Saved {len(ordered_issue_ids)} issues with related data")
    print(f"      Comments:    {total_comments}")
    print(f"      Links:       {total_links}")
    print(f"      Attachments: {total_attachments}")
    print(f"     Per-issue files: {related_dir}/")
    print(f"     Index file:      {index_filepath}")

    # IMPORTANT: this now returns a dict, not a list of full record dicts —
    # main()'s summary-building code needs to read it as such (see below).
    return {
        "issue_ids": ordered_issue_ids,
        "total_comments": total_comments,
        "total_links": total_links,
        "total_attachments": total_attachments,
    }

def load_csv(filename):
    """Load CSV from transformed directory."""
    filepath = os.path.join(TRANSFORM_DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"      {filename} not found, skipping")
        return pd.DataFrame()
    df = pd.read_csv(
        filepath,
        encoding="utf-8-sig",
        low_memory=False,
        keep_default_na=False,  # stop "NA"/"NULL"/etc. being read as NaN
        na_values=[],
    )
    df = df.where(pd.notna(df), None)
    print(f"     Loaded {filename}: {len(df)} rows")
    return df


def save_json(data, filename):
    """Save payload JSON file."""
    filepath = os.path.join(PAYLOAD_DATA_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"     {filename} — {len(data)} records")
    return filepath


def is_empty(value):
    """Check if a value is empty/null."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def safe_str(value):
    """Convert value to string safely."""
    if is_empty(value):
        return ""
    return str(value).strip()


def safe_int(value):
    """Convert value to int safely."""
    if is_empty(value):
        return None
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def safe_float(value):
    """Convert value to float safely."""
    if is_empty(value):
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None

def normalize_name(name):
    if not name:
        return ""
    return re.sub(r'\s+', ' ', str(name).strip().lower())

USER_MAP = load_user_mapping()
SPRINT_MAP = load_sprint_mapping()
#SPRINT_BOARD_ID = load_sprint_mapping()  # rename for clarity
# LINK_TYPE_MAP, DEFAULT_LINK_TYPE, DEFAULT_LINK_DIRECTION = load_link_type_mapping()
# Runtime child map (id -> name) for the 'sub_project' cascading field.
# Parent-side mapping (project key -> parent value) for both sub_project
# and theme now lives in mappings.xlsx ('cascading_field' sheet + the
# parent_map_sheet it points to), loaded inside CustomFieldPayloadBuilder.
SUBPROJECT_MAP = load_subprojects_from_api()
FIELD_TYPE_LOOKUP_FILE = os.path.join(DATA_ROOT, "mapping", "field_type_lookup.json")
# PRODUCT_SQUAD_FIELD_ID = "customfield_10973"
# JIRA_PRODUCT_VALUE, JIRA_SQUAD_VALUE = get_product_squad_from_env()
PRODUCT_SQUAD_FIELD_ID = "customfield_10973"
SUBPROJECT_SQUAD_SHEET = "Subproject to Squad Mapping"
# Values in the "FR Subproject" column that mean "this project has no
# subprojects" — collapsed to the same fallback key ("") so they match
# issues where sub_project_id is empty/unresolved.
NO_SUBPROJECT_LABELS = {"", "no subprojects for this fr project"}
# Literal placeholder text used in the "Jira Product" column of the
# Subproject to Squad Mapping sheet to mean "no product — don't create
# the field". Treated identically to an actually-blank cell.
# Literal placeholder text used in the Subproject to Squad Mapping sheet
# to mean "this column has no real value" — treated identically to an
# actually-blank cell. Product and Squad columns use different marker text.
MIGRATE_EMPTY_MARKERS = {
    "migrate with product value empty",
    "migrate with squad value empty",
}
FIELD_TYPE_LOOKUP_FILE = os.path.join(DATA_ROOT, "mapping", "field_type_lookup.json")


def classify_field(field):
    schema = field.get("schema", {}) or {}
    custom_type = schema.get("custom", "")
    base_type = schema.get("type", "")

    if custom_type.endswith(":textfield"):
        return "short_text"
    if custom_type.endswith(":textarea"):
        return "long_text"
    if base_type == "string" and not custom_type:
        return "short_text"
    return "other"


def build_field_type_lookup():
    """
    Fetches every field's schema from Jira and builds a fresh lookup —
    runs automatically every time prepare_payload.py runs, so newly
    created custom fields in Jira are picked up with zero manual steps.
    Falls back to whatever's cached on disk if the API call fails
    (network issue, credentials not ready yet, etc.) rather than blocking
    the whole payload build.
    """
    jira_url   = os.getenv("JIRA_URL")
    jira_email = os.getenv("JIRA_EMAIL")
    jira_token = os.getenv("JIRA_API_TOKEN")

    if not (jira_url and jira_email and jira_token):
        print("  [Field Types] JIRA_URL/EMAIL/TOKEN not set — using cached lookup if available")
        return load_cached_field_type_lookup()

    try:
        resp = requests.get(
            f"{jira_url}/rest/api/3/field",
            headers={"Accept": "application/json"},
            auth=(jira_email, jira_token),
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  [Field Types] Fetch failed (HTTP {resp.status_code}) — using cached lookup")
            return load_cached_field_type_lookup()

        fields = resp.json()
        lookup = {}
        counts = {"short_text": 0, "long_text": 0, "other": 0}

        for f in fields:
            fid = f.get("id")
            if not fid:
                continue
            ftype = classify_field(f)
            lookup[fid] = {
                "type":   ftype,
                "name":   f.get("name"),
                "custom": (f.get("schema") or {}).get("custom", ""),
            }
            counts[ftype] += 1

        os.makedirs(os.path.dirname(FIELD_TYPE_LOOKUP_FILE), exist_ok=True)
        with open(FIELD_TYPE_LOOKUP_FILE, "w", encoding="utf-8") as f:
            json.dump(lookup, f, indent=2)

        print(f"  [Field Types] Refreshed from Jira: {len(lookup)} fields "
              f"(short_text={counts['short_text']}, long_text={counts['long_text']}, "
              f"other={counts['other']})")
        return lookup

    except Exception as e:
        print(f"  [Field Types] Fetch error ({e}) — using cached lookup if available")
        return load_cached_field_type_lookup()


def load_cached_field_type_lookup():
    if os.path.exists(FIELD_TYPE_LOOKUP_FILE):
        with open(FIELD_TYPE_LOOKUP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    print("  [Field Types] No cached lookup found either — trimming will be skipped for unresolved fields")
    return {}


FIELD_TYPE_LOOKUP = build_field_type_lookup()


def get_field_type(cf_id):
    entry = FIELD_TYPE_LOOKUP.get(cf_id)
    return entry["type"] if entry else None


def trim_custom_field_value(cf_id, cf_value, row_issue_id):
    field_type = get_field_type(cf_id)

    if isinstance(cf_value, dict) and cf_value.get("type") == "doc":
        length = _adf_text_length(cf_value)
        if length > JIRA_CONTENT_CHAR_LIMIT:
            print(f"  [WARN] {cf_id} (long_text/ADF) trimmed {length:,} -> "
                  f"{JIRA_CONTENT_CHAR_LIMIT:,} chars (issue {row_issue_id})")
            return enforce_adf_char_limit(cf_value, max_chars=JIRA_CONTENT_CHAR_LIMIT)
        return cf_value

    if isinstance(cf_value, str):
        if field_type == "short_text":
            limit = 255
        elif field_type == "long_text":
            limit = JIRA_CONTENT_CHAR_LIMIT
        else:
            if len(cf_value) > 255:
                print(f"  [WARN] {cf_id} has UNRESOLVED type and is {len(cf_value):,} "
                      f"chars (issue {row_issue_id}) — NOT trimmed, may fail on Jira")
            return cf_value

        if len(cf_value) > limit:
            print(f"  [WARN] {cf_id} ({field_type}) trimmed {len(cf_value):,} -> "
                  f"{limit:,} chars (issue {row_issue_id})")
            return cf_value[:limit]
        return cf_value

    return cf_value

def _is_migrate_empty_marker(value):
    """Treat literal 'Migrate with Product/Squad value empty' sheet text as blank."""
    return safe_str(value).strip().lower() in MIGRATE_EMPTY_MARKERS

def load_parent_project_map():
    """
    mappings.xlsx -> 'parent_field' sheet: fw_project_key | parent_project
    Returns {source_key (upper): parent_project_value}
    """
    if not os.path.exists(USER_MAPPING_FILE):
        print("  [FR Project] mappings.xlsx not found — PARENT_PROJECT_MAP empty")
        return {}
    try:
        df = pd.read_excel(USER_MAPPING_FILE, sheet_name="parent_field", dtype=str)
    except Exception as e:
        print(f"  [FR Project] No 'parent_field' sheet found ({e})")
        return {}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    key_col, val_col = df.columns[0], df.columns[1]   # fw_project_key, parent_project

    mapping = {}
    for _, row in df.iterrows():
        k = safe_str(row.get(key_col))
        v = safe_str(row.get(val_col))
        if k and v:
            mapping[k.upper()] = v
    print(f"  [FR Project] Loaded {len(mapping)} entries from 'parent_field' sheet")
    return mapping

PARENT_PROJECT_MAP = load_parent_project_map()

def load_subproject_squad_mapping():
    """
    mappings.xlsx -> 'Subproject to Squad Mapping' sheet:
        FR Project Key | FR Subproject | Jira Product | Jira Squad

    Returns:
        mapping: { (project_key_lower, subproject_key_lower): (product, squad) }
                 subproject_key is "" for the project-level "no subprojects" row.
        projects_with_subprojects: set of project_key_lower values that have
                 at least one row with a REAL (non-blank, non-placeholder)
                 subproject name — i.e. confirmed to have subprojects.
    """
    if not os.path.exists(USER_MAPPING_FILE):
        print(f"  [Product/Squad] {USER_MAPPING_FILE} not found — SUBPROJECT_SQUAD_MAP empty")
        return {}, set()

    try:
        df = pd.read_excel(USER_MAPPING_FILE, sheet_name=SUBPROJECT_SQUAD_SHEET, dtype=str)
    except Exception as e:
        print(f"  [Product/Squad] No '{SUBPROJECT_SQUAD_SHEET}' sheet found ({e})")
        return {}, set()

    df.columns = [c.strip() for c in df.columns]

    mapping = {}
    projects_with_subprojects = set()

    for _, row in df.iterrows():
        project_key = safe_str(row.get("FR Project Key"))
        subproject  = safe_str(row.get("FR Subproject"))
        product     = safe_str(row.get("Jira Product"))
        squad       = safe_str(row.get("Jira Squad"))

        if not project_key:
            continue

        subproject_norm = subproject.strip().lower()
        is_no_subproject_marker = subproject_norm in NO_SUBPROJECT_LABELS

        if subproject_norm and not is_no_subproject_marker:
            # Real named subproject -> this project DOES have subprojects
            projects_with_subprojects.add(project_key.lower())

        if not product and not squad:
            continue

        subproject_key = "" if is_no_subproject_marker else subproject_norm
        key = (project_key.lower(), subproject_key)
        mapping[key] = (product, squad)

    print(f"  [Product/Squad] Loaded {len(mapping)} entries, "
          f"{len(projects_with_subprojects)} projects confirmed to have subprojects")
    return mapping, projects_with_subprojects

SUBPROJECT_SQUAD_MAP, PROJECTS_WITH_SUBPROJECTS = load_subproject_squad_mapping()

# Tracks Product/Squad resolution outcomes across the full run, printed
# as a summary at the end of prepare_issues().
PRODUCT_SQUAD_STATS = {
    "total_issues": 0,
    "sub_project_id_present": 0,       # row has a non-empty sub_project_id
    "resolved_via_cache": 0,           # sub_project_id present AND found in SUBPROJECT_MAP
    "unresolved_sub_project_id": 0,    # sub_project_id present but NOT in SUBPROJECT_MAP (bad id)
    "no_sub_project_id": 0,            # row has no sub_project_id at all
    "field_created": 0,                # customfield_10973 ended up in the payload
    "skipped_no_fallback": 0,          # project has subprojects, issue has none -> skipped
    "skipped_no_mapping": 0,           # no matching row in the sheet at all
    "skipped_product_blank": 0,        # mapping row found but Jira Product empty
}

def load_attachment_mapping() -> dict:
    """
    Load attachment_mapping.json produced by upload_attachments.py.
 
    Returns the full dict, or an empty skeleton if the file is missing.
    """
    if not os.path.exists(ATTACHMENT_MAPPING_FILE):
        print(f"  WARNING: {ATTACHMENT_MAPPING_FILE} not found.")
        print("  Run upload_attachments.py before prepare_payload.py.")
        return {"by_local_path": {}, "by_source_comment_id": {}}
 
    with open(ATTACHMENT_MAPPING_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def build_code_block_node(code_text: str, language: str = "") -> dict:
    lines = code_text.split("\n")
    cleaned_lines = [line.rstrip() for line in lines]
    while cleaned_lines and not cleaned_lines[0].strip():
        cleaned_lines.pop(0)
    while cleaned_lines and not cleaned_lines[-1].strip():
        cleaned_lines.pop()
    joined = "\n".join(cleaned_lines)
    if not joined:
        return None
    return {
        "type": "codeBlock",
        "attrs": {"language": language},
        "content": [{"type": "text", "text": joined}]
    }

# =============================================================
# ATLASSIAN DOCUMENT FORMAT (ADF) CONVERTERS
# =============================================================

def html_to_adf(html_content):
    """
    Convert HTML content to Atlassian Document Format (ADF).

    Handles:
      - <p> paragraphs
      - <b>, <strong> bold text
      - <i>, <em> italic text
      - <code> inline code
      - <pre> code blocks
      - <ul>, <ol> lists
      - <li> list items
      - <h1> to <h6> headings
      - <br> line breaks
      - Plain text fallback
    """
    if is_empty(html_content):
        return make_adf_doc([make_adf_paragraph("No description provided.")])

    content_str = str(html_content).strip()

    if not bool(re.search(r'<[a-zA-Z]', content_str)):
        return text_to_adf(content_str)

    try:
        nodes = []

        content_str = html.unescape(content_str)

        code_blocks = []
        def save_code_block(m):
            code_blocks.append(m.group(1))
            return f"__CODEBLOCK_{len(code_blocks)-1}__"
        content_str = re.sub(r'<pre[^>]*>(.*?)</pre>', save_code_block, content_str, flags=re.DOTALL)

        blocks = re.split(
            r'(<h[1-6][^>]*>.*?</h[1-6]>|<p[^>]*>.*?</p>|<ul[^>]*>.*?</ul>|<ol[^>]*>.*?</ol>)',
            content_str,
            flags=re.DOTALL
        )

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            # ── FIX: split block on __CODEBLOCK_N__ tokens first ──
            # This handles the case where a codeblock placeholder and
            # trailing text (e.g. a link) end up in the same block
            sub_blocks = re.split(r'(__CODEBLOCK_\d+__)', block)

            for sub_block in sub_blocks:
                sub_block = sub_block.strip()
                if not sub_block:
                    continue

                # ── Codeblock placeholder ──
                code_match = re.match(r'__CODEBLOCK_(\d+)__', sub_block)
                if code_match:
                    idx = int(code_match.group(1))
                    code_text = re.sub(r'<[^>]+>', '', code_blocks[idx])
                    code_text = html.unescape(code_text)
                    node = build_code_block_node(code_text)
                    if node:
                        nodes.append(node)
                    continue

                # ── Heading ──
                h_match = re.match(r'<h([1-6])[^>]*>(.*?)</h[1-6]>', sub_block, re.DOTALL)
                if h_match:
                    level = int(h_match.group(1))
                    text = strip_tags(h_match.group(2))
                    if text:
                        nodes.append({
                            "type": "heading",
                            "attrs": {"level": level},
                            "content": [{"type": "text", "text": text}]
                        })
                    continue

                # ── Bullet list ──
                if re.match(r'<ul', sub_block, re.IGNORECASE):
                    items = re.findall(r'<li[^>]*>(.*?)</li>', sub_block, re.DOTALL)
                    if items:
                        list_items = []
                        for item in items:
                            item_text = strip_tags(item).strip()
                            if item_text:
                                list_items.append({
                                    "type": "listItem",
                                    "content": [make_adf_paragraph(item_text)]
                                })
                        if list_items:
                            nodes.append({
                                "type": "bulletList",
                                "content": list_items
                            })
                    continue

                # ── Ordered list ──
                if re.match(r'<ol', sub_block, re.IGNORECASE):
                    items = re.findall(r'<li[^>]*>(.*?)</li>', sub_block, re.DOTALL)
                    if items:
                        list_items = []
                        for item in items:
                            item_text = strip_tags(item).strip()
                            if item_text:
                                list_items.append({
                                    "type": "listItem",
                                    "content": [make_adf_paragraph(item_text)]
                                })
                        if list_items:
                            nodes.append({
                                "type": "orderedList",
                                "content": list_items
                            })
                    continue

                # ── Paragraph ──
                p_match = re.match(r'<p[^>]*>(.*?)</p>', sub_block, re.DOTALL)
                if p_match:
                    inline_content = parse_inline(p_match.group(1))
                    if inline_content:
                        nodes.append({
                            "type": "paragraph",
                            "content": inline_content
                        })
                    continue

                # ── Plain text / inline HTML (links, bold, etc) ──
                plain_inline = parse_inline(sub_block)
                if plain_inline:
                    nodes.append({
                        "type": "paragraph",
                        "content": plain_inline
                    })

        if not nodes:
            nodes = [make_adf_paragraph(strip_tags(content_str))]

        return make_adf_doc(nodes)

    except Exception:
        plain = strip_tags(content_str)
        return text_to_adf(plain)
def text_to_adf(text):
    if is_empty(text):
        return make_adf_doc([make_adf_paragraph("No description provided.")])
 
    raw = str(text).strip()
    nodes = []
 
    parts = re.split(r'(```[\w]*\n.*?```)', raw, flags=re.DOTALL)
 
    for part in parts:
        if not part.strip():
            continue
 
        fence_match = re.match(r'```(\w*)\n(.*?)```', part, re.DOTALL)
        if fence_match:
            lang      = fence_match.group(1).strip()
            code_text = fence_match.group(2)
            node = build_code_block_node(code_text, language=lang)
            if node:
                nodes.append(node)
            continue
 
        # Regular paragraph lines
        for line in part.split('\n'):
            line = line.strip()
            if line:
                inline_nodes = parse_text_with_links(line)
                nodes.append({
                    "type": "paragraph",
                    "content": inline_nodes if inline_nodes else []
                })
 
    if not nodes:
        nodes = [make_adf_paragraph("No description provided.")]
 
    return make_adf_doc(nodes)

def tokenize_mentions_and_text(text):
    if is_empty(text):
        return []

    pattern = r'@([A-Za-z0-9._\-]+(?:\s+[A-Za-z0-9._\-]+)*?)(?=\s+(?:to|on|in|at)\b|$)'

    nodes = []
    last_index = 0

    for match in re.finditer(pattern, text):
        start, end = match.span()
        raw_name = match.group(1).strip()

        if start > last_index:
            nodes.append({
                "type": "text",
                "text": text[last_index:start]
            })

        normalized = normalize_name(raw_name)
        account_id = USER_MAP.get(normalized)

        if account_id:
            nodes.append({
                "type": "mention",
                "attrs": {
                    "id": account_id,
                    "text": f"@{raw_name}"
                }
            })
        else:
            nodes.append({
                "type": "text",
                "text": f"@{raw_name}"
            })

        last_index = end

    if last_index < len(text):
        nodes.append({
            "type": "text",
            "text": text[last_index:]
        })

    return nodes

def parse_inline(html_text):
    if is_empty(html_text):
        return []

    content = []

    parts = re.split(
        r'(<a [^>]*>.*?</a>'
        r'|<strong><em>.*?</em></strong>'
        r'|<em><strong>.*?</strong></em>'
        r'|<b><i>.*?</i></b>'
        r'|<i><b>.*?</b></i>'
        r'|<strong>.*?</strong>'
        r'|<b>.*?</b>'
        r'|<em>.*?</em>'
        r'|<i>.*?</i>'
        r'|<code>.*?</code>'
        r'|<span[^>]*class="fr-class-code"[^>]*>.*?</span>'
        r'|<span[^>]*class="[^"]*at-users[^"]*"[^>]*>.*?</span>'   # ← ADD THIS
        r'|<span[^>]*>.*?</span>'
        r'|<br\s*/?>)',
        html_text,
        flags=re.DOTALL | re.IGNORECASE
    )

    for part in parts:
        if not part.strip() and part != " ":
            continue

        part_clean = part.strip()

        # ── Line break ──
        if re.match(r'<br\s*/?>', part_clean, re.IGNORECASE):
            content.append({"type": "hardBreak"})
            continue

        # ── Link ──
        link_match = re.match(r'<a [^>]*href=["\'](.*?)["\'][^>]*>(.*?)</a>', part_clean, re.IGNORECASE | re.DOTALL)
        if link_match:
            url = link_match.group(1).strip()
            link_text = strip_tags(link_match.group(2)).strip() or url
            content.append({
                "type": "text",
                "text": link_text,
                "marks": [{"type": "link", "attrs": {"href": url}}]
            })
            continue

        # ── Bold + Italic combined <strong><em> or <em><strong> ──
        bold_italic_match = re.match(
            r'<(?:strong><em|em><strong)>(.*?)</(?:em></strong|strong></em)>',
            part_clean, re.IGNORECASE | re.DOTALL
        )
        if bold_italic_match:
            text = strip_tags(bold_italic_match.group(1))
            if text:
                content.append({
                    "type": "text",
                    "text": text,
                    "marks": [{"type": "strong"}, {"type": "em"}]
                })
            continue

        # ── Bold ──
        b_match = re.match(r'<(?:b|strong)>(.*?)</(?:b|strong)>', part_clean, re.IGNORECASE | re.DOTALL)
        if b_match:
            text = strip_tags(b_match.group(1))
            if text:
                content.append({
                    "type": "text",
                    "text": text,
                    "marks": [{"type": "strong"}]
                })
            continue

        # ── Italic ──
        i_match = re.match(r'<(?:i|em)>(.*?)</(?:i|em)>', part_clean, re.IGNORECASE | re.DOTALL)
        if i_match:
            text = strip_tags(i_match.group(1))
            if text:
                content.append({
                    "type": "text",
                    "text": text,
                    "marks": [{"type": "em"}]
                })
            continue

        # ── Inline code span (fr-class-code) or <code> ──
        code_span_match = re.match(
            r'<span[^>]*class="fr-class-code"[^>]*>(.*?)</span>',
            part_clean, re.IGNORECASE | re.DOTALL
        )
        if code_span_match:
            text = strip_tags(code_span_match.group(1))
            if text:
                content.append({
                    "type": "text",
                    "text": text,
                    "marks": [{"type": "code"}]
                })
            continue

        code_match = re.match(r'<code>(.*?)</code>', part_clean, re.IGNORECASE | re.DOTALL)
        if code_match:
            text = strip_tags(code_match.group(1))
            if text:
                content.append({
                    "type": "text",
                    "text": text,
                    "marks": [{"type": "code"}]
                })
            continue

        # ── Inline code span (fr-class-code) ──
        code_span_match = re.match(
            r'<span[^>]*class="fr-class-code"[^>]*>(.*?)</span>',
            part_clean, re.IGNORECASE | re.DOTALL
        )
        if code_span_match:
            text = strip_tags(code_span_match.group(1))
            if text:
                content.append({
                    "type": "text",
                    "text": text,
                    "marks": [{"type": "code"}]
                })
            continue

        # ── @mention span (at-users) ──          ← ADD THIS BLOCK
        mention_span_match = re.match(
            r'<span[^>]*class="[^"]*at-users[^"]*"[^>]*data-id="(\d+)"[^>]*>(.*?)</span>',
            part_clean, re.IGNORECASE | re.DOTALL
        )
        if mention_span_match:
            mention_text = strip_tags(mention_span_match.group(2)).strip()
            raw_name     = mention_text.lstrip("@").strip()
            normalized   = normalize_name(raw_name)
            account_id   = USER_MAP.get(normalized)

            if account_id:
                content.append({
                    "type": "mention",
                    "attrs": {
                        "id": account_id,
                        "text": mention_text if mention_text.startswith("@") else f"@{raw_name}"
                    }
                })
            else:
                print(f"  [WARN] @mention '{raw_name}' not found in USER_MAP — rendering as text")
                content.append({
                    "type": "text",
                    "text": mention_text if mention_text.startswith("@") else f"@{raw_name}"
                })
            continue

        # ── Generic span (strip tag, keep text) ──
        span_match = re.match(r'<span[^>]*>(.*?)</span>', part_clean, re.IGNORECASE | re.DOTALL)
        if span_match:
            text = strip_tags(span_match.group(1))
            if text:
                content.extend(tokenize_mentions_and_text(text))
            continue

        # ── Plain text ──
        plain = strip_tags(part)
        if plain.strip():
            content.extend(tokenize_mentions_and_text(plain))

    return content

def strip_tags(html_text):
    """Remove all HTML tags from a string."""
    if is_empty(html_text):
        return ""
    clean = re.sub(r'<[^>]+>', ' ', str(html_text))
    clean = html.unescape(clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def make_adf_doc(nodes):
    """Create root ADF document node."""
    return {
        "version": 1,
        "type": "doc",
        "content": nodes if nodes else [make_adf_paragraph("")]
    }


def make_adf_paragraph(text=""):
    """Create ADF paragraph node."""
    if not text or not text.strip():
        return {"type": "paragraph", "content": []}
    return {
        "type": "paragraph",
        "content": [{"type": "text", "text": str(text)}]
    }


# =============================================================
# JIRA COMMENT/DESCRIPTION SIZE LIMIT
# =============================================================
# Jira Cloud rejects a comment/description body once its total
# rendered text exceeds ~32,767 characters, returning:
#   {"comment": "CONTENT_LIMIT_EXCEEDED"}
# We stay well under that with a safety margin (formatting/markup
# overhead in ADF isn't 1:1 with visible character count).
JIRA_CONTENT_CHAR_LIMIT = 28000


def _adf_text_length(node) -> int:
    """Recursively sum the length of every 'text' field in an ADF node/tree."""
    total = 0
    if isinstance(node, dict):
        if node.get("type") == "text":
            total += len(node.get("text", ""))
        for v in node.values():
            total += _adf_text_length(v)
    elif isinstance(node, list):
        for item in node:
            total += _adf_text_length(item)
    return total


def enforce_adf_char_limit(adf_body: dict, max_chars: int = JIRA_CONTENT_CHAR_LIMIT) -> dict:
    """
    If an ADF document's total text length exceeds max_chars, truncate
    it at a top-level node boundary (paragraph/heading/etc.) so the
    result is always well-formed ADF, and append a note explaining the
    truncation. Never raises — worst case returns the doc untouched.
    """
    total_len = _adf_text_length(adf_body)
    if total_len <= max_chars:
        return adf_body

    nodes = adf_body.get("content", [])
    kept  = []
    used  = 0

    for node in nodes:
        node_len = _adf_text_length(node)

        if used + node_len <= max_chars:
            kept.append(node)
            used += node_len
            continue

        # This node alone would push us over — if it's the first node
        # (single huge paragraph), trim its own text nodes to fit
        # rather than dropping it entirely.
        if not kept:
            remaining = max_chars - used
            trimmed = _trim_adf_node_text(node, remaining)
            if trimmed is not None:
                kept.append(trimmed)
        break

    kept.append({
        "type": "paragraph",
        "content": [{
            "type": "text",
            "text": (
                f"⚠️ [Content truncated — original content was "
                f"{total_len:,} characters, exceeding Jira's size limit. "
                f"Refer to the source system for the full content.]"
            ),
            "marks": [{"type": "em"}],
        }],
    })

    adf_body["content"] = kept
    return adf_body


def _trim_adf_node_text(node, max_chars: int):
    """Trim the first 'text' field(s) inside a single ADF node to fit max_chars."""
    if not isinstance(node, dict):
        return None
    node = dict(node)
    remaining = max_chars
    new_content = []
    for child in node.get("content", []):
        if isinstance(child, dict) and child.get("type") == "text":
            text = child.get("text", "")
            if remaining <= 0:
                break
            if len(text) > remaining:
                child = {**child, "text": text[:remaining]}
                remaining = 0
            else:
                remaining -= len(text)
            new_content.append(child)
        else:
            new_content.append(child)
    node["content"] = new_content
    return node


def build_attachment_link_node(attachment_entry: dict) -> dict:
    """
    Build an ADF paragraph node that links to a Jira attachment.
 
    Uses the content_url returned by Jira after upload — this is a
    direct, authenticated URL that renders as a preview in Jira.
 
    Example ADF output:
        {
            "type": "paragraph",
            "content": [
                {
                    "type": "text",
                    "text": "screenshot.png",
                    "marks": [{"type": "link", "attrs": {"href": "https://..."}}]
                }
            ]
        }
    """
    filename    = attachment_entry.get("filename", "attachment")
    content_url = attachment_entry.get("content_url", "")
    file_type   = attachment_entry.get("file_type", "document")
 
    label = f"[Image] {filename}" if file_type == "image" else f"[Attachment] {filename}"
 
    return {
        "type": "paragraph",
        "content": [
            {
                "type": "text",
                "text": label,
                "marks": [
                    {
                        "type": "link",
                        "attrs": {"href": content_url},
                    }
                ],
            }
        ],
    }


def build_issue_payload(row, attachment_mapping: dict = None, parent_type_by_id: dict = None):
    """
    Convert one transformed issue row  Jira issue creation payload.
    Custom fields are now handled dynamically via CustomFieldPayloadBuilder.

    attachment_mapping (optional): loaded from load_attachment_mapping().
    When provided, any inline images/documents that belonged to this
    issue's description (keyed by source_issue_id under
    "by_source_issue_id") get appended as ADF link nodes at the end of
    the description — mirroring what build_comment_payload() does for
    comments. The description is also truncated to stay under Jira's
    content size limit, with room reserved for the attachment links.
    """
    fields = {}

    # NOTE: cascading (parent/child) select fields — sub_project
    # (customfield_10973) and theme (customfield_10900) — are no longer
    # built here. They're fully Excel-driven and handled inside
    # CustomFieldPayloadBuilder.build_custom_fields() (see the
    # 'cascading_field' sheet in mappings.xlsx), including mismatch
    # fallback to the default field, same as any other custom field.

    project_key = safe_str(row.get("jira_project_key")) or safe_str(row.get("project_key"))
    if project_key:
        fields["project"] = {"key": project_key}
 
    title = safe_str(row.get("title") or row.get("summary", ""))
    if not title:
        title = f"Issue {row.get('issue_id', 'Unknown')}"

    title_clean = title.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    title_clean = "".join(ch if not unicodedata.category(ch).startswith("C") else " " for ch in title_clean)
    title_clean = " ".join(title_clean.split()).strip()
    fields["summary"] = title_clean[:255]
    if "\n" in title or "\r" in title or "\t" in title:
        print(f"  [DEBUG TITLE] Cleaned newline in: {repr(title[:80])}")

    fields["summary"] = title_clean[:255]
    
    try:
        description = row.get("description")
        if not is_empty(description):
            desc_str = safe_str(description)
            if bool(re.search(r'<[a-zA-Z]', desc_str)):
                fields["description"] = html_to_adf(desc_str)
            else:
                fields["description"] = text_to_adf(desc_str)
    except Exception as e:
        print(f"  [WARN] Description ADF conversion failed for issue "
              f"{row.get('issue_id', '?')}: {e} — falling back to plain text")
        try:
            fields["description"] = text_to_adf(
                strip_tags(safe_str(row.get("description", "")))
            )
        except Exception:
            pass  # skip description entirely if all else fails

    # ── Enforce content size limit + embed attachment links into the
    # description ── Mirrors build_comment_payload()'s handling, but
    # keyed by source_issue_id instead of source_comment_id since these
    # files were referenced inline in the issue's own description
    # rather than in a comment.
    if attachment_mapping:
        source_issue_id_for_att = safe_str(row.get("source_issue_id") or row.get("issue_id"))
        attachments_for_issue = attachment_mapping.get(
            "by_source_issue_id", {}
        ).get(source_issue_id_for_att, [])

        reserved_for_attachments = sum(
            len(a.get("filename", "attachment")) + 15  # "[Attachment] " / "[Image] " + slack
            for a in attachments_for_issue
        )

        if "description" in fields:
            fields["description"] = enforce_adf_char_limit(
                fields["description"],
                max_chars=max(JIRA_CONTENT_CHAR_LIMIT - reserved_for_attachments, 1000),
            )

        if attachments_for_issue:
            images    = [a for a in attachments_for_issue if a.get("file_type") == "image"]
            documents = [a for a in attachments_for_issue if a.get("file_type") != "image"]

            if "description" not in fields:
                fields["description"] = make_adf_doc([])

            adf_nodes = fields["description"].get("content", [])
            for img in images:
                adf_nodes.append(build_attachment_link_node(img))
            for doc in documents:
                adf_nodes.append(build_attachment_link_node(doc))
            fields["description"]["content"] = adf_nodes

    issue_type = safe_str(row.get("jira_issue_type") or row.get("issue_type_name", "Task"))
    fields["issuetype"] = {"name": issue_type}
    freshrelease_issue_type = safe_str(row.get("issue_type_name"))
    if issue_type in ["Bug", "Iteration Bug", "Security Bug"] and freshrelease_issue_type:
        fields[FW_BUG_FIELD] = freshrelease_issue_type

    # Bug Category (customfield_10454, select list) — driven by the
    # "In case of Bug - Type" column on the issue_type sheet. Only applies
    # when the issue is mapped to Jira's "Bug" type AND the source
    # Freshrelease work item type has a category configured in the sheet.
    # We stash the value on the row under "cf_bug_category" so it flows
    # through the same custom-field builder pipeline (Pass 1 of
    # build_custom_fields) already used for Security Bug's Bug Category —
    # this keeps the existing select-list option validation/matching intact
    # instead of writing the raw value straight into the payload.
    if issue_type == "Bug":
        bug_category_value = BUG_CATEGORY_MAP.get(freshrelease_issue_type)
        if bug_category_value:
            row["cf_bug_category"] = bug_category_value
 
    priority = safe_str(row.get("jira_priority") or row.get("priority_name", "Medium"))
    if priority:
        fields["priority"] = {"name": priority}
 
    if INCLUDE_ASSIGNEE:
        assignee_id = safe_str(row.get("jira_assignee_id"))
        if assignee_id and assignee_id not in ("nan", "None", ""):
            fields["assignee"] = {"accountId": assignee_id}
        else:
            fields["assignee"] = {"accountId": None}
 
    if INCLUDE_REPORTER:
        reporter_id = safe_str(row.get("jira_reporter_id"))
        if reporter_id and reporter_id not in ("nan", "None", ""):
            fields["reporter"] = {"accountId": reporter_id}
 
    if INCLUDE_LABELS:
        tags_str = safe_str(row.get("tags", ""))
        clean_tags = []
        if tags_str:
            tags = [t.strip() for t in tags_str.split(",") if t.strip()]
            clean_tags = [re.sub(r'\s+', '_', t) for t in tags]

        # Append "ChangeReview" as an extra label whenever the source
        # Freshrelease work item type is "ChangeReview" AND it is mapped to
        # Jira's "Task" issue type — regardless of whether a label value
        # already exists on the issue.
        if freshrelease_issue_type == "ChangeReview" and issue_type == "Task":
            if "ChangeReview" not in clean_tags:
                clean_tags.append("ChangeReview")

        if clean_tags:
            fields["labels"] = clean_tags
 
    if INCLUDE_DUE_DATE:
        due_date = safe_str(row.get("due_by"))
        if due_date and due_date not in ("None", "nan", "NaT", ""):
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', due_date)
            if date_match:
                fields["duedate"] = date_match.group(1)
 
    if INCLUDE_START_DATE:
        start_date_raw = row.get("start_date")
        if start_date_raw is not None and not pd.isna(start_date_raw):
            start_date = str(start_date_raw).strip()
            if start_date not in ("", "None", "nan", "NaT", "nat"):
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', start_date)
                if date_match:
                    fields[START_DATE_FIELD] = date_match.group(1)
 
    if INCLUDE_STORY_POINTS:
        sp = safe_float(row.get("story_points"))
        if sp is not None:
            sp_field = get_story_points_field()
            fields[sp_field] = sp
 
    if INCLUDE_SOURCE_KEY:
        source_key = safe_str(row.get("issue_key"))
        if source_key and source_key not in ("nan", "None", ""):
            fields[SOURCE_KEY_CUSTOM_FIELD] = source_key
 
    if INCLUDE_COMPONENTS:
        component = safe_str(row.get("component"))
        if component:
            fields["components"] = [{"name": component}]
 
    if INCLUDE_FIX_VERSIONS:
        release_name = safe_str(row.get("release_name"))
        if release_name:
            fields["fixVersions"] = [{"name": release_name}]
    
    fw_project_key = (
        safe_str(row.get("fw_project_key"))
        or safe_str(row.get("source_project_key"))
        or safe_str(row.get("project_key"))
    )
    if fw_project_key:
        parent_value = PARENT_PROJECT_MAP.get(fw_project_key.upper())
        if parent_value:
            row["fr_project_value"] = parent_value

    
    raw_sub_project_id = safe_str(row.get("sub_project_id"))
    sub_name = ""
    PRODUCT_SQUAD_STATS["total_issues"] += 1

    if raw_sub_project_id and raw_sub_project_id not in ("nan", "None", ""):
        PRODUCT_SQUAD_STATS["sub_project_id_present"] += 1
        sub_key  = raw_sub_project_id.split(".")[0]
        sub_name = SUBPROJECT_MAP.get(sub_key) or SUBPROJECT_MAP.get(sub_key.upper()) or ""
        if sub_name:
            row["fr_subproject_value"] = sub_name
            PRODUCT_SQUAD_STATS["resolved_via_cache"] += 1
        else:
            PRODUCT_SQUAD_STATS["unresolved_sub_project_id"] += 1
    else:
        PRODUCT_SQUAD_STATS["no_sub_project_id"] += 1

    if INCLUDE_CUSTOM_FIELDS:
        skip_cf = {START_DATE_FIELD, SOURCE_KEY_CUSTOM_FIELD, get_story_points_field(), "description"}

        builder   = get_cf_builder()
        cf_fields = builder.build_custom_fields(row)

        for cf_id, cf_value in cf_fields.items():
            if cf_id not in skip_cf:
                fields[cf_id] = cf_value
                fields[cf_id] = trim_custom_field_value(cf_id, cf_value, row.get('issue_id', '?'))
            else:
                print(f"  [DEBUG PAYLOAD] SKIPPED {cf_id} because it is in skip_cf")

    # ── Product / Squad cascading field (customfield_10973) ──
    lookup_project_key = fw_project_key or safe_str(
        row.get("fw_project_key") or row.get("source_project_key") or row.get("project_key")
    )

    squad_entry = None
    skip_reason = None

    if lookup_project_key:
        project_key_norm = lookup_project_key.lower()
        has_subprojects = project_key_norm in PROJECTS_WITH_SUBPROJECTS

        if sub_name:
            squad_entry = SUBPROJECT_SQUAD_MAP.get((project_key_norm, sub_name.lower()))
        elif has_subprojects:
            skip_reason = (
                f"project '{lookup_project_key}' has subprojects in cache but this "
                f"issue has no sub_project_id — no fallback applied"
            )
        else:
            squad_entry = SUBPROJECT_SQUAD_MAP.get((project_key_norm, ""))

    jira_product, jira_squad = squad_entry if squad_entry else ("", "")

    # Treat the "Migrate with Product/Squad value empty" markers the same
    # as a truly blank cell, independently per column:
    #   - Product marker -> product blank -> field skipped entirely
    #   - Squad marker   -> squad blank   -> product-only, child=None
    if _is_migrate_empty_marker(jira_product):
        jira_product = ""
    if _is_migrate_empty_marker(jira_squad):
        jira_squad = ""

    if jira_product:
        fields[PRODUCT_SQUAD_FIELD_ID] = {
            "value": jira_product,
            "child": {"value": jira_squad} if jira_squad else None
        }
        PRODUCT_SQUAD_STATS["field_created"] += 1
    else:
        fields.pop(PRODUCT_SQUAD_FIELD_ID, None)
        if skip_reason:
            print(f"  [DEBUG PAYLOAD] SKIPPED {PRODUCT_SQUAD_FIELD_ID} — {skip_reason}")
            PRODUCT_SQUAD_STATS["skipped_no_fallback"] += 1
        elif squad_entry:
            print(
                f"  [DEBUG PAYLOAD] SKIPPED {PRODUCT_SQUAD_FIELD_ID} — mapping found for "
                f"project '{lookup_project_key}' / subproject '{sub_name or '(none)'}' but Jira Product is blank "
                f"(Squad='{jira_squad or '(blank)'}')"
            )
            PRODUCT_SQUAD_STATS["skipped_product_blank"] += 1
        else:
            print(
                f"  [DEBUG PAYLOAD] SKIPPED {PRODUCT_SQUAD_FIELD_ID} — no mapping for "
                f"project '{lookup_project_key or '(blank)'}' / subproject '{sub_name or '(blank)'}'"
            )
            PRODUCT_SQUAD_STATS["skipped_no_mapping"] += 1
    parent_id = safe_str(row.get("parent_id", "")).split(".")[0]
    epic_id   = safe_str(row.get("epic_id",   "")).split(".")[0]
    if parent_id in ("nan", "None", ""):
        parent_id = ""
    if epic_id in ("nan", "None", ""):
        epic_id = ""

    SUBTASK_TYPES = {
        "Review Sub-task", "PR Intake Bot", "Deployment Ticket",
        "DS POC Sub-task", "Sub-task", "Iteration Bug", "VQA",
        "Ticket Intake Bot",
    }
    jira_issue_type_val = safe_str(row.get("jira_issue_type"))
    if jira_issue_type_val == "Dependency Request":
        parent_id = ""
        epic_id = ""

    if jira_issue_type_val not in SUBTASK_TYPES:
        parent_type = (parent_type_by_id or {}).get(parent_id, "") if parent_id else ""

        if parent_type == jira_issue_type_val:
            # Same type as parent (e.g. Story under Story) -> not a real
            # Jira parent/child relationship -> clear both.
            parent_id = ""
            epic_id = ""
        elif parent_id == "":
            # Different type from parent, but no parent_id to attach to
            # anyway -> clear epic_id too for consistency.
            parent_id = ""
            epic_id = ""
        # else: different type AND a real parent_id exists -> keep both.

    jira_status = safe_str(row.get("jira_status") or row.get("status_name", ""))
 
    return {
        "source_issue_id":    safe_str(row.get("source_issue_id") or row.get("issue_id")),
        "source_project_key": safe_str(row.get("source_project_key") or row.get("project_key")),
        "source_parent_id":   parent_id,
        "source_epic_id":     epic_id,
        "source_sprint_id":   safe_str(row.get("sprint_id", "")),
        "source_release_id":  safe_str(row.get("release_id", "")),
        "jira_issue_type":    safe_str(row.get("jira_issue_type")),
        "jira_status":        jira_status,
        "migration_sequence": safe_int(row.get("migration_sequence")),
        "payload":            {"fields": fields}
    }


def parse_text_with_links(text):
    """
    Convert plain text URLs into Jira ADF link format with dynamic text.

    Example:
    "see this G-Drive link (https://abc.com)"
     "see this " + clickable "G-Drive link"
    """
    if is_empty(text):
        return []

    parts = re.split(r'(\(https?://[^\s]+?\)|https?://[^\s]+)', text)
    nodes = []

    for part in parts:
        if not part:
            continue

        part_stripped = part.strip()

        # ── (URL) format — use preceding words as link text ──
        match = re.match(r'\((https?://[^\s]+)\)', part_stripped)
        if match:
            url = match.group(1).rstrip(')')

            if nodes and nodes[-1]["type"] == "text":
                prev_text = nodes[-1]["text"].rstrip()
                words = prev_text.split()
                if len(words) >= 2:
                    link_text = " ".join(words[-2:])
                elif len(words) == 1:
                    link_text = words[0]
                else:
                    link_text = url

                nodes[-1]["text"] = " ".join(words[:-2]) if len(words) > 2 else ""

                if not nodes[-1]["text"]:
                    nodes.pop()
            else:
                link_text = url

            nodes.append({
                "type": "text",
                "text": link_text,
                "marks": [{
                    "type": "link",
                    "attrs": {"href": url}
                }]
            })
            continue

        # ── Direct URL ──
        if re.match(r'https?://', part_stripped):
            url = part_stripped.rstrip(')')
            nodes.append({
                "type": "text",
                "text": url,
                "marks": [{
                    "type": "link",
                    "attrs": {"href": url}
                }]
            })
            continue

        # ── Plain text — preserve original spacing ──
        if part:                # ← use part not part_stripped to keep spaces
            nodes.extend(tokenize_mentions_and_text(part))

    return nodes

def extract_name_from_email(email):
    if not email or "@" not in email:
        return None

    local = email.split("@")[0]
    local = re.sub(r"[^a-zA-Z._ ]", " ", local)

    parts = re.split(r"[._]", local)
    return " ".join([p.capitalize() for p in parts if p.strip()])


def build_comment_payload(row, attachment_mapping: dict) -> dict:
    """
    Convert one comment row  Jira comment payload (ADF).
 
    Differences from original:
      - Accepts attachment_mapping so it can embed images/files
      - Images become ADF mediaSingle nodes (inline rendered image)
      - Documents become ADF paragraph link nodes
      - Falls back to a plain text link if media token is unavailable
      - Enforces Jira's content size limit, reserving room for the
        attachment link nodes appended afterwards
    """
    content          = safe_str(row.get("content", ""))
    author_name      = safe_str(row.get("source_user_name") or row.get("user_name", ""))
    created_at       = safe_str(row.get("created_at", ""))
    source_comment_id = safe_str(row.get("comment_id") or row.get("source_comment_id", ""))
 
    if author_name or created_at:
        author_display = author_name if author_name and author_name != "nan" else "Unknown"
        header = f"[Migrated] Originally by: {author_display}"
        if created_at:
            header += f" on {created_at}"
        full_text = f"{header}\n\n{content}" if content else header
    else:
        full_text = content or "No content"
 
    if bool(re.search(r'<[a-zA-Z]', full_text)):
        adf_body = html_to_adf(full_text)
    else:
        adf_body = text_to_adf(full_text)
 
    attachments_for_comment = attachment_mapping.get(
        "by_source_comment_id", {}
    ).get(source_comment_id, [])

    # Reserve room for the attachment link nodes we're about to add,
    # so the FINAL payload (text + links) stays under Jira's limit —
    # not just the text portion on its own.
    reserved_for_attachments = sum(
        len(a.get("filename", "attachment")) + 15  # "[Attachment] " / "[Image] " + slack
        for a in attachments_for_comment
    )
    adf_body = enforce_adf_char_limit(
        adf_body, max_chars=max(JIRA_CONTENT_CHAR_LIMIT - reserved_for_attachments, 1000)
    )

    if attachments_for_comment:
        images    = [a for a in attachments_for_comment if a.get("file_type") == "image"]
        documents = [a for a in attachments_for_comment if a.get("file_type") != "image"]
 
        adf_nodes = adf_body.get("content", [])
 
        for img in images:
            adf_nodes.append(build_attachment_link_node(img))
 
        for doc in documents:
            adf_nodes.append(build_attachment_link_node(doc))
 
        adf_body["content"] = adf_nodes
 
    payload = {"body": adf_body}
 
    return {
        "source_issue_id":   safe_str(row.get("source_issue_id") or row.get("issue_id")),
        "source_comment_id": source_comment_id,
        "jira_author_id":    safe_str(row.get("jira_author_id")),
        "payload":           payload,
    }


def prepare_dependencies():
    """
    Build payload for predecessor/successor dependencies.
 
    Reads the flattened dependencies.csv where each row is one link.
 
    relationship = "blocks"         Jira link type "Blocks"
                                     inwardIssue  = this issue (blocker)
                                     outwardIssue = linked issue (blocked)
 
    relationship = "is_blocked_by"  Jira link type "Blocks"
                                     inwardIssue  = linked issue (blocker)
                                     outwardIssue = this issue (blocked)
 
    Output: data/payload/dependencies.json
    """
    print("\n Preparing dependency payloads...")
 
    df = load_csv("dependencies.csv")
    if df.empty:
        print("      No dependencies.csv found")
        return []
 
    payloads = []
 
    for _, row in df.iterrows():
        issue_id         = safe_str(row.get("issue_id", "")).split(".")[0].strip()
        linked_issue_id  = safe_str(row.get("linked_issue_id", "")).split(".")[0].strip()
        relationship     = safe_str(row.get("relationship", "blocks"))
        dep_type         = safe_str(row.get("dependency_type", "finish_to_start"))
        linked_issue_key = safe_str(row.get("linked_issue_key", ""))
        linked_proj_id   = safe_str(row.get("linked_issue_project_id", ""))
 
        if not issue_id or not linked_issue_id:
            continue
 
        payloads.append({
            "source_issue_id":         issue_id,
            "linked_source_id":        linked_issue_id,
            "linked_issue_key":        linked_issue_key,
            "linked_issue_project_id": linked_proj_id,
            "relationship":            relationship,
            "dependency_type":         dep_type,
            "jira_link_type":          "Blocks",
        })
 
    save_json(payloads, "dependencies.json")
    print(f"      {len(payloads)} dependency links prepared")
    return payloads

def build_link_payload(row):
    """
    Convert one issue link row → Jira issue link payload.
    Uses source_link_type + link_direction written by transform.py.

    Direction logic:
      outward = source issue DOES the action  (e.g. "blocks" target)
                → outwardIssue = source, inwardIssue = target
      inward  = source issue RECEIVES action  (e.g. "is blocked by" target)
                → inwardIssue = source, outwardIssue = target
    """
    source_issue_id  = safe_str(row.get("source_issue_id")).split(".")[0].strip()
    target_issue_id  = safe_str(row.get("target_issue_id")).split(".")[0].strip()
    jira_link_type   = safe_str(row.get("jira_link_type")) or "Relates"
    direction        = safe_str(row.get("link_direction", "outward")).lower().strip()

    if direction not in ("inward", "outward"):
        direction = "outward"

    if direction == "outward":
        inward_placeholder  = "__PLACEHOLDER_TARGET__"
        outward_placeholder = "__PLACEHOLDER_SOURCE__"
    else:
        inward_placeholder  = "__PLACEHOLDER_SOURCE__"
        outward_placeholder = "__PLACEHOLDER_TARGET__"

    return {
        "source_issue_id":  source_issue_id,
        "target_issue_id":  target_issue_id,
        "source_link_type": safe_str(row.get("source_link_type")),
        "jira_link_type":   jira_link_type,
        "direction":        direction,
        "jira_source_key":  None,
        "jira_target_key":  None,
        "payload": {
            "type": {"name": jira_link_type},
            "inwardIssue":  {"key": inward_placeholder},
            "outwardIssue": {"key": outward_placeholder}
        }
    }


def build_version_payload(row):
    start_date = safe_str(row.get("start_date", ""))
    end_date = safe_str(row.get("end_date", ""))

    payload = {
        "name": safe_str(row.get("release_name")),
        "description": safe_str(row.get("description", "")),
        "project": safe_str(row.get("jira_project_key")),
        "released": row.get("release_status_name") == "released",
        "archived": row.get("release_status_name") == "archived",
    }

    start_match = re.search(r'(\d{4}-\d{2}-\d{2})', start_date)
    end_match = re.search(r'(\d{4}-\d{2}-\d{2})', end_date)

    start_val = start_match.group(1) if start_match else None
    end_val = end_match.group(1) if end_match else None

    if start_val and end_val:
        start_val = _fix_start_before_end(start_val, end_val)

    if start_val:
        payload["startDate"] = start_val
    if end_val:
        payload["releaseDate"] = end_val

    return {
        "source_release_id": safe_str(row.get("source_release_id") or row.get("release_id")),
        "jira_project_key": safe_str(row.get("jira_project_key")),
        "payload": payload
    }


def build_sprint_payload(row, board_id):
    source_sprint_id = safe_str(row.get("source_sprint_id") or row.get("sprint_id"))

    jira_project_key = safe_str(row.get("jira_project_key"))
    mapped_board_id = SPRINT_MAP.get(jira_project_key)

    resolved_board_id = int(
        mapped_board_id or
        board_id or
        0
    )

    if not mapped_board_id:
        print(f"  WARNING: No board_id found for project '{jira_project_key}' — sprint '{row.get('sprint_name')}'")

    payload = {
    "name": safe_str(row.get("sprint_name"))[:30],
    "goal": safe_str(row.get("description", "")),
    "originBoardId": resolved_board_id,
    }

    start = safe_str(row.get("start_date") or row.get("planned_start_date"))
    end = safe_str(row.get("end_date") or row.get("planned_end_date"))

    start_match = re.search(r'(\d{4}-\d{2}-\d{2})', start)
    end_match = re.search(r'(\d{4}-\d{2}-\d{2})', end)

    start_val = start_match.group(1) if start_match else None
    end_val = end_match.group(1) if end_match else None


    if start_val and end_val:
        start_val = _fix_start_before_end(start_val, end_val)

    if start_val:
        payload["startDate"] = start_val
    if end_val:
        payload["endDate"] = end_val

    return {
        "source_sprint_id": source_sprint_id,
        "jira_project_key": jira_project_key,
        "payload": payload
    }

def prepare_issues(attachment_mapping: dict = None):
    """
    Build payload for all issues.
    Sorted by migration_sequence (Epics first).

    attachment_mapping: loaded from load_attachment_mapping(). When
    provided, description ADF bodies get inline attachment links
    appended and the content-size limit is enforced (see
    build_issue_payload). If omitted, this loads it directly so the
    function can still be called standalone.
    """
    print("\n Preparing issue payloads...")

    if attachment_mapping is None:
        attachment_mapping = load_attachment_mapping()

    df = load_csv("issues.csv")
    if df.empty:
        return []

    if "migration_sequence" in df.columns:
        df = df.sort_values("migration_sequence", na_position="last")

    # Lookup of every issue's own mapped jira_issue_type, keyed by its
    # source_issue_id — used inside build_issue_payload() to find out
    # "what type is my PARENT" (a parent is just another row in this same
    # dataframe, already carrying its own jira_issue_type).
    parent_type_by_id = {
        safe_str(row.get("issue_id")): safe_str(row.get("jira_issue_type") or row.get("issue_type_name", ""))
        for _, row in df.iterrows()
    }

    payloads = []
    skipped = []

    for _, row in df.iterrows():
        project_key = safe_str(row.get("jira_project_key") or row.get("project_key"))
        title = safe_str(row.get("title") or row.get("summary", ""))
        issue_type = safe_str(row.get("jira_issue_type") or row.get("issue_type_name", ""))

        if not project_key:
            skipped.append({
                "issue_id": row.get("issue_id"),
                "reason": "Missing project_key"
            })
            continue

        if not title:
            skipped.append({
                "issue_id": row.get("issue_id"),
                "reason": "Missing title/summary"
            })
            continue

        if not issue_type:
            skipped.append({
                "issue_id": row.get("issue_id"),
                "reason": "Missing issue_type"
            })
            continue

        payloads.append(build_issue_payload(row, attachment_mapping, parent_type_by_id))

    save_json(payloads, "issues.json")

    # ── Lightweight meta + per-issue full files (NEW) ──────────────────
    # issues.json above is kept as-is for backward compatibility with
    # Retry_failed_issues.py and any other tooling that reads it directly.
    # These additional outputs let bulk_load_to_jira.py avoid loading the
    # entire multi-GB issues.json into memory at once: issues_meta.json
    # has every issue but WITHOUT the heavy "payload" (fields/description)
    # content, and issues_full/{source_issue_id}.json holds that heavy
    # content per-issue, loaded on demand only when actually needed.
    issues_full_dir = os.path.join(PAYLOAD_DATA_DIR, "issues_full")
    os.makedirs(issues_full_dir, exist_ok=True)

    issues_meta = []
    for p in payloads:
        source_id = p.get("source_issue_id")
        if source_id:
            full_filepath = os.path.join(issues_full_dir, f"{source_id}.json")
            with open(full_filepath, "w", encoding="utf-8") as f:
                json.dump(p, f, ensure_ascii=False)  # no indent — smaller files

        meta_item = {k: v for k, v in p.items() if k != "payload"}
        issues_meta.append(meta_item)

    save_json(issues_meta, "issues_meta.json")
    print(f"      Also wrote lean {len(issues_meta)}-record issues_meta.json "
          f"+ {len(issues_meta)} per-issue files under issues_full/")

    if skipped:
        save_json(skipped, "issues_skipped.json")
        print(f"      Skipped {len(skipped)} issues (see issues_skipped.json)")
    
    # ── Product / Squad resolution summary ──
    s = PRODUCT_SQUAD_STATS
    print("\n  Product/Squad (customfield_10973) resolution summary:")
    print(f"      Total issues processed:              {s['total_issues']}")
    print(f"      With sub_project_id set:              {s['sub_project_id_present']}")
    print(f"        Resolved via SUBPROJECT_MAP cache:  {s['resolved_via_cache']}")
    print(f"        NOT found in cache (bad/stale id):  {s['unresolved_sub_project_id']}")
    print(f"      Without sub_project_id:               {s['no_sub_project_id']}")
    print(f"      ── Field created in payload:           {s['field_created']}")
    print(f"      ── Skipped (no fallback, has subs):    {s['skipped_no_fallback']}")
    print(f"      ── Skipped (no mapping in sheet):      {s['skipped_no_mapping']}")
    print(f"      ── Skipped (mapped but Product blank): {s['skipped_product_blank']}")
    save_json(s, "product_squad_stats.json")

    return payloads


def prepare_links():
    """Build payload for all issue links."""
    print("\n Preparing issue link payloads...")
    df = load_csv("issue_links.csv")
    if df.empty:
        return []

    payloads = [build_link_payload(row) for _, row in df.iterrows()]
    save_json(payloads, "links.json")
    return payloads


def prepare_versions():
    """Build payload for all releases/versions."""
    print("\n Preparing version payloads...")
    df = load_csv("releases.csv")
    if df.empty:
        return []

    payloads = [build_version_payload(row) for _, row in df.iterrows()]
    save_json(payloads, "versions.json")
    return payloads


def prepare_comments(attachment_mapping: dict = None) -> list:
    """
    Build payload for all comments, embedding attachment links.
 
    Args:
        attachment_mapping: loaded from load_attachment_mapping().
                            Pass None to skip attachment embedding
                            (falls back to original behaviour).
    """
    print("\n  Preparing comment payloads...")
 
    if attachment_mapping is None:
        attachment_mapping = {"by_local_path": {}, "by_source_comment_id": {}}
 
    df = load_csv("comments.csv")
    if df.empty:
        return []
 
    payloads = [
        build_comment_payload(row, attachment_mapping)
        for _, row in df.iterrows()
    ]
 
    save_json(payloads, "comments.json")
    return payloads
 

def prepare_sprints(board_id=None):
    """
    Build payload for all sprints.

      board_id must be set to your Jira board ID.
    Find it:
        GET https://your-domain.atlassian.net/rest/agile/1.0/board
         find board for your project  note the "id" field
    """
    print("\n Preparing sprint payloads...")

    if not board_id:
        print("      board_id not set — sprint payloads will have placeholder ID 0")
        print("      Update BOARD_ID in prepare_payload.py after finding your board ID")

    df = load_csv("sprints.csv")
    if df.empty:
        return []

    payloads = [build_sprint_payload(row, board_id or 0) for _, row in df.iterrows()]
    save_json(payloads, "sprints.json")
    return payloads


def build_activity_payload(row):
    """
    Convert one activity row  Jira history entry.
    
    NOTE: Jira doesn't allow POST to history - this is for documentation only.
    History is created automatically when you update issues.
    
    This payload shows what WOULD be the change if applied via issue update.
    """
    
    changes = {}
    
    for col in row.index:
        if col.startswith("change_") and col.endswith("_new"):
            field_name = col.replace("change_", "").replace("_new", "")
            old_col = f"change_{field_name}_old"
            
            old_value = row.get(old_col)
            new_value = row.get(col)
            
            if pd.isna(new_value) or str(old_value) == str(new_value):
                continue
            
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

    if "created_at" in df.columns:
        df = df.sort_values("created_at")

    payloads = []
    for _, row in df.iterrows():
        payload = build_activity_payload(row)
        
        if payload.get("changes"):
            payloads.append(payload)

    save_json(payloads, "activities.json")
    
    print(f"        Activities saved for reference only")
    print(f"        Jira creates history automatically when you update issues")
    print(f"        Use this file to verify what changed on each issue")
    
    return payloads


def build_summary(issues, comments, links, versions, sprints, activities):
    """Build summary of all payloads."""
    summary = {
        "generated_at": datetime.now().isoformat(),
        "counts": {
            "issues": len(issues),
            "comments": len(comments),
            "links": len(links),
            "versions": len(versions),
            "sprints": len(sprints),
            "activities": len(activities),  
        },
        "issue_type_breakdown": {},
        "project_breakdown": {},
        "status_breakdown": {},
        "flags": {
            "include_assignee": INCLUDE_ASSIGNEE,
            "include_reporter": INCLUDE_REPORTER,
            "include_labels": INCLUDE_LABELS,
            "include_due_date": INCLUDE_DUE_DATE,
            "include_story_points": INCLUDE_STORY_POINTS,
            "include_components": INCLUDE_COMPONENTS,
            "include_fix_versions": INCLUDE_FIX_VERSIONS,
            "include_custom_fields": INCLUDE_CUSTOM_FIELDS,
        },
        "notes": [
            "issues.json    POST /rest/api/3/issue (one by one)",
            "comments.json  POST /rest/api/3/issue/{key}/comment (after issues created)",
            "links.json     POST /rest/api/3/issueLink (after issues created)",
            "versions.json  POST /rest/api/3/version (before issues if using fix versions)",
            "sprints.json   POST /rest/agile/1.0/sprint (requires Jira Software board)",
            "activities.json  REFERENCE ONLY (history auto-created)",
            "Load order: versions  sprints  issues  comments  links",
        ]
    }

    for item in issues:
        it = item.get("jira_issue_type", "Unknown")
        pk = item.get("source_project_key", "Unknown")

        summary["issue_type_breakdown"][it] = summary["issue_type_breakdown"].get(it, 0) + 1
        summary["project_breakdown"][pk] = summary["project_breakdown"].get(pk, 0) + 1

    save_json(summary, "payload_summary.json")
    return summary


BOARD_ID = None


def main():
    print("=" * 60)
    print("  PREPARE JIRA PAYLOAD")
    print(f"  Input:   {TRANSFORM_DATA_DIR}/")
    print(f"  Output:  {PAYLOAD_DATA_DIR}/")
    print(f"  Time:    {datetime.now().isoformat()}")
    print("=" * 60)
    
    # ── Step 1: Load attachment mapping FIRST so both issues and
    # comments can embed attachment links + enforce content limits ──
    attachment_mapping = load_attachment_mapping()

    issues = prepare_issues(attachment_mapping)
    comments = prepare_comments(attachment_mapping)
    links = prepare_links()
    versions = prepare_versions()
    sprints = prepare_sprints(BOARD_ID)
    activities = prepare_activities()
    dependencies = prepare_dependencies()
    
    # ── Step 2: Group related data by issue ─────────────
    grouped_data = group_related_data_by_issue(
        issues,
        comments,
        links,
        attachment_mapping
    )
    
    # ── Step 3: Save grouped payload ────────────────────
    grouped_payload_result = save_grouped_payload(grouped_data)
    grouped_issue_ids = grouped_payload_result["issue_ids"]
    
    # ── Step 4: Build summary ──────────────────────────────────
    summary = build_summary(issues, comments, links, versions, sprints, activities)
    
    # ── Add grouped payload info to summary ───────────────────
    total_issues = len(grouped_issue_ids)
    summary["grouped_payload"] = {
        "enabled": True,
        "file": "issues_with_related_index.json",
        "total_issues": total_issues,
        "avg_comments_per_issue": (
            grouped_payload_result["total_comments"] / total_issues if total_issues else 0
        ),
        "avg_links_per_issue": (
            grouped_payload_result["total_links"] / total_issues if total_issues else 0
        ),
    }
    
    save_json(summary, "payload_summary.json")

    print(f"\n{'=' * 60}")
    print("   PAYLOAD READY")
    print(f"{'=' * 60}")
    print(f"  Issues:   {summary['counts']['issues']}")
    print(f"  Comments: {summary['counts']['comments']}")
    print(f"  Links:    {summary['counts']['links']}")
    print(f"  Versions: {summary['counts']['versions']}")
    print(f"  Sprints:  {summary['counts']['sprints']}")
    print(f"  Activities: {summary['counts']['activities']} (reference)")
    print(f"  Dependencies: {len(dependencies)}")
    
    print(f"\n  GROUPED PAYLOAD:")
    print(f"    File: issues_with_related_index.json + issues_with_related/*.json")
    print(f"    Issues: {summary['grouped_payload']['total_issues']}")
    print(f"    Avg comments/issue: {summary['grouped_payload']['avg_comments_per_issue']:.1f}")
    print(f"    Avg links/issue: {summary['grouped_payload']['avg_links_per_issue']:.1f}")
    
    print(f"\n  Saved to: {PAYLOAD_DATA_DIR}/")
    print(f"\n  Load order:")
    print(f"    1. versions.json")
    print(f"    2. sprints.json")
    print(f"    3. issues_with_related_index.json + issues_with_related/*.json (parallel mode)")
    print(f"    4. dependencies.json (final pass)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()