"""
============================================================
MAPPING CONFIGURATION  —  Excel-driven
============================================================
All mappings are stored in  mappings.xlsx
Edit that file to change any source  Jira value mapping.

Sheet layout
  project        : source_key | jira_key       | default (YES)
  issue_type     : source_label | jira_name     | default (YES)
  status         : source_label | jira_status   | default (YES)
  priority       : source_label | jira_name     | default (YES)
  user           : source_email | jira_account_id | map_unmapped_to_me
  custom_field   : source_cf_name | jira_customfield_id | role
  resolution     : source_label | jira_resolution | default (YES)
  link_type      : source_label | jira_link_type  | default (YES)
  sprint_state   : source_id | jira_state
  release_status : source_id | jira_status
  fields_to_skip : field_name

HOW TO USE:
  1. Open mappings.xlsx and update the values
  2. Import this module — mappings are loaded automatically
============================================================
"""
import pandas as pd
from pathlib import Path
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
MAPPINGS_FILE = Path(__file__).parent / "mappings.xlsx"


# def _load(sheet: str) -> pd.DataFrame:
#     return pd.read_excel(
#         MAPPINGS_FILE,
#         sheet_name=sheet,
#         dtype=str
#     ).fillna("")

def _load(sheet: str) -> pd.DataFrame:
    df = pd.read_excel(
        MAPPINGS_FILE,
        sheet_name=sheet,
        dtype=str
    ).fillna("")
    # Strip leading/trailing whitespace from every string cell so a stray
    # space in Excel (e.g. "Requirement Change Request ") doesn't silently
    # break exact-match lookups against already-stripped source values.
    for col in df.columns:
        df[col] = df[col].str.strip()
    return df

def _default(df: pd.DataFrame, value_col: str) -> str:

    # Check if default column exists
    if "default" not in df.columns:
        return ""

    # Normalize default column values
    default_series = (
        df["default"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Filter rows where default = YES
    hit = df[default_series == "YES"]

    return (
        str(hit.iloc[0][value_col]).strip()
        if not hit.empty
        else ""
    )

# ── project ──────────────────────────────────────────────────────────────────
# The "project" sheet can now optionally carry a Sub_project column:
#
#   source_key | jira_key | default | Sub_project
#   FS         | EX       |         |
#   FD         | CX       |         |
#   FD         | CE       |         | SRE
#   FS         | CE       |         | Night's Watch
#   FD         | PLG      |         | skybound
#   FD         | PLG      |         | Specops
#   FS         | PLAT     |         | Lords of the rails
#   FS         | PLAT     |         | Vanguard
#
# A row WITHOUT a Sub_project value is the plain source_key -> jira_key
# mapping (used when an issue has no sub_project, or its sub_project can't
# be resolved). A row WITH a Sub_project value only applies to issues from
# that source_key that also belong to that named sub_project — so the same
# source_key (e.g. "FS") can now fan out to different Jira project keys
# depending on which sub_project the issue is in.
_proj = _load("project")

_SUBPROJECT_COL = next(
    (c for c in _proj.columns if c.strip().lower() in ("sub_project", "subproject", "sub project")),
    None
)

# source_key -> jira_key, built ONLY from rows with no Sub_project value.
# This is the fallback mapping for a project that has no sub_projects, or
# for issues whose sub_project can't be resolved.
PROJECT_MAP: dict[str, str] = {}

# (source_key upper, sub_project name lower) -> jira_key, built from rows
# that DO specify a Sub_project.
PROJECT_SUBPROJECT_MAP: dict[tuple[str, str], str] = {}

for _, _row in _proj.iterrows():
    _src_key  = str(_row.get("source_key", "")).strip()
    _jira_key = str(_row.get("jira_key", "")).strip()
    _sub_val  = str(_row.get(_SUBPROJECT_COL, "")).strip() if _SUBPROJECT_COL else ""

    if not _src_key or not _jira_key:
        continue

    if _sub_val:
        PROJECT_SUBPROJECT_MAP[(_src_key.upper(), _sub_val.lower())] = _jira_key
    else:
        PROJECT_MAP[_src_key] = _jira_key

DEFAULT_PROJECT: str = _default(_proj, "jira_key")


def get_project_key(source_key: str, subproject_name: str = "") -> str:
    """
    Resolve the Jira project key for an issue, taking sub_project into
    account.

    Priority:
      1. (source_key, subproject_name) exact match in PROJECT_SUBPROJECT_MAP
         — e.g. ("FD", "sre") -> "CE"
      2. source_key alone in PROJECT_MAP — the row with no Sub_project value
         — e.g. "FD" -> "CX"
      3. DEFAULT_PROJECT — the row marked default = YES, if any
    """
    src = str(source_key or "").strip()
    sub = str(subproject_name or "").strip().lower()

    if src and sub:
        jira_key = PROJECT_SUBPROJECT_MAP.get((src.upper(), sub))
        if jira_key:
            return jira_key

    # Exact match on source_key
    if src in PROJECT_MAP:
        return PROJECT_MAP[src]

    # Case-insensitive fallback, consistent with map_value()'s behaviour
    for key, val in PROJECT_MAP.items():
        if key.lower() == src.lower():
            return val

    return DEFAULT_PROJECT


# ── issue type ────────────────────────────────────────────────────────────────
_it = _load("issue_type")

ISSUE_TYPE_MAP: dict[str, str] = dict(
    zip(_it["Freshrelease Work Item Type"], _it["Mapped Jira Work item Type"])
)

DEFAULT_ISSUE_TYPE: str = _default(_it, "Mapped Jira Work item Type")

# "In case of Bug - Type" column (issue_type sheet): Freshrelease Work Item
# Type -> Bug Category value (customfield_10454, select list). Only rows
# with a non-empty value are kept — most issue types don't have one.
_BUG_CAT_COL = "In case of Bug - Type"
if _BUG_CAT_COL in _it.columns:
    BUG_CATEGORY_MAP: dict[str, str] = {
        str(fw_type).strip(): str(bug_cat).strip()
        for fw_type, bug_cat in zip(_it["Freshrelease Work Item Type"], _it[_BUG_CAT_COL])
        if str(bug_cat).strip()
    }
else:
    BUG_CATEGORY_MAP = {}

_st = _load("status")

STATUS_MAP = {}

for _, row in _st.iterrows():

    issue_type = str(
        row["Freshrelease Work Item Type"]
    ).strip()

    fw_status = str(
        row["FW Statuses"]
    ).strip()

    jira_status = str(
        row["Mapped Jira Statuses"]
    ).strip()

    if issue_type not in STATUS_MAP:
        STATUS_MAP[issue_type] = {}

    STATUS_MAP[issue_type][fw_status] = jira_status

DEFAULT_STATUS: str = _default(
    _st,
    "Mapped Jira Statuses"
)

# ── priority ──────────────────────────────────────────────────────────────────
_pr = _load("priority")

PRIORITY_MAP: dict[str, str] = dict(
    zip(_pr["source_label"], _pr["jira_name"])
)

DEFAULT_PRIORITY: str = _default(_pr, "jira_name")


# ── user ──────────────────────────────────────────────────────────────────────
_usr = _load("user")

USER_MAP: dict[str, str] = {
    row["source_email"]: row["jira_account_id"]
    for _, row in _usr.iterrows()
    if row["source_email"] and row["jira_account_id"]
}

MAP_UNMAPPED_USERS_TO_ME: bool = (
    _usr["map_unmapped_to_me"]
    .str.upper()
    .eq("YES")
    .any()
)


_unmapped_hit = _usr[
    _usr["map_unmapped_to_me"].str.strip().str.upper() == "YES"
]
MY_ACCOUNT_ID: str = (
    str(_unmapped_hit.iloc[0]["jira_account_id"]).strip()
    if not _unmapped_hit.empty
    else os.getenv("JIRA_ACCOUNT_ID", "YOUR_ACCOUNT_ID_HERE")
)

# ── custom fields ─────────────────────────────────────────────────────────────
_cf = _load("custom_field")


CUSTOM_FIELD_MAP = {}

for _, row in _cf.iterrows():

    issue_type = str(
        row["Issue Type Label"]
    ).strip()

    source_cf = str(
        row["Field Name"]
    ).strip()

    jira_cf = str(
        row["Jira Field ID"]
    ).strip()

    if issue_type not in CUSTOM_FIELD_MAP:
        CUSTOM_FIELD_MAP[issue_type] = {}

    CUSTOM_FIELD_MAP[issue_type][source_cf] = jira_cf

# Default custom field fallback
DEFAULT_CUSTOM_FIELD: str = ""
DEFAULT_CUSTOM_FIELD_VALUE: str = ""

for _, row in _cf.iterrows():

    if str(row.get("default", "")).strip().upper() == "YES":

        DEFAULT_CUSTOM_FIELD = str(
            # row["jira_customfield_id"]
            row["Jira Field ID"]
        ).strip()

        DEFAULT_CUSTOM_FIELD_VALUE = (
            str(row["default_value"]).strip()
            if "default_value" in _cf.columns
            else ""
        )

        break

def get_custom_field_mapping(
    issue_type: str,
    source_cf_name: str,
    field_value: str
) -> tuple[str | None, str]:

    type_cf_map = CUSTOM_FIELD_MAP.get(
        issue_type,
        {}
    )

    jira_field_id = type_cf_map.get(
        source_cf_name
    )

    # Normal mapping
    if jira_field_id:
        return jira_field_id, field_value

    # Fallback mapping
    if DEFAULT_CUSTOM_FIELD:
        return (
            DEFAULT_CUSTOM_FIELD,
            DEFAULT_CUSTOM_FIELD_VALUE
        )

    return None, field_value


# ── special role-based custom fields ─────────────────────────────────────────
# Rows with a "role" column are global fields — not issue-type specific.
# Non-coders can change the Jira Field ID in the excel without touching code.

ROLE_FIELD_MAP: dict[str, str] = {}

if "role" in _cf.columns:
    for _, row in _cf.iterrows():
        role     = str(row.get("role", "")).strip().lower()
        jira_cf  = str(row.get("Jira Field ID", "")).strip()
        if role and jira_cf:
            ROLE_FIELD_MAP[role] = jira_cf

# Export named constants so code can import them by name
SOURCE_KEY_CUSTOM_FIELD: str = ROLE_FIELD_MAP.get("source_key", "customfield_11501")
START_DATE_FIELD:        str = ROLE_FIELD_MAP.get("start_date", "customfield_10015")
FW_BUG_FIELD:            str = ROLE_FIELD_MAP.get("fw_bug",     "customfield_11502")


# ── resolution ────────────────────────────────────────────────────────────────
_res = _load("resolution")

RESOLUTION_MAP: dict[str, str] = dict(
    zip(_res["source_label"], _res["jira_resolution"])
)

DEFAULT_RESOLUTION: str = _default(
    _res,
    "jira_resolution"
)


# ── link type ─────────────────────────────────────────────────────────────────
_lt = _load("link_type")

# LINK_TYPE_MAP now stores {source_label: {jira_link_type, direction}}
LINK_TYPE_MAP: dict[str, dict] = {}

for _, row in _lt.iterrows():
    source_label   = str(row["source_label"]).strip().lower()
    jira_link_type = str(row["jira_link_type"]).strip()
    direction      = str(row.get("direction", "outward")).strip().lower() or "outward"

    if source_label and jira_link_type:
        LINK_TYPE_MAP[source_label] = {
            "jira_link_type": jira_link_type,
            "direction":      direction,
        }

DEFAULT_LINK_TYPE: str = _default(_lt, "jira_link_type")

# Default direction comes from the row marked default=YES
_lt_default_row = _lt[
    _lt["default"].str.strip().str.upper() == "YES"
]
DEFAULT_LINK_DIRECTION: str = (
    str(_lt_default_row.iloc[0]["direction"]).strip().lower()
    if not _lt_default_row.empty and "direction" in _lt.columns
    else "outward"
)


# ── sprint state ──────────────────────────────────────────────────────────────
_ss = _load("sprint_state")

SPRINT_STATE_MAP: dict[int, str] = {
    int(row["source_id"]): row["jira_state"]
    for _, row in _ss.iterrows()
}


# ── release status ────────────────────────────────────────────────────────────
_rs = _load("release_status")

RELEASE_STATUS_MAP: dict[int, str] = {
    int(row["source_id"]): row["jira_status"]
    for _, row in _rs.iterrows()
}


# ── fields to skip ────────────────────────────────────────────────────────────
_fs = _load("fields_to_skip")

FIELDS_TO_SKIP: list[str] = (
    _fs["field_name"]
    .dropna()
    .astype(str)
    .tolist()
)


# ── exports ──────────────────────────────────────────────────────────────────
__all__ = [
    "PROJECT_MAP",
    "PROJECT_SUBPROJECT_MAP",
    "get_project_key",
    "DEFAULT_PROJECT",
    "ISSUE_TYPE_MAP",
    "DEFAULT_ISSUE_TYPE",
    "BUG_CATEGORY_MAP",
    "STATUS_MAP",
    "DEFAULT_STATUS",
    "PRIORITY_MAP",
    "DEFAULT_PRIORITY",
    "USER_MAP",
    "MAP_UNMAPPED_USERS_TO_ME",
    "MY_ACCOUNT_ID",
    "CUSTOM_FIELD_MAP",
    "DEFAULT_CUSTOM_FIELD",
    "DEFAULT_CUSTOM_FIELD_VALUE",
    "get_custom_field_mapping",
    "ROLE_FIELD_MAP",
    "SOURCE_KEY_CUSTOM_FIELD",
    "START_DATE_FIELD",
    "FW_BUG_FIELD",
    "RESOLUTION_MAP",
    "DEFAULT_RESOLUTION",
    "LINK_TYPE_MAP",
    "DEFAULT_LINK_TYPE",
    "DEFAULT_LINK_DIRECTION",
    "SPRINT_STATE_MAP",
    "RELEASE_STATUS_MAP",
    "FIELDS_TO_SKIP",
]