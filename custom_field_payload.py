import os
import json
import re
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dotenv import load_dotenv
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
load_dotenv()

# =============================================================
# CONFIGURATION
# =============================================================
MAPPINGS_FILE       = "mappings.xlsx"
PROJECT_KEY         = os.getenv("project_key")
DATA_ROOT           = f"{PROJECT_KEY}_data" if PROJECT_KEY else "data"
SCHEMA_CACHE_FILE   = f"{DATA_ROOT}/mapping/jira_field_schema_cache.json"
# Built separately by build_field_type_cache.py — {fid: {"type","name","custom"}}.
# Used as a second source of human-readable field names for
# _display_label_for(), since this cache is sometimes fresher / more
# complete than SCHEMA_CACHE_FILE for a given field.
FIELD_TYPE_LOOKUP_FILE = f"{DATA_ROOT}/mapping/field_type_lookup.json"
MAPPING_SHEET       = "custom_field"
FIELD_MAPPING_SHEET = "field_mapping"
CASCADING_FIELD_SHEET = "cascading_field"

# Project-scoped option cache — replaces the old global field/{id}/context
# option cache. Options for select/radio/checkbox/multiselect/cascading
# fields are now fetched per PROJECT (and per issue type within it) via
# createmeta, because the same custom field can be configured with
# different allowed values per project. Keyed as:
#   { project_key: { jira_field_id: [allowed values...] } }
PROJECT_OPTIONS_CACHE_FILE = f"{DATA_ROOT}/mapping/jira_field_project_options_cache.json"
# Bump this whenever the createmeta-fetching logic changes in a way that
# could make previously-cached project option lists incomplete/incorrect.
# A mismatch forces one clean refetch instead of trusting stale cached data.
# v2: fixed a pagination bug where the fields endpoint (createmeta/{project}
# /issuetypes/{id}) has NO "isLast" key in its response — treating it as
# always-True after page 1 silently truncated results to the first 50
# fields, dropping any field (e.g. customfield_10899) positioned later.
PROJECT_OPTIONS_CACHE_VERSION = 2

os.makedirs(f"{DATA_ROOT}/mapping", exist_ok=True)


# =============================================================
# SOURCE TYPE → JIRA TYPE COMPATIBILITY MAP
# =============================================================
SOURCE_TYPE_COMPAT: dict[str, set[tuple]] = {
    "dropdown":        {("option", ""), ("string", ""), ("any", "")},
    "select":          {("option", ""), ("string", ""), ("any", "")},
    "single_select":   {("option", ""), ("string", ""), ("any", "")},
    "multiselect":     {("array", "option"), ("array", "string")},
    "multi_select":    {("array", "option"), ("array", "string")},
    "checkbox":        {("array", "option"), ("boolean", ""), ("array", "string")},
    "multicheckboxes": {("array", "option"), ("array", "string")},
    "radio":           {("option", ""), ("string", ""), ("any", "")},
    "radiobuttons":    {("option", ""), ("string", ""), ("any", "")},
    "text":            {("string", ""), ("text", ""), ("any", "")},
    "textfield":       {("string", ""), ("text", ""), ("any", "")},
    "textarea":        {("string", ""), ("text", ""), ("any", "")},
    "paragraph":       {("string", ""), ("text", ""), ("any", "")},
    "string":          {("string", ""), ("text", ""), ("any", "")},
    "label":           {("string", ""), ("array", "string"), ("any", "")},
    "number":          {("number", "")},
    "integer":         {("number", "")},
    "float":           {("number", "")},
    "numeric":         {("number", "")},
    "story_points":    {("number", "")},
    "date":            {("date", ""), ("datetime", "")},
    "datetime":        {("datetime", ""), ("date", "")},
    "datepicker":      {("date", ""), ("datetime", "")},
    "url":             {("string", ""), ("any", "")},
    "uri":             {("string", ""), ("any", "")},
    "user":            {("user", ""), ("array", "user"), ("string", "")},
    "assignee":        {("user", ""), ("array", "user")},
    "people":          {("user", ""), ("array", "user"), ("string", "")},
    "multiuserpicker": {("array", "user"), ("user", "")},
    "userpicker":      {("user", ""), ("string", "")},
    "boolean":         {("boolean", ""), ("string", ""), ("any", "")},
    "toggle":          {("boolean", ""), ("string", ""), ("any", "")},
    "version":         {("version", ""), ("array", "version")},
    "release":         {("version", ""), ("array", "version")},
    "fixversion":      {("version", ""), ("array", "version")},
    "labels":          {("array", "string"), ("string", "")},
    "cascadingselect": {("option", ""), ("array", "option")},
    "cascading":       {("option", ""), ("array", "option")},
    "group":           {("group", ""), ("array", "group")},
    "richtext":        {("string", ""), ("text", ""), ("any", "")},
    "wiki":            {("string", ""), ("text", ""), ("any", "")},
}


# =============================================================
# HELPERS
# =============================================================

def is_empty(value):
    if value is None:
        return True
    if isinstance(value, float):
        import math
        if math.isnan(value):
            return True
    if isinstance(value, str) and value.strip() in ("", "nan", "None", "NaT", "none"):
        return True
    return False


def safe_str(value):
    if is_empty(value):
        return ""
    return str(value).strip()


def safe_list(value):
    if is_empty(value):
        return []
    s = safe_str(value)
    return [v.strip() for v in s.split(",") if v.strip()]


# =============================================================
# SENTINEL
# =============================================================

class _FallbackToDefault:
    """
    Returned by _format_value when a value must go to the default field.
    """
    def __init__(self, field_id: str, display: str, reason: str):
        self.field_id = field_id
        self.display  = display
        self.reason   = reason

    def __repr__(self):
        return f"<FallbackToDefault field={self.field_id} reason={self.reason}>"


# =============================================================
# FIELD REGISTRY ENTRY
# =============================================================

class _FieldEntry:
    """
    Single source of truth for one custom field.

    Holds every piece of information known about a field so any part
    of the builder can look up label, type, schema, mismatch status
    with a single dict lookup — no repeated joins, no scattered dicts.

    Attributes:
        jira_field_id  — "customfield_10206"
        cf_col         — canonical source column name, e.g. "cf_vulnerability_type"
        field_label    — human label from field_mapping sheet, e.g. "Vulnerability Type"
        source_type    — type from field_mapping sheet, e.g. "dropdown"
        jira_type      — from API schema, e.g. "option"
        jira_items     — from API schema, e.g. ""
        jira_custom    — from API schema, e.g. "com.atlassian.jira.plugin.system.customfieldtypes:select"
        type_mismatch  — True if source_type incompatible with jira_type
    """
    __slots__ = (
        "jira_field_id", "cf_col", "field_label",
        "source_type", "jira_type", "jira_items", "jira_custom",
        "type_mismatch",
    )

    def __init__(self, jira_field_id: str):
        self.jira_field_id = jira_field_id
        self.cf_col        = ""
        self.field_label   = ""
        self.source_type   = ""
        self.jira_type     = ""
        self.jira_items    = ""
        self.jira_custom   = ""
        self.type_mismatch = False

    def display_label(self) -> str:
        """
        Human-readable label for this field, used in the default field
        consolidated text and in log messages.

        Priority:
            1. Field Label from field_mapping sheet  → "Vulnerability Type"
            2. Field Name  from field_mapping sheet  → "cf_vulnerability_type"
            3. Jira Field ID as last resort          → "customfield_10206"
        """
        return self.field_label or self.cf_col or self.jira_field_id

    def __repr__(self):
        return (
            f"<FieldEntry {self.jira_field_id} "
            f"cf={self.cf_col!r} label={self.field_label!r} "
            f"src={self.source_type} jira={self.jira_type}[{self.jira_items}] "
            f"mismatch={self.type_mismatch}>"
        )


# =============================================================
# CASCADING (PARENT/CHILD) FIELD CONFIG ENTRY
# =============================================================

class _CascadeFieldConfig:
    """
    Excel-driven config for one cascading select field (e.g. sub_project,
    theme). Loaded from the 'cascading_field' sheet in mappings.xlsx.

    Attributes:
        field_name         — "sub_project", "theme", ...
        jira_field_id      — "customfield_10973", "customfield_10900"
        parent_source_col  — row column used to look up the parent value
        child_source_col   — row column holding the raw child id
                              (ignored entirely when null_child=True)
        null_child         — True  → child is ALWAYS null in the payload
                              (e.g. theme)
                              False → normal parent/child resolution
                              (e.g. sub_project)
        parent_map         — {source_key: parent_value}, from parent_map_sheet.
                              EMPTY when no parent_map_sheet is configured —
                              in that case the parent value is validated
                              directly against Jira's live field_options
                              cache instead (see _resolve_cascade_parent_value).
        child_map          — {child_id: child_value}, from child_map_sheet
                              and/or injected at runtime via
                              set_cascading_child_map()
        display_label      — label used in default-field fallback text
    """
    __slots__ = (
        "field_name", "jira_field_id", "parent_source_col", "child_source_col",
        "null_child", "parent_map", "child_map", "display_label",
    )

    def __init__(self, field_name: str):
        self.field_name        = field_name
        self.jira_field_id     = ""
        self.parent_source_col = ""
        self.child_source_col  = ""
        self.null_child        = False
        self.parent_map: dict  = {}
        self.child_map: dict   = {}
        self.display_label     = field_name.replace("_", " ").title()

    def __repr__(self):
        return (
            f"<CascadeFieldConfig {self.field_name} id={self.jira_field_id} "
            f"parent_col={self.parent_source_col!r} child_col={self.child_source_col!r} "
            f"null_child={self.null_child} parent_map={len(self.parent_map)} "
            f"child_map={len(self.child_map)}>"
        )


# =============================================================
# MAIN CLASS
# =============================================================

class CustomFieldPayloadBuilder:
    """
    Dynamically builds Jira custom field payloads based on field schema.

    Usage:
        builder = CustomFieldPayloadBuilder(jira_url, jira_email, jira_api_token)
        builder.load()
        cf_payload = builder.build_custom_fields(row)
    """

    def __init__(self, jira_url, jira_email, jira_api_token):
        self.jira_url    = jira_url.rstrip("/")
        self.auth        = HTTPBasicAuth(jira_email, jira_api_token)
        self.headers     = {"Accept": "application/json", "Content-Type": "application/json"}

        # ── Core lookups ───────────────────────────────────────
        # Field registry: {jira_field_id: _FieldEntry}
        # Single source of truth — built at load time from all sources.
        self.field_registry:  dict[str, _FieldEntry] = {}

        # cf_col → jira_field_id  (fast lookup during build_custom_fields)
        self.cf_mapping:      dict[str, str]          = {}

        # project_key → { jira_field_id → [allowed option values] }
        # Fetched on demand via createmeta, scoped per project (see
        # _ensure_project_options_loaded / _fetch_project_scoped_options).
        self.project_field_options: dict[str, dict[str, list]] = {}

        # project_keys we've already fetched createmeta options for in this
        # run, so repeated rows for the same project don't re-hit the API.
        self._project_options_loaded: set[str] = set()

        # Full Jira schema: jira_field_id → schema dict (id/name/type only —
        # NOT options; options are project-scoped, see above)
        self.field_schema:    dict[str, dict]         = {}

        # Secondary name source: jira_field_id → {"type","name","custom"},
        # read from field_type_lookup.json (built by build_field_type_cache.py).
        # Consulted by _display_label_for() when field_schema doesn't have
        # a name for a given field (e.g. stale SCHEMA_CACHE_FILE).
        self.field_type_lookup: dict[str, dict]       = {}

        # User map: name/email → accountId
        self.user_map:        dict[str, str]          = {}

        # Excel-configured cascading (parent/child) select fields:
        # {field_name: _CascadeFieldConfig}, e.g. "sub_project", "theme"
        self.cascading_fields: dict[str, _CascadeFieldConfig] = {}

        # Default paragraph field
        self.default_cf_field_id: str = ""
        self.default_cf_value:    str = ""

        # Per-row state (reset each call to build_custom_fields)
        self._pending_fallbacks: list = []

        self._loaded = False

    # ──────────────────────────────────────────────────────────
    # LOAD — orchestrates all sub-loaders
    # ──────────────────────────────────────────────────────────

    def load(self, force_refresh=False):
        """
        Load order:
          1. custom_field sheet   → cf_mapping + default field
          2. field_mapping sheet  → field_label + source_type per field
          3. user sheet           → user_map
          4. cascading_field sheet→ cascading_fields (sub_project, theme, ...)
          5. Jira API / cache     → field_schema (id/name/type only)
          6. Build field_registry → joins all of the above
          7. Type-mismatch scan   → flags mismatched fields in registry

        NOTE: field OPTIONS (allowed values for select/radio/checkbox/
        multiselect/cascading fields) are no longer fetched here. They are
        project-scoped now (see _ensure_project_options_loaded) and are
        fetched lazily, per project, the first time a row for that project
        is processed in build_custom_fields()/build_cascading_fields().
        """
        self._load_cf_mapping()
        self._load_field_mapping_sheet()
        self._load_user_mapping()
        self._load_cascading_fields()
        self._load_field_schema(force_refresh=force_refresh)
        self._load_field_type_lookup()
        if force_refresh and os.path.exists(PROJECT_OPTIONS_CACHE_FILE):
            os.remove(PROJECT_OPTIONS_CACHE_FILE)
            self.project_field_options = {}
            self._project_options_loaded = set()
        else:
            self._load_project_options_cache()
        self._build_field_registry()
        self._detect_type_mismatches()
        self._loaded = True

        mismatches = sum(1 for e in self.field_registry.values() if e.type_mismatch)
        print(
            f"  [CustomFieldBuilder] Ready — "
            f"{len(self.cf_mapping)//2} CF mappings | "
            f"{len(self.field_registry)} registry entries | "
            f"{len(self.field_schema)} schemas | "
            f"{len(self.user_map)} users | "
            f"{len(self.cascading_fields)} cascading fields | "
            f"{mismatches} type mismatches"
        )

    # ── 1. custom_field sheet ─────────────────────────────────

    def _load_cf_mapping(self):
        """
        Load source_name → jira_field_id from custom_field sheet.
        Also detects the default paragraph field (default=YES).
        """
        self.cf_mapping          = {}
        self.default_cf_field_id = ""
        self.default_cf_value    = ""

        if not os.path.exists(MAPPINGS_FILE):
            print(f"  [CustomFieldBuilder] WARNING: {MAPPINGS_FILE} not found")
            return

        try:
            try:
                df = pd.read_excel(MAPPINGS_FILE, sheet_name=MAPPING_SHEET)
            except Exception:
                df = pd.read_excel(MAPPINGS_FILE, sheet_name=0)

            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

            name_col = next(
                (c for c in df.columns if c in (
                    "field_label", "name", "source_name",
                    "cf_name", "field_name", "source_cf_name"
                )), None
            )
            id_col = next(
                (c for c in df.columns if c in (
                    "jira_field_id", "jira_customfield_id", "customfield_id"
                )), None
            )

            if not name_col or not id_col:
                print(
                    f"  [CustomFieldBuilder] WARNING: Could not find required columns. "
                    f"Available: {list(df.columns)}"
                )
                return

            for _, row in df.iterrows():
                name     = safe_str(row.get(name_col))
                field_id = safe_str(row.get(id_col))
                if name and field_id:
                    self.cf_mapping[name]         = field_id
                    self.cf_mapping[name.lower()]  = field_id

            print(
                f"  [CustomFieldBuilder] Loaded {len(self.cf_mapping)//2} "
                f"CF mappings from '{MAPPING_SHEET}' sheet"
            )

            for _, row in df.iterrows():
                if safe_str(row.get("default", "")).upper() == "YES":
                    field_id = safe_str(row.get(id_col, ""))
                    if field_id:
                        self.default_cf_field_id = field_id
                        self.default_cf_value    = (
                            safe_str(row.get("default_value", ""))
                            if "default_value" in df.columns else ""
                        )
                        print(
                            f"  [CustomFieldBuilder] Default field → "
                            f"{field_id} (value: '{self.default_cf_value}')"
                        )
                        break

            if not self.default_cf_field_id:
                print(
                    f"  [CustomFieldBuilder] WARNING: No default=YES row found. "
                    f"Unmapped/mismatched values will be silently dropped."
                )

        except Exception as e:
            print(f"  [CustomFieldBuilder] ERROR loading CF mapping: {e}")

    # ── 2. field_mapping sheet ────────────────────────────────

    def _load_field_mapping_sheet(self):
        """
        Load Field Label + Field Type from field_mapping sheet.

        Builds two internal lookups (keyed by lowercased Field Name):
            _fm_labels: {"cf_vulnerability_type": "Vulnerability Type"}
            _fm_types:  {"cf_vulnerability_type": "dropdown"}

        These are consumed by _build_field_registry() and not exposed
        directly — the registry is the only public interface.
        """
        self._fm_labels: dict[str, str] = {}
        self._fm_types:  dict[str, str] = {}

        if not os.path.exists(MAPPINGS_FILE):
            return

        try:
            df = pd.read_excel(MAPPINGS_FILE, sheet_name=FIELD_MAPPING_SHEET, dtype=str)
        except Exception as e:
            print(
                f"  [CustomFieldBuilder] WARNING: Could not load '{FIELD_MAPPING_SHEET}' "
                f"sheet: {e}. Label resolution and type-check will be skipped."
            )
            return

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        name_col  = "field_name"  if "field_name"  in df.columns else None
        type_col  = "field_type"  if "field_type"  in df.columns else None
        label_col = "field_label" if "field_label" in df.columns else None

        if not name_col:
            print(
                f"  [CustomFieldBuilder] WARNING: 'Field Name' column missing "
                f"in '{FIELD_MAPPING_SHEET}' sheet."
            )
            return

        for _, row in df.iterrows():
            field_name = safe_str(row.get(name_col)).lower()
            if not field_name:
                continue
            if type_col:
                ft = safe_str(row.get(type_col)).lower()
                if ft:
                    self._fm_types[field_name] = ft
            if label_col:
                fl = safe_str(row.get(label_col))
                if fl:
                    self._fm_labels[field_name] = fl

        print(
            f"  [CustomFieldBuilder] Loaded {len(self._fm_labels)} field labels, "
            f"{len(self._fm_types)} field types from '{FIELD_MAPPING_SHEET}' sheet"
        )

    # ── 3. user sheet ─────────────────────────────────────────

    def _load_user_mapping(self):
        if not os.path.exists(MAPPINGS_FILE):
            return
        try:
            df = pd.read_excel(MAPPINGS_FILE, sheet_name="user")
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            name_col  = next((c for c in df.columns if "name"    in c), None)
            email_col = next((c for c in df.columns if "email"   in c), None)
            id_col    = next((c for c in df.columns if "account" in c or "jira_id" in c), None)
            for _, row in df.iterrows():
                account_id = safe_str(row.get(id_col, "")) if id_col else ""
                if not account_id:
                    continue
                if name_col:
                    name = safe_str(row.get(name_col, "")).lower()
                    if name:
                        self.user_map[name] = account_id
                if email_col:
                    email = safe_str(row.get(email_col, "")).lower()
                    if email:
                        self.user_map[email] = account_id
        except Exception as e:
            print(f"  [CustomFieldBuilder] WARNING: Could not load user mapping: {e}")

    # ── 4. cascading_field sheet ──────────────────────────────

    # def _load_two_col_sheet(self, sheet_name: str) -> dict:
    #     """
    #     Generic 2-column sheet loader: first column = key, second column =
    #     value. Used for cascading field parent_map_sheet / child_map_sheet.
    #     Keys are stored both as-is and upper-cased for lenient lookup.
    #     """
    #     try:
    #         df = pd.read_excel(MAPPINGS_FILE, sheet_name=sheet_name, dtype=str)
    #     except Exception as e:
    #         print(f"  [CustomFieldBuilder] WARNING: Could not load sheet '{sheet_name}': {e}")
    #         return {}

    #     if df.shape[1] < 2:
    #         print(f"  [CustomFieldBuilder] WARNING: Sheet '{sheet_name}' needs at least 2 columns")
    #         return {}

    #     key_col, val_col = df.columns[0], df.columns[1]
    #     mapping = {}
    #     for _, r in df.iterrows():
    #         k = safe_str(r.get(key_col))
    #         v = safe_str(r.get(val_col))
    #         if k and v:
    #             mapping[k]        = v
    #             mapping[k.upper()] = v
    #     return mapping
    def _load_two_col_sheet(self, sheet_name: str) -> dict:
        """
        2-column sheet loader for cascading field parent_map_sheet /
        child_map_sheet, e.g. the 'parent_field' sheet:

            fw_project_key | parent_project
            TOPS           | TOPS
            SECURITY       | security engineering
            AB1            | AI BU

        Column resolution:
          1. Prefer columns explicitly named "fw_project_key"/"project_key"
             (key) and "parent_project"/"parent_value"/"value" (value) —
             case/space-insensitive.
          2. Fall back to the first two columns positionally, with a warning.

        Keys are stored both as-is and upper-cased for lenient lookup.
        """
        try:
            df = pd.read_excel(MAPPINGS_FILE, sheet_name=sheet_name, dtype=str)
        except Exception as e:
            print(f"  [CustomFieldBuilder] WARNING: Could not load sheet '{sheet_name}': {e}")
            return {}

        if df.shape[1] < 2:
            print(f"  [CustomFieldBuilder] WARNING: Sheet '{sheet_name}' needs at least 2 columns")
            return {}

        norm_cols = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
        key_candidates = ("fw_project_key", "project_key", "source_project_key", "key")
        val_candidates = ("parent_project", "parent_value", "value", "parent")

        key_col = next((c for c, n in norm_cols.items() if n in key_candidates), None)
        val_col = next((c for c, n in norm_cols.items() if n in val_candidates), None)

        if key_col and val_col:
            print(
                f"  [CustomFieldBuilder] '{sheet_name}': matched key column "
                f"'{key_col}' → value column '{val_col}' by header name"
            )
        else:
            key_col, val_col = df.columns[0], df.columns[1]
            print(
                f"  [CustomFieldBuilder] WARNING: '{sheet_name}': could not "
                f"match expected header names {key_candidates} / "
                f"{val_candidates} — falling back to positional columns "
                f"'{key_col}' (key) / '{val_col}' (value). Verify this is correct!"
            )

        mapping = {}
        for _, r in df.iterrows():
            k = safe_str(r.get(key_col))
            v = safe_str(r.get(val_col))
            if k and v:
                mapping[k]         = v
                mapping[k.upper()] = v

        print(
            f"  [CustomFieldBuilder] '{sheet_name}': loaded {len(mapping)//2} "
            f"key→value entries — sample: {dict(list(mapping.items())[:3])}"
        )
        return mapping

    def _load_cascading_fields(self):
        """
        Load Excel-configured cascading (parent → child) select fields —
        e.g. sub_project (customfield_10973) and theme (customfield_10900) —
        from the 'cascading_field' sheet in mappings.xlsx.

        Expected columns (case/space-insensitive):
            field_name          e.g. "sub_project", "theme"
            jira_field_id        e.g. "customfield_10973"
            parent_source_col   row column used to look up the parent value,
                                 e.g. "fw_project_key". If left blank, falls
                                 back to fw_project_key / source_project_key /
                                 project_key on the row (same as legacy
                                 sub_project behaviour).
            child_source_col    row column holding the raw child id, e.g.
                                 "sub_project_id". Leave blank when
                                 null_child = YES.
            null_child           YES/NO — YES forces the child to ALWAYS be
                                 null in the payload (e.g. theme).
            parent_map_sheet    OPTIONAL. Sheet name with 2 columns:
                                 key | parent value — use this ONLY when the
                                 raw source value needs to be TRANSLATED
                                 into a different Jira value (e.g. sub_project:
                                 project key -> parent project name).
                                 Leave BLANK when the raw source value should
                                 be validated directly against Jira's own
                                 live-fetched option list for this field and
                                 passed through unchanged (e.g. theme:
                                 cf_theme's value already matches a real
                                 customfield_10900 option — no static list
                                 to maintain).
            child_map_sheet     (optional) sheet name with 2 columns:
                                 id | child value. If omitted, a child map can
                                 be supplied at runtime via
                                 set_cascading_child_map() — e.g. the
                                 API-fetched sub_project id→name cache.
            display_label        (optional) label used in the default-field
                                 fallback text on mismatch; defaults to a
                                 title-cased field_name.
        """
        self.cascading_fields = {}

        if not os.path.exists(MAPPINGS_FILE):
            return

        try:
            df = pd.read_excel(MAPPINGS_FILE, sheet_name=CASCADING_FIELD_SHEET, dtype=str)
        except Exception as e:
            print(
                f"  [CustomFieldBuilder] No '{CASCADING_FIELD_SHEET}' sheet found "
                f"({e}) — no cascading (parent/child) fields configured."
            )
            return

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        for _, row in df.iterrows():
            field_name    = safe_str(row.get("field_name")).lower()
            jira_field_id = safe_str(row.get("jira_field_id"))
            if not field_name or not jira_field_id:
                continue

            cfg = _CascadeFieldConfig(field_name)
            cfg.jira_field_id     = jira_field_id
            cfg.parent_source_col = safe_str(row.get("parent_source_col"))
            cfg.child_source_col  = safe_str(row.get("child_source_col"))
            cfg.null_child        = safe_str(row.get("null_child")).upper() in ("YES", "TRUE", "1")
            cfg.display_label     = (
                safe_str(row.get("display_label"))
                or field_name.replace("_", " ").title()
            )

            parent_map_sheet = safe_str(row.get("parent_map_sheet"))
            if parent_map_sheet:
                cfg.parent_map = self._load_two_col_sheet(parent_map_sheet)

            child_map_sheet = safe_str(row.get("child_map_sheet"))
            if child_map_sheet:
                cfg.child_map = self._load_two_col_sheet(child_map_sheet)

            self.cascading_fields[field_name] = cfg

        print(
            f"  [CustomFieldBuilder] Loaded {len(self.cascading_fields)} cascading "
            f"field(s) from '{CASCADING_FIELD_SHEET}' sheet: "
            f"{list(self.cascading_fields.keys())}"
        )

    def set_cascading_child_map(self, field_name: str, mapping: dict, overwrite: bool = False):
        """
        Inject a runtime-computed child map for a cascading field — e.g. the
        API-fetched sub_project id→name cache in prepare_payload.py.

        Only takes effect if the field is configured in the 'cascading_field'
        sheet. If that field already has a child_map loaded from an Excel
        child_map_sheet, the Excel data wins unless overwrite=True.
        """
        cfg = self.cascading_fields.get(field_name.lower())
        if not cfg:
            print(
                f"  [CustomFieldBuilder] WARNING: cascading field '{field_name}' "
                f"is not configured in the '{CASCADING_FIELD_SHEET}' sheet — "
                f"child map ignored."
            )
            return
        if cfg.child_map and not overwrite:
            return
        cfg.child_map = dict(mapping or {})
        print(
            f"  [CustomFieldBuilder] Registered {len(cfg.child_map)} runtime "
            f"child-map entries for cascading field '{field_name}'"
        )

    # ── 5. Jira schema + options ──────────────────────────────

    def _load_field_schema(self, force_refresh=False):
        """
        Loads field SCHEMA only (id, name, type, custom type) — a global,
        non-project-specific lookup via GET /rest/api/3/field. This is fine
        to keep global: it's just "what type is this field", not "what
        values are allowed for this field in this project", which is the
        part that has to be project-scoped (see _fetch_project_scoped_options).
        """
        if not force_refresh and os.path.exists(SCHEMA_CACHE_FILE):
            try:
                with open(SCHEMA_CACHE_FILE, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                self.field_schema = cache.get("fields", {})
                cached_at = cache.get("cached_at", "unknown")
                print(
                    f"  [CustomFieldBuilder] Schema loaded from cache "
                    f"({len(self.field_schema)} fields, cached {cached_at})"
                )
                return
            except Exception:
                pass
        self._fetch_schema_from_api()

    def _load_field_type_lookup(self):
        """
        Loads field_type_lookup.json (built separately by
        build_field_type_cache.py) as a secondary source of field names.
        Read-only — this builder never writes this file. Missing/unreadable
        file is not an error; it's an optional extra name source.
        """
        if not os.path.exists(FIELD_TYPE_LOOKUP_FILE):
            return
        try:
            with open(FIELD_TYPE_LOOKUP_FILE, "r", encoding="utf-8") as f:
                self.field_type_lookup = json.load(f)
            print(
                f"  [CustomFieldBuilder] Loaded {len(self.field_type_lookup)} "
                f"field names from '{FIELD_TYPE_LOOKUP_FILE}'"
            )
        except Exception as e:
            print(
                f"  [CustomFieldBuilder] WARNING: Could not load "
                f"'{FIELD_TYPE_LOOKUP_FILE}': {e}"
            )

    def _fetch_schema_from_api(self):
        print(f"  [CustomFieldBuilder] Fetching field schema from Jira API...")
        try:
            resp = requests.get(
                f"{self.jira_url}/rest/api/3/field",
                headers=self.headers, auth=self.auth, timeout=30
            )
            resp.raise_for_status()
            for field in resp.json():
                fid = field.get("id", "")
                if not fid.startswith("customfield_"):
                    continue
                s = field.get("schema", {})
                self.field_schema[fid] = {
                    "id":       fid,
                    "name":     field.get("name", ""),
                    "type":     s.get("type",     ""),
                    "items":    s.get("items",    ""),
                    "system":   s.get("system",   ""),
                    "custom":   s.get("custom",   ""),
                    "customId": s.get("customId", ""),
                }

            with open(SCHEMA_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "cached_at": datetime.now().isoformat(),
                    "fields": self.field_schema,
                }, f, indent=4)
            print(f"  [CustomFieldBuilder] Fetched + cached {len(self.field_schema)} field schemas.")

        except Exception as e:
            print(f"  [CustomFieldBuilder] ERROR fetching schema: {e}")

    # ── 5b. Project-scoped field OPTIONS (createmeta) ─────────

    def _load_project_options_cache(self):
        """Load the on-disk project-options cache into memory, once."""
        if not os.path.exists(PROJECT_OPTIONS_CACHE_FILE):
            return
        try:
            with open(PROJECT_OPTIONS_CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("cache_version") != PROJECT_OPTIONS_CACHE_VERSION:
                print(
                    "  [CustomFieldBuilder] Project-options cache predates "
                    "the createmeta rewrite — ignoring stale cache, will "
                    "refetch per project on demand."
                )
                return
            self.project_field_options = cache.get("projects", {})
            self._project_options_loaded = set(self.project_field_options.keys())
            print(
                f"  [CustomFieldBuilder] Project-scoped options loaded from "
                f"cache for {len(self._project_options_loaded)} project(s)."
            )
        except Exception as e:
            print(f"  [CustomFieldBuilder] Could not read project-options cache: {e}")

    def _save_project_options_cache(self):
        try:
            with open(PROJECT_OPTIONS_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "cached_at":     datetime.now().isoformat(),
                    "cache_version": PROJECT_OPTIONS_CACHE_VERSION,
                    "projects":      self.project_field_options,
                }, f, indent=4)
        except Exception as e:
            print(f"  [CustomFieldBuilder] Could not write project-options cache: {e}")

    def _ensure_project_options_loaded(self, project_key: str):
        """
        Lazily fetch + cache createmeta-based field options for one Jira
        project, the first time a row for that project is seen. No-ops on
        every subsequent call for the same project in this run.
        """
        if not project_key or project_key in self._project_options_loaded:
            return
        self._fetch_project_scoped_options(project_key)
        self._project_options_loaded.add(project_key)

    def _fetch_project_scoped_options(self, project_key: str):
        """
        Fetch allowed option values for EVERY custom field (select, radio,
        checkbox, multiselect, cascading — not just cascading) for one
        project, using createmeta instead of the field/{id}/context/option
        endpoints.

        Why: field/{id}/context/option is scoped to a CONTEXT, which is
        frequently shared/global across multiple projects, so its option
        list can contain values that are valid for project A but not for
        project B. createmeta is computed by Jira PER project + issue type,
        so it only ever returns what's actually valid for that project.

        Two levels of pagination are involved and BOTH are handled here:
          1. GET /rest/api/2/issue/createmeta/{project}/issuetypes
             — paginated list of issue types in this project.
          2. GET /rest/api/2/issue/createmeta/{project}/issuetypes/{id}
             — paginated list of field metadata for that issue type.
        Options are unioned across all issue types in the project, since
        the cascading/select fields we validate here (e.g. Theme) aren't
        expected to differ option-by-option between issue types within the
        same project — only between projects.
        """
        print(
            f"  [CustomFieldBuilder] Fetching createmeta field options for "
            f"project '{project_key}'..."
        )
        options_for_project: dict[str, list] = {}
        seen_values: dict[str, set] = {}

        try:
            # ── Step 1: paginate through issue types for this project ──
            issue_type_ids = []
            start_at = 0
            max_results = 50
            fetched_so_far = 0
            while True:
                r1 = requests.get(
                    f"{self.jira_url}/rest/api/2/issue/createmeta/{project_key}/issuetypes",
                    headers=self.headers, auth=self.auth, timeout=30,
                    params={"startAt": start_at, "maxResults": max_results},
                )
                r1.raise_for_status()
                page = r1.json()
                page_types = page.get("issueTypes", [])
                issue_type_ids.extend(it.get("id") for it in page_types if it.get("id"))
                fetched_so_far += len(page_types)
                total = page.get("total", fetched_so_far)

                # This endpoint does document "isLast", but we don't trust
                # it alone — same reasoning as step 2 below: fall back to
                # total/fetched-so-far so a missing/lying isLast can never
                # truncate the list silently.
                if not page_types or page.get("isLast", fetched_so_far >= total):
                    break
                start_at += max_results

            if not issue_type_ids:
                print(
                    f"  [CustomFieldBuilder] WARNING: no issue types found via "
                    f"createmeta for project '{project_key}' — check the "
                    f"project key and 'Create issues' permission."
                )

            # ── Step 2: for each issue type, paginate through its fields ──
            for issue_type_id in issue_type_ids:
                start_at = 0
                fetched_so_far = 0
                while True:
                    r2 = requests.get(
                        f"{self.jira_url}/rest/api/2/issue/createmeta/"
                        f"{project_key}/issuetypes/{issue_type_id}",
                        headers=self.headers, auth=self.auth, timeout=30,
                        params={"startAt": start_at, "maxResults": max_results},
                    )
                    r2.raise_for_status()
                    page = r2.json()
                    fields_page = page.get("fields", [])
                    total = page.get("total", len(fields_page))

                    for f in fields_page:
                        fid = f.get("fieldId", "")
                        if not fid.startswith("customfield_"):
                            continue
                        allowed_values = f.get("allowedValues")
                        if not allowed_values:
                            continue

                        bucket = options_for_project.setdefault(fid, [])
                        seen   = seen_values.setdefault(fid, set())

                        for opt in allowed_values:
                            # For cascading selects, allowedValues here is
                            # already the PARENT-level list (children are
                            # nested under opt["children"], never mixed in
                            # flat like the old context/option endpoint) —
                            # so no parentOptionId filtering needed anymore.
                            v = opt.get("value")
                            if v and v not in seen:
                                seen.add(v)
                                bucket.append(v)

                    fetched_so_far += len(fields_page)

                    # IMPORTANT: unlike the issue-types-list endpoint above,
                    # this fields endpoint does NOT return an "isLast" key
                    # in its response (confirmed against Atlassian's own
                    # docs — the response only has fields/maxResults/
                    # startAt/total). Relying on page.get("isLast", True)
                    # here would silently default to True and stop after
                    # page 1 EVERY time, no matter how many fields exist —
                    # which is exactly why a field further down the list
                    # (e.g. one at position >50 like customfield_10899)
                    # would go missing from the cache even though the field
                    # itself is completely valid. We must compute "last
                    # page" ourselves from total instead.
                    if not fields_page or fetched_so_far >= total:
                        break
                    start_at += max_results

            self.project_field_options[project_key] = options_for_project
            self._save_project_options_cache()
            print(
                f"  [CustomFieldBuilder] Cached options for "
                f"{len(options_for_project)} field(s) in project "
                f"'{project_key}' (across {len(issue_type_ids)} issue type(s))."
            )
        except Exception as e:
            print(
                f"  [CustomFieldBuilder] ERROR fetching createmeta options "
                f"for project '{project_key}': {e}"
            )
            self.project_field_options.setdefault(project_key, {})

    # ── 6. Build field registry ───────────────────────────────

    def _build_field_registry(self):
        """
        Join cf_mapping + field_mapping sheet + Jira schema into one
        _FieldEntry per Jira field ID.

        This is the single source of truth used everywhere else:
            - display label for consolidated default field text
            - source type for mismatch detection
            - schema type for _format_value routing
            - mismatch flag for fast-path in build_custom_fields
        """
        self.field_registry = {}

        # Also build reverse: jira_field_id → canonical cf_col name
        # (the original field name before lowercasing, from cf_mapping)
        _jira_to_cf: dict[str, str] = {}
        for source_name, jira_field_id in self.cf_mapping.items():
            # cf_mapping stores both original + lower — keep the one with cf_ prefix
            if source_name.startswith("cf_") or (
                source_name == source_name.lower()
                and jira_field_id not in _jira_to_cf
            ):
                _jira_to_cf[jira_field_id] = source_name

        # Build one entry per unique jira_field_id
        seen_ids = set()
        for source_name, jira_field_id in self.cf_mapping.items():
            if jira_field_id in seen_ids:
                continue
            seen_ids.add(jira_field_id)

            entry             = _FieldEntry(jira_field_id)
            entry.cf_col      = _jira_to_cf.get(jira_field_id, source_name)
            cf_key            = entry.cf_col.lower()

            # From field_mapping sheet
            entry.field_label = self._fm_labels.get(cf_key, "")
            entry.source_type = self._fm_types.get(cf_key, "")

            # From Jira schema
            schema            = self.field_schema.get(jira_field_id, {})
            entry.jira_type   = schema.get("type",   "")
            entry.jira_items  = schema.get("items",  "")
            entry.jira_custom = schema.get("custom", "")

            self.field_registry[jira_field_id] = entry

        print(
            f"  [CustomFieldBuilder] Field registry built — "
            f"{len(self.field_registry)} entries"
        )

    # ── 7. Type-mismatch detection ────────────────────────────

    def _detect_type_mismatches(self):
        """
        Compare source_type vs Jira schema type for every registry entry.
        Flags entry.type_mismatch = True and logs a warning once at load time.
        Runtime lookup is O(1) via entry.type_mismatch.
        """
        print("\n  [CustomFieldBuilder] Running type-mismatch detection...")
        mismatch_count = 0

        for jira_field_id, entry in self.field_registry.items():
            if not entry.source_type or not entry.jira_type:
                continue

            compat_set = SOURCE_TYPE_COMPAT.get(entry.source_type)
            if compat_set is None:
                print(
                    f"  [CustomFieldBuilder] WARNING: Unknown source type "
                    f"'{entry.source_type}' for '{entry.cf_col}' — "
                    f"add it to SOURCE_TYPE_COMPAT to enable type checking"
                )
                continue

            jira_pair = (entry.jira_type, entry.jira_items)
            if jira_pair not in compat_set:
                entry.type_mismatch = True
                mismatch_count += 1
                print(
                    f"  [CF TYPE MISMATCH] '{entry.display_label()}' → {jira_field_id}\n"
                    f"    source type : {entry.source_type}\n"
                    f"    jira type   : {entry.jira_type}"
                    + (f"[{entry.jira_items}]" if entry.jira_items else "")
                    + f"\n    → Values will go to default field at runtime"
                )

        if mismatch_count == 0:
            print("  [CustomFieldBuilder] No type mismatches found ✓")
        else:
            print(
                f"  [CustomFieldBuilder] {mismatch_count} mismatch(es) — "
                f"their values will be redirected to the default field."
            )
        print()

    # ──────────────────────────────────────────────────────────
    # CASCADING (PARENT/CHILD) FIELDS — sub_project, theme, ...
    # ──────────────────────────────────────────────────────────

    def _resolve_cascade_parent_value(
        self, cfg: "_CascadeFieldConfig", raw_parent_key: str, project_key: str = ""
    ):
        """
        Resolve the parent (top-level) value for a cascading field, in one
        of two modes depending on how it's configured in the
        'cascading_field' sheet:

          1. Excel translation table (parent_map_sheet is set, e.g.
             sub_project -> 'parent_field'): the raw source value (a project
             key) is TRANSLATED into a different display value via the
             static Excel sheet. Returns None if not found.

          2. Live Jira options validation (no parent_map_sheet configured,
             e.g. theme): the raw backend value (e.g. cf_theme) is expected
             to ALREADY match one of Jira's real option values for this
             field IN THIS PROJECT. We validate it against
             self.project_field_options[project_key] — fetched via
             createmeta for that exact project (see
             _fetch_project_scoped_options) — and pass it through
             unchanged on a match. Returns None if the value isn't a
             recognized Jira option for this field in this project (even
             if it IS a valid option in some other project).
        """
        if cfg.parent_map:
            return (
                cfg.parent_map.get(raw_parent_key)
                or cfg.parent_map.get(raw_parent_key.upper())
            )

        # Live Jira options validation mode (no parent_map_sheet).
        # IMPORTANT: unlike plain option/select fields elsewhere in this
        # class (which lenient-pass-through when the cache is empty),
        # cascading select fields get NO benefit of the doubt. A cascading
        # select is one of the pickiest field types in Jira — sending a
        # value that isn't an exact, real option 400s the ENTIRE issue
        # create/update request, not just this field. So: if we cannot
        # positively confirm the raw value against a real, non-empty,
        # PROJECT-SCOPED options list, we do NOT guess — we return None so
        # the caller diverts the whole field to the default field instead.
        allowed = self.project_field_options.get(project_key, {}).get(cfg.jira_field_id)
        if not allowed:
            return None
        return raw_parent_key if raw_parent_key in allowed else None

    def build_cascading_fields(self, row) -> dict:
        """
        Build payload entries for every Excel-configured cascading select
        field (see 'cascading_field' sheet) — e.g. sub_project
        (customfield_10973) and theme (customfield_10900).

        Rules:
          - No parent source value on the row  → field skipped entirely
            (nothing to migrate for this row, not a mismatch).
          - Parent value present but NOT found in the Excel parent map
            → MISMATCH → the whole field is diverted to the default field,
            exactly like any other custom-field mismatch. Nothing is sent
            to the actual Jira cascading field.
          - null_child = YES (e.g. theme) → child is ALWAYS null in the
            payload, regardless of any child data present on the row.
          - null_child = NO (e.g. sub_project):
              * no child id on the row      → {"value": parent, "child": None}
              * child id resolves           → {"value": parent,
                                                 "child": {"value": child}}
              * child id given but NOT found → MISMATCH → whole field
                diverted to the default field, same as a parent mismatch.

        Any mismatch is appended to self._pending_fallbacks so
        build_custom_fields() can fold it into the same default-field
        consolidation used for every other field.
        """
        result = {}

        # Target Jira project for this row (set by transform.py). Needed to
        # look up createmeta-scoped options for live-validation cascading
        # fields like theme — see _resolve_cascade_parent_value.
        row_project_key = safe_str(row.get("jira_project_key")) or safe_str(row.get("project_key"))
        self._ensure_project_options_loaded(row_project_key)

        for field_name, cfg in self.cascading_fields.items():
            if not cfg.jira_field_id:
                continue

            # # ── Resolve parent source value ──
            # if cfg.parent_source_col:
            #     # Explicitly configured column (e.g. "cf_theme" for theme,
            #     # or "fw_project_key" for sub_project) — use it as-is.
            #     # An empty value here means "no data for this field on this
            #     # row", NOT "fall back to the project key".
            #     raw_parent_key = safe_str(row.get(cfg.parent_source_col))
            # else:
            #     # No column configured — legacy default: derive the parent
            #     # key from the project key (this is what sub_project relied
            #     # on before the sheet was introduced).
            #     raw_parent_key = ""
            #     for alt_col in ("fw_project_key", "source_project_key", "project_key"):
            #         raw_parent_key = safe_str(row.get(alt_col))
            #         if raw_parent_key:
            #             break

            # if not raw_parent_key:
            #     # Nothing supplied for this field on this row — skip quietly.
            #     continue
            # ── Resolve parent source value ──
            # Try the explicitly configured column first. If it's empty or
            # doesn't exist on the row (e.g. Excel says "fw_project_key" but
            # the actual CSV column is "project_key"), fall back to the
            # other common project-key column names before giving up.
            raw_parent_key = ""
            tried_cols = []

            if cfg.parent_source_col:
                tried_cols.append(cfg.parent_source_col)
                raw_parent_key = safe_str(row.get(cfg.parent_source_col))

            if not raw_parent_key:
                for alt_col in ("fw_project_key", "source_project_key", "project_key"):
                    if alt_col in tried_cols:
                        continue
                    tried_cols.append(alt_col)
                    raw_parent_key = safe_str(row.get(alt_col))
                    if raw_parent_key:
                        if cfg.parent_source_col:
                            print(
                                f"  [CF CASCADE] '{field_name}': configured "
                                f"parent_source_col '{cfg.parent_source_col}' was "
                                f"empty/missing on this row — used fallback "
                                f"column '{alt_col}' instead"
                            )
                        break

            if not raw_parent_key:
                print(
                    f"  [CF CASCADE] '{field_name}': no parent key found in "
                    f"any of {tried_cols} for this row — field skipped"
                )
                continue

            parent_value = self._resolve_cascade_parent_value(cfg, raw_parent_key, row_project_key)
            if not parent_value:
                print(
                    f"  [CF FALLBACK] '{raw_parent_key}' not found in "
                    f"'{field_name}' parent map — moved to default field"
                )
                self._pending_fallbacks.append(
                    (cfg.display_label, f"{raw_parent_key} (parent not mapped)")
                )
                continue

            # ── null_child fields (e.g. theme): child is ALWAYS null ──
            if cfg.null_child:
                result[cfg.jira_field_id] = {"value": parent_value, "child": None}
                continue

            # ── normal parent/child fields (e.g. sub_project) ──
            raw_child = safe_str(row.get(cfg.child_source_col)) if cfg.child_source_col else ""
            if not raw_child:
                # No child supplied — parent-only is a valid, non-mismatch state.
                result[cfg.jira_field_id] = {"value": parent_value, "child": None}
                continue

            child_key   = raw_child.split(".")[0]  # "36870.0" -> "36870"
            child_value = cfg.child_map.get(child_key) or cfg.child_map.get(child_key.upper())

            if not child_value:
                print(
                    f"  [CF FALLBACK] child '{raw_child}' not found in "
                    f"'{field_name}' child map — moved to default field"
                )
                self._pending_fallbacks.append(
                    (cfg.display_label, f"{raw_parent_key} / {raw_child}")
                )
                continue

            result[cfg.jira_field_id] = {
                "value": parent_value,
                "child": {"value": child_value},
            }

        return result

    # ──────────────────────────────────────────────────────────
    # BUILD PAYLOAD — main entry point
    # ──────────────────────────────────────────────────────────

    def build_custom_fields(self, row) -> dict:
        """
        Build the custom fields payload for one issue row.

        Deduplication strategy (industrial standard):
        ─────────────────────────────────────────────
        A single Jira field can appear in the row under multiple column
        names (e.g. cf_vulnerability_type AND customfield_10206 because
        transform.py copied the value).  We track every jira_field_id
        that has been written to `result` in `processed_field_ids`.
        Any subsequent column that resolves to the same jira_field_id
        is skipped entirely — value already handled, no duplication.

        Fallback rules:
        ───────────────
        1. Field in registry with type_mismatch=True  → default field
        2. _format_value returns _FallbackToDefault    → default field
        3. Column not in any mapping                   → default field

        Display label in default field:
        ────────────────────────────────
        Uses _FieldEntry.display_label() which prioritises:
            field_label (from field_mapping sheet)
            → cf_col (source field name)
            → jira_field_id (last resort)
        """
        result              = {}
        processed_field_ids = set()
        self._pending_fallbacks = []   # reset per row
        unmapped_parts      = []

        # Target Jira project for this row — every option/select/radio/
        # checkbox/multiselect field is now validated against createmeta
        # options SCOPED TO THIS PROJECT (see _format_value), not a global
        # cross-project list.
        row_project_key = safe_str(row.get("jira_project_key")) or safe_str(row.get("project_key"))
        self._ensure_project_options_loaded(row_project_key)

        # ── Pass 0: Excel-configured cascading fields (sub_project, theme, ...) ──
        # These IDs are always fully owned by build_cascading_fields() — mark
        # them processed up front (success or mismatch) so Pass 1 / Pass 2
        # never touch them again, even if a stray cf_* column for the same
        # Jira field id exists in the row.
        for cfg in self.cascading_fields.values():
            if cfg.jira_field_id:
                processed_field_ids.add(cfg.jira_field_id)

        cascading_result = self.build_cascading_fields(row)
        result.update(cascading_result)

        # Drain Pass-0 (cascading field) mismatches into unmapped_parts
        unmapped_parts.extend(
            f"{label} : {val}" for label, val in self._pending_fallbacks
        )
        self._pending_fallbacks = []

        # ── Pass 1: mapping-driven (cf_mapping keys) ───────────
        seen_norm = set()
        for source_name, jira_field_id in self.cf_mapping.items():
            norm = source_name.lower()
            if norm in seen_norm:
                continue
            seen_norm.add(norm)

            if jira_field_id in processed_field_ids:
                continue

            value = row.get(source_name)
            if is_empty(value):
                value = row.get(norm)
            if is_empty(value):
                continue

            entry = self.field_registry.get(jira_field_id)

            # Type-mismatch fast-path
            if entry and entry.type_mismatch:
                label = entry.display_label()
                self._pending_fallbacks.append((label, safe_str(value)))
                processed_field_ids.add(jira_field_id)   # mark as handled
                continue

            schema    = self.field_schema.get(jira_field_id, {})
            formatted = self._format_value(jira_field_id, schema, value, entry, row_project_key)

            if isinstance(formatted, _FallbackToDefault):
                label = self._display_label_for(jira_field_id, entry, source_name)
                self._pending_fallbacks.append((label, safe_str(value)))
                processed_field_ids.add(jira_field_id)
            elif formatted is not None:
                result[jira_field_id] = formatted
                processed_field_ids.add(jira_field_id)

        # Drain Pass-1 fallbacks into unmapped_parts
        unmapped_parts.extend(
            f"{label} : {val}"
            for label, val in self._pending_fallbacks
        )
        self._pending_fallbacks = []

        # ── Pass 2: column-driven (cf_* / customfield_* columns) ──
        for source_col in row.index:
            if not (source_col.startswith("cf_") or source_col.startswith("customfield_")):
                continue

            value         = row.get(source_col)
            if is_empty(value):
                continue
            jira_field_id = self._resolve_field_id(source_col)

            # ── Already handled (deduplication) ───────────────
            # This is the key fix: if transform.py wrote the value into
            # BOTH cf_vulnerability_type AND customfield_10206, Pass 1
            # (or an earlier Pass-2 iteration) will have added
            # customfield_10206 to processed_field_ids, so the second
            # column is silently skipped — no duplication.
            if jira_field_id and jira_field_id in processed_field_ids:
                continue

            if jira_field_id:
                # Explicitly mapped to default field → consolidate
                if jira_field_id == self.default_cf_field_id:
                    entry        = self.field_registry.get(jira_field_id)
                    display_name = self._display_label_for(
                        jira_field_id, entry,
                        source_col[3:] if source_col.startswith("cf_") else source_col,
                    )
                    str_val = safe_str(value) if not is_empty(value) else ""
                    unmapped_parts.append(f"{display_name} : {str_val}")
                    continue

                entry = self.field_registry.get(jira_field_id)

                # Type-mismatch fast-path
                # if entry and entry.type_mismatch:
                #     unmapped_parts.append(
                #         f"{entry.display_label()} : {safe_str(value) if not is_empty(value) else ''}"
                #     )
                #     processed_field_ids.add(jira_field_id)
                #     continue
                
                if entry and entry.type_mismatch:
                    if not is_empty(value):
                        unmapped_parts.append(f"{entry.display_label()} : {safe_str(value)}")
                    processed_field_ids.add(jira_field_id)
                    continue
                schema    = self.field_schema.get(jira_field_id, {})
                formatted = self._format_value(jira_field_id, schema, value, entry, row_project_key)

                # if isinstance(formatted, _FallbackToDefault):
                #     label = entry.display_label() if entry else (
                #         source_col[3:] if source_col.startswith("cf_") else source_col
                #     )
                #     unmapped_parts.append(
                #         f"{label} : {safe_str(value) if not is_empty(value) else ''}"
                #     )
                if isinstance(formatted, _FallbackToDefault):
                    label = self._display_label_for(
                        jira_field_id, entry,
                        source_col[3:] if source_col.startswith("cf_") else source_col,
                    )
                    if not is_empty(value):
                        unmapped_parts.append(f"{label} : {safe_str(value)}")

                    # Drain any partial fallbacks from array fields
                    for lbl, val in self._pending_fallbacks:
                        unmapped_parts.append(f"{lbl} : {val}")
                    self._pending_fallbacks = []
                    processed_field_ids.add(jira_field_id)

                elif formatted is not None:
                    result[jira_field_id] = formatted
                    processed_field_ids.add(jira_field_id)
                    # Drain partial fallbacks from array option fields
                    for lbl, val in self._pending_fallbacks:
                        unmapped_parts.append(f"{lbl} : {val}")
                    self._pending_fallbacks = []

            else:
                # Truly unmapped column — use field_mapping label if available
                cf_key       = source_col.lower()
                display_name = (
                    self._fm_labels.get(cf_key)
                    or self._fm_labels.get(cf_key[3:] if cf_key.startswith("cf_") else cf_key)
                    or (source_col[3:] if source_col.startswith("cf_") else source_col)
                )
                str_val = safe_str(value) if not is_empty(value) else ""
                unmapped_parts.append(f"{display_name} : {str_val}")

        # ── Consolidate into default paragraph field (ADF) ────
        if unmapped_parts and self.default_cf_field_id:
            result[self.default_cf_field_id] = self._make_adf(
                "\n".join(unmapped_parts)
            )
            print(
                f"  [CF DEFAULT] {len(unmapped_parts)} field(s) "
                f"consolidated into '{self.default_cf_field_id}' (ADF)"
            )

        return result

    # ──────────────────────────────────────────────────────────
    # RESOLVE FIELD ID
    # ──────────────────────────────────────────────────────────

    def _resolve_field_id(self, source_col: str):
        """Resolve a source column name to a Jira custom field ID."""
        if source_col.startswith("customfield_"):
            return source_col
        if source_col in self.cf_mapping:
            return self.cf_mapping[source_col]
        if source_col.lower() in self.cf_mapping:
            return self.cf_mapping[source_col.lower()]
        stripped = source_col[3:] if source_col.startswith("cf_") else source_col
        if stripped in self.cf_mapping:
            return self.cf_mapping[stripped]
        if stripped.lower() in self.cf_mapping:
            return self.cf_mapping[stripped.lower()]
        return None

    # ──────────────────────────────────────────────────────────
    # ADF BUILDER
    # ──────────────────────────────────────────────────────────

    def _make_adf(self, text: str) -> dict:
        """
        Wrap plain text in Atlassian Document Format.
        Each non-empty line becomes its own paragraph node.

        Example for "Vulnerability Type : Cross Site Scripting\nSource : Internal":
            { "version": 1, "type": "doc", "content": [
                {"type": "paragraph", "content": [{"type": "text", "text": "Vulnerability Type : Cross Site Scripting"}]},
                {"type": "paragraph", "content": [{"type": "text", "text": "Source : Internal"}]}
            ]}
        """
        lines = [line for line in text.split("\n") if line.strip()]
        nodes = [
            {"type": "paragraph", "content": [{"type": "text", "text": line}]}
            for line in lines
        ] or [{"type": "paragraph", "content": []}]

        return {"version": 1, "type": "doc", "content": nodes}


    def _display_label_for(self, field_id: str, entry: "_FieldEntry | None" = None,
                            source_name: str = "") -> str:
        """
        Best available human-readable label for a Jira field ID.

        Priority:
            1. entry.display_label()       — field_label / cf_col from mappings.xlsx
            2. field_schema "name"          — from jira_field_schema_cache.json
                                               (this builder's own schema cache)
            3. field_type_lookup "name"     — from field_type_lookup.json
                                               (build_field_type_cache.py's cache;
                                               used as a fallback since it's
                                               sometimes fresher / more complete
                                               than #2 for a given field)
            4. source_name (original column from the row), if given
            5. field_id                     — absolute last resort
        """
        if entry:
            return entry.display_label()
        schema_name = self.field_schema.get(field_id, {}).get("name", "")
        if schema_name:
            return schema_name
        lookup_name = self.field_type_lookup.get(field_id, {}).get("name", "")
        if lookup_name:
            return lookup_name
        return source_name or field_id

    def _format_value(
        self,
        field_id,
        schema:   dict,
        raw_value,
        entry:    "_FieldEntry | None" = None,
        project_key: str = "",
    ) -> any:
        """
        Format a raw value into the correct Jira API format based on schema.

        project_key: the target Jira project for this row. Used to look up
        createmeta-scoped allowed options for option/select/radio/checkbox/
        multiselect fields (see project_field_options) instead of a global,
        cross-project option list.

        Returns:
            formatted value   — ready for Jira API
            None              — skip this field
            _FallbackToDefault — redirect to default field
        """
        field_type    = schema.get("type",   "")
        items_type    = schema.get("items",  "")
        custom_type   = schema.get("custom", "")
        display_label = self._display_label_for(field_id, entry)
        str_value     = safe_str(raw_value)

        if not field_type:
            if not str_value:
                return None
            # System fields (description, environment, etc.) aren't returned
            # by /rest/api/3/field with a "customfield_" id, so they never
            # get a schema entry here (field_type == ""). These are ADF
            # fields in the v3 API regardless.
            if field_id in ("description", "environment"):
                return self._make_adf(str_value)
            return str_value

        # if field_type in ("string", "text", "any"):
        #     if not str_value:
        #         return None
        #     # Multi-line / rich-text custom fields (Text Field (multi-line),
        #     # wiki-style renderer fields, etc.) report schema type "string"
        #     # just like plain text fields, but the Jira Cloud v3 API still
        #     # requires their value to be an ADF document. Sending a plain
        #     # string to one of these raises:
        #     #   "The field value is not valid Atlassian Document Format (ADF)"
        #     # We detect this from the field's own "custom" type (fetched from
        #     # Jira, never hardcoded per-field) rather than from a field ID.
        #     ct = custom_type.lower()
        #     if "textarea" in ct or "richtext" in ct or "wiki" in ct:
        #         return self._make_adf(str_value)
        #     return str_value
        if field_type in ("string", "text", "any"):
            if not str_value:
                return None
            ct = custom_type.lower()
            if "textarea" in ct or "richtext" in ct or "wiki" in ct:
                return self._make_adf(str_value)
            if "labels" in ct:
                return re.sub(r'\s+', '_', str_value)
            return str_value

        if field_type == "number":
            try:
                f = float(str_value)
                return int(f) if f == int(f) else f
            except (ValueError, TypeError):
                print(f"  [CF FALLBACK] '{str_value}' cannot be cast to number for {field_id} — moved to default field")
                return _FallbackToDefault(field_id, f"{display_label} : {str_value}", "coerce_fail:number")

        if field_type == "date":
            match = re.search(r'(\d{4}-\d{2}-\d{2})', str_value)
            if match:
                return match.group(1)
            print(f"  [CF FALLBACK] '{str_value}' is not a valid date for {field_id} — moved to default field")
            return _FallbackToDefault(field_id, f"{display_label} : {str_value}", "coerce_fail:date")

        if field_type == "datetime":
            match = re.search(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2})', str_value)
            if match:
                return match.group(1).replace(" ", "T") + ":00.000+0000"
            match = re.search(r'(\d{4}-\d{2}-\d{2})', str_value)
            if match:
                return match.group(1) + "T00:00:00.000+0000"
            print(f"  [CF FALLBACK] '{str_value}' is not a valid datetime for {field_id} — moved to default field")
            return _FallbackToDefault(field_id, f"{display_label} : {str_value}", "coerce_fail:datetime")

        if field_type == "array":
            values = safe_list(raw_value)
            if not values:
                return None

            if (items_type == "option"
                    or "multiselect"     in custom_type.lower()
                    or "multicheckboxes" in custom_type.lower()
                    or "radiobuttons"    in custom_type.lower()):

                allowed      = self.project_field_options.get(project_key, {}).get(field_id, [])
                valid_result = []
                for v in values:
                    if not v:
                        continue
                    if not allowed or v in allowed:
                        valid_result.append({"value": v})
                    else:
                        print(f"  [CF FALLBACK] '{v}' not in allowed options for {field_id} — moved to default field")
                        self._pending_fallbacks.append((display_label, v))
                return valid_result if valid_result else None

            # if items_type in ("string", ""):
            #     return [v for v in values if v]
            if items_type in ("string", ""):
                if "labels" in custom_type.lower():
                    # Jira labels can't contain spaces — sanitize instead of
                    # letting the API reject the whole issue creation.
                    return [re.sub(r'\s+', '_', v) for v in values if v]
                return [v for v in values if v]

            if items_type == "user":
                result = []
                for v in values:
                    aid = self._resolve_user(v)
                    if aid:
                        result.append({"accountId": aid})
                    elif v:
                        print(f"  [CF FALLBACK] '{v}' could not be resolved to a user for {field_id} — moved to default field")
                        self._pending_fallbacks.append((display_label, v))
                return result if result else None
            if items_type == "group":
                return [{"name": v} for v in values if v]

            if items_type == "version":
                return [{"name": v} for v in values if v]

            return [v for v in values if v]

        if field_type == "option" or (
            "select" in custom_type.lower()
            and "multiselect" not in custom_type.lower()
        ):
            if not str_value:
                return None
            # allowed = self.project_field_options.get(project_key, {}).get(field_id, [])
            # if allowed and str_value not in allowed:
            #     print(f"  [CF FALLBACK] '{str_value}' not in allowed options for {field_id} — moved to default field")
            #     return _FallbackToDefault(field_id, f"{display_label} : {str_value}", "option_mismatch")
            # return {"value": str_value}
            allowed = self.project_field_options.get(project_key, {}).get(field_id, [])
            if allowed and str_value not in allowed:
                # Exact match failed — retry case-insensitively before giving
                # up. Jira's createmeta option spelling is authoritative, so
                # if we find a case-insensitive match, send Jira's own
                # spelling rather than our source string.
                lower_map = {a.lower(): a for a in allowed}
                canonical = lower_map.get(str_value.lower())
                if canonical:
                    return {"value": canonical}
                print(f"  [CF FALLBACK] '{str_value}' not in allowed options for {field_id} — moved to default field")
                return _FallbackToDefault(field_id, f"{display_label} : {str_value}", "option_mismatch")
            return {"value": str_value}

        if field_type == "user":
            aid = self._resolve_user(str_value)
            if aid:
                return {"accountId": aid}
            if str_value:
                print(f"  [CF FALLBACK] '{str_value}' could not be resolved to a user for {field_id} — moved to default field")
                return _FallbackToDefault(field_id, f"{display_label} : {str_value}", "coerce_fail:user")
            return None

        if field_type == "group":
            return {"name": str_value} if str_value else None

        if field_type == "version":
            return {"name": str_value} if str_value else None

        if field_type == "boolean":
            lower = str_value.lower()
            if lower in ("true", "1", "yes", "false", "0", "no"):
                return lower in ("true", "1", "yes")
            print(f"  [CF FALLBACK] '{str_value}' is not a valid boolean for {field_id} — moved to default field")
            return _FallbackToDefault(field_id, f"{display_label} : {str_value}", "coerce_fail:boolean")

        # Anything reaching this point is a field type we have no explicit,
        # verified way to build a valid Jira payload for — e.g. "priority",
        # "status", "issuelinks", "sd-customerrequesttype" (Service Desk
        # Request Type / "Customer Request Type" field), or any other
        # complex/system type Jira might report. Previously this fell
        # through to `return str_value`, which silently sent a raw string
        # to fields like customfield_10010 and produced errors such as
        # "Invalid customer request value". Per the intended design (see
        # type-mismatch handling above), unknown/unsupported types must
        # always be redirected to the default field, never sent as-is.
        if not str_value:
            return None

        print(f"  [CF FALLBACK] Unsupported/unknown field type '{field_type}' for {field_id} — moved to default field")
        return _FallbackToDefault(field_id, f"{display_label} : {str_value}", f"unsupported_type:{field_type}")

    # ──────────────────────────────────────────────────────────
    # RESOLVE USER
    # ──────────────────────────────────────────────────────────

    def _resolve_user(self, value: str):
        if not value:
            return None
        normalized = value.strip().lower()
        if normalized in self.user_map:
            return self.user_map[normalized]
        if len(value) > 20 and value.replace(":", "").isalnum():
            return value
        return None

    # ──────────────────────────────────────────────────────────
    # UTILITIES
    # ──────────────────────────────────────────────────────────

    def refresh_schema(self):
        """Force refresh field schema + all cached project options, and
        re-run mismatch detection."""
        for f in [SCHEMA_CACHE_FILE, PROJECT_OPTIONS_CACHE_FILE]:
            if os.path.exists(f):
                os.remove(f)
        self.project_field_options   = {}
        self._project_options_loaded = set()
        self._fetch_schema_from_api()
        self._build_field_registry()
        self._detect_type_mismatches()

    def print_schema_for_fields(self):
        """Debug — print full registry as a table."""
        print("\n  [CustomFieldBuilder] Field Registry:")
        print(f"  {'Label':<30} {'CF Col':<30} {'Jira ID':<25} {'Src Type':<15} {'Jira Type':<15} {'Mismatch'}")
        print("  " + "-" * 130)
        for fid, entry in self.field_registry.items():
            jira_type_str = entry.jira_type + (f"[{entry.jira_items}]" if entry.jira_items else "")
            mismatch      = "⚠ MISMATCH" if entry.type_mismatch else "✓"
            print(
                f"  {entry.field_label:<30} {entry.cf_col:<30} {fid:<25} "
                f"{entry.source_type:<15} {jira_type_str:<15} {mismatch}"
            )
        print()

    def get_unmapped_columns(self, row) -> list:
        """Return CF columns in the row that have no mapping."""
        return [
            col for col in row.index
            if (col.startswith("cf_") or col.startswith("customfield_"))
            and not self._resolve_field_id(col)
        ]

    def get_type_mismatched_fields(self) -> dict:
        """Return summary of all type-mismatched fields."""
        return {
            fid: {
                "label":      entry.field_label,
                "cf_col":     entry.cf_col,
                "source_type": entry.source_type,
                "jira_type":  entry.jira_type,
                "jira_items": entry.jira_items,
            }
            for fid, entry in self.field_registry.items()
            if entry.type_mismatch
        }