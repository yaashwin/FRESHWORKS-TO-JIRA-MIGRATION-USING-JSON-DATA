# create_mappings_excel.py
import pandas as pd
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
sheets = {
    "project": pd.DataFrame({
        "source_key": ["ESTR", "AB1", "HI"],
        "jira_key":   ["ESTR", "BANK", "BANK"],
        "default":    ["YES", "", ""]
    }),
    "issue_type": pd.DataFrame({
        "source_label": ["Task", "Epic", "Story", "Bug", "Sub-task"],
        "jira_name":    ["Task", "Epic", "Story", "Bug", "Sub-task"],
        "default":      ["YES", "", "", "", ""]
    }),
    "status": pd.DataFrame({
        "source_label": ["Open", "In Progress", "Done", "Closed"],
        "jira_status":  ["To Do", "In Progress", "Done", "Done"],
        "default":      ["YES", "", "", ""]
    }),
    "priority": pd.DataFrame({
        "source_label": ["Medium", "High", "Low", "Urgent"],
        "jira_name":    ["Medium", "High", "Low", "Highest"],
        "default":      ["YES", "", "", ""]
    }),
    "user": pd.DataFrame({
        "source_email":       ["john@company.com", "sarah@company.com", ""],
        "jira_account_id":    ["5f7abc123def", "5f7xyz789jkl", ""],
        "map_unmapped_to_me": ["", "", "YES"]
    }),
    "custom_field": pd.DataFrame({
        "source_cf_name":      ["cf_qa_owner", "cf_release_plan", "cf_ask_type"],
        "jira_customfield_id": ["", "customfield_10050", ""]
    }),
    "resolution": pd.DataFrame({
        "source_label":   ["Fixed", "Duplicate"],
        "jira_resolution":["Done", "Duplicate"],
        "default":        ["YES", ""]
    }),
    "link_type": pd.DataFrame({
        "source_label":   ["blocks", "relates to", "duplicates"],
        "jira_link_type": ["Blocks", "Relates", "Duplicate"],
        "default":        ["", "YES", ""]
    }),
    "sprint_state": pd.DataFrame({
        "source_id": ["1", "2", "3"],
        "jira_state": ["active", "closed", "future"]
    }),
    "release_status": pd.DataFrame({
        "source_id":   ["1", "2", "3"],
        "jira_status": ["unreleased", "released", "archived"]
    }),
    "fields_to_skip": pd.DataFrame({
        "field_name": ["links", "following", "workflow_id",
                       "form_id", "position", "progress_pulse"]
    }),
}

with pd.ExcelWriter("mappings.xlsx", engine="openpyxl") as writer:
    for sheet_name, df in sheets.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(" mappings.xlsx created successfully")
print("   Open it and fill in your actual values")