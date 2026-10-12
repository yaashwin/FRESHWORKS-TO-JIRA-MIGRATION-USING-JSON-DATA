"""
Builds field_type_lookup.json — a normalized map of every Jira custom field
to its type category (short_text / long_text / other), used by
prepare_payload.py to trim custom field values to the correct length limit
before posting.

Run this once up front, and re-run it any time a new field is added in
Jira (takes a few seconds — it's a single API call).

Usage:
    python build_field_type_cache.py FSALES_data
"""
import os
import sys
import json
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

JIRA_URL       = os.getenv("JIRA_URL")
JIRA_EMAIL     = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")


def classify_field(field):
    schema = field.get("schema", {}) or {}
    custom_type = schema.get("custom", "")
    base_type = schema.get("type", "")

    if custom_type.endswith(":textfield"):
        return "short_text"
    if custom_type.endswith(":textarea"):
        return "long_text"
    if base_type == "string" and not custom_type:
        # Built-in single-line string fields (e.g. summary) — treat as short_text
        return "short_text"
    return "other"


def main():
    if len(sys.argv) < 2:
        print("Usage: python build_field_type_cache.py <DATA_ROOT>  (e.g. FSALES_data)")
        sys.exit(1)

    if not (JIRA_URL and JIRA_EMAIL and JIRA_API_TOKEN):
        print("✖ JIRA_URL / JIRA_EMAIL / JIRA_API_TOKEN not set (check your .env / cwd).")
        sys.exit(1)

    data_root = sys.argv[1]
    mapping_dir = os.path.join(data_root, "mapping")
    os.makedirs(mapping_dir, exist_ok=True)
    output_path = os.path.join(mapping_dir, "field_type_lookup.json")

    print(f"Fetching field definitions from {JIRA_URL}/rest/api/3/field ...")
    resp = requests.get(
        f"{JIRA_URL}/rest/api/3/field",
        headers={"Accept": "application/json"},
        auth=(JIRA_EMAIL, JIRA_API_TOKEN),
        timeout=30,
    )

    if resp.status_code != 200:
        print(f"✖ Failed: HTTP {resp.status_code}")
        print(resp.text[:500])
        sys.exit(1)

    fields = resp.json()
    print(f"Retrieved {len(fields)} fields from Jira.\n")

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

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(lookup, f, indent=2)

    print(f"✔ Wrote {output_path}")
    print(f"    short_text: {counts['short_text']}")
    print(f"    long_text:  {counts['long_text']}")
    print(f"    other:      {counts['other']}")
    print(f"\nSample long_text fields (the ones that get the big limit):")
    for fid, info in lookup.items():
        if info["type"] == "long_text":
            print(f"    {fid}  —  {info['name']}")


if __name__ == "__main__":
    main()