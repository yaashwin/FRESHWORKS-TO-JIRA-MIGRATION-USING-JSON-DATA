# Freshworks (Freshrelease) → Jira Migration Pipeline

A JSON/CSV-driven, multi-phase migration pipeline that takes a Freshworks (Freshrelease) project export, flattens and transforms it using Excel-based field mappings, builds Jira REST API v3 payloads, and bulk-loads everything into Jira Cloud using a pool of parallel service accounts.

It migrates: **issues (epics, stories, tasks, sub-tasks), comments, issue links, dependencies, attachments, sprints, releases (Jira versions), and status transitions**, while preserving parent/epic hierarchy and resolving users, statuses, priorities, custom fields, and project routing through `mappings.xlsx`.

---

## Table of contents

1. [How it works](#how-it-works)
2. [Tech stack](#tech-stack)
3. [Repository structure](#repository-structure)
4. [Prerequisites](#prerequisites)
5. [Configuration (`.env`)](#configuration-env)
6. [Service accounts and parallelism](#service-accounts-and-parallelism)
7. [Mapping file (`mappings.xlsx`)](#mapping-file-mappingsxlsx)
8. [Running the pipeline](#running-the-pipeline)
9. [Load phase in detail](#load-phase-in-detail)
10. [Output folders and resume behaviour](#output-folders-and-resume-behaviour)
11. [Error handling and retries](#error-handling-and-retries)
12. [Monitoring and logs](#monitoring-and-logs)
13. [Security notes](#security-notes)
14. [Known limitations](#known-limitations)

---

## How it works

`master_runner.py` runs five phases in order. Each phase is a separate script launched as a subprocess, with its own log file. If a critical phase fails, the pipeline stops and tells you how to resume.

| Phase | Name | Script | What it does |
|------:|------|--------|--------------|
| 1 | Extraction | `test_refactored_shared_pool/rawdata_from_zip.py` | Converts the client's zip export (issues, comments, links, docs, sprints, releases) into a `raw/` folder structure. A timing report (`combine_api_timing.py`) runs after this phase. |
| 2 | Flatten | `flatten.py` | Flattens the raw nested JSON into structured CSV files. |
| 3 | Transform | `transform.py` | Applies field mappings: issue types, statuses, priorities, users, custom fields, resolutions, link types. |
| 4 | Prepare payload | `prepare_payload.py` | Builds Jira REST API v3 JSON payloads (ADF descriptions/comments, custom fields, sprint/version info, hierarchy metadata) from the transformed CSVs. |
| 5 | Load to Jira | `bulk_load_to_jira.py` | Bulk-uploads versions, sprints, issues, comments, links, dependencies, and attachments using parallel workers. |

```
Zip export ─► raw/ JSON ─► CSVs ─► mapped CSVs ─► Jira payload JSON ─► Jira Cloud
   (1)          (1)         (2)        (3)              (4)               (5)
```

---

## Tech stack

| Area | Technology |
|------|-----------|
| Language | Python 3.10+ (uses `X \| None` type-hint syntax) |
| HTTP | `requests` (Basic Auth with email + API token) |
| Data handling | `pandas`, `openpyxl` (reading/writing `.xlsx`) |
| Config | `python-dotenv` |
| Concurrency | `concurrent.futures.ThreadPoolExecutor`, `threading` (locks, events, thread-locals) |
| Target API | Jira Cloud REST API v3 (`/rest/api/3/...`) and Jira Software Agile API (`/rest/agile/1.0/...`) |
| Content format | Atlassian Document Format (ADF), converted from Freshworks HTML |
| Mapping config | Excel workbook (`mappings.xlsx`), editable by non-developers |
| Source API (optional) | Freshrelease sub-projects endpoint (`BASE_URL` + `AUTH_TOKEN`) used to resolve sub-project names |

Python packages to install:

```bash
pip install requests pandas openpyxl python-dotenv attrs
```

---

## Repository structure

```
.
├── master_runner.py              # Orchestrates phases 1–5, logging, resume flags
├── flatten.py                    # Phase 2: raw JSON → CSV
├── transform.py                  # Phase 3: apply mappings to CSVs
├── prepare_payload.py            # Phase 4: build Jira payloads (ADF, custom fields, hierarchy)
├── custom_field_payload.py       # Builds custom-field payload values from mappings
├── mappings.py                   # Loads every sheet of mappings.xlsx into Python dicts
├── create_mappings_excel.py      # Helper to generate the mappings workbook template
├── build_field_type_cache.py     # Caches Jira field schemas (short text / long text / other)
├── bulk_load_to_jira.py          # Phase 5: parallel, wave-based loader
├── load_to_jira.py               # Earlier, simpler loader
├── upload_attachments.py         # Parallel attachment uploader (shares the credential pool)
├── jira_credentials.py           # Multi-service-account pool (thread-pinned, round-robin)
├── Retry_failed_issues.py        # Re-attempts issues that failed during load
├── retry_dependencies.py         # Re-attempts pending cross-project dependencies
├── run_logger.py                 # Logging helper
├── test_refactored_shared_pool/  # Extraction code and raw data per project
└── .gitignore
```

> `mappings.xlsx` and `.env` are not committed. You must supply them (see below).

---

## Prerequisites

- Python 3.10 or newer
- A Jira Cloud site with a target project (and Jira Software boards, if you are migrating sprints)
- One or more Jira **service accounts** with API tokens (see next sections)
- The client's Freshworks/Freshrelease zip export
- A completed `mappings.xlsx`

---

## Configuration (`.env`)

Create a `.env` file in the repo root.

### Required

```env
JIRA_URL=https://your-site.atlassian.net
JIRA_EMAIL=service-account-1@yourcompany.com
JIRA_API_TOKEN=your_api_token_1
JIRA_ACCOUNT_ID=your_jira_account_id      # used as fallback for unmapped users
project_key=ABC                            # drives data/log folder names
```

### Additional service accounts (optional, recommended)

Numbering must be contiguous starting at `2`. The pool stops at the first gap.

```env
JIRA_EMAIL_2=service-account-2@yourcompany.com
JIRA_API_TOKEN_2=your_api_token_2
JIRA_ACCOUNT_ID_2=...

JIRA_EMAIL_3=service-account-3@yourcompany.com
JIRA_API_TOKEN_3=your_api_token_3
JIRA_ACCOUNT_ID_3=...
```

### Optional: Freshrelease sub-project lookup

```env
BASE_URL=https://your-freshrelease-api-base
AUTH_TOKEN=your_freshrelease_token
```

### Optional: tuning knobs (defaults shown)

| Variable | Default | Controls |
|----------|--------:|----------|
| `MAX_ISSUE_WORKERS` | 3 | Parallel workers creating issues |
| `VERSION_MAX_WORKERS` | 6 | Parallel version creation |
| `SPRINT_MAX_WORKERS` | 6 | Parallel sprint creation |
| `SPRINT_ASSIGN_MAX_WORKERS` | 6 | Parallel sprint assignment |
| `VERSION_ASSIGN_MAX_WORKERS` | 6 | Parallel fix-version assignment |
| `COMMENT_MAX_WORKERS` | 6 | Parallel comment posting (across issues) |
| `PATCH_MAX_WORKERS` | 6 | Parallel comment patching with attachment links |
| `LINK_MAX_WORKERS` | 20 | Parallel issue-link creation |
| `DEPENDENCY_MAX_WORKERS` | 20 | Parallel dependency creation |
| `ATTACHMENT_MAX_WORKERS` | 6 | Parallel attachment uploads |
| `MAX_PHASE_PASSES` | 5 | Auto-retry passes for comments / comment patching |
| `PHASE_COOLDOWN_SECONDS` | 60 | Wait between auto-retry passes |
| `ATTACHMENT_MAX_PASSES` | 5 | Auto-retry passes for attachments |
| `ATTACHMENT_COOLDOWN_SECONDS` | 60 | Wait between attachment passes |
| `ATTACHMENT_MAPPING_SAVE_EVERY` | 20 | Checkpoint interval for attachment mapping |
| `RATE_MONITOR_INTERVAL_SECONDS` | 10 | How often API rate stats are printed |

Hard-coded constants worth knowing: `BULK_BATCH_SIZE = 30` (issues per bulk-create call), `SPRINT_CHUNK_SIZE = 50` (Jira's limit for sprint assignment), `MAX_RELATED_WORKERS = 10`, `RATE_LIMIT_DELAY = 0.5s` per call per worker (0.2s for attachments).

---

## Service accounts and parallelism

### How the credential pool works (`jira_credentials.py`)

- Account #1 is the plain `JIRA_EMAIL` / `JIRA_API_TOKEN`. A single-account `.env` works with no changes.
- Accounts #2, #3, … are read from `JIRA_EMAIL_n` / `JIRA_API_TOKEN_n`.
- Each worker **thread** is assigned one account the first time it calls Jira, using a global round-robin counter, and keeps it for the thread's lifetime.
- The same pool is shared by `bulk_load_to_jira.py` and `upload_attachments.py`.
- If you have more workers than accounts, accounts are reused and some threads share one account.

### How many accounts do you need?

| Setup | Accounts | Notes |
|-------|---------:|-------|
| Minimum | 1 | Works, but every worker shares one account. |
| Default recommended | 3 | Matches `MAX_ISSUE_WORKERS = 3`, one account per issue-creation worker. |
| Higher throughput | 6 | Matches the 6-worker phases (comments, attachments, versions, sprints). |
| 1:1 for every phase | 20 | Only if you want a dedicated account for each link/dependency worker. Usually unnecessary. |

Rule of thumb: **number of accounts ≈ the largest worker count you actually run in the heavy phases** (issues, comments, attachments). Lower the worker counts rather than adding accounts if you hit 429s.

### Concurrency per phase (defaults)

| Phase | Workers |
|-------|--------:|
| Versions | 6 |
| Sprints | 6 |
| Issue creation (per wave, bulk batches of 30) | 3 |
| Related data per issue | up to 10 |
| Sprint assignment | 6 |
| Version assignment | 6 |
| Comments (parallel across issues, ordered within an issue) | 6 |
| Comment patching | 6 |
| Issue links | 20 |
| Dependencies | 20 |
| Attachments | 6 |

### Recommended permissions for each service account

Each account is a real Jira user. Grant access to the target project(s) with at least: Browse Projects, Create Issues, Edit Issues, Assign Issues, Assignable User, Modify Reporter (if reporters are set from the source), Add Comments, Create Attachments, Link Issues, Transition Issues, Schedule/Manage Sprints, and Administer Projects (for creating versions). On Jira Cloud, each account normally consumes a licensed seat.

### Important caveat about rate limits

The pool is designed on the assumption that each account has its own throttle. Atlassian has historically described Jira Cloud rate limiting as shared across users and apps, with no per-user quota, and the exact policy depends on your site and authentication method. Treat extra accounts as a way to spread load and attribute activity, not as a guaranteed multiplier of your quota. Check the [Atlassian rate-limiting documentation](https://developer.atlassian.com/cloud/jira/platform/rate-limiting/) for your plan and tune worker counts using the built-in rate monitor.

---

## Mapping file (`mappings.xlsx`)

All business mapping lives in Excel, so non-developers can change it without touching code. `mappings.py` loads it at import time.

| Sheet | Purpose |
|-------|---------|
| `project` | Source project key → Jira project key. Optional `Sub_project` column routes one source project to different Jira projects. A row marked `default = YES` is the fallback. |
| `issue_type` | Freshrelease work item type → Jira issue type; optional "In case of Bug - Type" category. |
| `status` | Per issue type: Freshworks status → Jira status. |
| `priority` | Source priority → Jira priority. |
| `user` | Source email/name → Jira `accountId`; `map_unmapped_to_me` sends unmapped users to a fallback account. |
| `custom_field` | Per issue type: source field name → Jira custom field ID. Rows with a `role` column define global fields (`source_key`, `start_date`, `fw_bug`). |
| `resolution` | Source resolution → Jira resolution. |
| `link_type` | Source link label → Jira link type and direction. |
| `sprint_state` / `release_status` | Numeric source state → Jira sprint state / version status. |
| `fields_to_skip` | Fields that must not be migrated. |
| `sprint` | Jira project key → board ID, used for sprint creation. |
| `parent_field` | Source project key → parent value for cascading fields. |
| `Subproject to Squad Mapping` | FR project + sub-project → Jira Product and Squad values. |

`create_mappings_excel.py` can generate a starting template.

---

## Running the pipeline

```bash
# Full pipeline (phases 1–5)
python master_runner.py

# Resume from a specific phase
python master_runner.py --from-phase 3

# Run only certain phases
python master_runner.py --phases 2 3 4

# List all phases
python master_runner.py --list
```

Recommended order for a new project:

1. Fill in `.env` and `mappings.xlsx`.
2. Place the client zip export where `rawdata_from_zip.py` expects it.
3. Run phases 1–4 and review the payload JSON in `{project_key}_data/payload/`.
4. Run phase 5 (load).
5. Run the retry scripts for anything that failed.

### Loader-only commands

```bash
python bulk_load_to_jira.py                  # load everything (resumes automatically)
python bulk_load_to_jira.py --retry-failed   # retry issues that failed earlier
python bulk_load_to_jira.py --patch-comments # patch posted comments with attachment links
python Retry_failed_issues.py                # standalone retry of failed issues
python retry_dependencies.py                 # retry cross-project dependencies once more projects are migrated
python upload_attachments.py                 # upload attachments on their own
```

---

## Load phase in detail

`bulk_load_to_jira.py` runs these steps in order. Feature flags at the top of the file switch each step on or off.

1. **Versions** are created in parallel. If one already exists, it is looked up and mapped instead of failing.
2. **Sprints** are created in parallel against the board ID from the `sprint` sheet.
3. **Issues** are created in **waves**, so parents exist before children:
   - Wave 1: Epics
   - Wave 2: Issues belonging to an epic (and issues with no parent/epic)
   - Wave 3: Issues with a parent
   - Wave 4: Sub-tasks
   
   Within each wave, issues are sent through Jira's bulk-create endpoint in batches of 30, falling back to single creation when a batch fails.
4. **Sprint and version assignment** are applied in parallel (sprint assignment in chunks of 50, Jira's limit).
5. **Status transitions** are applied so issues land in the mapped Jira status.
6. **Attachments** are uploaded through `upload_attachments.py`.
7. **Comments** are posted after attachments, in order within each issue, in parallel across issues.
8. **Comment patching** adds attachment links to already-posted comments.
9. **Links** (remaining issue links) and **dependencies** are created in parallel. Dependencies whose target is not yet migrated are saved to `pending_dependencies.json` for `retry_dependencies.py`.

Extra behaviours:

- **Sub-task project rule:** sub-tasks always use their parent's project key.
- **Fallback user:** if Jira rejects an assignee or reporter, only that field is swapped to a fallback account and the create is retried immediately, so children can still resolve the parent.
- **Live link-target lookup:** if a link target is missing from the local mapping, the loader searches Jira by the source-key custom field and caches the result.
- **Lean memory use:** per-issue JSON files (`issues_full/`, `issues_with_related/`) are read on demand instead of loading multi-GB files.

---

## Output folders and resume behaviour

With `project_key=ABC`:

```
ABC_data/
├── transformed/                 # CSVs after mapping (phase 3)
├── payload/                     # Jira payloads (phase 4)
│   ├── issues_meta.json
│   ├── issues_full/             # one file per issue
│   ├── issues_with_related/     # one file per issue + comments, links, attachments
│   ├── versions.json
│   ├── sprints.json
│   └── ...
└── mapping/
    ├── id_mapping.json          # source ID → Jira key/ID (the resume checkpoint)
    ├── attachment_mapping.json
    ├── failed_issues.json
    ├── failed_singles.json
    ├── pending_dependencies.json
    ├── subprojects_cache.json
    └── field_type_lookup.json

ABC_logs/                        # one log per phase + a master log
test_refactored_shared_pool/ABC_data/raw/   # extracted raw data
```

`RESUME_MODE = True` by default: anything already in `id_mapping.json` is skipped, so you can re-run safely after a crash.

---

## Error handling and retries

- **429 rate limits:** the loader reads `Retry-After`, logs the response headers and body, sleeps, and counts the hit. Because `MAX_RETRIES` is 1, a throttled call is reported as failed and picked up by the cooldown passes below.
- **Auto-retry with cooldown:** comments, comment patching, and attachments rerun up to 5 passes, waiting 60 seconds between passes. Only still-missing items are retried.
- **Failure files:** failed issues are written to `failed_issues.json` and `failed_singles.json` immediately, so they survive crashes.
- **Parent-mapping errors** are counted and reported separately.

---

## Monitoring and logs

- Every phase writes its own timestamped log to `{project_key}_logs/`, plus a master log of the whole run.
- During loading, a background thread prints API usage every 10 seconds: requests in the last second, last minute, total this run, counts by HTTP status, and 429 hits.
- A final summary shows the duration of each phase and total run time.

---

## Security notes

- Never commit `.env`, API tokens, or `mappings.xlsx` containing real account IDs.
- `bulk_load_to_jira.py` currently contains a hard-coded `FALLBACK_USER_ACCOUNT_ID`. Move it to `.env` before sharing the repo more widely.
- Use dedicated service accounts with project-scoped permissions, and revoke their tokens after the migration.

---

## Known limitations

- Extraction expects a Freshworks zip export; it is not a live API crawl.
- Some field IDs (for example the Product/Squad cascading field) are hard-coded in `prepare_payload.py` and need adjusting for a different Jira site.
- Rate-limit behaviour depends on your Jira Cloud plan; start with the defaults and increase workers gradually.
- No `requirements.txt` or `.env.sample` is committed yet. Adding both would make setup easier.

---

## License

No license file is currently included. Add one (for example MIT) if you intend to share or reuse this project.
