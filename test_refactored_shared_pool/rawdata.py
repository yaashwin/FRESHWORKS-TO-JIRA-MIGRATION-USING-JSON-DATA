# # """
# # ============================================================
# # rawdata_from_zip.py — CONVERT A ZIPPED DS_EXPORT
# #               -> {project_key}_data/raw/ structure
# #               (same output layout the live API pipeline produced)
# # ============================================================

# # WHY THIS EXISTS
# # ----------------
# # The old pipeline (pipeline_orchestrator.py + extract_*.py) hit the live
# # API and wrote each project's data to its own top-level folder:

# #     {PROJECT_KEY}_data/raw/{issues,comments,links,dependencies,
# #                              documents,sprints,releases,projects,
# #                              reference,logs}/...

# # Now the backend instead hands us a single .zip export containing one
# # folder per data type (issue_details/, comments/, dependencies_blocks/,
# # documents/, issue_links/, sprints/, statuses/, issue_types/,
# # project_details/, integrated_resources/, releases/), each holding
# # *.jsonl files. This script converts that zip into EXACTLY the same
# # {project_key}_data/raw/... layout as before, so every downstream tool
# # (flatten.py, reports, etc.) keeps working unchanged.

# # Two things the zip does NOT contain that the old pipeline used to
# # fetch live:
# #   1. Comment inline images / comment attachments (only HTML content +
# #      a "documents" API link are present in the export)
# #   2. Actual document file bytes (only metadata + a download "link" are
# #      present)
# # So this script still uses BASE_URL + AUTH_TOKEN from .env to download
# # those specific files — everything else comes from the zip, no other
# # API calls are made.

# # USAGE
# # -----
# #     python rawdata_from_zip.py path/to/export.zip
# #     python rawdata_from_zip.py path/to/already_extracted_folder
# #     python rawdata_from_zip.py                       # auto-finds a
# #                                                        # single *.zip in CWD

# # .env required:
# #     BASE_URL     - used only to resolve/download attachment & image links
# #     AUTH_TOKEN   - used only to authenticate those downloads
# #     project_key  - OPTIONAL. If set, ONLY that project is converted
# #                    (matched against the "XXXX-123" issue-key prefix).
# #                    Leave blank/unset to convert every project found
# #                    in the export.

# # REQUIRES (same folder as this script):
# #     shared_http.py          - shared requests.Session / connection pool
# #     shared_thread_pool.py   - shared ThreadPoolExecutor for downloads

# # ============================================================
# # CONFIRMED SCHEMAS (unchanged from the original rawdata.py)
# # ============================================================
# # issue_details/*.jsonl   -> {"issue": {...}, "issue_types": [...],
# #                              "users": [...], "priorities": [...],
# #                              "statuses": [...]}
# # comments/*.jsonl        -> {"<issue_id>": {"comments": [...], "users": [...]}}
# # dependencies_blocks/*.jsonl (and/or dependencies_blocked_by/*.jsonl)
# #                         -> {"<issue_id>": {"issue_dependencies": [
# #                                {"id":.., "predecessor_id":.., "successor_id":..,
# #                                 "dependency_type":..}], "issues":[...]}}
# # documents/*.jsonl       -> {"<issue_id>": {"documents": [...], "users": [...]}}
# # issue_links/*.jsonl     -> {"<issue_id>": {"issue_links": [...],
# #                              "issue_types": [...], "issues": [...]}}
# # integrated_resources/*.jsonl -> {"<issue_id>": {"integrated_resources": [...]}}
# # sprints/*.jsonl, releases/*.jsonl -> ONE FLAT RECORD PER LINE
# #                              (sprints: "primary_workspace_id" == project id,
# #                               releases: "project_id" directly)
# # statuses/*.jsonl, issue_types/*.jsonl -> one flat record per line
# # project_details/*.jsonl -> {"project": {...}, "users": [...]}
# # ============================================================
# # """

# # import os
# # import re
# # import sys
# # import json
# # import glob
# # import time
# # import zipfile
# # import tempfile
# # from collections import defaultdict
# # from urllib.parse import urlparse
# # from concurrent.futures import as_completed

# # import requests
# # from dotenv import load_dotenv

# # sys.stdout.reconfigure(encoding="utf-8")
# # sys.stderr.reconfigure(encoding="utf-8")

# # from shared_http import get_shared_session
# # from shared_thread_pool import get_executor, shutdown_executor

# # # =============================================================
# # # CONFIGURATION
# # # =============================================================
# # load_dotenv()
# # BASE_URL   = os.getenv("BASE_URL")
# # AUTH_TOKEN = os.getenv("AUTH_TOKEN")
# # # Optional filter — if set, ONLY this project is converted.
# # FILTER_PROJECT_KEY = (os.getenv("project_key") or "").strip()

# # HEADERS = {
# #     "Authorization": f"Token {AUTH_TOKEN}",
# #     "Content-Type": "application/json",
# # }

# # SESSION = get_shared_session()
# # DOWNLOAD_WORKERS = int(os.getenv("EXTRACTION_MAX_WORKERS", "5"))

# # # Folders the export is expected to contain (checked to auto-locate the
# # # real export root inside an extracted zip, which sometimes nests
# # # everything one level deeper e.g. export/issue_details/...).
# # EXPECTED_SUBDIRS = {"issue_details", "comments", "documents", "issue_links"}

# # download_stats_lock = None  # set in main() once we know we're threaded


# # # =============================================================
# # # ZIP HANDLING
# # # =============================================================
# # def resolve_export_dir(path_arg):
# #     """
# #     Accepts either a .zip file or an already-extracted folder and
# #     returns a directory path containing issue_details/, comments/, etc.
# #     """
# #     if path_arg is None:
# #         zips = glob.glob("*.zip")
# #         if len(zips) == 1:
# #             path_arg = zips[0]
# #         elif len(zips) > 1:
# #             raise SystemExit(f"Multiple .zip files found in CWD, pass one explicitly: {zips}")
# #         else:
# #             raise SystemExit("No path given and no .zip found in CWD.")

# #     if os.path.isdir(path_arg):
# #         return locate_export_root(path_arg)

# #     if os.path.isfile(path_arg) and path_arg.lower().endswith(".zip"):
# #         extract_dir = tempfile.mkdtemp(prefix="ds_export_")
# #         print(f"  Extracting {path_arg} -> {extract_dir}")
# #         with zipfile.ZipFile(path_arg, "r") as zf:
# #             zf.extractall(extract_dir)
# #         return locate_export_root(extract_dir)

# #     raise SystemExit(f"'{path_arg}' is neither a directory nor a .zip file.")


# # def locate_export_root(base_dir):
# #     """Search base_dir (and one level of subfolders) for the folder that
# #     actually contains issue_details/, comments/, etc."""
# #     candidates = [base_dir] + [
# #         os.path.join(base_dir, d) for d in os.listdir(base_dir)
# #         if os.path.isdir(os.path.join(base_dir, d))
# #     ]
# #     for c in candidates:
# #         try:
# #             entries = set(os.listdir(c))
# #         except (FileNotFoundError, NotADirectoryError):
# #             continue
# #         if EXPECTED_SUBDIRS & entries:
# #             return c
# #     raise SystemExit(
# #         f"Could not find issue_details/, comments/, etc. inside '{base_dir}'. "
# #         f"Check the zip's internal folder structure."
# #     )


# # # =============================================================
# # # GENERIC HELPERS
# # # =============================================================
# # def read_jsonl(filepath):
# #     rows = []
# #     if not os.path.exists(filepath):
# #         return rows
# #     with open(filepath, "r", encoding="utf-8") as f:
# #         for line_no, line in enumerate(f, 1):
# #             line = line.strip()
# #             if not line:
# #                 continue
# #             try:
# #                 rows.append(json.loads(line))
# #             except json.JSONDecodeError as e:
# #                 print(f"  [warn] bad JSON at {filepath}:{line_no} — {e}")
# #     return rows


# # def iter_keyed_records(export_dir, folder):
# #     """Yields (issue_id_str, payload_dict) for every line across every
# #     *.jsonl file in `folder`, where each line looks like:
# #         {"<issue_id>": {...payload...}}
# #     """
# #     for filepath in sorted(glob.glob(os.path.join(export_dir, folder, "*.jsonl"))):
# #         with open(filepath, "r", encoding="utf-8") as f:
# #             for line_no, line in enumerate(f, 1):
# #                 line = line.strip()
# #                 if not line:
# #                     continue
# #                 try:
# #                     obj = json.loads(line)
# #                 except json.JSONDecodeError as e:
# #                     print(f"  [warn] bad JSON at {filepath}:{line_no} — {e}")
# #                     continue
# #                 if not isinstance(obj, dict):
# #                     continue
# #                 for k, v in obj.items():
# #                     yield k, v


# # def write_json(filepath, data):
# #     os.makedirs(os.path.dirname(filepath), exist_ok=True)
# #     tmp = filepath + ".tmp"
# #     with open(tmp, "w", encoding="utf-8") as f:
# #         json.dump(data, f, ensure_ascii=False, indent=4)
# #     os.replace(tmp, filepath)


# # def raw_root(project_key):
# #     """The per-project output root — matches the live pipeline's
# #     f"{PROJECT_KEY}_data/raw" convention exactly."""
# #     return f"{project_key}_data/raw"


# # def project_key_from_issue_key(issue_key):
# #     """'FBOTS-5' -> 'FBOTS'."""
# #     if not issue_key or "-" not in issue_key:
# #         return "UNKNOWN"
# #     return issue_key.rsplit("-", 1)[0]


# # def merge_by_id(target_map, records):
# #     for r in records or []:
# #         if isinstance(r, dict) and "id" in r:
# #             target_map[r["id"]] = r


# # def wanted_project(project_key):
# #     """Honor the optional project_key filter from .env."""
# #     if not FILTER_PROJECT_KEY:
# #         return True
# #     return project_key.lower() == FILTER_PROJECT_KEY.lower()


# # # =============================================================
# # # GLOBAL LOOKUPS
# # # =============================================================
# # issue_meta = {}                  # issue_id(int) -> {"key","project_key","title"}
# # project_id_to_key = {}           # project_id(int) -> project_key
# # users_map = defaultdict(dict)    # project_key -> {user_id: user}
# # priorities_map = defaultdict(dict)   # project_key -> {priority_id: priority}
# # issue_types_bonus = defaultdict(dict)  # project_key -> {issue_type_id: dict}


# # # =============================================================
# # # ATTACHMENT / IMAGE DOWNLOADING
# # # (adapted from the old extract_comments.py / extract_documents.py —
# # #  only these two things still touch the live API, since the zip
# # #  export doesn't carry the actual file bytes)
# # # =============================================================
# # def api_get(endpoint, params=None, retries=3):
# #     """Small GET-with-retry helper, same policy as the old extractors."""
# #     url = f"{BASE_URL}/{endpoint.lstrip('/')}"
# #     for attempt in range(retries):
# #         try:
# #             resp = SESSION.get(url, headers=HEADERS, params=params, timeout=30)
# #             resp.raise_for_status()
# #             time.sleep(0.2)
# #             return resp.json()
# #         except requests.exceptions.RequestException as e:
# #             if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
# #                 time.sleep(30)
# #                 continue
# #             if attempt == retries - 1:
# #                 print(f"  [warn] GET {endpoint} failed: {e}")
# #                 return None
# #             time.sleep(1 * (attempt + 1))
# #     return None


# # def download_binary(url, file_path):
# #     """Stream-download a single file to disk. Returns True on success."""
# #     if os.path.exists(file_path):
# #         return True
# #     if not url.startswith("http"):
# #         url = f"{BASE_URL}{url}" if url.startswith("/") else f"{BASE_URL}/{url}"
# #     try:
# #         download_headers = {"Authorization": f"Token {AUTH_TOKEN}"}
# #         resp = SESSION.get(url, headers=download_headers, timeout=60, stream=True)
# #         if resp.status_code != 200:
# #             return False
# #         os.makedirs(os.path.dirname(file_path), exist_ok=True)
# #         with open(file_path, "wb") as f:
# #             for chunk in resp.iter_content(chunk_size=8192):
# #                 if chunk:
# #                     f.write(chunk)
# #         return True
# #     except Exception as e:
# #         print(f"  [warn] download failed for {url}: {e}")
# #         return False


# # IMG_SRC_PATTERN = r'<img[^>]+src=["\'](.*?)["\']'
# # IMG_CONTENT_PATTERN = r'<img[^>]*>(https?://[^<]+)</img>'


# # def download_comment_images(project_key, comment_id, html_content):
# #     if not html_content or not isinstance(html_content, str):
# #         return
# #     urls = re.findall(IMG_SRC_PATTERN, html_content, re.IGNORECASE)
# #     urls += re.findall(IMG_CONTENT_PATTERN, html_content, re.IGNORECASE)
# #     if not urls:
# #         return
# #     images_dir = os.path.join(
# #         raw_root(project_key), "comments", project_key,
# #         "attachments", f"comment_{comment_id}", "images"
# #     )
# #     for idx, img_url in enumerate(urls):
# #         parsed = urlparse(img_url)
# #         filename = os.path.basename(parsed.path)
# #         if not filename or "." not in filename:
# #             ext = parsed.path.split(".")[-1] if "." in parsed.path else "jpg"
# #             filename = f"image_{idx}.{ext}"
# #         download_binary(img_url, os.path.join(images_dir, filename))


# # def download_comment_attachments(project_key, comment_id, documents_link):
# #     """documents_link is a relative API endpoint (e.g. from
# #     comment['links']['documents']) that must be GET'd to list the
# #     actual documents, each of which is then downloaded by its own link."""
# #     if not documents_link:
# #         return
# #     data = api_get(documents_link.lstrip("/"))
# #     if not data:
# #         return
# #     documents = data.get("documents", [])
# #     if not documents:
# #         return
# #     attachments_dir = os.path.join(
# #         raw_root(project_key), "comments", project_key,
# #         "attachments", f"comment_{comment_id}", "documents"
# #     )
# #     for doc in documents:
# #         link = doc.get("link")
# #         if not link:
# #             continue
# #         filename = doc.get("attachment_file_name", f"file_{doc.get('id')}")
# #         download_binary(link, os.path.join(attachments_dir, filename))


# # def download_document_file(project_key, doc):
# #     """Download one document/attachment file referenced by a
# #     documents/*.jsonl record. Mutates doc with local_file_path."""
# #     link = doc.get("link")
# #     if not link:
# #         return
# #     album_name = doc.get("album_name", "unknown")
# #     filename = doc.get("attachment_file_name", f"file_{doc.get('id')}")
# #     file_path = os.path.join(
# #         raw_root(project_key), "documents", project_key,
# #         "attachments", album_name, filename
# #     )
# #     if download_binary(link, file_path):
# #         doc["local_file_path"] = file_path


# # # =============================================================
# # # 1) PROJECT DETAILS
# # # target: {project}_data/raw/projects/{project_key}_details.json
# # #         (the FULL raw row — matches extract_project_details.py's
# # #          save_json(data, "projects", f"{project_key}_details.json"))
# # # =============================================================
# # def convert_project_details(export_dir):
# #     print("\nConverting project_details...")
# #     written = 0
# #     for filepath in sorted(glob.glob(os.path.join(export_dir, "project_details", "*.jsonl"))):
# #         for row in read_jsonl(filepath):
# #             project = row.get("project")
# #             if not isinstance(project, dict) or "id" not in project:
# #                 print(f"  [skip] no usable 'project' in row: {str(row)[:150]}")
# #                 continue
# #             project_key = project.get("key", "UNKNOWN")
# #             if not wanted_project(project_key):
# #                 continue

# #             project_id_to_key[project["id"]] = project_key
# #             merge_by_id(users_map[project_key], row.get("users", []))

# #             out_path = os.path.join(raw_root(project_key), "projects", f"{project_key}_details.json")
# #             write_json(out_path, row)
# #             written += 1

# #     print(f"  Wrote {written} project files")
# #     print(f"  project_id -> key map: {project_id_to_key}")


# # # =============================================================
# # # 2) ISSUES
# # # target: {project}_data/raw/issues/{project_key}/issue_{id}.json
# # #         + {project}_data/raw/issues/{project_key}_issues_list.json
# # # =============================================================
# # def convert_issues(export_dir):
# #     print("\nConverting issue_details...")
# #     files = sorted(glob.glob(os.path.join(export_dir, "issue_details", "*.jsonl")))
# #     print(f"  Files found: {len(files)}")

# #     issues_by_project = defaultdict(list)
# #     count = 0
# #     for filepath in files:
# #         for row in read_jsonl(filepath):
# #             issue = row.get("issue")
# #             if not isinstance(issue, dict):
# #                 print(f"  [skip] no 'issue' key in row: {str(row)[:150]}")
# #                 continue

# #             issue_id = issue.get("id")
# #             issue_key = issue.get("key")
# #             if issue_id is None or issue_key is None:
# #                 print(f"  [skip] missing id/key: {str(issue)[:150]}")
# #                 continue

# #             project_key = project_key_from_issue_key(issue_key)
# #             if not wanted_project(project_key):
# #                 continue

# #             issue_meta[issue_id] = {
# #                 "key": issue_key,
# #                 "project_key": project_key,
# #                 "title": issue.get("title") or issue.get("name") or "",
# #             }

# #             merge_by_id(users_map[project_key], row.get("users", []))
# #             merge_by_id(priorities_map[project_key], row.get("priorities", []))
# #             merge_by_id(issue_types_bonus[project_key], row.get("issue_types", []))

# #             out_path = os.path.join(raw_root(project_key), "issues", project_key, f"issue_{issue_id}.json")
# #             write_json(out_path, row)
# #             issues_by_project[project_key].append(issue)
# #             count += 1

# #     for project_key, issues in issues_by_project.items():
# #         combined_path = os.path.join(raw_root(project_key), "issues", f"{project_key}_issues_list.json")
# #         write_json(combined_path, {"issues": issues, "total_count": len(issues)})

# #     print(f"  Wrote {count} issue files across {len(issues_by_project)} project(s)")


# # # =============================================================
# # # 3) COMMENTS  (+ real image/attachment downloads)
# # # target: {project}_data/raw/comments/{project_key}/comments_{issue_id}.json
# # # =============================================================
# # def convert_comments(export_dir):
# #     print("\nConverting comments (+ downloading images/attachments)...")
# #     executor = get_executor()
# #     futures = []
# #     count = 0

# #     for issue_id_str, payload in iter_keyed_records(export_dir, "comments"):
# #         if not isinstance(payload, dict):
# #             continue
# #         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
# #         meta = issue_meta.get(issue_id, {})
# #         project_key = meta.get("project_key", "UNKNOWN")
# #         if not wanted_project(project_key):
# #             continue

# #         comments = payload.get("comments", [])
# #         merge_by_id(users_map[project_key], payload.get("users", []))

# #         for comment in comments:
# #             comment_id = comment.get("id")
# #             content = comment.get("content", "")
# #             documents_link = (comment.get("links") or {}).get("documents")

# #             futures.append(executor.submit(download_comment_images, project_key, comment_id, content))
# #             if documents_link:
# #                 futures.append(executor.submit(download_comment_attachments, project_key, comment_id, documents_link))

# #         out_path = os.path.join(raw_root(project_key), "comments", project_key, f"comments_{issue_id_str}.json")
# #         write_json(out_path, {
# #             "comments": comments,
# #             "users": payload.get("users", []),
# #         })
# #         count += 1

# #     print(f"  Wrote {count} comment files — waiting for {len(futures)} attachment/image downloads...")
# #     for f in as_completed(futures):
# #         try:
# #             f.result()
# #         except Exception as e:
# #             print(f"  [warn] download task error: {e}")
# #     print("  Downloads complete.")


# # # =============================================================
# # # 4) DEPENDENCIES
# # # target: {project}_data/raw/dependencies/{project_key}/deps_{issue_id}.json
# # # =============================================================
# # def convert_dependencies(export_dir):
# #     print("\nConverting dependencies...")

# #     all_deps = {}
# #     embedded_issue_lookup = {}
# #     sources_used = []

# #     for folder in ["dependencies_blocks", "dependencies_blocked_by"]:
# #         folder_path = os.path.join(export_dir, folder)
# #         if not os.path.isdir(folder_path):
# #             print(f"  [note] '{folder}' folder not present — skipping (not required)")
# #             continue
# #         sources_used.append(folder)
# #         for issue_id_str, payload in iter_keyed_records(export_dir, folder):
# #             if not isinstance(payload, dict):
# #                 continue
# #             for dep in payload.get("issue_dependencies", []):
# #                 if isinstance(dep, dict) and "id" in dep:
# #                     all_deps[dep["id"]] = dep
# #             for embedded_issue in payload.get("issues", []):
# #                 if isinstance(embedded_issue, dict) and "id" in embedded_issue:
# #                     embedded_issue_lookup[embedded_issue["id"]] = {
# #                         "key": embedded_issue.get("key", ""),
# #                         "title": embedded_issue.get("title") or embedded_issue.get("name") or "",
# #                     }

# #     print(f"  Sources used: {sources_used}")
# #     print(f"  Unique dependency records found: {len(all_deps)}")

# #     def resolve_linked(issue_id):
# #         if issue_id in issue_meta:
# #             return issue_meta[issue_id]["key"], issue_meta[issue_id]["title"]
# #         if issue_id in embedded_issue_lookup:
# #             e = embedded_issue_lookup[issue_id]
# #             return e["key"], e["title"]
# #         return "", ""

# #     deps_by_issue = defaultdict(lambda: {"successor_dependencies": [], "predecessor_dependencies": []})

# #     for dep in all_deps.values():
# #         dep_id = dep.get("id")
# #         pred = dep.get("predecessor_id")
# #         succ = dep.get("successor_id")
# #         dep_type = dep.get("dependency_type", "finish_to_start")

# #         if pred is not None and succ is not None:
# #             succ_key, succ_title = resolve_linked(succ)
# #             deps_by_issue[pred]["successor_dependencies"].append({
# #                 "id": dep_id, "predecessor_id": pred, "successor_id": succ,
# #                 "linked_issue_id": succ, "linked_issue_key": succ_key,
# #                 "linked_issue_title": succ_title, "linked_issue_project_id": "",
# #                 "dependency_type": dep_type,
# #             })
# #             pred_key, pred_title = resolve_linked(pred)
# #             deps_by_issue[succ]["predecessor_dependencies"].append({
# #                 "id": dep_id, "predecessor_id": pred, "successor_id": succ,
# #                 "linked_issue_id": pred, "linked_issue_key": pred_key,
# #                 "linked_issue_title": pred_title, "linked_issue_project_id": "",
# #                 "dependency_type": dep_type,
# #             })

# #     count = 0
# #     for issue_id, payload in deps_by_issue.items():
# #         meta = issue_meta.get(issue_id, {})
# #         project_key = meta.get("project_key", "UNKNOWN")
# #         if not wanted_project(project_key):
# #             continue
# #         # Match the live pipeline: only write a file if there's actually
# #         # something to say (extract_dependencies.py skips empty results).
# #         if not payload["successor_dependencies"] and not payload["predecessor_dependencies"]:
# #             continue
# #         out_path = os.path.join(raw_root(project_key), "dependencies", project_key, f"deps_{issue_id}.json")
# #         write_json(out_path, {
# #             "issue_id": issue_id,
# #             "successor_dependencies": payload["successor_dependencies"],
# #             "predecessor_dependencies": payload["predecessor_dependencies"],
# #         })
# #         count += 1

# #     print(f"  Wrote {count} dependency files")


# # # =============================================================
# # # 5) DOCUMENTS  (+ real file downloads)
# # # target: {project}_data/raw/documents/{project_key}/docs_{issue_id}.json
# # # =============================================================
# # def convert_documents(export_dir):
# #     print("\nConverting documents (+ downloading files)...")
# #     executor = get_executor()
# #     futures = []
# #     count = 0

# #     for issue_id_str, payload in iter_keyed_records(export_dir, "documents"):
# #         if not isinstance(payload, dict):
# #             continue
# #         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
# #         meta = issue_meta.get(issue_id, {})
# #         project_key = meta.get("project_key", "UNKNOWN")
# #         if not wanted_project(project_key):
# #             continue

# #         documents = payload.get("documents", [])
# #         # Match the live pipeline: don't write a file when there are no documents.
# #         if not documents:
# #             continue

# #         merge_by_id(users_map[project_key], payload.get("users", []))

# #         for doc in documents:
# #             futures.append(executor.submit(download_document_file, project_key, doc))

# #         out_path = os.path.join(raw_root(project_key), "documents", project_key, f"docs_{issue_id_str}.json")
# #         write_json(out_path, {
# #             "issue_id": issue_id if issue_id is not None else issue_id_str,
# #             "project_key": project_key,
# #             "documents": documents,
# #             "users": payload.get("users") or None,
# #         })
# #         count += 1

# #     print(f"  Wrote {count} document files — waiting for {len(futures)} downloads...")
# #     for f in as_completed(futures):
# #         try:
# #             f.result()
# #         except Exception as e:
# #             print(f"  [warn] download task error: {e}")
# #     print("  Downloads complete.")


# # # =============================================================
# # # 6) ISSUE LINKS
# # # target: {project}_data/raw/links/{project_key}/links_{issue_id}.json
# # # =============================================================
# # def convert_issue_links(export_dir):
# #     print("\nConverting issue_links...")
# #     count = 0
# #     for issue_id_str, payload in iter_keyed_records(export_dir, "issue_links"):
# #         if not isinstance(payload, dict):
# #             continue
# #         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
# #         meta = issue_meta.get(issue_id, {})
# #         project_key = meta.get("project_key", "UNKNOWN")
# #         if not wanted_project(project_key):
# #             continue

# #         merge_by_id(issue_types_bonus[project_key], payload.get("issue_types", []))

# #         out_path = os.path.join(raw_root(project_key), "links", project_key, f"links_{issue_id_str}.json")
# #         write_json(out_path, {
# #             "issue_links": payload.get("issue_links", []),
# #             "issue_types": payload.get("issue_types", []),
# #             "issues": payload.get("issues", []),
# #         })
# #         count += 1
# #     print(f"  Wrote {count} issue_links files")


# # # =============================================================
# # # 7) INTEGRATED RESOURCES  (flat, no project subfolder — matches
# # #    extract_integrated_resources.py exactly)
# # # target: {project}_data/raw/integrated_resources/ir_{issue_id}.json
# # # =============================================================
# # def convert_integrated_resources(export_dir):
# #     print("\nConverting integrated_resources...")
# #     count = 0
# #     for issue_id_str, payload in iter_keyed_records(export_dir, "integrated_resources"):
# #         if not isinstance(payload, dict):
# #             continue
# #         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
# #         meta = issue_meta.get(issue_id, {})
# #         project_key = meta.get("project_key", "UNKNOWN")
# #         if not wanted_project(project_key):
# #             continue

# #         out_path = os.path.join(raw_root(project_key), "integrated_resources", f"ir_{issue_id_str}.json")
# #         write_json(out_path, {"integrated_resources": payload.get("integrated_resources", [])})
# #         count += 1
# #     print(f"  Wrote {count} integrated_resources files")


# # # =============================================================
# # # 8) SPRINTS
# # # target: {project}_data/raw/sprints/{project_key}_sprints.json
# # # =============================================================
# # def convert_sprints(export_dir):
# #     print("\nConverting sprints...")
# #     sprints_by_project = defaultdict(list)

# #     for filepath in sorted(glob.glob(os.path.join(export_dir, "sprints", "*.jsonl"))):
# #         for sprint in read_jsonl(filepath):
# #             project_id = sprint.get("primary_workspace_id")
# #             project_key = project_id_to_key.get(project_id, "UNKNOWN")
# #             if project_key == "UNKNOWN":
# #                 print(f"  [warn] sprint id={sprint.get('id')} has unrecognized primary_workspace_id={project_id}")
# #             if not wanted_project(project_key):
# #                 continue
# #             sprints_by_project[project_key].append(sprint)

# #     count = 0
# #     for project_key, sprints in sprints_by_project.items():
# #         out_path = os.path.join(raw_root(project_key), "sprints", f"{project_key}_sprints.json")
# #         write_json(out_path, {"sprints": sprints})
# #         count += 1

# #     total = sum(len(v) for v in sprints_by_project.values())
# #     print(f"  Wrote {count} sprint files ({total} sprints)")


# # # =============================================================
# # # 9) RELEASES
# # # target: {project}_data/raw/releases/{project_key}_releases.json
# # # =============================================================
# # def convert_releases(export_dir):
# #     print("\nConverting releases...")
# #     releases_by_project = defaultdict(list)

# #     for filepath in sorted(glob.glob(os.path.join(export_dir, "releases", "*.jsonl"))):
# #         for release in read_jsonl(filepath):
# #             project_id = release.get("project_id")
# #             project_key = project_id_to_key.get(project_id, "UNKNOWN")
# #             if project_key == "UNKNOWN":
# #                 print(f"  [warn] release id={release.get('id')} has unrecognized project_id={project_id}")
# #             if not wanted_project(project_key):
# #                 continue
# #             releases_by_project[project_key].append(release)

# #     count = 0
# #     for project_key, releases in releases_by_project.items():
# #         out_path = os.path.join(raw_root(project_key), "releases", f"{project_key}_releases.json")
# #         write_json(out_path, {"releases": releases})
# #         count += 1

# #     total = sum(len(v) for v in releases_by_project.values())
# #     print(f"  Wrote {count} release files ({total} releases)")


# # # =============================================================
# # # 10/11/12) REFERENCE DATA — statuses, issue_types, priorities, users
# # # target: {project}_data/raw/reference/{name}.json
# # #
# # # NOTE: statuses/*.jsonl and issue_types/*.jsonl are not explicitly
# # # project-tagged in the confirmed export schema, so (matching the old
# # # pipeline's per-project /issue_types API call as closely as this
# # # export allows) the SAME global status/issue_type list is written into
# # # every project's reference/ folder. Priorities and users ARE split
# # # correctly per project since they're harvested from project-scoped
# # # issue/comment/document records above.
# # # =============================================================
# # def convert_reference_lists(export_dir, project_keys):
# #     print("\nConverting statuses / issue_types (shared across projects)...")

# #     def load_flat(subdir):
# #         merged = {}
# #         for filepath in sorted(glob.glob(os.path.join(export_dir, subdir, "*.jsonl"))):
# #             for row in read_jsonl(filepath):
# #                 if isinstance(row, dict) and "id" in row:
# #                     merged[row["id"]] = row
# #         return merged

# #     statuses = load_flat("statuses")
# #     issue_types = load_flat("issue_types")

# #     for project_key in project_keys:
# #         merged_types = dict(issue_types)
# #         for k, v in issue_types_bonus.get(project_key, {}).items():
# #             merged_types.setdefault(k, v)

# #         write_json(os.path.join(raw_root(project_key), "reference", "statuses.json"), list(statuses.values()))
# #         write_json(os.path.join(raw_root(project_key), "reference", "issue_types.json"), list(merged_types.values()))
# #         write_json(os.path.join(raw_root(project_key), "reference", "priorities.json"),
# #                    list(priorities_map.get(project_key, {}).values()))
# #         write_json(os.path.join(raw_root(project_key), "reference", "users.json"),
# #                    list(users_map.get(project_key, {}).values()))

# #     print(f"  Wrote reference/ files for {len(project_keys)} project(s)")


# # # =============================================================
# # # MAIN
# # # =============================================================
# # def main():
# #     path_arg = sys.argv[1] if len(sys.argv) > 1 else None
# #     export_dir = resolve_export_dir(path_arg)

# #     print("=" * 60)
# #     print("  rawdata_from_zip.py: DS_EXPORT.zip -> {project}_data/raw/")
# #     print(f"  Source : {os.path.abspath(export_dir)}")
# #     print(f"  Filter : {FILTER_PROJECT_KEY or '(none — converting all projects found)'}")
# #     print("=" * 60)

# #     if not BASE_URL or not AUTH_TOKEN:
# #         print("  [warn] BASE_URL/AUTH_TOKEN not set — comment images, comment "
# #               "attachments, and document files will NOT be downloaded.")

# #     # Order matters: projects first (project_id->key map), then issues
# #     # (issue_meta lookup), then everything that depends on those.
# #     convert_project_details(export_dir)
# #     convert_issues(export_dir)
# #     convert_comments(export_dir)
# #     convert_documents(export_dir)
# #     convert_dependencies(export_dir)
# #     convert_issue_links(export_dir)
# #     convert_integrated_resources(export_dir)
# #     convert_sprints(export_dir)
# #     convert_releases(export_dir)

# #     all_project_keys = sorted(set(issue_meta[i]["project_key"] for i in issue_meta) | set(project_id_to_key.values()))
# #     if FILTER_PROJECT_KEY:
# #         all_project_keys = [p for p in all_project_keys if p.lower() == FILTER_PROJECT_KEY.lower()]
# #     convert_reference_lists(export_dir, all_project_keys)

# #     shutdown_executor(wait=True)

# #     print("\n" + "=" * 60)
# #     print(f"  DONE. Projects converted: {all_project_keys}")
# #     print("  Each one now has its own {project_key}_data/raw/ folder,")
# #     print("  matching the original live-API pipeline's output exactly.")
# #     print("=" * 60)


# # if __name__ == "__main__":
# #     main()
# """
# ============================================================
# rawdata_from_zip.py — CONVERT A ZIPPED DS_EXPORT
#               -> {project_key}_data/raw/ structure
#               (same output layout the live API pipeline produced)
# ============================================================

# WHY THIS EXISTS
# ----------------
# The old pipeline (pipeline_orchestrator.py + extract_*.py) hit the live
# API and wrote each project's data to its own top-level folder:

#     {PROJECT_KEY}_data/raw/{issues,comments,links,dependencies,
#                              documents,sprints,releases,projects,
#                              reference,logs}/...

# Now the backend instead hands us a single .zip export containing one
# folder per data type (issue_details/, comments/, dependencies_blocks/,
# documents/, issue_links/, sprints/, statuses/, issue_types/,
# project_details/, integrated_resources/, releases/), each holding
# *.jsonl files. This script converts that zip into EXACTLY the same
# {project_key}_data/raw/... layout as before, so every downstream tool
# (flatten.py, reports, etc.) keeps working unchanged.

# Two things the zip does NOT contain that the old pipeline used to
# fetch live:
#   1. Comment inline images / comment attachments (only HTML content +
#      a "documents" API link are present in the export)
#   2. Actual document file bytes (only metadata + a download "link" are
#      present)
# So this script still uses BASE_URL + AUTH_TOKEN from .env to download
# those specific files — everything else comes from the zip, no other
# API calls are made.

# USAGE
# -----
#     python rawdata_from_zip.py path/to/export.zip
#     python rawdata_from_zip.py path/to/already_extracted_folder
#     python rawdata_from_zip.py                       # auto-finds a
#                                                        # single *.zip in CWD

# .env required:
#     BASE_URL     - used only to resolve/download attachment & image links
#     AUTH_TOKEN   - used only to authenticate those downloads
#     project_key  - OPTIONAL. If set, ONLY that project is converted
#                    (matched against the "XXXX-123" issue-key prefix).
#                    Leave blank/unset to convert every project found
#                    in the export.

# REQUIRES (same folder as this script):
#     shared_http.py          - shared requests.Session / connection pool
#     shared_thread_pool.py   - shared ThreadPoolExecutor for downloads

# ============================================================
# CONFIRMED SCHEMAS (unchanged from the original rawdata.py)
# ============================================================
# issue_details/*.jsonl   -> {"issue": {...}, "issue_types": [...],
#                              "users": [...], "priorities": [...],
#                              "statuses": [...]}
# comments/*.jsonl        -> {"<issue_id>": {"comments": [...], "users": [...]}}
# dependencies_blocks/*.jsonl (and/or dependencies_blocked_by/*.jsonl)
#                         -> {"<issue_id>": {"issue_dependencies": [
#                                {"id":.., "predecessor_id":.., "successor_id":..,
#                                 "dependency_type":..}], "issues":[...]}}
# documents/*.jsonl       -> {"<issue_id>": {"documents": [...], "users": [...]}}
# issue_links/*.jsonl     -> {"<issue_id>": {"issue_links": [...],
#                              "issue_types": [...], "issues": [...]}}
# integrated_resources/*.jsonl -> {"<issue_id>": {"integrated_resources": [...]}}
# sprints/*.jsonl, releases/*.jsonl -> ONE FLAT RECORD PER LINE
#                              (sprints: "primary_workspace_id" == project id,
#                               releases: "project_id" directly)
# statuses/*.jsonl, issue_types/*.jsonl -> one flat record per line
# project_details/*.jsonl -> {"project": {...}, "users": [...]}
# ============================================================
# """

# import os
# import re
# import sys
# import json
# import glob
# import time
# import zipfile
# import tempfile
# from collections import defaultdict
# from urllib.parse import urlparse
# from concurrent.futures import as_completed

# import requests
# from dotenv import load_dotenv

# sys.stdout.reconfigure(encoding="utf-8")
# sys.stderr.reconfigure(encoding="utf-8")

# from shared_http import get_shared_session
# from shared_thread_pool import get_executor, shutdown_executor

# # =============================================================
# # CONFIGURATION
# # =============================================================
# load_dotenv()
# BASE_URL   = os.getenv("BASE_URL")
# AUTH_TOKEN = os.getenv("AUTH_TOKEN")
# # Optional filter — if set, ONLY this project is converted.
# FILTER_PROJECT_KEY = (os.getenv("project_key") or "").strip()

# HEADERS = {
#     "Authorization": f"Token {AUTH_TOKEN}",
#     "Content-Type": "application/json",
# }

# SESSION = get_shared_session()
# # Actual worker count is controlled by shared_thread_pool.py's own read of
# # EXTRACTION_MAX_WORKERS (default 3) — set it in .env to speed downloads up.
# RATE_LIMIT_DELAY = float(os.getenv("API_RATE_LIMIT_DELAY", "0.2"))

# # Folders the export is expected to contain (checked to auto-locate the
# # real export root inside an extracted zip, which sometimes nests
# # everything one level deeper e.g. export/issue_details/...).
# EXPECTED_SUBDIRS = {"issue_details", "comments", "documents", "issue_links"}

# download_stats_lock = None  # set in main() once we know we're threaded


# # =============================================================
# # ZIP HANDLING
# # =============================================================
# def resolve_export_dir(path_arg):
#     """
#     Accepts either a .zip file or an already-extracted folder and
#     returns a directory path containing issue_details/, comments/, etc.
#     """
#     if path_arg is None:
#         zips = glob.glob("*.zip")
#         if len(zips) == 1:
#             path_arg = zips[0]
#         elif len(zips) > 1:
#             raise SystemExit(f"Multiple .zip files found in CWD, pass one explicitly: {zips}")
#         else:
#             raise SystemExit("No path given and no .zip found in CWD.")

#     if os.path.isdir(path_arg):
#         return locate_export_root(path_arg)

#     if os.path.isfile(path_arg) and path_arg.lower().endswith(".zip"):
#         extract_dir = tempfile.mkdtemp(prefix="ds_export_")
#         print(f"  Extracting {path_arg} -> {extract_dir}")
#         with zipfile.ZipFile(path_arg, "r") as zf:
#             zf.extractall(extract_dir)
#         return locate_export_root(extract_dir)

#     raise SystemExit(f"'{path_arg}' is neither a directory nor a .zip file.")


# def locate_export_root(base_dir):
#     """Search base_dir (and one level of subfolders) for the folder that
#     actually contains issue_details/, comments/, etc."""
#     candidates = [base_dir] + [
#         os.path.join(base_dir, d) for d in os.listdir(base_dir)
#         if os.path.isdir(os.path.join(base_dir, d))
#     ]
#     for c in candidates:
#         try:
#             entries = set(os.listdir(c))
#         except (FileNotFoundError, NotADirectoryError):
#             continue
#         if EXPECTED_SUBDIRS & entries:
#             return c
#     raise SystemExit(
#         f"Could not find issue_details/, comments/, etc. inside '{base_dir}'. "
#         f"Check the zip's internal folder structure."
#     )


# # =============================================================
# # GENERIC HELPERS
# # =============================================================
# def read_jsonl(filepath):
#     rows = []
#     if not os.path.exists(filepath):
#         return rows
#     with open(filepath, "r", encoding="utf-8") as f:
#         for line_no, line in enumerate(f, 1):
#             line = line.strip()
#             if not line:
#                 continue
#             try:
#                 rows.append(json.loads(line))
#             except json.JSONDecodeError as e:
#                 print(f"  [warn] bad JSON at {filepath}:{line_no} — {e}")
#     return rows


# def iter_keyed_records(export_dir, folder):
#     """Yields (issue_id_str, payload_dict) for every line across every
#     *.jsonl file in `folder`, where each line looks like:
#         {"<issue_id>": {...payload...}}
#     """
#     for filepath in sorted(glob.glob(os.path.join(export_dir, folder, "*.jsonl"))):
#         with open(filepath, "r", encoding="utf-8") as f:
#             for line_no, line in enumerate(f, 1):
#                 line = line.strip()
#                 if not line:
#                     continue
#                 try:
#                     obj = json.loads(line)
#                 except json.JSONDecodeError as e:
#                     print(f"  [warn] bad JSON at {filepath}:{line_no} — {e}")
#                     continue
#                 if not isinstance(obj, dict):
#                     continue
#                 for k, v in obj.items():
#                     yield k, v


# def write_json(filepath, data):
#     os.makedirs(os.path.dirname(filepath), exist_ok=True)
#     tmp = filepath + ".tmp"
#     with open(tmp, "w", encoding="utf-8") as f:
#         json.dump(data, f, ensure_ascii=False, indent=4)
#     os.replace(tmp, filepath)


# def raw_root(project_key):
#     """The per-project output root — matches the live pipeline's
#     f"{PROJECT_KEY}_data/raw" convention exactly."""
#     return f"{project_key}_data/raw"


# def project_key_from_issue_key(issue_key):
#     """'FBOTS-5' -> 'FBOTS'."""
#     if not issue_key or "-" not in issue_key:
#         return "UNKNOWN"
#     return issue_key.rsplit("-", 1)[0]


# def merge_by_id(target_map, records):
#     for r in records or []:
#         if isinstance(r, dict) and "id" in r:
#             target_map[r["id"]] = r


# def wanted_project(project_key):
#     """Honor the optional project_key filter from .env."""
#     if not FILTER_PROJECT_KEY:
#         return True
#     return project_key.lower() == FILTER_PROJECT_KEY.lower()


# # =============================================================
# # GLOBAL LOOKUPS
# # =============================================================
# issue_meta = {}                  # issue_id(int) -> {"key","project_key","title"}
# project_id_to_key = {}           # project_id(int) -> project_key
# users_map = defaultdict(dict)    # project_key -> {user_id: user}
# priorities_map = defaultdict(dict)   # project_key -> {priority_id: priority}
# issue_types_bonus = defaultdict(dict)  # project_key -> {issue_type_id: dict}


# # =============================================================
# # ATTACHMENT / IMAGE DOWNLOADING
# # (adapted from the old extract_comments.py / extract_documents.py —
# #  only these two things still touch the live API, since the zip
# #  export doesn't carry the actual file bytes)
# # =============================================================
# def api_get(endpoint, params=None, retries=3):
#     """Small GET-with-retry helper, same policy as the old extractors."""
#     url = f"{BASE_URL}/{endpoint.lstrip('/')}"
#     for attempt in range(retries):
#         try:
#             resp = SESSION.get(url, headers=HEADERS, params=params, timeout=30)
#             resp.raise_for_status()
#             if RATE_LIMIT_DELAY:
#                 time.sleep(RATE_LIMIT_DELAY)
#             return resp.json()
#         except requests.exceptions.RequestException as e:
#             if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
#                 time.sleep(30)
#                 continue
#             if attempt == retries - 1:
#                 print(f"  [warn] GET {endpoint} failed: {e}")
#                 return None
#             time.sleep(1 * (attempt + 1))
#     return None


# # Fail fast instead of hanging 60s per dead link. (connect_timeout, read_timeout)
# DOWNLOAD_CONNECT_TIMEOUT = float(os.getenv("DOWNLOAD_CONNECT_TIMEOUT", "8"))
# DOWNLOAD_READ_TIMEOUT = float(os.getenv("DOWNLOAD_READ_TIMEOUT", "60"))

# # Once a URL fails (or succeeds), remember it so the same dead/duplicate
# # link referenced from many different comments/issues isn't retried
# # over and over — this is what was causing the same confluence.* URL to
# # time out 3 separate times above.
# _url_result_cache = {}
# _url_cache_lock = None  # created lazily to avoid importing threading at module top twice


# def _get_url_cache_lock():
#     global _url_cache_lock
#     if _url_cache_lock is None:
#         from threading import Lock
#         _url_cache_lock = Lock()
#     return _url_cache_lock


# def download_binary(url, file_path):
#     """Stream-download a single file to disk. Returns True on success.
#     Caches failures per-URL so a broken/unreachable link referenced from
#     many comments only pays the timeout cost once."""
#     if os.path.exists(file_path):
#         return True
#     if not url.startswith("http"):
#         url = f"{BASE_URL}{url}" if url.startswith("/") else f"{BASE_URL}/{url}"

#     lock = _get_url_cache_lock()
#     with lock:
#         cached = _url_result_cache.get(url)
#     if cached is False:
#         return False  # known-bad URL, don't retry the timeout again
#     if isinstance(cached, str):
#         # already downloaded once elsewhere — copy instead of re-fetching
#         try:
#             import shutil
#             os.makedirs(os.path.dirname(file_path), exist_ok=True)
#             shutil.copyfile(cached, file_path)
#             return True
#         except Exception:
#             pass  # fall through and try a fresh download

#     try:
#         download_headers = {"Authorization": f"Token {AUTH_TOKEN}"}
#         resp = SESSION.get(
#             url, headers=download_headers,
#             timeout=(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT),
#             stream=True,
#         )
#         if resp.status_code != 200:
#             with lock:
#                 _url_result_cache[url] = False
#             return False
#         os.makedirs(os.path.dirname(file_path), exist_ok=True)
#         with open(file_path, "wb") as f:
#             for chunk in resp.iter_content(chunk_size=8192):
#                 if chunk:
#                     f.write(chunk)
#         with lock:
#             _url_result_cache[url] = file_path
#         return True
#     except Exception as e:
#         print(f"  [warn] download failed for {url}: {e}")
#         with lock:
#             _url_result_cache[url] = False
#         return False


# IMG_SRC_PATTERN = r'<img[^>]+src=["\'](.*?)["\']'
# IMG_CONTENT_PATTERN = r'<img[^>]*>(https?://[^<]+)</img>'


# def download_comment_images(project_key, comment_id, html_content):
#     if not html_content or not isinstance(html_content, str):
#         return
#     urls = re.findall(IMG_SRC_PATTERN, html_content, re.IGNORECASE)
#     urls += re.findall(IMG_CONTENT_PATTERN, html_content, re.IGNORECASE)
#     if not urls:
#         return
#     images_dir = os.path.join(
#         raw_root(project_key), "comments", project_key,
#         "attachments", f"comment_{comment_id}", "images"
#     )
#     for idx, img_url in enumerate(urls):
#         parsed = urlparse(img_url)
#         filename = os.path.basename(parsed.path)
#         if not filename or "." not in filename:
#             ext = parsed.path.split(".")[-1] if "." in parsed.path else "jpg"
#             filename = f"image_{idx}.{ext}"
#         download_binary(img_url, os.path.join(images_dir, filename))


# def download_comment_attachments(project_key, comment_id, documents_link):
#     """documents_link is a relative API endpoint (e.g. from
#     comment['links']['documents']) that must be GET'd to list the
#     actual documents, each of which is then downloaded by its own link."""
#     if not documents_link:
#         return
#     data = api_get(documents_link.lstrip("/"))
#     if not data:
#         return
#     documents = data.get("documents", [])
#     if not documents:
#         return
#     attachments_dir = os.path.join(
#         raw_root(project_key), "comments", project_key,
#         "attachments", f"comment_{comment_id}", "documents"
#     )
#     for doc in documents:
#         link = doc.get("link")
#         if not link:
#             continue
#         filename = doc.get("attachment_file_name", f"file_{doc.get('id')}")
#         download_binary(link, os.path.join(attachments_dir, filename))


# def download_document_file(project_key, doc):
#     """Download one document/attachment file referenced by a
#     documents/*.jsonl record. Mutates doc with local_file_path."""
#     link = doc.get("link")
#     if not link:
#         return
#     album_name = doc.get("album_name", "unknown")
#     filename = doc.get("attachment_file_name", f"file_{doc.get('id')}")
#     file_path = os.path.join(
#         raw_root(project_key), "documents", project_key,
#         "attachments", album_name, filename
#     )
#     if download_binary(link, file_path):
#         doc["local_file_path"] = file_path


# # =============================================================
# # 1) PROJECT DETAILS
# # target: {project}_data/raw/projects/{project_key}_details.json
# #         (the FULL raw row — matches extract_project_details.py's
# #          save_json(data, "projects", f"{project_key}_details.json"))
# # =============================================================
# def convert_project_details(export_dir):
#     print("\nConverting project_details...")
#     written = 0
#     for filepath in sorted(glob.glob(os.path.join(export_dir, "project_details", "*.jsonl"))):
#         for row in read_jsonl(filepath):
#             project = row.get("project")
#             if not isinstance(project, dict) or "id" not in project:
#                 print(f"  [skip] no usable 'project' in row: {str(row)[:150]}")
#                 continue
#             project_key = project.get("key", "UNKNOWN")
#             if not wanted_project(project_key):
#                 continue

#             project_id_to_key[project["id"]] = project_key
#             merge_by_id(users_map[project_key], row.get("users", []))

#             out_path = os.path.join(raw_root(project_key), "projects", f"{project_key}_details.json")
#             write_json(out_path, row)
#             written += 1

#     print(f"  Wrote {written} project files")
#     print(f"  project_id -> key map: {project_id_to_key}")


# # =============================================================
# # 2) ISSUES
# # target: {project}_data/raw/issues/{project_key}/issue_{id}.json
# #         + {project}_data/raw/issues/{project_key}_issues_list.json
# # =============================================================
# def convert_issues(export_dir):
#     print("\nConverting issue_details...")
#     files = sorted(glob.glob(os.path.join(export_dir, "issue_details", "*.jsonl")))
#     print(f"  Files found: {len(files)}")

#     issues_by_project = defaultdict(list)
#     count = 0
#     for filepath in files:
#         for row in read_jsonl(filepath):
#             issue = row.get("issue")
#             if not isinstance(issue, dict):
#                 print(f"  [skip] no 'issue' key in row: {str(row)[:150]}")
#                 continue

#             issue_id = issue.get("id")
#             issue_key = issue.get("key")
#             if issue_id is None or issue_key is None:
#                 print(f"  [skip] missing id/key: {str(issue)[:150]}")
#                 continue

#             project_key = project_key_from_issue_key(issue_key)
#             if not wanted_project(project_key):
#                 continue

#             issue_meta[issue_id] = {
#                 "key": issue_key,
#                 "project_key": project_key,
#                 "title": issue.get("title") or issue.get("name") or "",
#             }

#             merge_by_id(users_map[project_key], row.get("users", []))
#             merge_by_id(priorities_map[project_key], row.get("priorities", []))
#             merge_by_id(issue_types_bonus[project_key], row.get("issue_types", []))

#             out_path = os.path.join(raw_root(project_key), "issues", project_key, f"issue_{issue_id}.json")
#             write_json(out_path, row)
#             issues_by_project[project_key].append(issue)
#             count += 1

#     for project_key, issues in issues_by_project.items():
#         combined_path = os.path.join(raw_root(project_key), "issues", f"{project_key}_issues_list.json")
#         write_json(combined_path, {"issues": issues, "total_count": len(issues)})

#     print(f"  Wrote {count} issue files across {len(issues_by_project)} project(s)")


# # =============================================================
# # 3) COMMENTS  (+ real image/attachment downloads)
# # target: {project}_data/raw/comments/{project_key}/comments_{issue_id}.json
# # =============================================================
# def convert_comments(export_dir):
#     print("\nConverting comments (+ downloading images/attachments)...")
#     executor = get_executor()
#     futures = []
#     count = 0

#     for issue_id_str, payload in iter_keyed_records(export_dir, "comments"):
#         if not isinstance(payload, dict):
#             continue
#         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
#         meta = issue_meta.get(issue_id, {})
#         project_key = meta.get("project_key", "UNKNOWN")
#         if not wanted_project(project_key):
#             continue

#         comments = payload.get("comments", [])
#         merge_by_id(users_map[project_key], payload.get("users", []))

#         for comment in comments:
#             comment_id = comment.get("id")
#             content = comment.get("content", "")
#             documents_link = (comment.get("links") or {}).get("documents")

#             futures.append(executor.submit(download_comment_images, project_key, comment_id, content))
#             if documents_link:
#                 futures.append(executor.submit(download_comment_attachments, project_key, comment_id, documents_link))

#         out_path = os.path.join(raw_root(project_key), "comments", project_key, f"comments_{issue_id_str}.json")
#         write_json(out_path, {
#             "comments": comments,
#             "users": payload.get("users", []),
#         })
#         count += 1

#     print(f"  Wrote {count} comment files — waiting for {len(futures)} attachment/image downloads...")
#     for f in as_completed(futures):
#         try:
#             f.result()
#         except Exception as e:
#             print(f"  [warn] download task error: {e}")
#     print("  Downloads complete.")


# # =============================================================
# # 4) DEPENDENCIES
# # target: {project}_data/raw/dependencies/{project_key}/deps_{issue_id}.json
# # =============================================================
# def convert_dependencies(export_dir):
#     print("\nConverting dependencies...")

#     all_deps = {}
#     embedded_issue_lookup = {}
#     sources_used = []

#     for folder in ["dependencies_blocks", "dependencies_blocked_by"]:
#         folder_path = os.path.join(export_dir, folder)
#         if not os.path.isdir(folder_path):
#             print(f"  [note] '{folder}' folder not present — skipping (not required)")
#             continue
#         sources_used.append(folder)
#         for issue_id_str, payload in iter_keyed_records(export_dir, folder):
#             if not isinstance(payload, dict):
#                 continue
#             for dep in payload.get("issue_dependencies", []):
#                 if isinstance(dep, dict) and "id" in dep:
#                     all_deps[dep["id"]] = dep
#             for embedded_issue in payload.get("issues", []):
#                 if isinstance(embedded_issue, dict) and "id" in embedded_issue:
#                     embedded_issue_lookup[embedded_issue["id"]] = {
#                         "key": embedded_issue.get("key", ""),
#                         "title": embedded_issue.get("title") or embedded_issue.get("name") or "",
#                     }

#     print(f"  Sources used: {sources_used}")
#     print(f"  Unique dependency records found: {len(all_deps)}")

#     def resolve_linked(issue_id):
#         if issue_id in issue_meta:
#             return issue_meta[issue_id]["key"], issue_meta[issue_id]["title"]
#         if issue_id in embedded_issue_lookup:
#             e = embedded_issue_lookup[issue_id]
#             return e["key"], e["title"]
#         return "", ""

#     deps_by_issue = defaultdict(lambda: {"successor_dependencies": [], "predecessor_dependencies": []})

#     for dep in all_deps.values():
#         dep_id = dep.get("id")
#         pred = dep.get("predecessor_id")
#         succ = dep.get("successor_id")
#         dep_type = dep.get("dependency_type", "finish_to_start")

#         if pred is not None and succ is not None:
#             succ_key, succ_title = resolve_linked(succ)
#             deps_by_issue[pred]["successor_dependencies"].append({
#                 "id": dep_id, "predecessor_id": pred, "successor_id": succ,
#                 "linked_issue_id": succ, "linked_issue_key": succ_key,
#                 "linked_issue_title": succ_title, "linked_issue_project_id": "",
#                 "dependency_type": dep_type,
#             })
#             pred_key, pred_title = resolve_linked(pred)
#             deps_by_issue[succ]["predecessor_dependencies"].append({
#                 "id": dep_id, "predecessor_id": pred, "successor_id": succ,
#                 "linked_issue_id": pred, "linked_issue_key": pred_key,
#                 "linked_issue_title": pred_title, "linked_issue_project_id": "",
#                 "dependency_type": dep_type,
#             })

#     count = 0
#     for issue_id, payload in deps_by_issue.items():
#         meta = issue_meta.get(issue_id, {})
#         project_key = meta.get("project_key", "UNKNOWN")
#         if not wanted_project(project_key):
#             continue
#         # Match the live pipeline: only write a file if there's actually
#         # something to say (extract_dependencies.py skips empty results).
#         if not payload["successor_dependencies"] and not payload["predecessor_dependencies"]:
#             continue
#         out_path = os.path.join(raw_root(project_key), "dependencies", project_key, f"deps_{issue_id}.json")
#         write_json(out_path, {
#             "issue_id": issue_id,
#             "successor_dependencies": payload["successor_dependencies"],
#             "predecessor_dependencies": payload["predecessor_dependencies"],
#         })
#         count += 1

#     print(f"  Wrote {count} dependency files")


# # =============================================================
# # 5) DOCUMENTS  (+ real file downloads)
# # target: {project}_data/raw/documents/{project_key}/docs_{issue_id}.json
# # =============================================================
# def convert_documents(export_dir):
#     print("\nConverting documents (+ downloading files)...")
#     executor = get_executor()
#     futures = []
#     count = 0

#     for issue_id_str, payload in iter_keyed_records(export_dir, "documents"):
#         if not isinstance(payload, dict):
#             continue
#         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
#         meta = issue_meta.get(issue_id, {})
#         project_key = meta.get("project_key", "UNKNOWN")
#         if not wanted_project(project_key):
#             continue

#         documents = payload.get("documents", [])
#         # Match the live pipeline: don't write a file when there are no documents.
#         if not documents:
#             continue

#         merge_by_id(users_map[project_key], payload.get("users", []))

#         for doc in documents:
#             futures.append(executor.submit(download_document_file, project_key, doc))

#         out_path = os.path.join(raw_root(project_key), "documents", project_key, f"docs_{issue_id_str}.json")
#         write_json(out_path, {
#             "issue_id": issue_id if issue_id is not None else issue_id_str,
#             "project_key": project_key,
#             "documents": documents,
#             "users": payload.get("users") or None,
#         })
#         count += 1

#     print(f"  Wrote {count} document files — waiting for {len(futures)} downloads...")
#     for f in as_completed(futures):
#         try:
#             f.result()
#         except Exception as e:
#             print(f"  [warn] download task error: {e}")
#     print("  Downloads complete.")


# # =============================================================
# # 6) ISSUE LINKS
# # target: {project}_data/raw/links/{project_key}/links_{issue_id}.json
# # =============================================================
# def convert_issue_links(export_dir):
#     print("\nConverting issue_links...")
#     count = 0
#     for issue_id_str, payload in iter_keyed_records(export_dir, "issue_links"):
#         if not isinstance(payload, dict):
#             continue
#         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
#         meta = issue_meta.get(issue_id, {})
#         project_key = meta.get("project_key", "UNKNOWN")
#         if not wanted_project(project_key):
#             continue

#         merge_by_id(issue_types_bonus[project_key], payload.get("issue_types", []))

#         out_path = os.path.join(raw_root(project_key), "links", project_key, f"links_{issue_id_str}.json")
#         write_json(out_path, {
#             "issue_links": payload.get("issue_links", []),
#             "issue_types": payload.get("issue_types", []),
#             "issues": payload.get("issues", []),
#         })
#         count += 1
#     print(f"  Wrote {count} issue_links files")


# # =============================================================
# # 7) INTEGRATED RESOURCES  (flat, no project subfolder — matches
# #    extract_integrated_resources.py exactly)
# # target: {project}_data/raw/integrated_resources/ir_{issue_id}.json
# # =============================================================
# def convert_integrated_resources(export_dir):
#     print("\nConverting integrated_resources...")
#     count = 0
#     for issue_id_str, payload in iter_keyed_records(export_dir, "integrated_resources"):
#         if not isinstance(payload, dict):
#             continue
#         issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
#         meta = issue_meta.get(issue_id, {})
#         project_key = meta.get("project_key", "UNKNOWN")
#         if not wanted_project(project_key):
#             continue

#         out_path = os.path.join(raw_root(project_key), "integrated_resources", f"ir_{issue_id_str}.json")
#         write_json(out_path, {"integrated_resources": payload.get("integrated_resources", [])})
#         count += 1
#     print(f"  Wrote {count} integrated_resources files")


# # =============================================================
# # 8) SPRINTS
# # target: {project}_data/raw/sprints/{project_key}_sprints.json
# # =============================================================
# def convert_sprints(export_dir):
#     print("\nConverting sprints...")
#     sprints_by_project = defaultdict(list)

#     for filepath in sorted(glob.glob(os.path.join(export_dir, "sprints", "*.jsonl"))):
#         for sprint in read_jsonl(filepath):
#             project_id = sprint.get("primary_workspace_id")
#             project_key = project_id_to_key.get(project_id, "UNKNOWN")
#             if project_key == "UNKNOWN":
#                 print(f"  [warn] sprint id={sprint.get('id')} has unrecognized primary_workspace_id={project_id}")
#             if not wanted_project(project_key):
#                 continue
#             sprints_by_project[project_key].append(sprint)

#     count = 0
#     for project_key, sprints in sprints_by_project.items():
#         out_path = os.path.join(raw_root(project_key), "sprints", f"{project_key}_sprints.json")
#         write_json(out_path, {"sprints": sprints})
#         count += 1

#     total = sum(len(v) for v in sprints_by_project.values())
#     print(f"  Wrote {count} sprint files ({total} sprints)")


# # =============================================================
# # 9) RELEASES
# # target: {project}_data/raw/releases/{project_key}_releases.json
# # =============================================================
# def convert_releases(export_dir):
#     print("\nConverting releases...")
#     releases_by_project = defaultdict(list)

#     for filepath in sorted(glob.glob(os.path.join(export_dir, "releases", "*.jsonl"))):
#         for release in read_jsonl(filepath):
#             project_id = release.get("project_id")
#             project_key = project_id_to_key.get(project_id, "UNKNOWN")
#             if project_key == "UNKNOWN":
#                 print(f"  [warn] release id={release.get('id')} has unrecognized project_id={project_id}")
#             if not wanted_project(project_key):
#                 continue
#             releases_by_project[project_key].append(release)

#     count = 0
#     for project_key, releases in releases_by_project.items():
#         out_path = os.path.join(raw_root(project_key), "releases", f"{project_key}_releases.json")
#         write_json(out_path, {"releases": releases})
#         count += 1

#     total = sum(len(v) for v in releases_by_project.values())
#     print(f"  Wrote {count} release files ({total} releases)")


# # =============================================================
# # 10/11/12) REFERENCE DATA — statuses, issue_types, priorities, users
# # target: {project}_data/raw/reference/{name}.json
# #
# # NOTE: statuses/*.jsonl and issue_types/*.jsonl are not explicitly
# # project-tagged in the confirmed export schema, so (matching the old
# # pipeline's per-project /issue_types API call as closely as this
# # export allows) the SAME global status/issue_type list is written into
# # every project's reference/ folder. Priorities and users ARE split
# # correctly per project since they're harvested from project-scoped
# # issue/comment/document records above.
# # =============================================================
# def convert_reference_lists(export_dir, project_keys):
#     print("\nConverting statuses / issue_types (shared across projects)...")

#     def load_flat(subdir):
#         merged = {}
#         for filepath in sorted(glob.glob(os.path.join(export_dir, subdir, "*.jsonl"))):
#             for row in read_jsonl(filepath):
#                 if isinstance(row, dict) and "id" in row:
#                     merged[row["id"]] = row
#         return merged

#     statuses = load_flat("statuses")
#     issue_types = load_flat("issue_types")

#     for project_key in project_keys:
#         merged_types = dict(issue_types)
#         for k, v in issue_types_bonus.get(project_key, {}).items():
#             merged_types.setdefault(k, v)

#         write_json(os.path.join(raw_root(project_key), "reference", "statuses.json"), list(statuses.values()))
#         write_json(os.path.join(raw_root(project_key), "reference", "issue_types.json"), list(merged_types.values()))
#         write_json(os.path.join(raw_root(project_key), "reference", "priorities.json"),
#                    list(priorities_map.get(project_key, {}).values()))
#         write_json(os.path.join(raw_root(project_key), "reference", "users.json"),
#                    list(users_map.get(project_key, {}).values()))

#     print(f"  Wrote reference/ files for {len(project_keys)} project(s)")


# # =============================================================
# # MAIN
# # =============================================================
# def main():
#     path_arg = sys.argv[1] if len(sys.argv) > 1 else None
#     export_dir = resolve_export_dir(path_arg)

#     print("=" * 60)
#     print("  rawdata_from_zip.py: DS_EXPORT.zip -> {project}_data/raw/")
#     print(f"  Source : {os.path.abspath(export_dir)}")
#     print(f"  Filter : {FILTER_PROJECT_KEY or '(none — converting all projects found)'}")
#     print("=" * 60)

#     if not BASE_URL or not AUTH_TOKEN:
#         print("  [warn] BASE_URL/AUTH_TOKEN not set — comment images, comment "
#               "attachments, and document files will NOT be downloaded.")

#     # Order matters: projects first (project_id->key map), then issues
#     # (issue_meta lookup), then everything that depends on those.
#     convert_project_details(export_dir)
#     convert_issues(export_dir)
#     convert_comments(export_dir)
#     convert_documents(export_dir)
#     convert_dependencies(export_dir)
#     convert_issue_links(export_dir)
#     convert_integrated_resources(export_dir)
#     convert_sprints(export_dir)
#     convert_releases(export_dir)

#     all_project_keys = sorted(set(issue_meta[i]["project_key"] for i in issue_meta) | set(project_id_to_key.values()))
#     if FILTER_PROJECT_KEY:
#         all_project_keys = [p for p in all_project_keys if p.lower() == FILTER_PROJECT_KEY.lower()]
#     convert_reference_lists(export_dir, all_project_keys)

#     shutdown_executor(wait=True)

#     print("\n" + "=" * 60)
#     print(f"  DONE. Projects converted: {all_project_keys}")
#     print("  Each one now has its own {project_key}_data/raw/ folder,")
#     print("  matching the original live-API pipeline's output exactly.")
#     print("=" * 60)


# if __name__ == "__main__":
#     main()
"""
============================================================
rawdata_from_zip.py — CONVERT A ZIPPED DS_EXPORT
              -> {project_key}_data/raw/ structure
              (same output layout the live API pipeline produced)
============================================================

WHY THIS EXISTS
----------------
The old pipeline (pipeline_orchestrator.py + extract_*.py) hit the live
API and wrote each project's data to its own top-level folder:

    {PROJECT_KEY}_data/raw/{issues,comments,links,dependencies,
                             documents,sprints,releases,projects,
                             reference,logs}/...

Now the backend instead hands us a single .zip export containing one
folder per data type (issue_details/, comments/, dependencies_blocks/,
documents/, issue_links/, sprints/, statuses/, issue_types/,
project_details/, integrated_resources/, releases/), each holding
*.jsonl files. This script converts that zip into EXACTLY the same
{project_key}_data/raw/... layout as before, so every downstream tool
(flatten.py, reports, etc.) keeps working unchanged.

Two things the zip does NOT contain that the old pipeline used to
fetch live:
  1. Comment inline images / comment attachments (only HTML content +
     a "documents" API link are present in the export)
  2. Actual document file bytes (only metadata + a download "link" are
     present)
So this script still uses BASE_URL + AUTH_TOKEN from .env to download
those specific files — everything else comes from the zip, no other
API calls are made.

USAGE
-----
    python rawdata_from_zip.py path/to/export.zip
    python rawdata_from_zip.py path/to/already_extracted_folder
    python rawdata_from_zip.py                       # auto-finds a
                                                       # single *.zip in CWD

.env required:
    BASE_URL     - used only to resolve/download attachment & image links
    AUTH_TOKEN   - used only to authenticate those downloads
    project_key  - OPTIONAL. If set, ONLY that project is converted
                   (matched against the "XXXX-123" issue-key prefix).
                   Leave blank/unset to convert every project found
                   in the export.

REQUIRES (same folder as this script):
    shared_http.py          - shared requests.Session / connection pool
    shared_thread_pool.py   - shared ThreadPoolExecutor for downloads

============================================================
CONFIRMED SCHEMAS (unchanged from the original rawdata.py)
============================================================
issue_details/*.jsonl   -> {"issue": {...}, "issue_types": [...],
                             "users": [...], "priorities": [...],
                             "statuses": [...]}
comments/*.jsonl        -> {"<issue_id>": {"comments": [...], "users": [...]}}
dependencies_blocks/*.jsonl (and/or dependencies_blocked_by/*.jsonl)
                        -> {"<issue_id>": {"issue_dependencies": [
                               {"id":.., "predecessor_id":.., "successor_id":..,
                                "dependency_type":..}], "issues":[...]}}
documents/*.jsonl       -> {"<issue_id>": {"documents": [...], "users": [...]}}
issue_links/*.jsonl     -> {"<issue_id>": {"issue_links": [...],
                             "issue_types": [...], "issues": [...]}}
integrated_resources/*.jsonl -> {"<issue_id>": {"integrated_resources": [...]}}
sprints/*.jsonl, releases/*.jsonl -> ONE FLAT RECORD PER LINE
                             (sprints: "primary_workspace_id" == project id,
                              releases: "project_id" directly)
statuses/*.jsonl, issue_types/*.jsonl -> one flat record per line
project_details/*.jsonl -> {"project": {...}, "users": [...]}
============================================================
"""

import os
import re
import sys
import json
import glob
import gzip
import time
import zipfile
import tempfile
from collections import defaultdict
from urllib.parse import urlparse
from concurrent.futures import as_completed

import requests
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from shared_http import get_shared_session
from shared_thread_pool import get_executor, shutdown_executor

# =============================================================
# CONFIGURATION
# =============================================================
load_dotenv()
BASE_URL   = os.getenv("BASE_URL")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
# Optional filter — if set, ONLY this project is converted.
FILTER_PROJECT_KEY = (os.getenv("project_key") or "").strip()

HEADERS = {
    "Authorization": f"Token {AUTH_TOKEN}",
    "Content-Type": "application/json",
}

SESSION = get_shared_session()
# Actual worker count is controlled by shared_thread_pool.py's own read of
# EXTRACTION_MAX_WORKERS (default 3) — set it in .env to speed downloads up.
RATE_LIMIT_DELAY = float(os.getenv("API_RATE_LIMIT_DELAY", "0.2"))
# If a target JSON file already exists (e.g. from an interrupted previous
# run), skip re-processing that record entirely instead of re-parsing,
# re-writing, and re-triggering its attachment/image downloads.
SKIP_EXISTING = os.getenv("SKIP_EXISTING", "true").strip().lower() != "false"

# Folders the export is expected to contain (checked to auto-locate the
# real export root inside an extracted zip, which sometimes nests
# everything one level deeper e.g. export/issue_details/...).
EXPECTED_SUBDIRS = {"issue_details", "comments", "documents", "issue_links"}

download_stats_lock = None  # set in main() once we know we're threaded


# =============================================================
# ZIP HANDLING
# =============================================================
def resolve_export_dir(path_arg):
    """
    Accepts either a .zip file or an already-extracted folder and
    returns a directory path containing issue_details/, comments/, etc.
    """
    if path_arg is None:
        zips = glob.glob("*.zip")
        if len(zips) == 1:
            path_arg = zips[0]
        elif len(zips) > 1:
            raise SystemExit(f"Multiple .zip files found in CWD, pass one explicitly: {zips}")
        else:
            raise SystemExit("No path given and no .zip found in CWD.")

    if os.path.isdir(path_arg):
        return locate_export_root(path_arg)

    if os.path.isfile(path_arg) and path_arg.lower().endswith(".zip"):
        extract_dir = tempfile.mkdtemp(prefix="ds_export_")
        print(f"  Extracting {path_arg} -> {extract_dir}")
        with zipfile.ZipFile(path_arg, "r") as zf:
            zf.extractall(extract_dir)
        return locate_export_root(extract_dir)

    raise SystemExit(f"'{path_arg}' is neither a directory nor a .zip file.")


def locate_export_root(base_dir):
    """Search base_dir (and one level of subfolders) for the folder that
    actually contains issue_details/, comments/, etc."""
    candidates = [base_dir] + [
        os.path.join(base_dir, d) for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    ]
    for c in candidates:
        try:
            entries = set(os.listdir(c))
        except (FileNotFoundError, NotADirectoryError):
            continue
        if EXPECTED_SUBDIRS & entries:
            return c
    raise SystemExit(
        f"Could not find issue_details/, comments/, etc. inside '{base_dir}'. "
        f"Check the zip's internal folder structure."
    )


# =============================================================
# GENERIC HELPERS
# =============================================================
def find_jsonl_files(directory):
    """Return every .jsonl AND .jsonl.gz file in `directory`, sorted.
    The client's export ships these as gzip-compressed (*.jsonl.gz);
    we read them straight out of the .gz without needing a separate
    `gunzip` step."""
    files = glob.glob(os.path.join(directory, "*.jsonl")) + \
            glob.glob(os.path.join(directory, "*.jsonl.gz"))
    return sorted(files)


def open_jsonl(filepath):
    """Open a .jsonl or .jsonl.gz file in text mode, transparently."""
    if filepath.endswith(".gz"):
        return gzip.open(filepath, "rt", encoding="utf-8")
    return open(filepath, "r", encoding="utf-8")


def read_jsonl(filepath):
    rows = []
    if not os.path.exists(filepath):
        return rows
    with open_jsonl(filepath) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  [warn] bad JSON at {filepath}:{line_no} — {e}")
    return rows


def iter_keyed_records(export_dir, folder):
    """Yields (issue_id_str, payload_dict) for every line across every
    *.jsonl / *.jsonl.gz file in `folder`, where each line looks like:
        {"<issue_id>": {...payload...}}
    """
    for filepath in find_jsonl_files(os.path.join(export_dir, folder)):
        with open_jsonl(filepath) as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"  [warn] bad JSON at {filepath}:{line_no} — {e}")
                    continue
                if not isinstance(obj, dict):
                    continue
                for k, v in obj.items():
                    yield k, v


def write_json(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    tmp = filepath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    os.replace(tmp, filepath)


def raw_root(project_key):
    """The per-project output root — matches the live pipeline's
    f"{PROJECT_KEY}_data/raw" convention exactly."""
    return f"{project_key}_data/raw"


PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "500"))


def progress_tick(label, n, every=PROGRESS_EVERY):
    """Print a heartbeat every `every` records so long-running loops never
    look silently hung — especially important on slow filesystems (e.g.
    WSL paths under /mnt/c) where thousands of small file writes can take
    minutes with zero output otherwise."""
    if n % every == 0:
        print(f"    ...{label}: {n} processed so far")


def project_key_from_issue_key(issue_key):
    """'FBOTS-5' -> 'FBOTS'."""
    if not issue_key or "-" not in issue_key:
        return "UNKNOWN"
    return issue_key.rsplit("-", 1)[0]


def merge_by_id(target_map, records):
    for r in records or []:
        if isinstance(r, dict) and "id" in r:
            target_map[r["id"]] = r


seen_project_keys = set()  # every project_key encountered, regardless of filter


def wanted_project(project_key):
    """Honor the optional project_key filter from .env."""
    seen_project_keys.add(project_key)
    if not FILTER_PROJECT_KEY:
        return True
    return project_key.lower() == FILTER_PROJECT_KEY.lower()


# =============================================================
# GLOBAL LOOKUPS
# =============================================================
issue_meta = {}                  # issue_id(int) -> {"key","project_key","title"}
project_id_to_key = {}           # project_id(int) -> project_key
users_map = defaultdict(dict)    # project_key -> {user_id: user}
priorities_map = defaultdict(dict)   # project_key -> {priority_id: priority}
issue_types_bonus = defaultdict(dict)  # project_key -> {issue_type_id: dict}


# =============================================================
# ATTACHMENT / IMAGE DOWNLOADING
# (adapted from the old extract_comments.py / extract_documents.py —
#  only these two things still touch the live API, since the zip
#  export doesn't carry the actual file bytes)
# =============================================================
def api_get(endpoint, params=None, retries=3):
    """Small GET-with-retry helper, same policy as the old extractors."""
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, headers=HEADERS, params=params, timeout=30)
            resp.raise_for_status()
            if RATE_LIMIT_DELAY:
                time.sleep(RATE_LIMIT_DELAY)
            return resp.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
                time.sleep(30)
                continue
            if attempt == retries - 1:
                print(f"  [warn] GET {endpoint} failed: {e}")
                return None
            time.sleep(1 * (attempt + 1))
    return None


# Fail fast instead of hanging 60s per dead link. (connect_timeout, read_timeout)
DOWNLOAD_CONNECT_TIMEOUT = float(os.getenv("DOWNLOAD_CONNECT_TIMEOUT", "8"))
DOWNLOAD_READ_TIMEOUT = float(os.getenv("DOWNLOAD_READ_TIMEOUT", "60"))

# Once a URL fails (or succeeds), remember it so the same dead/duplicate
# link referenced from many different comments/issues isn't retried
# over and over.
#
# A plain "check cache, then fetch" isn't enough: with many worker threads,
# two threads can both check the cache within the same few milliseconds —
# before either has actually finished the request — and both see nothing
# cached yet, so both go to the network. That's why the same dead URL was
# still timing out twice instead of once. The fix is a per-URL lock: the
# first thread to touch a given URL does the real work; any other thread
# that wants the same URL blocks until that's done, then just reads the
# cached result instead of making its own request.
_url_result_cache = {}
_url_locks = {}
_url_locks_dict_lock = None  # created lazily


def _get_url_lock(url):
    global _url_locks_dict_lock
    if _url_locks_dict_lock is None:
        from threading import Lock
        _url_locks_dict_lock = Lock()
    with _url_locks_dict_lock:
        lock = _url_locks.get(url)
        if lock is None:
            from threading import Lock
            lock = Lock()
            _url_locks[url] = lock
        return lock


def download_binary(url, file_path):
    """Stream-download a single file to disk. Returns True on success.
    Serializes per-URL (see comment above) so a broken/duplicate link
    referenced from many comments only ever hits the network once."""
    if os.path.exists(file_path):
        return True
    if not url.startswith("http"):
        url = f"{BASE_URL}{url}" if url.startswith("/") else f"{BASE_URL}/{url}"

    url_lock = _get_url_lock(url)
    with url_lock:
        # Re-check the cache now that we hold this URL's lock — if another
        # thread already resolved it while we were waiting, use that result
        # instead of making a second request.
        cached = _url_result_cache.get(url)
        if cached is False:
            return False  # known-bad URL, don't retry the timeout again
        if isinstance(cached, str):
            try:
                import shutil
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                shutil.copyfile(cached, file_path)
                return True
            except Exception:
                pass  # fall through and try a fresh download

        try:
            download_headers = {"Authorization": f"Token {AUTH_TOKEN}"}
            resp = SESSION.get(
                url, headers=download_headers,
                timeout=(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT),
                stream=True,
            )
            if resp.status_code != 200:
                _url_result_cache[url] = False
                return False
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            _url_result_cache[url] = file_path
            return True
        except Exception as e:
            print(f"  [warn] download failed for {url}: {e}")
            _url_result_cache[url] = False
            return False


IMG_SRC_PATTERN = r'<img[^>]+src=["\'](.*?)["\']'
IMG_CONTENT_PATTERN = r'<img[^>]*>(https?://[^<]+)</img>'


def download_comment_images(project_key, comment_id, html_content):
    if not html_content or not isinstance(html_content, str):
        return
    urls = re.findall(IMG_SRC_PATTERN, html_content, re.IGNORECASE)
    urls += re.findall(IMG_CONTENT_PATTERN, html_content, re.IGNORECASE)
    if not urls:
        return
    images_dir = os.path.join(
        raw_root(project_key), "comments", project_key,
        "attachments", f"comment_{comment_id}", "images"
    )
    for idx, img_url in enumerate(urls):
        parsed = urlparse(img_url)
        filename = os.path.basename(parsed.path)
        if not filename or "." not in filename:
            ext = parsed.path.split(".")[-1] if "." in parsed.path else "jpg"
            filename = f"image_{idx}.{ext}"
        download_binary(img_url, os.path.join(images_dir, filename))


def download_comment_attachments(project_key, comment_id, documents_link):
    """documents_link is a relative API endpoint (e.g. from
    comment['links']['documents']) that must be GET'd to list the
    actual documents, each of which is then downloaded by its own link."""
    if not documents_link:
        return
    data = api_get(documents_link.lstrip("/"))
    if not data:
        return
    documents = data.get("documents", [])
    if not documents:
        return
    attachments_dir = os.path.join(
        raw_root(project_key), "comments", project_key,
        "attachments", f"comment_{comment_id}", "documents"
    )
    for doc in documents:
        link = doc.get("link")
        if not link:
            continue
        filename = doc.get("attachment_file_name", f"file_{doc.get('id')}")
        download_binary(link, os.path.join(attachments_dir, filename))


def download_document_file(project_key, doc):
    """Download one document/attachment file referenced by a
    documents/*.jsonl record. Mutates doc with local_file_path."""
    link = doc.get("link")
    if not link:
        return
    album_name = doc.get("album_name", "unknown")
    filename = doc.get("attachment_file_name", f"file_{doc.get('id')}")
    file_path = os.path.join(
        raw_root(project_key), "documents", project_key,
        "attachments", album_name, filename
    )
    if download_binary(link, file_path):
        doc["local_file_path"] = file_path


# =============================================================
# 1) PROJECT DETAILS
# target: {project}_data/raw/projects/{project_key}_details.json
#         (the FULL raw row — matches extract_project_details.py's
#          save_json(data, "projects", f"{project_key}_details.json"))
# =============================================================
def convert_project_details(export_dir):
    print("\nConverting project_details...")
    written = 0
    for filepath in find_jsonl_files(os.path.join(export_dir, "project_details")):
        for row in read_jsonl(filepath):
            project = row.get("project")
            if not isinstance(project, dict) or "id" not in project:
                print(f"  [skip] no usable 'project' in row: {str(row)[:150]}")
                continue
            project_key = project.get("key", "UNKNOWN")
            if not wanted_project(project_key):
                continue

            project_id_to_key[project["id"]] = project_key
            merge_by_id(users_map[project_key], row.get("users", []))

            out_path = os.path.join(raw_root(project_key), "projects", f"{project_key}_details.json")
            write_json(out_path, row)
            written += 1

    print(f"  Wrote {written} project files")
    print(f"  project_id -> key map: {project_id_to_key}")


# =============================================================
# 2) ISSUES
# target: {project}_data/raw/issues/{project_key}/issue_{id}.json
#         + {project}_data/raw/issues/{project_key}_issues_list.json
# =============================================================
def convert_issues(export_dir):
    print("\nConverting issue_details...")
    files = find_jsonl_files(os.path.join(export_dir, "issue_details"))
    print(f"  Files found: {len(files)}")

    issues_by_project = defaultdict(list)
    count = 0
    seen = 0
    for file_idx, filepath in enumerate(files, 1):
        print(f"  Reading file {file_idx}/{len(files)}: {os.path.basename(filepath)}")
        for row in read_jsonl(filepath):
            seen += 1
            progress_tick("issues", seen)
            issue = row.get("issue")
            if not isinstance(issue, dict):
                print(f"  [skip] no 'issue' key in row: {str(row)[:150]}")
                continue

            issue_id = issue.get("id")
            issue_key = issue.get("key")
            if issue_id is None or issue_key is None:
                print(f"  [skip] missing id/key: {str(issue)[:150]}")
                continue

            project_key = project_key_from_issue_key(issue_key)
            if not wanted_project(project_key):
                continue

            issue_meta[issue_id] = {
                "key": issue_key,
                "project_key": project_key,
                "title": issue.get("title") or issue.get("name") or "",
            }

            merge_by_id(users_map[project_key], row.get("users", []))
            merge_by_id(priorities_map[project_key], row.get("priorities", []))
            merge_by_id(issue_types_bonus[project_key], row.get("issue_types", []))

            # NOTE: issue_meta/users_map/etc above are always populated even
            # on a skip, since later stages (comments, dependencies, links...)
            # depend on that in-memory lookup regardless of whether this
            # particular file already exists on disk.
            out_path = os.path.join(raw_root(project_key), "issues", project_key, f"issue_{issue_id}.json")
            if not (SKIP_EXISTING and os.path.exists(out_path)):
                write_json(out_path, row)
                count += 1
            issues_by_project[project_key].append(issue)

    for project_key, issues in issues_by_project.items():
        combined_path = os.path.join(raw_root(project_key), "issues", f"{project_key}_issues_list.json")
        write_json(combined_path, {"issues": issues, "total_count": len(issues)})

    print(f"  Wrote {count} issue files across {len(issues_by_project)} project(s)")


# =============================================================
# 3) COMMENTS  (+ real image/attachment downloads)
# target: {project}_data/raw/comments/{project_key}/comments_{issue_id}.json
# =============================================================
def convert_comments(export_dir):
    print("\nConverting comments (+ downloading images/attachments)...")
    executor = get_executor()
    futures = []
    count = 0
    seen = 0

    for issue_id_str, payload in iter_keyed_records(export_dir, "comments"):
        seen += 1
        progress_tick("comment records", seen)
        if not isinstance(payload, dict):
            continue
        issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
        meta = issue_meta.get(issue_id, {})
        project_key = meta.get("project_key", "UNKNOWN")
        if not wanted_project(project_key):
            continue

        out_path = os.path.join(raw_root(project_key), "comments", project_key, f"comments_{issue_id_str}.json")
        if SKIP_EXISTING and os.path.exists(out_path):
            continue

        comments = payload.get("comments", [])
        merge_by_id(users_map[project_key], payload.get("users", []))

        for comment in comments:
            comment_id = comment.get("id")
            content = comment.get("content", "")
            documents_link = (comment.get("links") or {}).get("documents")

            futures.append(executor.submit(download_comment_images, project_key, comment_id, content))
            if documents_link:
                futures.append(executor.submit(download_comment_attachments, project_key, comment_id, documents_link))

        write_json(out_path, {
            "comments": comments,
            "users": payload.get("users", []),
        })
        count += 1

    print(f"  Wrote {count} comment files — waiting for {len(futures)} attachment/image downloads...")
    done = 0
    for f in as_completed(futures):
        done += 1
        progress_tick("downloads", done)
        try:
            f.result()
        except Exception as e:
            print(f"  [warn] download task error: {e}")
    print("  Downloads complete.")


# =============================================================
# 4) DEPENDENCIES
# target: {project}_data/raw/dependencies/{project_key}/deps_{issue_id}.json
# =============================================================
def convert_dependencies(export_dir):
    print("\nConverting dependencies...")

    all_deps = {}
    embedded_issue_lookup = {}
    sources_used = []

    for folder in ["dependencies_blocks", "dependencies_blocked_by"]:
        folder_path = os.path.join(export_dir, folder)
        if not os.path.isdir(folder_path):
            print(f"  [note] '{folder}' folder not present — skipping (not required)")
            continue
        sources_used.append(folder)
        for issue_id_str, payload in iter_keyed_records(export_dir, folder):
            if not isinstance(payload, dict):
                continue
            for dep in payload.get("issue_dependencies", []):
                if isinstance(dep, dict) and "id" in dep:
                    all_deps[dep["id"]] = dep
            for embedded_issue in payload.get("issues", []):
                if isinstance(embedded_issue, dict) and "id" in embedded_issue:
                    embedded_issue_lookup[embedded_issue["id"]] = {
                        "key": embedded_issue.get("key", ""),
                        "title": embedded_issue.get("title") or embedded_issue.get("name") or "",
                    }

    print(f"  Sources used: {sources_used}")
    print(f"  Unique dependency records found: {len(all_deps)}")

    def resolve_linked(issue_id):
        if issue_id in issue_meta:
            return issue_meta[issue_id]["key"], issue_meta[issue_id]["title"]
        if issue_id in embedded_issue_lookup:
            e = embedded_issue_lookup[issue_id]
            return e["key"], e["title"]
        return "", ""

    deps_by_issue = defaultdict(lambda: {"successor_dependencies": [], "predecessor_dependencies": []})

    for dep in all_deps.values():
        dep_id = dep.get("id")
        pred = dep.get("predecessor_id")
        succ = dep.get("successor_id")
        dep_type = dep.get("dependency_type", "finish_to_start")

        if pred is not None and succ is not None:
            succ_key, succ_title = resolve_linked(succ)
            deps_by_issue[pred]["successor_dependencies"].append({
                "id": dep_id, "predecessor_id": pred, "successor_id": succ,
                "linked_issue_id": succ, "linked_issue_key": succ_key,
                "linked_issue_title": succ_title, "linked_issue_project_id": "",
                "dependency_type": dep_type,
            })
            pred_key, pred_title = resolve_linked(pred)
            deps_by_issue[succ]["predecessor_dependencies"].append({
                "id": dep_id, "predecessor_id": pred, "successor_id": succ,
                "linked_issue_id": pred, "linked_issue_key": pred_key,
                "linked_issue_title": pred_title, "linked_issue_project_id": "",
                "dependency_type": dep_type,
            })

    count = 0
    for issue_id, payload in deps_by_issue.items():
        meta = issue_meta.get(issue_id, {})
        project_key = meta.get("project_key", "UNKNOWN")
        if not wanted_project(project_key):
            continue
        # Match the live pipeline: only write a file if there's actually
        # something to say (extract_dependencies.py skips empty results).
        if not payload["successor_dependencies"] and not payload["predecessor_dependencies"]:
            continue
        out_path = os.path.join(raw_root(project_key), "dependencies", project_key, f"deps_{issue_id}.json")
        write_json(out_path, {
            "issue_id": issue_id,
            "successor_dependencies": payload["successor_dependencies"],
            "predecessor_dependencies": payload["predecessor_dependencies"],
        })
        count += 1

    print(f"  Wrote {count} dependency files")


# =============================================================
# 5) DOCUMENTS  (+ real file downloads)
# target: {project}_data/raw/documents/{project_key}/docs_{issue_id}.json
# =============================================================
def convert_documents(export_dir):
    print("\nConverting documents (+ downloading files)...")
    executor = get_executor()
    futures = []
    count = 0
    seen = 0

    for issue_id_str, payload in iter_keyed_records(export_dir, "documents"):
        seen += 1
        progress_tick("document records", seen)
        if not isinstance(payload, dict):
            continue
        issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
        meta = issue_meta.get(issue_id, {})
        project_key = meta.get("project_key", "UNKNOWN")
        if not wanted_project(project_key):
            continue

        documents = payload.get("documents", [])
        # Match the live pipeline: don't write a file when there are no documents.
        if not documents:
            continue

        out_path = os.path.join(raw_root(project_key), "documents", project_key, f"docs_{issue_id_str}.json")
        if SKIP_EXISTING and os.path.exists(out_path):
            continue

        merge_by_id(users_map[project_key], payload.get("users", []))

        for doc in documents:
            futures.append(executor.submit(download_document_file, project_key, doc))

        write_json(out_path, {
            "issue_id": issue_id if issue_id is not None else issue_id_str,
            "project_key": project_key,
            "documents": documents,
            "users": payload.get("users") or None,
        })
        count += 1

    print(f"  Wrote {count} document files — waiting for {len(futures)} downloads...")
    done = 0
    for f in as_completed(futures):
        done += 1
        progress_tick("downloads", done)
        try:
            f.result()
        except Exception as e:
            print(f"  [warn] download task error: {e}")
    print("  Downloads complete.")


# =============================================================
# 6) ISSUE LINKS
# target: {project}_data/raw/links/{project_key}/links_{issue_id}.json
# =============================================================
def convert_issue_links(export_dir):
    print("\nConverting issue_links...")
    count = 0
    for issue_id_str, payload in iter_keyed_records(export_dir, "issue_links"):
        if not isinstance(payload, dict):
            continue
        issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
        meta = issue_meta.get(issue_id, {})
        project_key = meta.get("project_key", "UNKNOWN")
        if not wanted_project(project_key):
            continue

        merge_by_id(issue_types_bonus[project_key], payload.get("issue_types", []))

        out_path = os.path.join(raw_root(project_key), "links", project_key, f"links_{issue_id_str}.json")
        if SKIP_EXISTING and os.path.exists(out_path):
            continue
        write_json(out_path, {
            "issue_links": payload.get("issue_links", []),
            "issue_types": payload.get("issue_types", []),
            "issues": payload.get("issues", []),
        })
        count += 1
    print(f"  Wrote {count} issue_links files")


# =============================================================
# 7) INTEGRATED RESOURCES  (flat, no project subfolder — matches
#    extract_integrated_resources.py exactly)
# target: {project}_data/raw/integrated_resources/ir_{issue_id}.json
# =============================================================
def convert_integrated_resources(export_dir):
    print("\nConverting integrated_resources...")
    count = 0
    for issue_id_str, payload in iter_keyed_records(export_dir, "integrated_resources"):
        if not isinstance(payload, dict):
            continue
        issue_id = int(issue_id_str) if issue_id_str.isdigit() else None
        meta = issue_meta.get(issue_id, {})
        project_key = meta.get("project_key", "UNKNOWN")
        if not wanted_project(project_key):
            continue

        out_path = os.path.join(raw_root(project_key), "integrated_resources", f"ir_{issue_id_str}.json")
        write_json(out_path, {"integrated_resources": payload.get("integrated_resources", [])})
        count += 1
    print(f"  Wrote {count} integrated_resources files")


# =============================================================
# 8) SPRINTS
# target: {project}_data/raw/sprints/{project_key}_sprints.json
# =============================================================
def convert_sprints(export_dir):
    print("\nConverting sprints...")
    sprints_by_project = defaultdict(list)

    for filepath in find_jsonl_files(os.path.join(export_dir, "sprints")):
        for sprint in read_jsonl(filepath):
            project_id = sprint.get("primary_workspace_id")
            project_key = project_id_to_key.get(project_id, "UNKNOWN")
            if project_key == "UNKNOWN":
                print(f"  [warn] sprint id={sprint.get('id')} has unrecognized primary_workspace_id={project_id}")
            if not wanted_project(project_key):
                continue
            sprints_by_project[project_key].append(sprint)

    count = 0
    for project_key, sprints in sprints_by_project.items():
        out_path = os.path.join(raw_root(project_key), "sprints", f"{project_key}_sprints.json")
        write_json(out_path, {"sprints": sprints})
        count += 1

    total = sum(len(v) for v in sprints_by_project.values())
    print(f"  Wrote {count} sprint files ({total} sprints)")


# =============================================================
# 9) RELEASES
# target: {project}_data/raw/releases/{project_key}_releases.json
# =============================================================
def convert_releases(export_dir):
    print("\nConverting releases...")
    releases_by_project = defaultdict(list)

    for filepath in find_jsonl_files(os.path.join(export_dir, "releases")):
        for release in read_jsonl(filepath):
            project_id = release.get("project_id")
            project_key = project_id_to_key.get(project_id, "UNKNOWN")
            if project_key == "UNKNOWN":
                print(f"  [warn] release id={release.get('id')} has unrecognized project_id={project_id}")
            if not wanted_project(project_key):
                continue
            releases_by_project[project_key].append(release)

    count = 0
    for project_key, releases in releases_by_project.items():
        out_path = os.path.join(raw_root(project_key), "releases", f"{project_key}_releases.json")
        write_json(out_path, {"releases": releases})
        count += 1

    total = sum(len(v) for v in releases_by_project.values())
    print(f"  Wrote {count} release files ({total} releases)")


# =============================================================
# 10/11/12) REFERENCE DATA — statuses, issue_types, priorities, users
# target: {project}_data/raw/reference/{name}.json
#
# NOTE: statuses/*.jsonl and issue_types/*.jsonl are not explicitly
# project-tagged in the confirmed export schema, so (matching the old
# pipeline's per-project /issue_types API call as closely as this
# export allows) the SAME global status/issue_type list is written into
# every project's reference/ folder. Priorities and users ARE split
# correctly per project since they're harvested from project-scoped
# issue/comment/document records above.
# =============================================================
def convert_reference_lists(export_dir, project_keys):
    print("\nConverting statuses / issue_types (shared across projects)...")

    def load_flat(subdir):
        merged = {}
        for filepath in find_jsonl_files(os.path.join(export_dir, subdir)):
            for row in read_jsonl(filepath):
                if isinstance(row, dict) and "id" in row:
                    merged[row["id"]] = row
        return merged

    statuses = load_flat("statuses")
    issue_types = load_flat("issue_types")

    for project_key in project_keys:
        merged_types = dict(issue_types)
        for k, v in issue_types_bonus.get(project_key, {}).items():
            merged_types.setdefault(k, v)

        write_json(os.path.join(raw_root(project_key), "reference", "statuses.json"), list(statuses.values()))
        write_json(os.path.join(raw_root(project_key), "reference", "issue_types.json"), list(merged_types.values()))
        write_json(os.path.join(raw_root(project_key), "reference", "priorities.json"),
                   list(priorities_map.get(project_key, {}).values()))
        write_json(os.path.join(raw_root(project_key), "reference", "users.json"),
                   list(users_map.get(project_key, {}).values()))

    print(f"  Wrote reference/ files for {len(project_keys)} project(s)")


# =============================================================
# MAIN
# =============================================================
def main():
    path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    export_dir = resolve_export_dir(path_arg)

    print("=" * 60)
    print("  rawdata_from_zip.py: DS_EXPORT.zip -> {project}_data/raw/")
    print(f"  Source : {os.path.abspath(export_dir)}")
    print(f"  Filter : {FILTER_PROJECT_KEY or '(none — converting all projects found)'}")
    print("=" * 60)

    if not BASE_URL or not AUTH_TOKEN:
        print("  [warn] BASE_URL/AUTH_TOKEN not set — comment images, comment "
              "attachments, and document files will NOT be downloaded.")

    # Order matters: projects first (project_id->key map), then issues
    # (issue_meta lookup), then everything that depends on those.
    convert_project_details(export_dir)
    convert_issues(export_dir)
    convert_comments(export_dir)
    convert_documents(export_dir)
    convert_dependencies(export_dir)
    convert_issue_links(export_dir)
    convert_integrated_resources(export_dir)
    convert_sprints(export_dir)
    convert_releases(export_dir)

    all_project_keys = sorted(set(issue_meta[i]["project_key"] for i in issue_meta) | set(project_id_to_key.values()))
    if FILTER_PROJECT_KEY:
        all_project_keys = [p for p in all_project_keys if p.lower() == FILTER_PROJECT_KEY.lower()]
    convert_reference_lists(export_dir, all_project_keys)

    shutdown_executor(wait=True)

    print("\n" + "=" * 60)
    if FILTER_PROJECT_KEY and not all_project_keys:
        print(f"  ⚠ project_key filter '{FILTER_PROJECT_KEY}' matched NOTHING.")
        if seen_project_keys:
            print(f"  Project keys actually found in this export: {sorted(seen_project_keys)}")
            print(f"  Fix: set project_key to one of the above in .env, or leave it blank.")
        else:
            print("  No project keys were found in this export at all — check that the "
                  "zip's folder names/schemas match what this script expects "
                  "(issue_details/, comments/, etc.).")
    else:
        print(f"  DONE. Projects converted: {all_project_keys}")
        print("  Each one now has its own {project_key}_data/raw/ folder,")
        print("  matching the original live-API pipeline's output exactly.")
    print("=" * 60)


if __name__ == "__main__":
    main()