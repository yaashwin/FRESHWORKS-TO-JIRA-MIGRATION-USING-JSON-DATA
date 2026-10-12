"""
Standalone script to extract documents/attachments for all issues.
PREREQUISITE: extract_issues_list.py must be run first.
Run this independently: python extract_documents.py
"""

import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
# =============================================================
# CONFIGURATION
# =============================================================
load_dotenv()
BASE_URL = os.getenv("BASE_URL")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
PROJECT_KEY = os.getenv("project_key")

RAW_DATA_DIR = "data/raw"
RATE_LIMIT_DELAY = 0.2
SKIP_EXISTING = True
MAX_WORKERS = 20  # Number of parallel threads

HEADERS = {
    "Authorization": f"Token {AUTH_TOKEN}",
    "Content-Type": "application/json"
}

# =============================================================
# THREAD-SAFE STATS
# =============================================================
stats_lock = Lock()
stats = {
    "script": "extract_documents",
    "started_at": None,
    "completed_at": None,
    "total_api_calls": 0,
    "failed_api_calls": 0,
    "issues_processed": 0,
    "issues_skipped": 0,
    "total_documents": 0,
    "documents_downloaded": 0,
    "documents_failed": 0,
    "issues_with_documents": 0,
    "issues_without_documents": 0,
    "errors": []
}

# =============================================================
# HELPER FUNCTIONS
# =============================================================
def api_get(endpoint, params=None, retries=3):
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
   
    with stats_lock:
        stats["total_api_calls"] += 1
 
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return response.json()
 
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
                time.sleep(30)
                continue
            if attempt == retries - 1:
                error_msg = f"{endpoint}: {str(e)}"
                with stats_lock:
                    stats["failed_api_calls"] += 1
                    stats["errors"].append({"endpoint": endpoint, "error": error_msg})
                return None
            time.sleep(1 * (attempt + 1))


def save_json(data, *path_parts):
    filepath = os.path.join(RAW_DATA_DIR, *path_parts)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    temp_filepath = filepath + ".tmp"
    
    with open(temp_filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    os.replace(temp_filepath, filepath)
    return filepath


def file_exists(*path_parts):
    return os.path.exists(os.path.join(RAW_DATA_DIR, *path_parts))


# =============================================================
# DOCUMENT DOWNLOAD
# =============================================================
def download_document_file(project_key, doc):
    """Download a single document file to local storage"""
    document_id = doc.get("id")
    link = doc.get("link")
    album_name = doc.get("album_name", "unknown")
    attachment_file_name = doc.get("attachment_file_name", f"file_{document_id}")
    
    if not link:
        with stats_lock:
            stats["documents_failed"] += 1
        return
    
    # Create directory structure
    attachments_dir = os.path.join(
        RAW_DATA_DIR,
        "documents",
        project_key,
        "attachments",
        album_name
    )
    os.makedirs(attachments_dir, exist_ok=True)
    
    # Full file path
    file_path = os.path.join(attachments_dir, attachment_file_name)
    
    # Skip if already downloaded
    if os.path.exists(file_path):
        doc["local_file_path"] = file_path
        return
    
    # Download the file
    try:
        download_headers = {
            "Authorization": f"Token {AUTH_TOKEN}"
        }
        
        response = requests.get(
            link,
            headers=download_headers,
            timeout=60,
            stream=True
        )
        
        if response.status_code == 200:
            # Write file in chunks
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Add local path to document metadata
            doc["local_file_path"] = file_path
            
            with stats_lock:
                stats["documents_downloaded"] += 1
        
        else:
            with stats_lock:
                stats["documents_failed"] += 1
    
    except Exception as e:
        with stats_lock:
            stats["documents_failed"] += 1


# =============================================================
# EXTRACTION
# =============================================================
# def extract_documents(project_key, issue_id):
#     """Extract issue documents/attachments"""
#     if SKIP_EXISTING and file_exists("documents", project_key, f"docs_{issue_id}.json"):
#         return True, "skipped", 0

#     endpoint = f"{project_key}/issues/{issue_id}/documents"
    
#     data = api_get(endpoint)
    
#     if not data:
#         # Save with null
#         payload = {
#             "issue_id": issue_id,
#             "project_key": project_key,
#             "documents": None,
#             "users": None
#         }
#         # save_json(payload, "documents", project_key, f"docs_{issue_id}.json")
#         return True, "processed", 0
    
#     documents = data.get("documents", [])
#     users = data.get("users", [])
    
#     # Download document files
#     if documents:
#         for doc in documents:
#             download_document_file(project_key, doc)
    
#     # Save metadata
#     payload = {
#         "issue_id": issue_id,
#         "project_key": project_key,
#         "documents": documents if documents else None,
#         "users": users if users else None
#     }
    
#     save_json(payload, "documents", project_key, f"docs_{issue_id}.json")
    
#     return True, "processed", len(documents) if documents else 0
def extract_documents(project_key, issue_id):
    """Extract issue documents/attachments"""

    if SKIP_EXISTING and file_exists("documents", project_key, f"docs_{issue_id}.json"):
        return True, "skipped", 0

    endpoint = f"{project_key}/issues/{issue_id}/documents"

    data = api_get(endpoint)

    # API failed
    if not data:
        return False, "failed", 0

    documents = data.get("documents", [])
    users = data.get("users", [])

    # =========================================================
    # NO DOCUMENTS → DON'T SAVE FILE
    # =========================================================
    if not documents:
        return True, "processed", 0

    # =========================================================
    # Download document files
    # =========================================================
    for doc in documents:
        download_document_file(project_key, doc)

    # =========================================================
    # Save only when documents exist
    # =========================================================
    payload = {
        "issue_id": issue_id,
        "project_key": project_key,
        "documents": documents,
        "users": users if users else None
    }

    save_json(payload, "documents", project_key, f"docs_{issue_id}.json")

    return True, "processed", len(documents)

def process_single_issue(project_key, issue_id, index, total):
    """Process a single issue - runs in parallel"""
    try:
        success, status, doc_count = extract_documents(project_key, issue_id)
        
        with stats_lock:
            if status == "processed":
                stats["issues_processed"] += 1
                stats["total_documents"] += doc_count
                
                if doc_count > 0:
                    stats["issues_with_documents"] += 1
                    print(f"[{index}/{total}]  Issue {issue_id} - {doc_count} document(s)")
                else:
                    stats["issues_without_documents"] += 1
                    print(f"[{index}/{total}]  Issue {issue_id} - no documents")
            elif status == "skipped":
                stats["issues_skipped"] += 1
                print(f"[{index}/{total}]   Issue {issue_id} - skipped (exists)")
            else:
                print(f"[{index}/{total}]  Issue {issue_id} - failed")
        
        return success, issue_id
    
    except Exception as e:
        print(f"[{index}/{total}]  Issue {issue_id} - error: {str(e)}")
        with stats_lock:
            stats["errors"].append({
                "issue_id": issue_id,
                "error": str(e)
            })
        return False, issue_id


def load_issues_list(project_key):
    """Load issues list from file"""
    filepath = os.getenv(
    "EXTRACTION_ISSUES_FILE",
    os.path.join(RAW_DATA_DIR, "issues", f"{project_key}_issues_list.json")
)
    
    if not os.path.exists(filepath):
        print(f" ERROR: Issues list not found at {filepath}")
        print(f"   Please run extract_issues_list.py first!")
        return None
    
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return data.get("issues", [])


# =============================================================
# MAIN
# =============================================================
def main():
    stats["started_at"] = datetime.now().isoformat()
    
    print("=" * 60)
    print("   EXTRACTING DOCUMENTS (PARALLEL)")
    print("=" * 60)
    print(f"  Project: {PROJECT_KEY}")
    print(f"  Workers: {MAX_WORKERS}")
    print(f"  Skip existing: {SKIP_EXISTING}")
    print("=" * 60)
    
    # Load issues list
    print("\n Loading issues list...")
    issues = load_issues_list(PROJECT_KEY)
    
    if not issues:
        return
    
    print(f" Loaded {len(issues)} issues")
    
    # Process issues in parallel
    print(f"\n Processing {len(issues)} issues with {MAX_WORKERS} workers...\n")
    
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                process_single_issue,
                PROJECT_KEY,
                issue["id"],
                i + 1,
                len(issues)
            ): issue["id"]
            for i, issue in enumerate(issues)
        }
        
        for future in as_completed(futures):
            issue_id = futures[future]
            try:
                success, _ = future.result()
                if success:
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f" Unexpected error for issue {issue_id}: {str(e)}")
                failed += 1
    
    stats["completed_at"] = datetime.now().isoformat()
    
    # Save stats
    stats_file = save_json(stats, "logs", "documents_extraction.json")
    
    print("\n" + "=" * 60)
    print("   SUMMARY")
    print("=" * 60)
    print(f"  Total Issues: {len(issues)}")
    print(f"  Processed: {stats['issues_processed']}")
    print(f"  Skipped: {stats['issues_skipped']}")
    print(f"  Failed: {failed}")
    print(f"  Total Documents (metadata): {stats['total_documents']}")
    print(f"  Documents Downloaded (files): {stats['documents_downloaded']}")
    print(f"  Documents Failed: {stats['documents_failed']}")
    print(f"  Issues WITH Documents: {stats['issues_with_documents']}")
    print(f"  Issues WITHOUT Documents: {stats['issues_without_documents']}")
    print(f"  Total API Calls: {stats['total_api_calls']}")
    print(f"  Failed API Calls: {stats['failed_api_calls']}")
    print(f"  Errors: {len(stats['errors'])}")
    print(f"  Stats saved to: {stats_file}")
    print("=" * 60)
    
    print("\n Documents extraction completed!\n")


if __name__ == "__main__":
    main()