"""
Standalone script to extract comments for all issues.
PREREQUISITE: extract_issues_list.py must be run first.
Run this independently: python extract_comments.py
"""

import requests
import json
import os
import time
import re
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import urlparse
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
    "script": "extract_comments",
    "started_at": None,
    "completed_at": None,
    "total_api_calls": 0,
    "failed_api_calls": 0,
    "issues_processed": 0,
    "issues_skipped": 0,
    "total_comments": 0,
    "comment_images": 0,
    "comment_attachments": 0,
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


def remove_html_tags(text):
    """Clean HTML tags from text"""
    return text  # Keep original for now, add cleaning if needed


# =============================================================
# IMAGE EXTRACTION
# =============================================================
def extract_and_download_comment_images(project_key, comment_id, user_id, html_content):
    """Extract and download images from HTML"""
    img_src_pattern = r'<img[^>]+src=["\'](.*?)["\']'
    img_content_pattern = r'<img[^>]*>(https?://[^<]+)</img>'
    
    image_urls = []
    src_matches = re.findall(img_src_pattern, html_content, re.IGNORECASE)
    image_urls.extend(src_matches)
    
    content_matches = re.findall(img_content_pattern, html_content, re.IGNORECASE)
    image_urls.extend(content_matches)
    
    if not image_urls:
        return
    
    for idx, img_url in enumerate(image_urls):
        download_comment_image(project_key, comment_id, user_id, img_url, idx)


def download_comment_image(project_key, comment_id, user_id, img_url, image_index):
    """Download comment image"""
    if not img_url.startswith("http"):
        if img_url.startswith("/"):
            img_url = f"{BASE_URL}{img_url}"
        else:
            img_url = f"{BASE_URL}/{img_url}"
    
    parsed_url = urlparse(img_url)
    original_filename = os.path.basename(parsed_url.path)
    
    if not original_filename or "." not in original_filename:
        ext = "jpg"
        if "." in parsed_url.path:
            ext = parsed_url.path.split(".")[-1]
        original_filename = f"image_{image_index}.{ext}"
    
    images_dir = os.path.join(
        RAW_DATA_DIR, "comments", project_key,
        "attachments", f"comment_{comment_id}", "images"
    )
    os.makedirs(images_dir, exist_ok=True)
    
    file_path = os.path.join(images_dir, original_filename)
    
    if os.path.exists(file_path):
        return
    
    try:
        download_headers = {"Authorization": f"Token {AUTH_TOKEN}"}
        
        response = requests.get(img_url, headers=download_headers, timeout=60, stream=True)
        
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            with stats_lock:
                stats["comment_images"] += 1
    
    except Exception as e:
        pass  # Silent fail for images


# =============================================================
# ATTACHMENT EXTRACTION
# =============================================================
def extract_comment_attachments(project_key, comment_id, documents_link):
    """Extract comment attachments"""
    endpoint = documents_link.lstrip("/")
    data = api_get(endpoint)
    
    if not data:
        return
    
    documents = data.get("documents", [])
    
    if not documents:
        return
    
    for doc in documents:
        download_comment_attachment(project_key, comment_id, doc)


def download_comment_attachment(project_key, comment_id, doc):
    """Download comment attachment"""
    document_id = doc.get("id")
    link = doc.get("link")
    attachment_file_name = doc.get("attachment_file_name", f"file_{document_id}")
    
    if not link:
        return
    
    attachments_dir = os.path.join(
        RAW_DATA_DIR, "comments", project_key,
        "attachments", f"comment_{comment_id}", "documents"
    )
    os.makedirs(attachments_dir, exist_ok=True)
    
    file_path = os.path.join(attachments_dir, attachment_file_name)
    
    if os.path.exists(file_path):
        doc["local_file_path"] = file_path
        return
    
    try:
        download_headers = {"Authorization": f"Token {AUTH_TOKEN}"}
        
        response = requests.get(link, headers=download_headers, timeout=60, stream=True)
        
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            doc["local_file_path"] = file_path
            
            with stats_lock:
                stats["comment_attachments"] += 1
    
    except Exception as e:
        pass  # Silent fail for attachments


# =============================================================
# EXTRACTION
# =============================================================
def extract_comments(project_key, issue_id):
    """Extract comments with images and attachments"""
    if SKIP_EXISTING and file_exists("comments", project_key, f"comments_{issue_id}.json"):
        return True, "skipped", 0

    data = api_get(
        f"{project_key}/issues/{issue_id}/comments",
        params={"include": "user"}
    )

    if data:
        comments = data.get("comments", [])
        
        if comments:
            for comment in comments:
                comment_id = comment.get("id")
                user_id = comment.get("user_id")
                
                # Extract images from HTML content
                content = comment.get("content", "")
                if content and isinstance(content, str):
                    extract_and_download_comment_images(
                        project_key, comment_id, user_id, content
                    )
                
                # Extract document attachments
                documents_link = comment.get("links", {}).get("documents")
                if documents_link:
                    extract_comment_attachments(project_key, comment_id, documents_link)
                
                # Clean HTML from content
                if "content" in comment and isinstance(comment["content"], str):
                    comment["content"] = remove_html_tags(comment["content"])
                if "body" in comment and isinstance(comment["body"], str):
                    comment["body"] = remove_html_tags(comment["body"])

            save_json(data, "comments", project_key, f"comments_{issue_id}.json")
            
            return True, "processed", len(comments)
        else:
            # Save empty comments
            # save_json(data, "comments", project_key, f"comments_{issue_id}.json")
            return True, "processed", 0
    
    return False, "failed", 0


def process_single_issue(project_key, issue_id, index, total):
    """Process a single issue - runs in parallel"""
    try:
        success, status, comment_count = extract_comments(project_key, issue_id)
        
        with stats_lock:
            if status == "processed":
                stats["issues_processed"] += 1
                stats["total_comments"] += comment_count
                if comment_count > 0:
                    print(f"[{index}/{total}]  Issue {issue_id} - {comment_count} comment(s)")
                else:
                    print(f"[{index}/{total}]  Issue {issue_id} - no comments")
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
    print("   EXTRACTING COMMENTS (PARALLEL)")
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
    stats_file = save_json(stats, "logs", "comments_extraction.json")
    
    print("\n" + "=" * 60)
    print("   SUMMARY")
    print("=" * 60)
    print(f"  Total Issues: {len(issues)}")
    print(f"  Processed: {stats['issues_processed']}")
    print(f"  Skipped: {stats['issues_skipped']}")
    print(f"  Failed: {failed}")
    print(f"  Total Comments: {stats['total_comments']}")
    print(f"  Comment Images: {stats['comment_images']}")
    print(f"  Comment Attachments: {stats['comment_attachments']}")
    print(f"  Total API Calls: {stats['total_api_calls']}")
    print(f"  Failed API Calls: {stats['failed_api_calls']}")
    print(f"  Errors: {len(stats['errors'])}")
    print(f"  Stats saved to: {stats_file}")
    print("=" * 60)
    
    print("\n Comments extraction completed!\n")


if __name__ == "__main__":
    main()