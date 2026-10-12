"""
Shared thread pool for all extraction scripts.

Previously each extract_*.py file created its OWN ThreadPoolExecutor
(e.g. `with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:`),
so when several extraction scripts ran at the same time (per page, per
type) the process could spin up N separate pools at once, each with its
own MAX_WORKERS threads all hitting the API concurrently.

This module gives every extractor a single, shared, process-wide pool.
All API-calling code paths (issue details, comments, links, dependencies,
documents, integrated resources, etc.) submit work here instead of
creating their own executor, so the total number of concurrent API
requests is capped by one number regardless of how many extractors are
active.
"""

import atexit
import os
from concurrent.futures import ThreadPoolExecutor

# Total worker threads shared across ALL extraction scripts/API calls.
# (Kept under the old env var name for backward compatibility.)
SHARED_MAX_WORKERS = int(os.getenv("EXTRACTION_MAX_WORKERS", "3"))

_executor = None


def get_executor():
    """Return the single shared ThreadPoolExecutor, creating it lazily.

    Safe to call from multiple modules/threads — the pool is created once
    per process and reused for every subsequent call.
    """
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(
            max_workers=SHARED_MAX_WORKERS,
            thread_name_prefix="extractor-pool",
        )
    return _executor


def shutdown_executor(wait=True):
    """Shut down the shared pool. Call this once, at the very end of a run
    (e.g. from run_all_issue_extractions.py), not from individual extractors,
    since the pool is shared between them.
    """
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=wait)
        _executor = None


# Best-effort safety net: if a script using this module exits without
# explicitly shutting down the pool (e.g. run standalone), clean it up
# instead of leaving dangling threads.
atexit.register(shutdown_executor, wait=False)