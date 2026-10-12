"""
shared_http.py

One requests.Session (and one HTTP connection pool) shared by every
extractor that runs inside this process.

Why a singleton getter instead of just exporting create_shared_session():
run_all_issue_extractions.py imports each extract_*.py module exactly once
per process (Python caches imports), and each of those modules builds its
ApiTracker at import time - not inside main(). If every extractor called
create_shared_session() itself, each import would still create its own
session, and we'd be right back to 6 separate connection pools. Routing
everyone through get_shared_session() guarantees they all get the exact
same Session object, no matter which module imports it first.
"""

import os
import requests
from requests.adapters import HTTPAdapter
from threading import Lock

_session = None
_session_lock = Lock()

# Keep the pool comfortably above the shared thread pool's worker count
# (shared_thread_pool.py reads the same env var) so worker threads never
# have to queue for a free connection. Falls back to 20 if unset/invalid.
try:
    _DEFAULT_POOL_SIZE = max(20, int(os.getenv("EXTRACTION_MAX_WORKERS", "3")) * 4)
except ValueError:
    _DEFAULT_POOL_SIZE = 20


def create_shared_session(pool_size: int = 20) -> requests.Session:
    """Build a new requests.Session with a single connection pool sized
    to comfortably cover every worker thread that will use it."""
    session = requests.Session()

    adapter = HTTPAdapter(
        pool_connections=1,     # one host -> one pool
        pool_maxsize=pool_size, # big enough for all worker threads combined
        pool_block=True,        # block (don't discard) if pool is briefly exhausted
        max_retries=0,          # retries are handled manually by ApiTracker
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def get_shared_session(pool_size: int = None) -> requests.Session:
    """Return the one process-wide shared session, creating it on first call."""
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:  # re-check inside the lock
                _session = create_shared_session(
                    pool_size=pool_size if pool_size is not None else _DEFAULT_POOL_SIZE
                )
    return _session