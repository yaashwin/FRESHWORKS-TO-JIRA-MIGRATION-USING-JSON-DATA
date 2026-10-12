"""
jira_credentials.py

Shared multi-service-account credential pool for Jira Basic Auth
(email + API token), used by both bulk_load_to_jira.py and
upload_attachments.py so worker threads across BOTH scripts draw from
the exact same pool of service accounts.

HOW IT WORKS
------------
- Account #1 is the plain, unsuffixed JIRA_EMAIL / JIRA_API_TOKEN /
  JIRA_ACCOUNT_ID vars. This means your existing single-account .env
  file keeps working with ZERO changes.
- Additional accounts are picked up automatically as JIRA_EMAIL_2 /
  JIRA_API_TOKEN_2 / JIRA_ACCOUNT_ID_2, JIRA_EMAIL_3 / ..., and so on,
  for as many numbered accounts as you define, in order, with no gaps.
- Each worker THREAD is assigned exactly one account, round-robin, the
  FIRST time that thread makes a Jira call — and keeps using that same
  account for every subsequent call for the rest of its life. So with
  MAX_ISSUE_WORKERS = 3 and 3 service accounts defined, each of the 3
  worker threads ends up pinned to its own dedicated account for the
  whole run — no two threads ever share an account's rate limit.
- If you set MAX_ISSUE_WORKERS higher than the number of accounts you
  defined (e.g. 10 workers, 6 accounts), accounts are simply reused
  round-robin — some threads will share an account. That still works,
  it just means those threads share that account's Jira rate limit.

Add this file's directory to both scripts (they're expected to sit in
the same folder) and just `import jira_credentials`.
"""
import os
import threading

_pool_lock = threading.Lock()
_accounts = []          # list of dicts: {"email", "token", "account_id", "label"}
_next_index = 0
_thread_assignment = threading.local()


def _load_accounts():
    """Reads all JIRA_EMAIL[_n] / JIRA_API_TOKEN[_n] / JIRA_ACCOUNT_ID[_n] pairs
    from the environment. Called once, lazily, the first time an account is
    requested (so it always runs AFTER load_dotenv() in the calling script)."""
    global _accounts
    accounts = []

    # Account #1 — unsuffixed vars (backward compatible with single-account .env)
    email = os.getenv("JIRA_EMAIL")
    token = os.getenv("JIRA_API_TOKEN")
    if email and token:
        accounts.append({
            "email":      email,
            "token":      token,
            "account_id": os.getenv("JIRA_ACCOUNT_ID", ""),
            "label":      "JIRA_EMAIL",
        })

    # Accounts #2, #3, #4, ... — JIRA_EMAIL_2 / JIRA_API_TOKEN_2, etc.
    # Stops at the first gap, so numbering must be contiguous starting at 2.
    i = 2
    while True:
        e = os.getenv(f"JIRA_EMAIL_{i}")
        t = os.getenv(f"JIRA_API_TOKEN_{i}")
        if not e or not t:
            break
        accounts.append({
            "email":      e,
            "token":      t,
            "account_id": os.getenv(f"JIRA_ACCOUNT_ID_{i}", ""),
            "label":      f"JIRA_EMAIL_{i}",
        })
        i += 1

    if not accounts:
        raise RuntimeError(
            "No Jira credentials found. Set JIRA_EMAIL / JIRA_API_TOKEN "
            "(account #1), and optionally JIRA_EMAIL_2 / JIRA_API_TOKEN_2, "
            "JIRA_EMAIL_3 / JIRA_API_TOKEN_3, ... for more service accounts, "
            "in your .env file. See .env.sample."
        )

    _accounts = accounts
    return accounts


def get_account_pool():
    """Returns the full list of loaded account dicts (loads once, on first call)."""
    global _accounts
    with _pool_lock:
        if not _accounts:
            _load_accounts()
        return list(_accounts)


def pool_size():
    return len(get_account_pool())


def get_assigned_account_for_this_thread():
    """Returns the full account dict {'email','token','account_id','label'}
    assigned to the CURRENT thread, assigning one (round-robin) on first use."""
    global _next_index

    assigned = getattr(_thread_assignment, "account", None)
    if assigned is not None:
        return assigned

    accounts = get_account_pool()
    with _pool_lock:
        idx = _next_index % len(accounts)
        _next_index += 1
    account = accounts[idx]
    _thread_assignment.account = account
    return account


def get_auth_for_this_thread():
    """Returns (email, token) tuple for the current thread's assigned account.
    Drop-in replacement for the old fixed (JIRA_EMAIL, JIRA_API_TOKEN) tuple."""
    account = get_assigned_account_for_this_thread()
    return account["email"], account["token"]


def get_account_id_for_this_thread():
    """Returns the JIRA_ACCOUNT_ID (if set) for whichever account this thread was assigned."""
    account = get_assigned_account_for_this_thread()
    return account.get("account_id", "")


def print_pool_summary():
    """Prints a masked summary of every loaded account — safe to log, never
    prints full tokens."""
    accounts = get_account_pool()
    print(f"  Loaded {len(accounts)} Jira service account(s):")
    for i, acc in enumerate(accounts, 1):
        token = acc["token"] or ""
        masked = (token[:4] + "…" + token[-4:]) if len(token) > 10 else "****"
        print(f"    [{i}] {acc['label']:14s} {acc['email']}   token={masked}")
