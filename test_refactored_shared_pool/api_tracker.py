import time
import requests
from datetime import datetime, timedelta
from threading import Lock
from requests.adapters import HTTPAdapter


class ApiTracker:
    def __init__(self, base_url, headers, rate_limit_delay=0.2,
                 stats=None, stats_lock=None, pool_maxsize=13, session=None):
        self.base_url = base_url
        self.headers = headers
        self.rate_limit_delay = rate_limit_delay

        if session is not None:
            # Shared session (e.g. from shared_http.get_shared_session()) -
            # reuse the caller's connection pool instead of building a new
            # one. Every ApiTracker that's handed the same session object
            # ends up sharing that one pool of TCP/TLS connections.
            self.session = session
            self.session.headers.update(headers)
        else:
            # No shared session was supplied: fall back to the original
            # behaviour - one Session per tracker, own connection pool.
            self.session = requests.Session()
            self.session.headers.update(headers)

            adapter = HTTPAdapter(
                pool_connections=1,         # one host → one connection pool
                pool_maxsize=pool_maxsize,  # supports all worker threads
                max_retries=0               # retries handled manually
            )
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)

        # Reuse the script's own stats dict so existing fields
        # (total_api_calls, failed_api_calls, errors, ...) keep working.
        self.stats = stats if stats is not None else {
            "total_api_calls": 0,
            "failed_api_calls": 0,
            "errors": []
        }
        self.stats_lock = stats_lock if stats_lock is not None else Lock()

        self._log_lock = Lock()
        self.call_timestamps = []        # one entry per attempted HTTP call
        self.rate_limit_events = []      # every 429/5xx occurrence
        self.permanent_failures = []     # endpoints that failed on both attempts

    # ---------------------------------------------------------------
    def _record_call(self):
        with self._log_lock:
            self.call_timestamps.append(datetime.now())
        with self.stats_lock:
            self.stats["total_api_calls"] += 1
            return self.stats["total_api_calls"]

    def _record_retryable(self, endpoint, call_number, request_id, status_code):
        with self._log_lock:
            self.rate_limit_events.append({
                "call_number": call_number,
                "endpoint": endpoint,
                "x-request-id": request_id,
                "status_code": status_code,
                "time": datetime.now().isoformat()
            })

    def _record_failure(self, endpoint, call_number, error, request_id=None,
                         attempt=None, permanent=False):
        with self.stats_lock:
            self.stats["failed_api_calls"] += 1
            self.stats["errors"].append({
                "endpoint": endpoint,
                "error": str(error),
                "call_number": call_number,
                "attempt": attempt,
                "x-request-id": request_id or "N/A"
            })
        if permanent:
            with self._log_lock:
                self.permanent_failures.append({
                    "endpoint": endpoint,
                    "call_number": call_number,
                    "error": str(error),
                    "x-request-id": request_id or "N/A",
                    "time": datetime.now().isoformat()
                })

    @staticmethod
    def _is_retryable(status_code):
        """
        429 (rate limited) and any 5xx server error (500, 502, 503, 504, ...)
        get the single 60s-wait-then-retry treatment. Other 4xx client
        errors (400, 401, 403, 404, ...) are not retried - they won't
        succeed on retry.
        """
        return status_code == 429 or status_code >= 500

    @staticmethod
    def _print_failed_block(resp, call_number, attempt, total_attempts):
        request_id = resp.headers.get('x-request-id', 'NOT FOUND')
        print("\n" + "=" * 80)
        print(f"[FAILED API]   {resp.request.method} {resp.url}")
        print(f"[STATUS]       {resp.status_code}")
        print(f"[X-REQUEST-ID] {request_id}")
        print(f"[API CALL #]   {call_number}")
        print(f"[ATTEMPT]      {attempt}/{total_attempts}")
        print("\n[RESPONSE HEADERS]")
        for k, v in resp.headers.items():
            print(f"  {k}: {v}")
        print("=" * 80 + "\n")
        return request_id

    # ---------------------------------------------------------------
    def get(self, endpoint, params=None):
        """GET with single-retry-on-429/5xx policy. Returns parsed JSON or None."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        for attempt in (1, 2):  # attempt 1 = first try, attempt 2 = the one 60s retry
            call_number = self._record_call()
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                time.sleep(self.rate_limit_delay)
                return response.json()

            except requests.exceptions.RequestException as e:
                resp = getattr(e, "response", None)

                if resp is not None:
                    request_id = self._print_failed_block(resp, call_number, attempt, 2)
                    status_code = resp.status_code

                    if self._is_retryable(status_code):
                        self._record_retryable(endpoint, call_number, request_id, status_code)
                        if attempt == 1:
                            print(f"[RETRYABLE {status_code}] on call #{call_number} for '{endpoint}'. "
                                  f"Waiting 60s, then a single retry...")
                            time.sleep(60)
                            continue
                        print(f"[RETRYABLE {status_code}] again on retry (call #{call_number}) "
                              f"for '{endpoint}'. Giving up, moving on.")
                        self._record_failure(endpoint, call_number,
                                              f"{status_code} after single retry",
                                              request_id, attempt=attempt, permanent=True)
                        return None

                    # Non-retryable HTTP error (e.g. 400/401/403/404): fail immediately
                    self._record_failure(endpoint, call_number, str(e), request_id,
                                          attempt=attempt, permanent=True)
                    return None

                else:
                    # Connection / DNS error: fail immediately, no retry cycles
                    print(f"[FAILED API] GET {url}  (call #{call_number}) - {e}")
                    self._record_failure(endpoint, call_number, str(e),
                                          attempt=attempt, permanent=True)
                    return None

        return None

    # ---------------------------------------------------------------
    def get_with_retry_download(self, fetch_fn, endpoint, call_number_label="DOWNLOAD"):
        """
        Generic wrapper for non-JSON (file download) calls that should follow
        the same 429/5xx policy. `fetch_fn` is a zero-arg callable performing the
        actual request and returning a `requests.Response` (raise_for_status
        already applied by caller) or raising RequestException.
        """
        for attempt in (1, 2):
            call_number = self._record_call()
            try:
                return fetch_fn(), call_number
            except requests.exceptions.RequestException as e:
                resp = getattr(e, "response", None)
                if resp is not None:
                    request_id = self._print_failed_block(resp, call_number, attempt, 2)
                    status_code = resp.status_code

                    if self._is_retryable(status_code):
                        self._record_retryable(endpoint, call_number, request_id, status_code)
                        if attempt == 1:
                            print(f"[RETRYABLE {status_code}] on call #{call_number} for '{endpoint}'. "
                                  f"Waiting 60s, then a single retry...")
                            time.sleep(60)
                            continue
                        print(f"[RETRYABLE {status_code}] again on retry (call #{call_number}) "
                              f"for '{endpoint}'. Giving up, moving on.")
                        self._record_failure(endpoint, call_number,
                                              f"{status_code} after single retry",
                                              request_id, attempt=attempt, permanent=True)
                        return None, call_number
                    self._record_failure(endpoint, call_number, str(e), request_id,
                                          attempt=attempt, permanent=True)
                    return None, call_number
                else:
                    print(f"[FAILED API] {call_number_label} {endpoint} (call #{call_number}) - {e}")
                    self._record_failure(endpoint, call_number, str(e),
                                          attempt=attempt, permanent=True)
                    return None, call_number
        return None, None

    # ---------------------------------------------------------------
    # FIXED wall-clock per-minute totals: HH:MM:00 - HH:MM:59
    # (This is what answers "total calls every minute".)
    # ---------------------------------------------------------------
    def per_minute_summary(self):
        """
        Bucket every recorded call into FIXED wall-clock 1-minute windows
        (e.g. 07:38:00-07:38:59, 07:39:00-07:39:59, ...), regardless of
        when the first call happened. This makes buckets comparable to
        the API provider's own per-minute rate-limit window.
        """
        with self._log_lock:
            timestamps = sorted(self.call_timestamps)
            events = list(self.rate_limit_events)

        if not timestamps:
            return []

        def minute_key(ts):
            return ts.replace(second=0, microsecond=0)

        # Build a dict keyed by the start of each wall-clock minute,
        # filling in any empty minutes in between so the report has
        # no gaps from start to end.
        first_minute = minute_key(timestamps[0])
        last_minute = minute_key(timestamps[-1])

        counts = {}
        cur = first_minute
        while cur <= last_minute:
            counts[cur] = 0
            cur += timedelta(minutes=1)

        for ts in timestamps:
            counts[minute_key(ts)] += 1

        event_counts = {}
        for ev in events:
            k = minute_key(datetime.fromisoformat(ev["time"]))
            event_counts[k] = event_counts.get(k, 0) + 1

        buckets = []
        for start in sorted(counts.keys()):
            end = start + timedelta(seconds=59)
            buckets.append({
                "from": start.strftime("%H:%M:%S"),
                "to": end.strftime("%H:%M:%S"),
                "api_calls": counts[start],
                "retryable_error_count": event_counts.get(start, 0)
            })
        return buckets

    def peak_minute(self):
        """Return the single busiest fixed wall-clock minute."""
        buckets = self.per_minute_summary()
        if not buckets:
            return None
        return max(buckets, key=lambda b: b["api_calls"])

    # ---------------------------------------------------------------
    # Permanent ("2 times api failure") count
    # ---------------------------------------------------------------
    def permanent_failure_count(self):
        """
        Count of endpoints that gave up entirely - i.e. failed on
        attempt 1 AND attempt 2 (429/5xx-twice), OR hit a non-retryable
        error and were abandoned. This is the "2 times api failure"
        number you asked for.
        """
        with self._log_lock:
            return len(self.permanent_failures)

    def permanent_failure_details(self):
        with self._log_lock:
            return list(self.permanent_failures)

    # ---------------------------------------------------------------
    def print_summary(self):
        with self._log_lock:
            timestamps = sorted(self.call_timestamps)

        print("\n" + "=" * 60)
        print("  API CALL TIMING SUMMARY")
        print("=" * 60)
        print(f"  Total API calls       : {self.stats['total_api_calls']}")
        print(f"  429/5xx occurrences   : {len(self.rate_limit_events)}")
        print(f"  Permanently failed    : {self.permanent_failure_count()}  "
              f"(failed on both attempt 1 and attempt 2 / non-retryable)")
        breakdown = self.status_code_breakdown()
        if breakdown:
            print("\n  Status code breakdown:")
            for code in sorted(breakdown, key=lambda c: (isinstance(c, str), c)):
                tag = "RATE LIMIT" if code == 429 else "SERVER ERROR" if isinstance(code, int) and code >= 500 else "OTHER"
                print(f"    {code} ({tag}): {breakdown[code]} occurrence(s)")

        if timestamps:
            print(f"  Run started           : {timestamps[0].strftime('%H:%M:%S')}")
            print(f"  Run ended             : {timestamps[-1].strftime('%H:%M:%S')}")

        if self.rate_limit_events:
            print("\n  429/5xx events:")
            for ev in self.rate_limit_events:
                print(f"    - call #{ev['call_number']}  status={ev.get('status_code', '?')}  "
                      f"endpoint='{ev['endpoint']}'  x-request-id={ev['x-request-id']}  at {ev['time']}")

        if self.permanent_failures:
            print("\n  Permanently failed endpoints (gave up after 2 attempts):")
            for pf in self.permanent_failures:
                print(f"    - call #{pf['call_number']}  endpoint='{pf['endpoint']}'  "
                      f"error={pf['error']}  x-request-id={pf['x-request-id']}  at {pf['time']}")

        print("\n  Per-minute breakdown (fixed wall-clock minutes):")
        for b in self.per_minute_summary():
            flag = f"  <-- {b['retryable_error_count']} x 429/5xx" if b["retryable_error_count"] else ""
            print(f"    {b['from']} - {b['to']} : {b['api_calls']} calls{flag}")

        peak = self.peak_minute()
        if peak:
            print(f"\n  Peak minute: {peak['from']} - {peak['to']} -> {peak['api_calls']} calls "
                  f"({peak['retryable_error_count']} were 429/5xx)")

        print("=" * 60 + "\n")

    def summary_dict(self):
        timestamps = sorted(self.call_timestamps)
        return {
            "total_api_calls": self.stats["total_api_calls"],
            "permanent_failure_count": self.permanent_failure_count(),
            "permanent_failures": self.permanent_failure_details(),
            "retryable_error_events": self.rate_limit_events,  # 429s and 5xx
            "per_minute_breakdown": self.per_minute_summary(),
            "peak_minute": self.peak_minute(),
            "run_started": timestamps[0].isoformat() if timestamps else None,
            "run_ended": timestamps[-1].isoformat() if timestamps else None,
            "status_code_breakdown": self.status_code_breakdown(),
        }

    def save_timing_log(self, path, source_name=None):
        """
        Save the FULL raw timing data (every call timestamp + every
        429/5xx event + every permanent failure) to a JSON file. This is
        what lets multiple scripts' ApiTracker data be merged later into
        one combined cross-script report, since summary_dict() alone
        loses the raw per-call timestamps needed to re-bucket across
        scripts.

        Call this once at the end of each extraction script, e.g.:
            tracker.save_timing_log("data/raw/logs/comments_timing.json",
                                     source_name="comments")
        """
        import json
        import os

        with self._log_lock:
            timestamps = sorted(self.call_timestamps)

        payload = {
            "source": source_name or "unknown",
            "total_api_calls": self.stats["total_api_calls"],
            "call_timestamps": [ts.isoformat() for ts in timestamps],
            "retryable_error_events": self.rate_limit_events,
            "permanent_failures": self.permanent_failure_details(),
            "status_code_breakdown": self.status_code_breakdown(),
        }

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        return path
    def record_attempt(self):
        """
        Independent attempt counter used ONLY for labeling/reporting
        (call_number in printed blocks, timing buckets). Does NOT touch
        stats["total_api_calls"] — that counter's meaning stays exactly
        as it was before (one increment per api_get() invocation).
        """
        with self._log_lock:
            self.call_timestamps.append(datetime.now())
            self._attempt_counter = getattr(self, "_attempt_counter", 0) + 1
            return self._attempt_counter

    def status_code_breakdown(self):
        """Count of occurrences per status code, e.g. {429: 12, 502: 3}."""
        breakdown = {}
        with self._log_lock:
            for ev in self.rate_limit_events:
                code = ev.get("status_code", "unknown")
                breakdown[code] = breakdown.get(code, 0) + 1
        return breakdown