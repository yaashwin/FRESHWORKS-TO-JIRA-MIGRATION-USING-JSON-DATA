"""
============================================================
run_logger.py
============================================================
Reusable start/end + pass/fail run logger for any script in
this pipeline.

Captures everything printed to the terminal (stdout + stderr)
and writes an identical copy to a timestamped log file, while
still showing it live in the terminal — the same idea as
master_runner.py's per-phase logging, but self-contained so a
script can produce its own log even when run completely
standalone (no master_runner.py involved at all).

Usage
-----
    from run_logger import RunLogger

    if __name__ == "__main__":
        with RunLogger("bulk_load_to_jira") as run_log:
            exit_code = main()
            run_log.set_exit_code(exit_code)

What you get in the log file (and terminal, identically):
    - Started at  : <timestamp>
    - Log file    : <path>
    - ... all normal script output ...
    - Finished at : <timestamp>
    - Total time  : Xm Ys
    - STATUS      : COMPLETED SUCCESSFULLY
                    or FAILED (exit code N)
                    or FAILED (uncaught exception) + full traceback

Log location
------------
    {PROJECT_KEY}_logs/{script_name}_{YYYYMMDD_HHMMSS}.log
    (falls back to logs/ if project_key isn't set)
============================================================
"""

import os
import sys
import time
import traceback
from datetime import datetime


class Tee:
    """
    Writes every write() call to multiple streams at once (e.g. the
    real terminal AND a log file), so nothing printed is ever
    terminal-only.
    """
    def __init__(self, *streams):
        self.streams = streams
 
    def write(self, data):
        for s in self.streams:
            s.write(data)
            s.flush()
 
    def flush(self):
        for s in self.streams:
            s.flush()
 
    def reconfigure(self, *args, **kwargs):
        # Several modules in this pipeline (prepare_payload.py, this
        # file itself, etc.) call sys.stdout.reconfigure(encoding="utf-8")
        # at import time. Once RunLogger has swapped sys.stdout/stderr
        # for a Tee, that call would otherwise raise AttributeError and
        # silently abort whatever import triggered it. Forward the call
        # to every real underlying stream that supports it instead.
        for s in self.streams:
            if hasattr(s, "reconfigure"):
                try:
                    s.reconfigure(*args, **kwargs)
                except Exception:
                    pass
 
    def isatty(self):
        # Some libraries probe this (progress bars, colorama, etc.)
        return any(getattr(s, "isatty", lambda: False)() for s in self.streams)
 
    def fileno(self):
        # subprocess and some loggers ask for a real file descriptor.
        for s in self.streams:
            if hasattr(s, "fileno"):
                return s.fileno()
        raise OSError("No underlying stream has a fileno")

class RunLogger:
    """
    Context manager that Tees stdout/stderr to a log file for the
    duration of the `with` block, and prints/logs a clear
    start/end/duration/pass-fail summary.

    Success/failure is determined by, in order:
      1. An uncaught exception propagating out of the `with` block
         -> always FAILED, full traceback captured in the log.
      2. The exit code you report via set_exit_code(code)
         -> non-zero = FAILED, zero = COMPLETED SUCCESSFULLY.
      3. If set_exit_code() is never called and nothing raised
         -> COMPLETED SUCCESSFULLY (assumed).
    """

    def __init__(self, script_name: str, project_key: str = None, log_dir: str = None):
        self.script_name = script_name
        self.project_key = project_key if project_key is not None else os.getenv("project_key")
        self.log_dir = log_dir or (f"{self.project_key}_logs" if self.project_key else "logs")

        self.exit_code = 0
        self.log_file_path = None

        self._orig_stdout = None
        self._orig_stderr = None
        self._log_file = None
        self._start_time = None
        self._start_dt = None

    def set_exit_code(self, code: int):
        """Call this after your entry point returns, to record its
        actual pass/fail result (e.g. main()'s return value)."""
        self.exit_code = code or 0

    def __enter__(self):
        os.makedirs(self.log_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file_path = os.path.join(
            self.log_dir, f"{self.script_name}_{timestamp}.log"
        )

        # utf-8 so ✔ / ✖ / ─ style characters used throughout the
        # pipeline's print statements don't break on write.
        self._log_file = open(self.log_file_path, "w", encoding="utf-8")

        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr
        sys.stdout = Tee(self._orig_stdout, self._log_file)
        sys.stderr = Tee(self._orig_stderr, self._log_file)

        self._start_time = time.time()
        self._start_dt = datetime.now()

        print("=" * 70)
        print(f"  RUN LOG: {self.script_name}")
        print(f"  Started at : {self._start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Log file   : {self.log_file_path}")
        print("=" * 70)
        print()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self._start_time
        mins, secs = int(duration // 60), int(duration % 60)
        end_dt = datetime.now()

        print()
        print("=" * 70)
        print(f"  Finished at : {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Total time  : {mins}m {secs}s")

        if exc_type is SystemExit:
            # exit(code) / sys.exit(code) raises SystemExit — this is
            # the NORMAL way scripts signal their exit code, not an
            # error in itself. Only a non-zero/non-None code means
            # failure; exit(0) or bare exit() must still be SUCCESS.
            code = exc_val.code if exc_val is not None else 0
            if code in (None, 0):
                print(f"  STATUS      : COMPLETED SUCCESSFULLY")
            else:
                print(f"  STATUS      : FAILED (exit code {code})")
            print("=" * 70)

        elif exc_type is not None:
            # Any other uncaught exception. Print the full traceback
            # NOW, while stdout/stderr are still Teed to the log file —
            # if we restored the streams first, the traceback would
            # only reach the terminal and never get saved.
            print(f"  STATUS      : FAILED (uncaught exception)")
            print("=" * 70)
            print()
            traceback.print_exception(exc_type, exc_val, exc_tb)

        elif self.exit_code and self.exit_code != 0:
            print(f"  STATUS      : FAILED (exit code {self.exit_code})")
            print("=" * 70)

        else:
            print(f"  STATUS      : COMPLETED SUCCESSFULLY")
            print("=" * 70)

        print(f"\nLog saved to: {self.log_file_path}")

        # Restore the real stdout/stderr and close the file.
        sys.stdout = self._orig_stdout
        sys.stderr = self._orig_stderr
        self._log_file.close()

        # Returning False re-raises any exception that occurred —
        # nothing is swallowed, so the script's own exit code /
        # any external wrapper (cron, systemd, master_runner.py)
        # still sees the failure normally.
        return False