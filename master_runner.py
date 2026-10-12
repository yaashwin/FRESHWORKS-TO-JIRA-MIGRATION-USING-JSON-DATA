import subprocess
import os
import time
import argparse
from datetime import datetime
import sys
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

load_dotenv()
PROJECT_KEY = os.getenv("project_key")
# =============================================================
# CONFIGURATION
# =============================================================

PHASES = [
    {
        "phase": 1,
        "name": "EXTRACTION",
        "script": "test_refactored_shared_pool/rawdata_from_zip.py",
        "description": "Convert the client's zip export (issues, comments, links, docs, sprints, releases) into raw/ structure",
        "critical": True,
    },
    {
        "phase": 2,
        "name": "FLATTEN",
        "script": "flatten.py",
        "description": "Flatten raw JSON files into structured CSVs",
        "critical": True,
    },
    {
        "phase": 3,
        "name": "TRANSFORM",
        "script": "transform.py",
        "description": "Apply field mappings (issue types, statuses, priorities, users, custom fields)",
        "critical": True,
    },
    {
        "phase": 4,
        "name": "PREPARE PAYLOAD",
        "script": "prepare_payload.py",
        "description": "Build Jira REST API JSON payloads from transformed CSVs",
        "critical": True,
    },
    {
        "phase": 5,
        "name": "LOAD TO JIRA",
        "script": "bulk_load_to_jira.py",  # ← UPDATED
        "description": "Bulk upload to Jira (versions, sprints, issues, comments, links, dependencies, attachments)",
        "critical": True,
    },
]

# LOG_DIR = "logs"
# os.makedirs(LOG_DIR, exist_ok=True)
LOG_DIR = f"{PROJECT_KEY}_logs" if PROJECT_KEY else "logs"
os.makedirs(LOG_DIR, exist_ok=True)

class Tee:
    """Writes every print()/log() call to both the terminal and a file,
    so nothing master_runner.py prints is ever terminal-only again."""
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self):
        for s in self.streams:
            s.flush()

# =============================================================
# HELPERS
# =============================================================

def log(message, level="INFO"):
    """Log with timestamp and level."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {message}")


def separator(title=""):
    """Print visual separator."""
    line = "=" * 70
    if title:
        print(f"\n{line}")
        print(f"  {title}")
        print(f"{line}")
    else:
        print(line)


def run_phase(phase_info):
    """
    Run a single pipeline phase as a subprocess.
    
    Supports:
    - Same-folder scripts
    - Subfolder scripts (e.g., seperate/pipeline_orchestrator.py)
    - Absolute paths
    - Proper logging + cwd handling
    
    Returns:
        (success: bool, duration_seconds: float)
    """
    phase_num   = phase_info["phase"]
    phase_name  = phase_info["name"]
    script      = phase_info["script"]
    description = phase_info["description"]

    separator(f"PHASE {phase_num}: {phase_name}")
    log(f"Script      : {script}")
    log(f"Description : {description}")
    log(f"Started at  : {datetime.now().strftime('%H:%M:%S')}")
    print()

    # =========================================================
    # BASE DIR (master_runner.py location)
    # =========================================================
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # =========================================================
    # Resolve script path
    # =========================================================
    if os.path.isabs(script):
        script_path = script
    else:
        script_path = os.path.join(BASE_DIR, script)

    # Normalize path
    script_path = os.path.abspath(script_path)

    # =========================================================
    # Check script exists
    # =========================================================
    if not os.path.exists(script_path):
        log(f"Script not found: {script_path}", level="ERROR")
        return False, 0.0 ,None

    # =========================================================
    # Log file per phase
    # =========================================================
    log_file_path = os.path.join(
        LOG_DIR,
        f"phase_{phase_num}_{phase_name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    start_time = time.time()

    try:
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,

            # IMPORTANT: Set cwd to script's directory
            # Ensures relative paths inside scripts work correctly
            cwd=os.path.dirname(script_path),

            env={**os.environ, "PYTHONUNBUFFERED": "1"}
        )

        output_lines = []

        # Stream output live + save to log file
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            for line in process.stdout:
                line_clean = line.rstrip()

                print(line_clean)         # Live console output
                log_file.write(line)      # Save to file
                output_lines.append(line_clean)

        return_code = process.wait()
        duration = time.time() - start_time

        print()

        if return_code == 0:
            log(f"Phase {phase_num} COMPLETED in {duration:.1f}s")
            log(f"Log saved: {log_file_path}")
            return True, duration , log_file_path
        else:
            log(f"Phase {phase_num} FAILED (exit code {return_code})", level="ERROR")
            log(f"Log saved: {log_file_path}", level="ERROR")
            return False, duration, log_file_path

    except Exception as e:
        duration = time.time() - start_time
        log(f"Phase {phase_num} ERROR: {str(e)}", level="ERROR")
        return False, duration, log_file_path


# =============================================================
# MAIN PIPELINE RUNNER
# =============================================================

def run_pipeline(from_phase=1, only_phases=None):
    """
    Run all phases sequentially.

    Args:
        from_phase  : Start from this phase number (default: 1)
        only_phases : List of specific phase numbers to run (overrides from_phase)
    """
    pipeline_start = time.time()
    run_start_time = datetime.now().isoformat()

    separator("MASTER PIPELINE RUNNER")
    log(f"Started    : {run_start_time}")
    log(f"From phase : {from_phase}")
    if only_phases:
        log(f"Only phases: {only_phases}")
    print()

    # Determine which phases to run
    if only_phases:
        phases_to_run = [p for p in PHASES if p["phase"] in only_phases]
    else:
        phases_to_run = [p for p in PHASES if p["phase"] >= from_phase]

    if not phases_to_run:
        log("No phases selected to run. Exiting.", level="ERROR")
        return

    log(f"Phases to run: {[p['phase'] for p in phases_to_run]}")
    print()

    # Track results
    results = []

    for phase_info in phases_to_run:
        success, duration , log_file_path = run_phase(phase_info)

        results.append({
            "phase": phase_info["phase"],
            "name": phase_info["name"],
            "script": phase_info["script"],
            "success": success,
            "duration": duration,
            "log_file_path": log_file_path,
        })

        if not success and phase_info["critical"]:
            log(
                f"\nPhase {phase_info['phase']} ({phase_info['name']}) failed and is marked CRITICAL.",
                level="ERROR"
            )
            log("Stopping pipeline. Fix the issue and re-run from this phase:", level="ERROR")
            log(f"  python master_runner.py --from-phase {phase_info['phase']}", level="ERROR")
            break

        if success:
            log("Moving to next phase...\n")
            time.sleep(1)  # Small pause between phases

            # Run timing report only after successful Phase 1
            if phase_info["phase"] == 1:
                log("Running combined API timing report...")

                base_dir = os.path.dirname(os.path.abspath(__file__))
                # combiner_script = os.path.join(base_dir, "combine_api_timing.py")
                combiner_script = os.path.join(base_dir, "test_refactored_shared_pool", "combine_api_timing.py")
                logs_dir = os.path.join(base_dir, "test_refactored_shared_pool", f"{PROJECT_KEY}_data" if PROJECT_KEY else "data", "raw", "logs")

                subprocess.run(
                    [sys.executable, combiner_script, logs_dir],
                    cwd=base_dir
                )

    # =============================================================
    # FINAL SUMMARY
    # =============================================================
    total_duration = time.time() - pipeline_start
    run_end_time = datetime.now().isoformat()

    separator("PIPELINE SUMMARY")
    print()

    all_success = all(r["success"] for r in results)

    for r in results:
        status_icon = " OK  " if r["success"] else " FAIL"
        mins = int(r["duration"] // 60)
        secs = int(r["duration"] % 60)
        print(f"  [{status_icon}] Phase {r['phase']}: {r['name']:<20}  {mins}m {secs}s")

    print()

    total_mins = int(total_duration // 60)
    total_secs = int(total_duration % 60)

    log(f"Started    : {run_start_time}")
    log(f"Finished   : {run_end_time}")
    log(f"Total time : {total_mins}m {total_secs}s")

    if all_success:
        log("ALL PHASES COMPLETED SUCCESSFULLY!")
        log("Your data has been fully migrated to Jira.")
    else:
        failed = [r for r in results if not r["success"]]
        log(f"{len(failed)} phase(s) failed.", level="ERROR")
        for f in failed:
            log(f"  - Phase {f['phase']}: {f['name']} ({f['script']})", level="ERROR")
        log(f"Check the {LOG_DIR}/ directory for detailed output.", level="ERROR")

    separator()
    print()
    
    phase5_result = next((r for r in results if r["phase"] == 5), None)
    if phase5_result and phase5_result.get("log_file_path"):
        try:
            with open(phase5_result["log_file_path"], "a", encoding="utf-8") as f:
                f.write("\n" + "=" * 70 + "\n")
                f.write("  PIPELINE SUMMARY — TOTAL TIMING PER PHASE\n")
                f.write("=" * 70 + "\n")
                for r in results:
                    status_icon = " OK  " if r["success"] else " FAIL"
                    mins = int(r["duration"] // 60)
                    secs = int(r["duration"] % 60)
                    f.write(f"  [{status_icon}] Phase {r['phase']}: {r['name']:<20}  {mins}m {secs}s\n")
                f.write("\n")
                f.write(f"  Started    : {run_start_time}\n")
                f.write(f"  Finished   : {run_end_time}\n")
                f.write(f"  Total time : {total_mins}m {total_secs}s\n")
                f.write("=" * 70 + "\n")
        except Exception as e:
            log(f"Could not append phase timing to phase 5 log: {e}", level="ERROR")


# =============================================================
# CLI ARGUMENT PARSING
# =============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Master Pipeline Runner — runs all migration phases sequentially",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python master_runner.py

  # Resume from phase 3
  python master_runner.py --from-phase 3

  # Run only specific phases
  python master_runner.py --phases 2 3 4

  # List all phases
  python master_runner.py --list
        """
    )
    parser.add_argument(
        "--from-phase",
        type=int,
        default=1,
        choices=[1, 2, 3, 4, 5],
        help="Start pipeline from this phase number (default: 1)"
    )
    parser.add_argument(
        "--phases",
        type=int,
        nargs="+",
        choices=[1, 2, 3, 4, 5],
        help="Run only specific phases, e.g. --phases 2 3"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all phases and exit"
    )
    return parser.parse_args()


def list_phases():
    """Display all available phases."""
    separator("AVAILABLE PHASES")
    for p in PHASES:
        critical_tag = "[CRITICAL]" if p["critical"] else ""
        print(f"\n  Phase {p['phase']}: {p['name']} {critical_tag}")
        print(f"    Script : {p['script']}")
        print(f"    Info   : {p['description']}")
    print()


# =============================================================
# ENTRY POINT
# =============================================================

if __name__ == "__main__":
    args = parse_args()

    if args.list:
        list_phases()
        sys.exit(0)

    master_log_path = os.path.join(
        LOG_DIR, f"master_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    log_fh = open(master_log_path, "a", encoding="utf-8")
    original_stdout, original_stderr = sys.stdout, sys.stderr
    sys.stdout = Tee(original_stdout, log_fh)
    sys.stderr = Tee(original_stderr, log_fh)

    print(f"[LOG] Full master run log (start/end time, total duration): {master_log_path}\n")

    try:
        run_pipeline(
            from_phase=args.from_phase,
            only_phases=args.phases,
        )
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Pipeline stopped by user.")
        print("To resume from where you left off, run:")
        print("  python master_runner.py --from-phase <phase_number>")
        sys.exit(1)
    finally:
        sys.stdout, sys.stderr = original_stdout, original_stderr
        log_fh.close()