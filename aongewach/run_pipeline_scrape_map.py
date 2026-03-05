#!/usr/bin/env python3
"""
Run Pipeline (Scrape + Compose + Channel Mapper only)

Flow:
  1. Scrape FANZO + WITM + Flashscore sources in parallel
  2. Merge/compose final weekly schedule
  3. Sync all schedule channels into channels.json with stable IDs
  4. Map schedule channels to existing channel IDs
"""

import argparse
import datetime as dt
import subprocess
import sys
import time


def run_step(script_name, description, extra_args=None, fail_on_error=True):
    print(f"\n{'=' * 60}")
    print(f"STEP: {description}")
    print(f"Running {script_name}...")
    print(f"{'=' * 60}\n")

    cmd = [sys.executable, "-u", script_name]
    if extra_args:
        cmd.extend(extra_args)

    try:
        result = subprocess.run(cmd, check=True)
        if result.returncode == 0:
            print(f"\n[SUCCESS] {script_name} completed.")
            return True
    except subprocess.CalledProcessError as exc:
        print(f"\n[ERROR] {script_name} failed with return code {exc.returncode}.")
        if fail_on_error:
            sys.exit(1)
        return False
    except Exception as exc:
        print(f"\n[ERROR] Failed to run {script_name}: {exc}")
        if fail_on_error:
            sys.exit(1)
        return False

    return True


def run_parallel_steps(steps, description, fail_on_error=True):
    """
    Run independent pipeline steps in parallel.
    Each step tuple: (script_name, step_description, extra_args_list_or_none).
    """
    print(f"\n{'=' * 60}")
    print(f"STEP: {description}")
    print(f"{'=' * 60}\n")

    processes = []
    for script_name, step_description, extra_args in steps:
        cmd = [sys.executable, "-u", script_name]
        if extra_args:
            cmd.extend(extra_args)
        print(f"[START] {step_description} ({script_name})")
        try:
            process = subprocess.Popen(cmd)
        except Exception as exc:
            print(f"\n[ERROR] Failed to start {script_name}: {exc}")
            for _, _, running_process in processes:
                if running_process.poll() is None:
                    running_process.terminate()
            if fail_on_error:
                sys.exit(1)
            return False
        processes.append((script_name, step_description, process))

    failures = []
    running = processes.copy()
    while running:
        for entry in list(running):
            script_name, _, process = entry
            return_code = process.poll()
            if return_code is None:
                continue

            running.remove(entry)
            if return_code == 0:
                print(f"[SUCCESS] {script_name} completed.")
            else:
                print(f"[ERROR] {script_name} failed with return code {return_code}.")
                failures.append((script_name, return_code))

        if failures and running:
            print("[ERROR] Terminating remaining parallel steps due to failure.")
            for _, _, process in running:
                if process.poll() is None:
                    process.terminate()
            for _, _, process in running:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
            break

        if running:
            time.sleep(0.2)

    if failures:
        if fail_on_error:
            sys.exit(1)
        return False

    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build composed schedule (FANZO primary + Flashscore football) and map channels without scanning playlists."
    )
    parser.add_argument("--date", default=None, help="Start date (YYYY-MM-DD). Default: today UTC.")
    parser.add_argument("--days", type=int, default=7, help="Number of days to scrape.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=7,
        help="Legacy compatibility option (unused after LiveSportTV retirement).",
    )
    parser.add_argument(
        "--max-tournaments",
        type=int,
        default=0,
        help="Legacy compatibility option (unused after LiveSportTV retirement).",
    )
    parser.add_argument(
        "--schedule-output",
        default="weekly_schedule.json",
        help="Output schedule file path.",
    )
    parser.add_argument(
        "--channels-file",
        default="channels.json",
        help="channels.json file to sync and use for mapping.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    fanzo_output = "weekly_schedule_fanzo.json"
    witm_output = "weekly_schedule_witm.json"
    flashscore_output = "weekly_schedule_flashscore.json"
    flashscore_csv_output = "weekly_schedule_flashscore.csv"
    fanzo_witm_output = "weekly_schedule_fanzo_enriched.json"

    fanzo_args = [
        "--days",
        str(args.days),
        "--include-soccer",
        "--output",
        fanzo_output,
    ]
    if args.date:
        fanzo_args.extend(["--date", args.date])
    witm_args = [
        "--days",
        str(args.days),
        "--output",
        witm_output,
    ]
    if args.date:
        witm_args.extend(["--date", args.date])
    flashscore_args = [
        "--days",
        str(args.days),
        "--output-json",
        flashscore_output,
        "--output-csv",
        flashscore_csv_output,
    ]
    if args.date:
        try:
            target_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
            utc_today = dt.datetime.now(dt.timezone.utc).date()
            day_start = (target_date - utc_today).days
            flashscore_args.extend(["--day-start", str(day_start)])
        except ValueError:
            print(f"[WARN] Invalid --date value '{args.date}'. Flashscore will use day-start=0 (UTC today).")

    run_parallel_steps(
        [
            (
                "scrape_schedule_fanzo.py",
                "Scraping Weekly FANZO Schedule (including soccer/football)",
                fanzo_args,
            ),
            (
                "scrape_schedule_witm.py",
                "Scraping Weekly WITM Schedule (non-soccer reinforcement)",
                witm_args,
            ),
            (
                "scrape_schedule_flashscore.py",
                "Scraping Weekly Flashscore Football Schedule",
                flashscore_args,
            ),
        ],
        "Scraping Weekly Schedules from FANZO + WITM + Flashscore (Parallel)",
    )

    run_step(
        "merge_fanzo_witm.py",
        "Merging FANZO + WITM (non-soccer reinforcement + logos)",
        extra_args=[
            "--fanzo",
            fanzo_output,
            "--witm",
            witm_output,
            "--output",
            fanzo_witm_output,
        ],
    )

    run_step(
        "compose_weekly_schedule.py",
        "Composing Final Weekly Schedule (FANZO Primary + Flashscore Football)",
        extra_args=[
            "--fanzo-witm",
            fanzo_witm_output,
            "--football-secondary",
            flashscore_output,
            "--output",
            args.schedule_output,
        ],
    )

    run_step(
        "sync_schedule_channels.py",
        "Syncing Schedule Channels into channels.json (stable IDs, no stream testing)",
        extra_args=[
            "--schedule",
            args.schedule_output,
            "--channels",
            args.channels_file,
        ],
    )

    run_step(
        "map_channels.py",
        "Mapping Schedule Channels to Existing Channel IDs",
        extra_args=[
            "--schedule-file",
            args.schedule_output,
            "--channels-file",
            args.channels_file,
        ],
    )

    print(f"\n{'=' * 60}")
    print("SCRAPE + MAP PIPELINE COMPLETE")
    print("Outputs: weekly_schedule.json, e104f869d64e3d41256d5398.json, channel_map.json")
    print(f"{'=' * 60}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
