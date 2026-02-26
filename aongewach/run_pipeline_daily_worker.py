#!/usr/bin/env python3
"""
Run Daily Schedule Worker

Flow (single UTC day):
  1. Scrape LiveSportTV + FANZO + WITM in parallel
  2. Merge/compose today's schedule
  3. Scan playlists/external URLs for today's channel list (validate working links)
  4. Map today's schedule channels to channel IDs
"""

import argparse
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
        description=(
            "Daily schedule worker: scrape one day, refresh channels from playlists/external URLs, and map."
        )
    )
    parser.add_argument("--date", default=None, help="Target date (YYYY-MM-DD). Default: today UTC.")
    parser.add_argument("--max-pages", type=int, default=7, help="Per-day /data-today pages.")
    parser.add_argument(
        "--max-tournaments",
        type=int,
        default=0,
        help="Tournament API cap for the day (0 = no cap).",
    )
    parser.add_argument(
        "--max-working-streams-per-channel",
        type=int,
        default=5,
        help="Max working stream URLs stored per channel.",
    )
    parser.add_argument("--test-workers", type=int, default=20, help="Parallel stream test workers.")
    parser.add_argument("--test-timeout", type=int, default=8, help="ffprobe timeout for stream tests.")
    parser.add_argument(
        "--test-retry-failed",
        type=int,
        default=1,
        help="Additional retries for failed stream tests.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    lsv_output = "weekly_schedule_livesporttv.json"
    fanzo_output = "weekly_schedule_fanzo.json"
    witm_output = "weekly_schedule_witm.json"
    fanzo_witm_output = "weekly_schedule_fanzo_enriched.json"
    schedule_output = "weekly_schedule.json"

    lsv_args = [
        "--days",
        "1",
        "--max-pages",
        str(args.max_pages),
        "--output",
        lsv_output,
    ]
    fanzo_args = ["--days", "1", "--output", fanzo_output]
    witm_args = ["--days", "1", "--output", witm_output]
    if args.date:
        lsv_args.extend(["--date", args.date])
        fanzo_args.extend(["--date", args.date])
        witm_args.extend(["--date", args.date])
    if args.max_tournaments > 0:
        lsv_args.extend(["--max-tournaments", str(args.max_tournaments)])

    run_parallel_steps(
        [
            (
                "scrape_schedule_livesporttv.py",
                "Scraping Daily Schedule from LiveSportTV",
                lsv_args,
            ),
            (
                "scrape_schedule_fanzo.py",
                "Scraping Daily Non-Soccer Schedule from FANZO",
                fanzo_args,
            ),
            (
                "scrape_schedule_witm.py",
                "Scraping Daily Non-Soccer Schedule from WITM",
                witm_args,
            ),
        ],
        "Scraping Daily Schedules from LiveSportTV + FANZO + WITM (Parallel)",
    )

    run_step(
        "merge_fanzo_witm.py",
        "Merging Daily FANZO + WITM Non-Soccer Schedule",
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
        "Composing Daily Schedule (LSTV Soccer + FANZO/WITM Non-Soccer)",
        extra_args=[
            "--livesporttv",
            lsv_output,
            "--fanzo-witm",
            fanzo_witm_output,
            "--output",
            schedule_output,
        ],
    )

    run_step(
        "scan_sports_channels.py",
        "Refreshing Working Links for Today's Schedule Channels",
        extra_args=[
            "channels.json",
            "--prune-non-target-channels",
            "--max-working-streams-per-channel",
            str(args.max_working_streams_per_channel),
            "--test-workers",
            str(args.test_workers),
            "--test-timeout",
            str(args.test_timeout),
            "--test-retry-failed",
            str(args.test_retry_failed),
        ],
    )

    run_step(
        "map_channels.py",
        "Mapping Daily Schedule Channels to Channel IDs",
        extra_args=None,
    )

    print(f"\n{'=' * 60}")
    print("DAILY SCHEDULE WORKER COMPLETE")
    print("Outputs: weekly_schedule.json, channels.json, e104f869d64e3d41256d5398.json")
    print(f"{'=' * 60}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
