#!/usr/bin/env python3
"""
Run Pipeline (LiveSportTV Scrape + Channel Mapper only)

Flow:
  1. Scrape schedule from LiveSportTV
  2. Map schedule channels to existing IPTV channel IDs
"""

import argparse
import os
import shutil
import subprocess
import sys


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape LiveSportTV and map channels without scanning playlists.")
    parser.add_argument("--date", default=None, help="Start date (YYYY-MM-DD). Default: today UTC.")
    parser.add_argument("--days", type=int, default=7, help="Number of days to scrape.")
    parser.add_argument("--max-pages", type=int, default=2, help="How many /data-today pages per day.")
    parser.add_argument(
        "--max-tournaments",
        type=int,
        default=0,
        help="Cap tournament API calls per day (0 = no cap).",
    )
    parser.add_argument(
        "--schedule-output",
        default="weekly_schedule.json",
        help="Output schedule file path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    scrape_args = [
        "--days",
        str(args.days),
        "--max-pages",
        str(args.max_pages),
        "--output",
        args.schedule_output,
    ]
    if args.date:
        scrape_args.extend(["--date", args.date])
    if args.max_tournaments > 0:
        scrape_args.extend(["--max-tournaments", str(args.max_tournaments)])

    run_step(
        "scrape_schedule_livesporttv.py",
        "Scraping Weekly Schedule from LiveSportTV",
        extra_args=scrape_args,
    )

    mapper_input = "weekly_schedule.json"
    if os.path.normcase(args.schedule_output) != os.path.normcase(mapper_input):
        shutil.copyfile(args.schedule_output, mapper_input)
        print(f"[INFO] Copied {args.schedule_output} -> {mapper_input} for mapper input.")

    run_step("map_channels.py", "Mapping Schedule Channels to Existing Channel IDs")

    print(f"\n{'=' * 60}")
    print("SCRAPE + MAP PIPELINE COMPLETE")
    print("Outputs: weekly_schedule.json, e104f869d64e3d41256d5398.json, channel_map.json")
    print(f"{'=' * 60}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
