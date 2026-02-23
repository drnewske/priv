#!/usr/bin/env python3
"""
Run Pipeline - Orchestrates the full schedule processing pipeline.

Steps:
  1. Scrape LiveSportTV schedule (soccer source)
  2. Scrape FANZO + WITM schedules (non-soccer sources)
  3. Merge/compose final weekly schedule
  4. Scan/update channels.json from configured playlists/providers
  5. Map schedule channels to IPTV stream URLs
"""

import subprocess
import sys


def run_step(script_name, description, extra_args=None, fail_on_error=True):
    print(f"\n{'=' * 50}")
    print(f"STEP: {description}")
    print(f"Running {script_name}...")
    print(f"{'=' * 50}\n")

    cmd = [sys.executable, "-u", script_name]
    if extra_args:
        cmd.extend(extra_args)

    try:
        result = subprocess.run(cmd, check=True)
        if result.returncode == 0:
            print(f"\n[SUCCESS] {script_name} completed.")
            return True
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] {script_name} failed with return code {e.returncode}.")
        if fail_on_error:
            sys.exit(1)
        return False
    except Exception as e:
        print(f"\n[ERROR] Failed to run {script_name}: {e}")
        if fail_on_error:
            sys.exit(1)
        return False

    return True


def main():
    # 1. Scrape LiveSportTV schedule (used for soccer/football only in final compose).
    run_step(
        "scrape_schedule_livesporttv.py",
        "Scraping Weekly Schedule from LiveSportTV",
        extra_args=[
            "--days",
            "7",
            "--max-pages",
            "7",
            "--geo-rules-file",
            "channel_geo_rules.json",
            "--output",
            "weekly_schedule_livesporttv.json",
        ],
    )

    # 2. Scrape FANZO non-soccer schedule.
    run_step(
        "scrape_schedule_fanzo.py",
        "Scraping Weekly Non-Soccer Schedule from FANZO",
        extra_args=[
            "--days",
            "7",
            "--output",
            "weekly_schedule_fanzo.json",
        ],
    )

    # 3. Scrape WITM non-soccer schedule for channel/logo reinforcement.
    run_step(
        "scrape_schedule_witm.py",
        "Scraping Weekly Non-Soccer Schedule from WITM",
        extra_args=[
            "--days",
            "7",
            "--output",
            "weekly_schedule_witm.json",
        ],
    )

    # 4. Merge FANZO with WITM (exact match enrichment).
    run_step(
        "merge_fanzo_witm.py",
        "Merging FANZO + WITM Non-Soccer Schedule",
        extra_args=[
            "--fanzo",
            "weekly_schedule_fanzo.json",
            "--witm",
            "weekly_schedule_witm.json",
            "--output",
            "weekly_schedule_fanzo_enriched.json",
        ],
    )

    # 5. Compose final weekly schedule:
    #    soccer from LiveSportTV + non-soccer from FANZO/WITM.
    run_step(
        "compose_weekly_schedule.py",
        "Composing Final Weekly Schedule (LSTV Soccer + FANZO/WITM Non-Soccer)",
        extra_args=[
            "--livesporttv",
            "weekly_schedule_livesporttv.json",
            "--fanzo-witm",
            "weekly_schedule_fanzo_enriched.json",
            "--output",
            "weekly_schedule.json",
        ],
    )

    # 6. Scan Sports Channels (Update channels.json based on composed schedule).
    run_step(
        "scan_sports_channels.py",
        "Scanning Playlists for Channels in Schedule (Inline Stream Validation)",
        extra_args=[
            "channels.json",
            "--max-working-streams-per-channel",
            "5",
            "--test-workers",
            "20",
            "--test-timeout",
            "8",
            "--test-retry-failed",
            "1",
        ],
    )

    # 7. Map Channels (Schedule Channels -> IPTV Stream URLs).
    run_step(
        "map_channels.py",
        "Mapping Schedule Channels to Playable IPTV Streams",
        extra_args=["--geo-rules-file", "channel_geo_rules.json"],
    )

    print(f"\n{'=' * 50}")
    print("PIPELINE COMPLETE")
    print("Output available in: e104f869d64e3d41256d5398.json")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
