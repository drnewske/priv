#!/usr/bin/env python3
"""
Run Pipeline - Orchestrates the full schedule processing pipeline.

Steps:
  1. Scrape weekly schedule from FANZO TV Guide API
  2. Scan/update channels.json from configured playlists/providers
  3. Validate channels with ffprobe/ffmpeg and prune dead streams
  4. Map schedule events to team IDs and logos
  5. Map schedule channels to IPTV stream URLs
"""

import subprocess
import sys


def run_step(script_name, description, extra_args=None, fail_on_error=True):
    print(f"\n{'=' * 50}")
    print(f"STEP: {description}")
    print(f"Running {script_name}...")
    print(f"{'=' * 50}\n")

    cmd = [sys.executable, script_name]
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
    # 1. Scrape Schedule (Weekly)
    run_step(
        "scrape_schedule_fanzo.py",
        "Scraping Weekly Schedule from FANZO TV Guide API",
    )

    # 2. Scan Sports Channels (Update channels.json based on schedule)
    run_step("scan_sports_channels.py", "Scanning Playlists for Channels in Schedule")

    # 3. Validate stream URLs and remove dead links
    run_step(
        "stream_tester.py",
        "Testing Stream URLs (ffprobe/ffmpeg) and pruning dead links",
        ["channels.json", "--workers", "12", "--timeout", "8", "--retry-failed", "1"],
    )

    # 4. Map Teams (Event Names -> Team IDs & Logos)
    run_step("map_schedule_to_teams.py", "Mapping Events to Team IDs and Logos")

    # 5. Map Channels (Schedule Channels -> IPTV Stream URLs)
    run_step("map_channels.py", "Mapping Schedule Channels to Playable IPTV Streams")

    print(f"\n{'=' * 50}")
    print("PIPELINE COMPLETE")
    print("Output available in: e104f869d64e3d41256d5398.json")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
