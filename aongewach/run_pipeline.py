#!/usr/bin/env python3
"""
Run Pipeline — Orchestrates the full schedule processing pipeline.

Steps:
  1. Scrape weekly schedule from wheresthematch.com
  2. Build/update teams database from TheSportsDB (bulk preload)
  3. Map schedule events to team IDs and logos
  4. Map schedule channels to IPTV stream URLs
"""

import os
import subprocess
import sys
import json
from datetime import datetime, timezone, timedelta


DB_FILE = os.path.join(os.path.dirname(__file__) or '.', 'spdb_teams.json')
DB_MAX_AGE_DAYS = 7  # Skip rebuild if DB is less than this many days old


def run_step(script_name, description):
    print(f"\n{'='*50}")
    print(f"STEP: {description}")
    print(f"Running {script_name}...")
    print(f"{'='*50}\n")

    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        if result.returncode == 0:
            print(f"\n[SUCCESS] {script_name} completed.")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] {script_name} failed with return code {e.returncode}.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Failed to run {script_name}: {e}")
        sys.exit(1)


def is_db_fresh():
    """Check if spdb_teams.json exists and is recent enough to skip rebuild."""
    if not os.path.exists(DB_FILE):
        return False

    try:
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        meta = data.get('_meta', {})
        last_built = meta.get('last_built')
        if not last_built:
            return False

        built_dt = datetime.fromisoformat(last_built)
        age = datetime.now(timezone.utc) - built_dt
        if age < timedelta(days=DB_MAX_AGE_DAYS):
            team_count = meta.get('team_count', 0)
            print(f"  Teams DB is fresh ({age.days}d old, {team_count} teams). Skipping rebuild.")
            return True
    except Exception:
        pass

    return False


def main():
    # 1. Scrape Schedule (Weekly)
    run_step("scrape_schedule.py", "Scraping Weekly Schedule from Wheresthematch.com")

    # 2. Build Teams Database (TheSportsDB bulk preload)
    if is_db_fresh():
        print(f"\n{'='*50}")
        print(f"STEP: Teams DB is up to date — skipping build_teams_db.py")
        print(f"{'='*50}\n")
    else:
        run_step("build_teams_db.py", "Building Teams Database from TheSportsDB (Bulk)")

    # 3. Map Teams (Event Names -> Team IDs & Logos)
    run_step("map_schedule_to_teams.py", "Mapping Events to Team IDs and Logos")

    # 4. Map Channels (Schedule Channels -> IPTV Stream URLs)
    run_step("map_channels.py", "Mapping Schedule Channels to Playable IPTV Streams")

    print(f"\n{'='*50}")
    print("PIPELINE COMPLETE")
    print("Output available in: e104f869d64e3d41256d5398.json")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
