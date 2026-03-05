#!/usr/bin/env python3
"""
Run Pipeline - Orchestrates the full schedule processing pipeline.

Steps:
  1. Scrape FANZO + WITM + Flashscore schedules in parallel
  2. Merge/compose final weekly schedule
  3. Sync schedule channels into channels.json with stable IDs
  4. Map schedule channels to channel IDs in final output
"""

import subprocess
import sys
import time


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


def run_parallel_steps(steps, description, fail_on_error=True):
    """
    Run independent pipeline steps in parallel.
    Each step tuple: (script_name, step_description, extra_args_list_or_none).
    """
    print(f"\n{'=' * 50}")
    print(f"STEP: {description}")
    print(f"{'=' * 50}\n")

    processes = []
    for script_name, step_description, extra_args in steps:
        cmd = [sys.executable, "-u", script_name]
        if extra_args:
            cmd.extend(extra_args)
        print(f"[START] {step_description} ({script_name})")
        try:
            process = subprocess.Popen(cmd)
        except Exception as e:
            print(f"\n[ERROR] Failed to start {script_name}: {e}")
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


def main():
    # 1-3. Scrape source schedules in parallel.
    run_parallel_steps(
        [
            (
                "scrape_schedule_fanzo.py",
                "Scraping Weekly FANZO Schedule (including soccer/football)",
                [
                    "--days",
                    "7",
                    "--include-soccer",
                    "--output",
                    "weekly_schedule_fanzo.json",
                ],
            ),
            (
                "scrape_schedule_witm.py",
                "Scraping Weekly WITM Schedule (non-soccer reinforcement)",
                [
                    "--days",
                    "7",
                    "--output",
                    "weekly_schedule_witm.json",
                ],
            ),
            (
                "scrape_schedule_flashscore.py",
                "Scraping Weekly Flashscore Football Schedule",
                [
                    "--days",
                    "7",
                    "--output-json",
                    "weekly_schedule_flashscore.json",
                    "--output-csv",
                    "weekly_schedule_flashscore.csv",
                ],
            ),
        ],
        "Scraping Weekly Schedules from FANZO + WITM + Flashscore (Parallel)",
    )

    # 4. Merge FANZO with WITM (exact match enrichment).
    run_step(
        "merge_fanzo_witm.py",
        "Merging FANZO + WITM (non-soccer reinforcement + logos)",
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
    #    FANZO(+WITM enrichment) + Flashscore football.
    run_step(
        "compose_weekly_schedule.py",
        "Composing Final Weekly Schedule (FANZO Primary + Flashscore Football)",
        extra_args=[
            "--fanzo-witm",
            "weekly_schedule_fanzo_enriched.json",
            "--football-secondary",
            "weekly_schedule_flashscore.json",
            "--output",
            "weekly_schedule.json",
        ],
    )

    run_step(
        "sync_schedule_channels.py",
        "Syncing Schedule Channels into channels.json (stable IDs, no stream testing)",
        extra_args=[
            "--schedule",
            "weekly_schedule.json",
            "--channels",
            "channels.json",
        ],
    )

    # 7. Map Channels (Schedule Channels -> Channel IDs).
    run_step(
        "map_channels.py",
        "Mapping Schedule Channels to Channel IDs",
        extra_args=None,
    )

    print(f"\n{'=' * 50}")
    print("PIPELINE COMPLETE")
    print("Output available in: e104f869d64e3d41256d5398.json")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
