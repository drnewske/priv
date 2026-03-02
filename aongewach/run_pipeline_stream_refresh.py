#!/usr/bin/env python3
"""
Run Pipeline (Dead-Stream-First Mode)

Flow:
  1. Test/prune existing streams in channels.json (mark dead URLs and remove them)
  2. Build composed weekly schedule:
     - FANZO + WITM + HuhSports in parallel
  3. Run batched playlist scan to refill/add streams and discover schedule channels
  4. Map schedule channels to IPTV stream IDs
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
    parser = argparse.ArgumentParser(description="Dead-stream-first schedule/channel pipeline.")

    parser.add_argument("--channels-file", default="channels.json", help="Path to channels DB")
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

    # Stream tester settings
    parser.add_argument("--stream-workers", type=int, default=20, help="Parallel testers for stream_tester.py")
    parser.add_argument("--stream-timeout", type=int, default=8, help="ffprobe timeout for stream_tester.py")
    parser.add_argument(
        "--stream-retry-failed",
        type=int,
        default=1,
        help="Extra ffprobe retries for stream_tester.py",
    )
    parser.add_argument(
        "--stream-retry-delay",
        type=float,
        default=0.35,
        help="Delay between stream_tester.py retries",
    )
    parser.add_argument(
        "--no-stream-ffmpeg-fallback",
        action="store_true",
        help="Disable ffmpeg fallback in stream_tester.py",
    )

    # Scanner settings
    parser.add_argument(
        "--max-working-streams-per-channel",
        type=int,
        default=5,
        help="Hard cap of working streams to keep per channel",
    )
    parser.add_argument("--scan-workers", type=int, default=20, help="Parallel workers per playlist scan")
    parser.add_argument("--scan-timeout", type=int, default=8, help="ffprobe timeout for playlist scanner")
    parser.add_argument(
        "--scan-retry-failed",
        type=int,
        default=1,
        help="Extra ffprobe retries in playlist scanner",
    )
    parser.add_argument(
        "--scan-retry-delay",
        type=float,
        default=0.35,
        help="Delay between scanner retries",
    )
    parser.add_argument(
        "--no-scan-ffmpeg-fallback",
        action="store_true",
        help="Disable ffmpeg fallback in playlist scanner",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # 1. Prune dead URLs from existing channels DB.
    stream_tester_args = [
        args.channels_file,
        "--workers",
        str(args.stream_workers),
        "--timeout",
        str(args.stream_timeout),
        "--retry-failed",
        str(args.stream_retry_failed),
        "--retry-delay",
        str(args.stream_retry_delay),
    ]
    if args.no_stream_ffmpeg_fallback:
        stream_tester_args.append("--no-ffmpeg-fallback")
    run_step(
        "stream_tester.py",
        "Testing Existing channels.json Streams and Pruning Dead URLs",
        extra_args=stream_tester_args,
    )

    # 2. Scrape source schedules in parallel and merge.
    scrape_fanzo_args = [
        "--days",
        str(args.days),
        "--include-soccer",
        "--output",
        "weekly_schedule_fanzo.json",
    ]
    scrape_witm_args = [
        "--days",
        str(args.days),
        "--output",
        "weekly_schedule_witm.json",
    ]
    scrape_huhsports_args = [
        "--days",
        str(args.days),
        "--output",
        "weekly_schedule_huhsports.json",
    ]
    if args.date:
        scrape_fanzo_args.extend(["--date", args.date])
        scrape_witm_args.extend(["--date", args.date])
        scrape_huhsports_args.extend(["--start-date", args.date])
    run_parallel_steps(
        [
            (
                "scrape_schedule_fanzo.py",
                "Scraping Weekly FANZO Schedule (including soccer/football)",
                scrape_fanzo_args,
            ),
            (
                "scrape_schedule_witm.py",
                "Scraping Weekly WITM Schedule (non-soccer reinforcement)",
                scrape_witm_args,
            ),
            (
                "scrape_schedule_huhsports.py",
                "Scraping Weekly HuhSports Football Schedule",
                scrape_huhsports_args,
            ),
        ],
        "Scraping Weekly Schedules from FANZO + WITM + HuhSports (Parallel)",
    )

    run_step(
        "merge_fanzo_witm.py",
        "Merging FANZO + WITM Schedule",
        extra_args=[
            "--fanzo",
            "weekly_schedule_fanzo.json",
            "--witm",
            "weekly_schedule_witm.json",
            "--output",
            "weekly_schedule_fanzo_enriched.json",
        ],
    )

    run_step(
        "compose_weekly_schedule.py",
        "Composing Final Weekly Schedule (FANZO Primary + HuhSports Football)",
        extra_args=[
            "--fanzo-witm",
            "weekly_schedule_fanzo_enriched.json",
            "--huhsports",
            "weekly_schedule_huhsports.json",
            "--output",
            "weekly_schedule.json",
        ],
    )

    # 4. Playlist scan (batch per playlist, parallel stream tests) to refill/add.
    scan_args = [
        args.channels_file,
        "--preserve-existing-streams",
        "--prune-non-target-channels",
        "--max-working-streams-per-channel",
        str(args.max_working_streams_per_channel),
        "--test-workers",
        str(args.scan_workers),
        "--test-timeout",
        str(args.scan_timeout),
        "--test-retry-failed",
        str(args.scan_retry_failed),
        "--test-retry-delay",
        str(args.scan_retry_delay),
    ]
    if args.no_scan_ffmpeg_fallback:
        scan_args.append("--no-ffmpeg-fallback")
    run_step(
        "scan_sports_channels.py",
        "Scanning Playlists in Batches to Refill/Add Working Streams",
        extra_args=scan_args,
    )

    # 5. Map channels.
    run_step(
        "map_channels.py",
        "Mapping Schedule Channels to Playable IPTV Streams",
        extra_args=None,
    )

    print(f"\n{'=' * 60}")
    print("DEAD-STREAM-FIRST PIPELINE COMPLETE")
    print("Output available in: e104f869d64e3d41256d5398.json")
    print(f"{'=' * 60}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
