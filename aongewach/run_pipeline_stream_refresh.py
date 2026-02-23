#!/usr/bin/env python3
"""
Run Pipeline (Dead-Stream-First Mode)

Flow:
  1. Test/prune existing streams in channels.json (mark dead URLs and remove them)
  2. Build composed weekly schedule:
     - LiveSportTV for soccer/football
     - FANZO + WITM for non-soccer
  3. Run batched playlist scan to refill/add streams and discover schedule channels
  4. Map schedule channels to IPTV stream IDs
"""

import argparse
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
    parser = argparse.ArgumentParser(description="Dead-stream-first schedule/channel pipeline.")

    parser.add_argument("--channels-file", default="channels.json", help="Path to channels DB")
    parser.add_argument("--date", default=None, help="Start date (YYYY-MM-DD). Default: today UTC.")
    parser.add_argument("--days", type=int, default=7, help="Number of days to scrape.")

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
    parser.add_argument("--max-pages", type=int, default=7, help="How many /data-today pages per day for scraper")
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

    # 2. Scrape LiveSportTV schedule (soccer source).
    scrape_lsv_args = [
        "--days",
        str(args.days),
        "--max-pages",
        str(args.max_pages),
        "--geo-rules-file",
        "channel_geo_rules.json",
        "--output",
        "weekly_schedule_livesporttv.json",
    ]
    if args.date:
        scrape_lsv_args.extend(["--date", args.date])
    run_step(
        "scrape_schedule_livesporttv.py",
        "Scraping Weekly Schedule from LiveSportTV",
        extra_args=scrape_lsv_args,
    )

    # 3. Scrape FANZO/WITM non-soccer and merge.
    scrape_fanzo_args = [
        "--days",
        str(args.days),
        "--output",
        "weekly_schedule_fanzo.json",
    ]
    if args.date:
        scrape_fanzo_args.extend(["--date", args.date])
    run_step(
        "scrape_schedule_fanzo.py",
        "Scraping Weekly Non-Soccer Schedule from FANZO",
        extra_args=scrape_fanzo_args,
    )

    scrape_witm_args = [
        "--days",
        str(args.days),
        "--output",
        "weekly_schedule_witm.json",
    ]
    if args.date:
        scrape_witm_args.extend(["--date", args.date])
    run_step(
        "scrape_schedule_witm.py",
        "Scraping Weekly Non-Soccer Schedule from WITM",
        extra_args=scrape_witm_args,
    )

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

    # 4. Playlist scan (batch per playlist, parallel stream tests) to refill/add.
    scan_args = [
        args.channels_file,
        "--preserve-existing-streams",
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
        extra_args=["--geo-rules-file", "channel_geo_rules.json"],
    )

    print(f"\n{'=' * 60}")
    print("DEAD-STREAM-FIRST PIPELINE COMPLETE")
    print("Output available in: e104f869d64e3d41256d5398.json")
    print(f"{'=' * 60}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
