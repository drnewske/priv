#!/usr/bin/env python3
"""
Daily channel test worker.

Flow:
  1. Select today's UTC events from weekly_schedule.json
  2. Prune dead URLs from channels.json
  3. Run scan_sports_channels.py against today's channels only
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from typing import Dict, List


def load_json(path: str):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: str, payload) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test stream links for today's UTC schedule channels.")
    parser.add_argument("--date", default=None, help="Target UTC date (YYYY-MM-DD). Default: today UTC.")
    parser.add_argument("--schedule", default="weekly_schedule.json", help="Input weekly schedule file.")
    parser.add_argument("--today-schedule", default="_schedule_today.json", help="Temp single-day schedule path.")
    parser.add_argument("--channels", default="channels.json", help="Channels DB path.")
    parser.add_argument("--workers", type=int, default=20, help="Parallel stream test workers.")
    parser.add_argument("--timeout", type=int, default=8, help="ffprobe timeout in seconds.")
    parser.add_argument("--retry-failed", type=int, default=1, help="Extra retries for failed stream tests.")
    parser.add_argument("--retry-delay", type=float, default=0.35, help="Delay between retries.")
    parser.add_argument("--no-ffmpeg-fallback", action="store_true", help="Disable ffmpeg fallback.")
    return parser.parse_args()


def target_date_iso(raw: str | None) -> str:
    if raw:
        return dt.datetime.strptime(raw.strip(), "%Y-%m-%d").date().isoformat()
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def build_today_schedule(schedule_payload: Dict, date_iso: str) -> Dict:
    days = schedule_payload.get("schedule", [])
    if not isinstance(days, list):
        return {"generated_at": None, "source": "daily-target", "schedule": []}

    chosen = [day for day in days if isinstance(day, dict) and str(day.get("date")) == date_iso]
    return {
        "generated_at": schedule_payload.get("generated_at"),
        "source": "daily-target",
        "schedule": chosen,
    }


def run_step(cmd: List[str], description: str) -> None:
    print(f"[STEP] {description}")
    print("       " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    args = parse_args()

    try:
        date_iso = target_date_iso(args.date)
    except ValueError:
        print(f"Invalid --date value: {args.date!r}. Expected YYYY-MM-DD.", file=sys.stderr)
        return 2

    schedule_payload = load_json(args.schedule)
    today_payload = build_today_schedule(schedule_payload, date_iso)
    if not today_payload.get("schedule"):
        print(f"No events found for UTC date {date_iso}. Nothing to test.")
        return 0

    save_json(args.today_schedule, today_payload)

    stream_tester_cmd = [
        sys.executable,
        "-u",
        "stream_tester.py",
        args.channels,
        "--workers",
        str(args.workers),
        "--timeout",
        str(args.timeout),
        "--retry-failed",
        str(args.retry_failed),
        "--retry-delay",
        str(args.retry_delay),
    ]
    if args.no_ffmpeg_fallback:
        stream_tester_cmd.append("--no-ffmpeg-fallback")
    run_step(stream_tester_cmd, "Prune dead URLs from channels DB")

    scan_cmd = [
        sys.executable,
        "-u",
        "scan_sports_channels.py",
        args.channels,
        "--schedule-file",
        args.today_schedule,
        "--preserve-existing-streams",
        "--max-working-streams-per-channel",
        "5",
        "--test-workers",
        str(args.workers),
        "--test-timeout",
        str(args.timeout),
        "--test-retry-failed",
        str(args.retry_failed),
        "--test-retry-delay",
        str(args.retry_delay),
    ]
    if args.no_ffmpeg_fallback:
        scan_cmd.append("--no-ffmpeg-fallback")
    run_step(scan_cmd, "Test today's schedule channels and refresh channels DB")

    print(f"[DONE] Daily channel tests completed for UTC date {date_iso}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
