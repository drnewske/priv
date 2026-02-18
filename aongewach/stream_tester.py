#!/usr/bin/env python3
"""
Validate channel stream URLs with ffprobe (and optional ffmpeg fallback), then
remove dead URLs from channels.json while preserving channel entries.
"""

import argparse
import json
import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Tuple

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class URLTestResult:
    url: str
    ok: bool
    method: str
    attempts: int
    elapsed_seconds: float


def load_json(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def collect_unique_urls(channels: Dict) -> List[str]:
    urls = set()
    for channel_data in channels.values():
        qualities = channel_data.get("qualities")
        if not isinstance(qualities, dict):
            continue
        for quality_urls in qualities.values():
            if not isinstance(quality_urls, list):
                continue
            for url in quality_urls:
                if isinstance(url, str):
                    cleaned = url.strip()
                    if cleaned:
                        urls.add(cleaned)
    return sorted(urls)


def run_ffprobe(ffprobe_bin: str, url: str, timeout: int, user_agent: str) -> bool:
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-rw_timeout",
        str(timeout * 1_000_000),
        "-analyzeduration",
        "1000000",
        "-probesize",
        "65536",
        "-user_agent",
        user_agent,
        "-show_entries",
        "stream=codec_type",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        url,
    ]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout + 2,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return False

    return result.returncode == 0 and bool(result.stdout.strip())


def run_ffmpeg(ffmpeg_bin: str, url: str, timeout: int, user_agent: str) -> bool:
    cmd = [
        ffmpeg_bin,
        "-v",
        "error",
        "-rw_timeout",
        str(timeout * 1_000_000),
        "-user_agent",
        user_agent,
        "-t",
        "6",
        "-i",
        url,
        "-f",
        "null",
        "-",
    ]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout + 4,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return False

    return result.returncode == 0


def test_single_url(
    url: str,
    ffprobe_bin: str,
    ffmpeg_bin: str,
    timeout: int,
    user_agent: str,
    allow_ffmpeg_fallback: bool,
    retry_failed: int,
    retry_delay: float,
) -> URLTestResult:
    started_at = time.time()
    attempts = max(0, retry_failed) + 1
    attempts_executed = 0
    for attempt in range(attempts):
        attempts_executed += 1
        if run_ffprobe(ffprobe_bin, url, timeout, user_agent):
            return URLTestResult(
                url=url,
                ok=True,
                method="ffprobe",
                attempts=attempts_executed,
                elapsed_seconds=time.time() - started_at,
            )
        if attempt < attempts - 1 and retry_delay > 0:
            time.sleep(retry_delay)

    if allow_ffmpeg_fallback and ffmpeg_bin:
        attempts_executed += 1
        ffmpeg_ok = run_ffmpeg(ffmpeg_bin, url, timeout, user_agent)
        return URLTestResult(
            url=url,
            ok=ffmpeg_ok,
            method="ffmpeg" if ffmpeg_ok else "dead",
            attempts=attempts_executed,
            elapsed_seconds=time.time() - started_at,
        )

    return URLTestResult(
        url=url,
        ok=False,
        method="dead",
        attempts=attempts_executed,
        elapsed_seconds=time.time() - started_at,
    )


def prune_dead_streams(
    db: Dict,
    url_health: Dict[str, bool],
) -> Tuple[int, int, int, int]:
    kept = 0
    removed = 0
    channels_touched = 0
    untested_kept = 0

    channels = db.get("channels", {})
    for _, channel_data in channels.items():
        qualities = channel_data.get("qualities")
        if not isinstance(qualities, dict):
            continue

        changed = False
        new_qualities = {}

        for quality, urls in qualities.items():
            if not isinstance(urls, list):
                continue

            alive_urls = []
            seen = set()
            for raw_url in urls:
                if not isinstance(raw_url, str):
                    continue
                cleaned_url = raw_url.strip()
                if not cleaned_url:
                    continue
                if cleaned_url in seen:
                    continue
                seen.add(cleaned_url)
                if cleaned_url in url_health:
                    if url_health[cleaned_url]:
                        alive_urls.append(cleaned_url)
                    else:
                        removed += 1
                else:
                    alive_urls.append(cleaned_url)
                    untested_kept += 1

            kept += len(alive_urls)

            if alive_urls:
                new_qualities[quality] = alive_urls

            if alive_urls != urls:
                changed = True

        if changed:
            channels_touched += 1
        channel_data["qualities"] = new_qualities if new_qualities else {}

    return kept, removed, channels_touched, untested_kept


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and prune dead stream URLs in channels.json")
    parser.add_argument("channels_file", nargs="?", default="channels.json", help="Path to channels.json")
    default_workers = min(32, max(8, (os.cpu_count() or 4) * 4))
    parser.add_argument("--workers", type=int, default=default_workers, help="Parallel URL test workers")
    parser.add_argument("--timeout", type=int, default=8, help="Per-URL probe timeout (seconds)")
    parser.add_argument("--max-urls", type=int, default=0, help="Optional cap for testing/debug")
    parser.add_argument("--ffprobe-bin", default="ffprobe", help="ffprobe binary path")
    parser.add_argument("--ffmpeg-bin", default="ffmpeg", help="ffmpeg binary path")
    parser.add_argument("--no-ffmpeg-fallback", action="store_true", help="Only use ffprobe")
    parser.add_argument("--retry-failed", type=int, default=1, help="Extra ffprobe retries on failure")
    parser.add_argument("--retry-delay", type=float, default=0.35, help="Seconds between retries")
    parser.add_argument("--progress-every", type=int, default=25, help="Print progress every N URLs (0 disables)")
    parser.add_argument("--verbose", action="store_true", help="Print every URL result")
    parser.add_argument("--show-failures", type=int, default=20, help="Show up to N failed URLs in summary")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP User-Agent")
    args = parser.parse_args()

    db = load_json(args.channels_file)
    channels = db.get("channels", {}) if isinstance(db, dict) else {}
    if not channels:
        print(f"No channels found in {args.channels_file}. Nothing to test.")
        return 0

    ffprobe_bin = shutil.which(args.ffprobe_bin)
    ffmpeg_bin = shutil.which(args.ffmpeg_bin)

    if not ffprobe_bin:
        print("ffprobe not found in PATH. Install ffmpeg/ffprobe to run stream validation.")
        return 1

    allow_ffmpeg_fallback = (not args.no_ffmpeg_fallback) and bool(ffmpeg_bin)
    if not allow_ffmpeg_fallback and not args.no_ffmpeg_fallback:
        print("ffmpeg not found; proceeding with ffprobe-only validation.")

    all_urls = collect_unique_urls(channels)
    urls = all_urls
    if args.max_urls > 0:
        urls = urls[: args.max_urls]

    tested_urls = len(urls)
    untested_urls = max(0, len(all_urls) - tested_urls)

    total_urls = len(urls)
    workers = max(1, args.workers)
    print("Stream tester configuration:")
    print(f"  File: {args.channels_file}")
    print(f"  Workers: {workers}")
    print(f"  Timeout per ffprobe attempt: {args.timeout}s")
    print(f"  Retry failed (extra attempts): {max(0, args.retry_failed)}")
    print(f"  FFmpeg fallback: {allow_ffmpeg_fallback}")
    print(f"  Progress every: {args.progress_every if args.progress_every > 0 else 'disabled'}")
    if args.max_urls > 0:
        print(f"  URL cap: {args.max_urls}")
    else:
        print("  URL cap: disabled (test all)")
    if tested_urls != len(all_urls):
        print(f"  Limited test mode: testing {tested_urls}/{len(all_urls)} URLs; untested URLs are kept.")

    per_url_worst_case = args.timeout * (max(0, args.retry_failed) + 1)
    if allow_ffmpeg_fallback:
        per_url_worst_case += args.timeout + 4
    rough_worst_case_seconds = (total_urls * per_url_worst_case) / workers if workers else 0
    print(f"  Rough worst-case runtime: {rough_worst_case_seconds / 60:.1f} minutes")
    print(f"Testing {total_urls} unique stream URLs...")
    if total_urls == 0:
        print("No stream URLs found. Nothing to prune.")
        return 0

    started = time.time()
    url_health: Dict[str, bool] = {}
    alive_so_far = 0
    dead_so_far = 0
    ffprobe_ok = 0
    ffmpeg_ok = 0
    failed_urls: List[str] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                test_single_url,
                url,
                ffprobe_bin,
                ffmpeg_bin,
                args.timeout,
                args.user_agent,
                allow_ffmpeg_fallback,
                args.retry_failed,
                args.retry_delay,
            ): url
            for url in urls
        }

        for idx, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            url_health[result.url] = result.ok

            if result.ok:
                alive_so_far += 1
                if result.method == "ffprobe":
                    ffprobe_ok += 1
                elif result.method == "ffmpeg":
                    ffmpeg_ok += 1
            else:
                dead_so_far += 1
                if len(failed_urls) < max(0, args.show_failures):
                    failed_urls.append(result.url)

            if args.verbose:
                status = "OK" if result.ok else "DEAD"
                print(
                    f"[{idx}/{total_urls}] {status:<4} via {result.method:<7} "
                    f"attempts={result.attempts} elapsed={result.elapsed_seconds:.2f}s {result.url}"
                )
            elif args.progress_every > 0 and (idx % args.progress_every == 0 or idx == total_urls):
                elapsed = max(0.001, time.time() - started)
                rate = idx / elapsed
                eta_seconds = (total_urls - idx) / rate if rate > 0 else 0
                print(
                    f"  Progress: {idx}/{total_urls} | Alive: {alive_so_far} | Dead: {dead_so_far} | "
                    f"ffprobe OK: {ffprobe_ok} | ffmpeg OK: {ffmpeg_ok} | "
                    f"Rate: {rate:.2f}/s | ETA: {eta_seconds / 60:.1f}m"
                )

    alive = sum(1 for ok in url_health.values() if ok)
    dead = total_urls - alive

    kept, removed, channels_touched, untested_kept = prune_dead_streams(db, url_health)

    metadata = db.setdefault("metadata", {})
    metadata["stream_tester"] = {
        "tested_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_urls_tested": total_urls,
        "total_unique_urls_in_file": len(all_urls),
        "untested_urls": untested_urls,
        "untested_urls_kept_after_prune": untested_kept,
        "alive_urls": alive,
        "dead_urls": dead,
        "ffprobe_successes": ffprobe_ok,
        "ffmpeg_successes": ffmpeg_ok,
        "urls_kept_after_prune": kept,
        "urls_removed": removed,
        "channels_touched": channels_touched,
        "ffmpeg_fallback_used": allow_ffmpeg_fallback,
        "timeout_seconds": args.timeout,
        "retry_failed": args.retry_failed,
        "retry_delay_seconds": args.retry_delay,
        "workers": workers,
    }

    save_json(args.channels_file, db)

    elapsed = time.time() - started
    print("\n[OK] Stream validation complete")
    print(f"  File: {args.channels_file}")
    print(f"  Tested: {total_urls} URLs")
    print(f"  Alive: {alive}")
    print(f"  Dead: {dead}")
    print(f"  ffprobe OK: {ffprobe_ok}")
    print(f"  ffmpeg OK: {ffmpeg_ok}")
    print(f"  Removed URLs: {removed}")
    print(f"  Untested URLs kept: {untested_kept}")
    print(f"  Channels updated: {channels_touched}")
    print(f"  Duration: {elapsed:.1f}s")
    if failed_urls:
        print(f"  Sample failed URLs ({len(failed_urls)} shown):")
        for failed_url in failed_urls:
            print(f"    - {failed_url}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
