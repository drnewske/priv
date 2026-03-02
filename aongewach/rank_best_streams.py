#!/usr/bin/env python3
"""
Pick best streams per channel and write:
  - primary + backups + reserve
  - candidate metadata
  - per-run stream health log (JSONL)
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
QUALITY_ORDER = ["4K", "FHD", "HD", "SD"]
POLICY_VERSION = "best-stream-v1"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def load_json(path: str):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: str, payload) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def url_hash(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def parse_iso_datetime(value: object) -> Optional[dt.datetime]:
    text = normalize_text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def safe_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_ratio(value: object) -> Optional[float]:
    text = normalize_text(value)
    if not text:
        return None
    if "/" in text:
        left, right = text.split("/", 1)
        num = safe_float(left)
        den = safe_float(right)
        if num is None or den is None or den == 0:
            return None
        return num / den
    return safe_float(text)


def domain_from_url(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower().strip()
    except Exception:
        host = ""
    return host


def quality_from_dims(width: Optional[int], height: Optional[int]) -> str:
    w = int(width or 0)
    h = int(height or 0)
    if w >= 2560 or h >= 1440:
        return "4K"
    if w >= 1920 or h >= 1080:
        return "FHD"
    if w >= 1280 or h >= 720:
        return "HD"
    return "SD"


def load_targets_from_schedule(schedule_file: str) -> List[str]:
    payload = load_json(schedule_file)
    days = payload.get("schedule", []) if isinstance(payload, dict) else []
    if not isinstance(days, list):
        return []
    out: List[str] = []
    seen = set()
    for day in days:
        events = day.get("events", []) if isinstance(day, dict) else []
        if not isinstance(events, list):
            continue
        for event in events:
            channels = event.get("channels", []) if isinstance(event, dict) else []
            if not isinstance(channels, list):
                continue
            for raw in channels:
                name = normalize_text(raw)
                if not name:
                    continue
                key = name.casefold()
                if key in seen:
                    continue
                seen.add(key)
                out.append(name)
    return out


def match_channel_name(existing_names: Iterable[str], target_name: str) -> Optional[str]:
    lookup = {name.casefold(): name for name in existing_names}
    return lookup.get(target_name.casefold())


def iter_channel_urls(channel_node: Dict) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    seen = set()
    qualities = channel_node.get("qualities")
    if isinstance(qualities, dict):
        for quality, urls in qualities.items():
            if not isinstance(urls, list):
                continue
            hint = normalize_text(quality) or "HD"
            for raw in urls:
                if not isinstance(raw, str):
                    continue
                url = raw.strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                out.append((url, hint))

    for key in ("primary",):
        entry = channel_node.get(key)
        if isinstance(entry, dict):
            url = normalize_text(entry.get("url"))
            hint = normalize_text(entry.get("quality")) or "HD"
            if url and url not in seen:
                seen.add(url)
                out.append((url, hint))

    for key in ("backups", "reserve", "candidates"):
        entries = channel_node.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            url = normalize_text(entry.get("url"))
            hint = normalize_text(entry.get("quality")) or "HD"
            if url and url not in seen:
                seen.add(url)
                out.append((url, hint))
    return out


def load_history(log_file: str, days: int) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    if not log_file or not os.path.exists(log_file):
        return out
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max(1, days))
    with open(log_file, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except Exception:
                continue
            if not isinstance(event, dict):
                continue
            h = normalize_text(event.get("url_hash"))
            if not h:
                continue
            tested_at = parse_iso_datetime(event.get("tested_at"))
            if tested_at is None or tested_at < cutoff:
                continue
            node = out.setdefault(h, {"tested": 0, "ok": 0, "last_ok_at": None})
            node["tested"] = int(node.get("tested", 0)) + 1
            if bool(event.get("ok")):
                node["ok"] = int(node.get("ok", 0)) + 1
                node["last_ok_at"] = tested_at.isoformat().replace("+00:00", "Z")
    return out


def ffprobe_probe(ffprobe_bin: str, url: str, timeout: int, user_agent: str) -> Tuple[bool, Dict, str, int]:
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-rw_timeout",
        str(timeout * 1_000_000),
        "-analyzeduration",
        "2500000",
        "-probesize",
        "1048576",
        "-user_agent",
        user_agent,
        "-show_entries",
        "stream=index,codec_type,codec_name,width,height,avg_frame_rate,bit_rate:format=format_name,bit_rate",
        "-of",
        "json",
        url,
    ]
    started = time.time()
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout + 3,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, {}, "ffprobe-timeout", int((time.time() - started) * 1000)
    except Exception as exc:
        return False, {}, f"ffprobe-error:{type(exc).__name__}", int((time.time() - started) * 1000)

    startup_ms = int((time.time() - started) * 1000)
    if result.returncode != 0:
        reason = normalize_text(result.stderr)[:180]
        return False, {}, f"ffprobe-fail:{reason or result.returncode}", startup_ms

    try:
        payload = json.loads(result.stdout or "{}")
    except Exception:
        return False, {}, "ffprobe-json-error", startup_ms
    if not isinstance(payload, dict):
        return False, {}, "ffprobe-json-invalid", startup_ms

    streams = payload.get("streams", [])
    has_video = isinstance(streams, list) and any(
        isinstance(stream, dict) and normalize_text(stream.get("codec_type")).lower() == "video" for stream in streams
    )
    if not has_video:
        return False, payload, "ffprobe-no-video", startup_ms
    return True, payload, "ffprobe-ok", startup_ms


def ffmpeg_continuity(ffmpeg_bin: str, url: str, timeout: int, seconds: int, user_agent: str) -> Tuple[bool, str]:
    cmd = [
        ffmpeg_bin,
        "-v",
        "error",
        "-rw_timeout",
        str(timeout * 1_000_000),
        "-user_agent",
        user_agent,
        "-t",
        str(max(4, seconds)),
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
            timeout=max(timeout + 4, seconds + timeout + 2),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "ffmpeg-timeout"
    except Exception as exc:
        return False, f"ffmpeg-error:{type(exc).__name__}"

    if result.returncode == 0:
        return True, "ffmpeg-ok"
    reason = normalize_text(result.stderr)[:180]
    return False, f"ffmpeg-fail:{reason or result.returncode}"


def extract_media(payload: Dict) -> Dict[str, object]:
    streams = payload.get("streams", []) if isinstance(payload, dict) else []
    if not isinstance(streams, list):
        streams = []
    format_payload = payload.get("format", {}) if isinstance(payload, dict) else {}
    if not isinstance(format_payload, dict):
        format_payload = {}

    video = None
    audio = None
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        ctype = normalize_text(stream.get("codec_type")).lower()
        if ctype == "video" and video is None:
            video = stream
        elif ctype == "audio" and audio is None:
            audio = stream

    width = int(video.get("width") or 0) if isinstance(video, dict) else 0
    height = int(video.get("height") or 0) if isinstance(video, dict) else 0
    fps = parse_ratio(video.get("avg_frame_rate")) if isinstance(video, dict) else None
    stream_br = safe_float(video.get("bit_rate")) if isinstance(video, dict) else None
    format_br = safe_float(format_payload.get("bit_rate"))
    bitrate = stream_br if stream_br and stream_br > 0 else format_br
    bitrate_kbps = int((bitrate or 0) / 1000) if bitrate else None

    return {
        "video_codec": normalize_text(video.get("codec_name")) or None if isinstance(video, dict) else None,
        "audio_codec": normalize_text(audio.get("codec_name")) or None if isinstance(audio, dict) else None,
        "width": width or None,
        "height": height or None,
        "fps": round(float(fps), 2) if fps else None,
        "bitrate_kbps": bitrate_kbps,
        "format_name": normalize_text(format_payload.get("format_name")) or None,
    }


def clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def startup_score(startup_ms: Optional[int]) -> float:
    if startup_ms is None:
        return 0.0
    if startup_ms <= 2500:
        return 1.0
    if startup_ms <= 4000:
        return 0.8
    if startup_ms <= 6000:
        return 0.5
    if startup_ms <= 8000:
        return 0.2
    return 0.0


def quality_score(width: Optional[int], height: Optional[int], fps: Optional[float], bitrate_kbps: Optional[int]) -> float:
    score = 0.0
    w = int(width or 0)
    h = int(height or 0)
    if w >= 2560 or h >= 1440:
        score += 0.56
    elif w >= 1920 or h >= 1080:
        score += 0.48
    elif w >= 1280 or h >= 720:
        score += 0.38
    elif w >= 960 or h >= 540:
        score += 0.24
    else:
        score += 0.1

    f = float(fps or 0.0)
    if f >= 50:
        score += 0.24
    elif f >= 25:
        score += 0.15
    elif f > 0:
        score += 0.08

    br = int(bitrate_kbps or 0)
    if br >= 7000:
        score += 0.2
    elif br >= 4000:
        score += 0.15
    elif br >= 2000:
        score += 0.11
    elif br > 0:
        score += 0.06

    return clamp(score)


def availability_score(history_node: Dict[str, object]) -> Tuple[float, float]:
    tested = int(history_node.get("tested", 0) or 0)
    ok = int(history_node.get("ok", 0) or 0)
    if tested <= 0:
        return 0.65, 0.0
    avail = clamp(ok / max(1, tested))
    trust = clamp(min(tested, 20) / 20.0)
    return avail, trust


def score_stream(
    ffprobe_ok: bool,
    continuity_ok: bool,
    startup_ms: Optional[int],
    media: Dict[str, object],
    history_node: Dict[str, object],
) -> float:
    if not ffprobe_ok:
        return 0.0
    avail, trust = availability_score(history_node)
    continuity = 1.0 if continuity_ok else 0.0
    startup = startup_score(startup_ms)
    quality = quality_score(
        width=media.get("width"),
        height=media.get("height"),
        fps=media.get("fps"),
        bitrate_kbps=media.get("bitrate_kbps"),
    )
    weighted = (0.40 * avail) + (0.20 * startup) + (0.20 * continuity) + (0.15 * quality) + (0.05 * trust)
    return round(100.0 * clamp(weighted), 2)


def quality_from_result(media: Dict[str, object], hint: str) -> str:
    derived = quality_from_dims(media.get("width"), media.get("height"))
    if derived in QUALITY_ORDER:
        return derived
    clean_hint = normalize_text(hint).upper()
    if clean_hint in QUALITY_ORDER:
        return clean_hint
    return "HD"


def stream_entry(result: Dict, hint: str) -> Dict[str, object]:
    media = result.get("media", {})
    return {
        "url": result["url"],
        "url_hash": result["url_hash"],
        "domain": result["domain"],
        "score": result["score"],
        "quality": quality_from_result(media, hint),
        "status": "ok" if result["ok"] else "failed",
        "startup_ms": result.get("startup_ms"),
        "ffprobe_ok": result.get("ffprobe_ok"),
        "continuity_ok": result.get("continuity_ok"),
        "ffprobe_reason": result.get("ffprobe_reason"),
        "continuity_reason": result.get("continuity_reason"),
        "video_codec": media.get("video_codec"),
        "audio_codec": media.get("audio_codec"),
        "width": media.get("width"),
        "height": media.get("height"),
        "fps": media.get("fps"),
        "bitrate_kbps": media.get("bitrate_kbps"),
        "format_name": media.get("format_name"),
        "history_tested": result.get("history_tested"),
        "history_ok": result.get("history_ok"),
        "tested_at": result.get("tested_at"),
        "last_ok_at": result.get("tested_at") if result["ok"] else None,
    }


def append_jsonl(path: str, rows: List[Dict[str, object]]) -> None:
    if not path or not rows:
        return
    with open(path, "a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_candidate(
    candidate: Dict[str, str],
    ffprobe_bin: str,
    ffmpeg_bin: Optional[str],
    timeout: int,
    continuity_seconds: int,
    user_agent: str,
    history_node: Dict[str, object],
) -> Dict:
    ffprobe_ok, payload, ffprobe_reason, startup_ms = ffprobe_probe(
        ffprobe_bin=ffprobe_bin,
        url=candidate["url"],
        timeout=timeout,
        user_agent=user_agent,
    )
    continuity_ok = ffprobe_ok
    continuity_reason = "continuity-skipped"
    if ffprobe_ok and ffmpeg_bin:
        continuity_ok, continuity_reason = ffmpeg_continuity(
            ffmpeg_bin=ffmpeg_bin,
            url=candidate["url"],
            timeout=timeout,
            seconds=continuity_seconds,
            user_agent=user_agent,
        )
    media = extract_media(payload if ffprobe_ok else {})
    tested = int(history_node.get("tested", 0) or 0)
    ok_count = int(history_node.get("ok", 0) or 0)
    score = score_stream(
        ffprobe_ok=ffprobe_ok,
        continuity_ok=continuity_ok,
        startup_ms=startup_ms if ffprobe_ok else None,
        media=media,
        history_node=history_node,
    )
    return {
        **candidate,
        "tested_at": utc_now_iso(),
        "ffprobe_ok": ffprobe_ok,
        "ffprobe_reason": ffprobe_reason,
        "continuity_ok": continuity_ok,
        "continuity_reason": continuity_reason,
        "ok": bool(ffprobe_ok and continuity_ok),
        "startup_ms": startup_ms if ffprobe_ok else None,
        "media": media,
        "history_tested": tested,
        "history_ok": ok_count,
        "score": score,
    }


def choose_backups(pass_results: List[Dict], max_count: int, used_domains: set) -> List[Dict]:
    picked: List[Dict] = []
    for result in pass_results:
        if len(picked) >= max_count:
            break
        domain = result["domain"]
        if domain and domain in used_domains:
            continue
        picked.append(result)
        if domain:
            used_domains.add(domain)
    if len(picked) >= max_count:
        return picked
    for result in pass_results:
        if len(picked) >= max_count:
            break
        if result in picked:
            continue
        picked.append(result)
    return picked


def build_qualities_from_selected(entries: List[Dict[str, object]], max_domains: int) -> Dict[str, List[str]]:
    out = {quality: [] for quality in QUALITY_ORDER}
    seen_domains = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = normalize_text(entry.get("url"))
        if not url:
            continue
        quality = normalize_text(entry.get("quality")).upper()
        if quality not in QUALITY_ORDER:
            quality = "HD"
        domain = normalize_text(entry.get("domain")).lower()
        if domain and domain not in seen_domains and len(seen_domains) >= max_domains:
            continue
        if domain:
            seen_domains.add(domain)
        out[quality].append(url)
    return {quality: urls for quality, urls in out.items() if urls}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank best stream URLs and select primary/backups.")
    parser.add_argument("--channels-file", default="channels.json", help="channels.json path")
    parser.add_argument("--schedule-file", default="weekly_schedule.json", help="schedule file for target channels")
    parser.add_argument("--all-channels", action="store_true", help="process all channels in channels.json")
    parser.add_argument("--log-file", default="stream_health_log.jsonl", help="append-only stream health JSONL log")
    parser.add_argument("--workers", type=int, default=20, help="parallel probe workers")
    parser.add_argument("--timeout", type=int, default=8, help="probe timeout seconds")
    parser.add_argument("--continuity-seconds", type=int, default=10, help="ffmpeg continuity sample seconds")
    parser.add_argument("--disable-continuity", action="store_true", help="disable ffmpeg continuity checks")
    parser.add_argument("--history-days", type=int, default=14, help="history window for availability score")
    parser.add_argument("--max-candidates-per-channel", type=int, default=40, help="candidate cap per channel")
    parser.add_argument("--max-candidates-output", type=int, default=10, help="stored candidate entries per channel")
    parser.add_argument("--max-backups", type=int, default=2, help="backup entries per channel")
    parser.add_argument("--max-reserve", type=int, default=2, help="reserve entries per channel")
    parser.add_argument("--max-streams-per-channel", type=int, default=5, help="max unique domains kept in qualities")
    parser.add_argument("--stale-grace-hours", type=int, default=48, help="keep last known primary if all fail")
    parser.add_argument("--ffprobe-bin", default="ffprobe", help="ffprobe binary path")
    parser.add_argument("--ffmpeg-bin", default="ffmpeg", help="ffmpeg binary path")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent for ffprobe/ffmpeg")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    channels_db = load_json(args.channels_file)
    channels_node = channels_db.get("channels", {}) if isinstance(channels_db, dict) else {}
    if not isinstance(channels_node, dict) or not channels_node:
        print(f"No channels found in {args.channels_file}.")
        return 1

    ffprobe_bin = shutil.which(args.ffprobe_bin)
    if not ffprobe_bin:
        print("ffprobe not found in PATH.")
        return 1
    ffmpeg_bin = None if args.disable_continuity else shutil.which(args.ffmpeg_bin)
    if not args.disable_continuity and not ffmpeg_bin:
        print("ffmpeg not found; continuity checks disabled.")

    if args.all_channels:
        target_names = list(channels_node.keys())
    else:
        target_names = load_targets_from_schedule(args.schedule_file)
    if not target_names:
        print("No target channels found. Nothing to rank.")
        return 0

    existing_names = list(channels_node.keys())
    candidates: List[Dict[str, str]] = []
    by_channel_hints: Dict[str, Dict[str, str]] = {}
    for target_name in target_names:
        canonical = match_channel_name(existing_names, target_name)
        if not canonical:
            continue
        node = channels_node.get(canonical)
        if not isinstance(node, dict):
            continue
        urls = iter_channel_urls(node)
        if args.max_candidates_per_channel > 0:
            urls = urls[: args.max_candidates_per_channel]
        hints = by_channel_hints.setdefault(canonical, {})
        for url, hint in urls:
            hints[url] = hint
            candidates.append(
                {
                    "channel": canonical,
                    "url": url,
                    "url_hash": url_hash(url),
                    "domain": domain_from_url(url),
                    "hint": hint,
                }
            )

    if not candidates:
        print("No stream candidates found.")
        return 0

    history = load_history(args.log_file, days=max(1, args.history_days))
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"[RANK] run={run_id} channels={len(by_channel_hints)} candidates={len(candidates)}")

    probe_results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                test_candidate,
                candidate,
                ffprobe_bin,
                ffmpeg_bin,
                max(1, args.timeout),
                max(4, args.continuity_seconds),
                args.user_agent,
                history.get(candidate["url_hash"], {}),
            ): candidate
            for candidate in candidates
        }
        total = len(futures)
        for idx, future in enumerate(as_completed(futures), start=1):
            probe_results.append(future.result())
            if idx % 50 == 0 or idx == total:
                print(f"  [RANK] probe progress {idx}/{total}")

    by_channel_results: Dict[str, List[Dict]] = {}
    for result in probe_results:
        by_channel_results.setdefault(result["channel"], []).append(result)
    for channel_name in by_channel_results:
        by_channel_results[channel_name].sort(key=lambda item: (-item["score"], item.get("startup_ms") or 9999999))

    stale_cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=max(1, args.stale_grace_hours))
    log_rows: List[Dict[str, object]] = []
    channels_with_primary = 0
    tested_total = 0
    passing_total = 0

    for channel_name, ranked in by_channel_results.items():
        node = channels_node.get(channel_name)
        if not isinstance(node, dict):
            continue
        tested_total += len(ranked)
        passing = [item for item in ranked if item["ok"]]
        passing_total += len(passing)

        primary = passing[0] if passing else None
        backups = []
        reserve = []
        if primary:
            used_domains = {primary["domain"]} if primary.get("domain") else set()
            backups = choose_backups(passing[1:], max(0, args.max_backups), used_domains)
            used_domains.update(item.get("domain") for item in backups if item.get("domain"))
            reserve = choose_backups(
                [item for item in passing[1:] if item not in backups],
                max(0, args.max_reserve),
                used_domains,
            )

        if primary is None:
            prev_primary = node.get("primary")
            prev_ok_at = parse_iso_datetime(prev_primary.get("last_ok_at")) if isinstance(prev_primary, dict) else None
            if isinstance(prev_primary, dict) and prev_ok_at and prev_ok_at >= stale_cutoff:
                node["primary"] = {**prev_primary, "stale_fallback": True}
                node["backups"] = node.get("backups") if isinstance(node.get("backups"), list) else []
                node["reserve"] = node.get("reserve") if isinstance(node.get("reserve"), list) else []
            else:
                node["primary"] = None
                node["backups"] = []
                node["reserve"] = []
        else:
            primary_entry = stream_entry(primary, primary.get("hint", "HD"))
            backup_entries = [stream_entry(item, item.get("hint", "HD")) for item in backups]
            reserve_entries = [stream_entry(item, item.get("hint", "HD")) for item in reserve]
            node["primary"] = primary_entry
            node["backups"] = backup_entries
            node["reserve"] = reserve_entries
            selected = [primary_entry] + backup_entries + reserve_entries
            node["qualities"] = build_qualities_from_selected(selected, max_domains=max(1, args.max_streams_per_channel))

        node["candidates"] = [stream_entry(item, item.get("hint", "HD")) for item in ranked[: max(1, args.max_candidates_output)]]
        node["stream_selection"] = {
            "policy_version": POLICY_VERSION,
            "run_id": run_id,
            "selected_at": utc_now_iso(),
            "tested_candidates": len(ranked),
            "passing_candidates": len(passing),
            "primary_score": node.get("primary", {}).get("score") if isinstance(node.get("primary"), dict) else None,
        }

        if isinstance(node.get("primary"), dict):
            channels_with_primary += 1

        for result in ranked:
            log_rows.append(
                {
                    "run_id": run_id,
                    "tested_at": result["tested_at"],
                    "channel": channel_name,
                    "channel_id": node.get("id") if isinstance(node.get("id"), int) else None,
                    "url": result["url"],
                    "url_hash": result["url_hash"],
                    "domain": result["domain"],
                    "ok": result["ok"],
                    "score": result["score"],
                    "ffprobe_ok": result["ffprobe_ok"],
                    "continuity_ok": result["continuity_ok"],
                    "ffprobe_reason": result["ffprobe_reason"],
                    "continuity_reason": result["continuity_reason"],
                    "startup_ms": result["startup_ms"],
                    "video_codec": result["media"].get("video_codec"),
                    "audio_codec": result["media"].get("audio_codec"),
                    "width": result["media"].get("width"),
                    "height": result["media"].get("height"),
                    "fps": result["media"].get("fps"),
                    "bitrate_kbps": result["media"].get("bitrate_kbps"),
                    "format_name": result["media"].get("format_name"),
                    "policy_version": POLICY_VERSION,
                }
            )

    append_jsonl(args.log_file, log_rows)

    metadata = channels_db.setdefault("metadata", {})
    metadata["best_stream_ranker"] = {
        "policy_version": POLICY_VERSION,
        "run_id": run_id,
        "ranked_at": utc_now_iso(),
        "target_channels": len(by_channel_results),
        "channels_with_primary": channels_with_primary,
        "tested_candidates": tested_total,
        "passing_candidates": passing_total,
        "workers": max(1, args.workers),
        "timeout_seconds": max(1, args.timeout),
        "continuity_seconds": max(4, args.continuity_seconds),
        "history_days": max(1, args.history_days),
        "log_file": args.log_file,
    }
    save_json(args.channels_file, channels_db)

    print(
        f"[RANK] done run={run_id} channels={len(by_channel_results)} "
        f"primary={channels_with_primary} tested={tested_total} pass={passing_total}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
