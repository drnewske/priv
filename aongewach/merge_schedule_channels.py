#!/usr/bin/env python3
"""
Merge weekly FANZO schedule with Where's The Match channels.

Exact match key:
  - Same day
  - Same normalized event name
  - Same normalized event time (UTC minute when ISO is present, otherwise HH:MM)
  - Optional sport must match when available

For matched events, channels are unioned:
  merged = FANZO channels + any missing channels from Where's The Match.
"""

import argparse
import datetime as dt
import json
import os
import re
import sys
from typing import Dict, List, Optional, Tuple


WHITESPACE_RE = re.compile(r"\s+")
VS_TOKEN_RE = re.compile(r"\s+(?:v|vs)\s+", re.IGNORECASE)
YEAR_TOKEN_RE = re.compile(r"\b20\d{2}\b")
NON_WORD_RE = re.compile(r"[^a-z0-9\s]")


def load_json(path: str) -> Dict:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: str, payload: Dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return WHITESPACE_RE.sub(" ", value.strip()).lower()


def canonical_event_name(value: object) -> str:
    """
    Canonical event name normalization (deterministic, non-fuzzy):
      - lowercase
      - standardize 'v'/'vs' separator
      - normalize (W)/(M) markers
      - remove year tokens and punctuation
      - collapse spaces
    """
    text = normalize_text(value)
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = text.replace("(w)", " women ").replace("(m)", " men ")
    text = VS_TOKEN_RE.sub(" vs ", text)
    text = YEAR_TOKEN_RE.sub(" ", text)
    text = NON_WORD_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def parse_iso_to_clock(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
        return parsed.strftime("%H:%M")
    except ValueError:
        pass
    # Fallback for unknown ISO-ish strings.
    match = re.search(r"\b(\d{2}):(\d{2})\b", text)
    if not match:
        return None
    return f"{match.group(1)}:{match.group(2)}"


def normalize_time_key(_day_date: str, event: Dict) -> Optional[str]:
    iso_clock = parse_iso_to_clock(event.get("start_time_iso"))
    if iso_clock:
        return iso_clock

    raw_time = event.get("time")
    if not isinstance(raw_time, str):
        return None
    cleaned = raw_time.strip()
    if not cleaned:
        return None
    try:
        parsed = dt.datetime.strptime(cleaned, "%H:%M")
    except ValueError:
        return None
    return parsed.strftime("%H:%M")


def build_name_key(day_date: str, event: Dict) -> Optional[Tuple[str, str]]:
    name_key = canonical_event_name(event.get("name"))
    if not name_key:
        return None
    return (day_date, name_key)


def clean_channels(channels: object) -> List[str]:
    if not isinstance(channels, list):
        return []
    cleaned = []
    seen = set()
    for item in channels:
        if not isinstance(item, str):
            continue
        channel = item.strip()
        if not channel:
            continue
        key = channel.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(channel)
    return cleaned


def merge_channel_lists(fanzo_channels: object, witm_channels: object) -> List[str]:
    merged = clean_channels(fanzo_channels)
    seen = {name.lower() for name in merged}
    for channel in clean_channels(witm_channels):
        key = channel.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(channel)
    return merged


def build_witm_lookup(
    witm_schedule: List[Dict],
) -> Tuple[
    Dict[Tuple[str, str], List[Dict]],
    Dict[Tuple[str, str, str], List[Dict]],
]:
    """
    Build deterministic lookups:
      - by (day, canonical_name)
      - by (day, canonical_name, HH:MM clock)
    """
    by_name: Dict[Tuple[str, str], List[Dict]] = {}
    by_name_time: Dict[Tuple[str, str, str], List[Dict]] = {}

    for day in witm_schedule:
        day_date = day.get("date")
        if not isinstance(day_date, str):
            continue
        for event in day.get("events", []):
            if not isinstance(event, dict):
                continue
            name_key = build_name_key(day_date, event)
            if not name_key:
                continue

            by_name.setdefault(name_key, []).append(event)

            time_key = normalize_time_key(day_date, event)
            if time_key:
                by_name_time.setdefault((name_key[0], name_key[1], time_key), []).append(event)

    return by_name, by_name_time


def collect_channels_from_events(events: List[Dict]) -> List[str]:
    merged = []
    seen = set()
    for event in events:
        for channel in clean_channels(event.get("channels")):
            key = channel.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(channel)
    return merged


def merge_payloads(fanzo_payload: Dict, witm_payload: Dict) -> Tuple[Dict, Dict[str, int]]:
    fanzo_schedule = fanzo_payload.get("schedule")
    witm_schedule = witm_payload.get("schedule")
    if not isinstance(fanzo_schedule, list):
        raise ValueError("Invalid FANZO payload: missing schedule array.")
    if not isinstance(witm_schedule, list):
        raise ValueError("Invalid Where's The Match payload: missing schedule array.")

    witm_by_name, witm_by_name_time = build_witm_lookup(witm_schedule)
    matched_events = 0
    channels_added = 0
    ambiguous_skips = 0
    matched_by_name_only = 0
    matched_by_name_and_time = 0

    for day in fanzo_schedule:
        day_date = day.get("date")
        if not isinstance(day_date, str):
            continue

        for event in day.get("events", []):
            if not isinstance(event, dict):
                continue
            name_key = build_name_key(day_date, event)
            if not name_key:
                continue

            candidates_by_name = witm_by_name.get(name_key, [])
            if not candidates_by_name:
                continue

            before_channels = clean_channels(event.get("channels"))
            fanzo_clock = normalize_time_key(day_date, event)

            matched_candidates: List[Dict] = []
            if fanzo_clock:
                matched_candidates = witm_by_name_time.get(
                    (name_key[0], name_key[1], fanzo_clock),
                    [],
                )
                if matched_candidates:
                    matched_by_name_and_time += 1

            if not matched_candidates:
                if len(candidates_by_name) == 1:
                    matched_candidates = candidates_by_name
                    matched_by_name_only += 1
                else:
                    # If all duplicates share one clock, treat as one event split across rows/channels.
                    clocks = {
                        normalize_time_key(day_date, candidate)
                        for candidate in candidates_by_name
                        if normalize_time_key(day_date, candidate)
                    }
                    if len(clocks) == 1:
                        matched_candidates = candidates_by_name
                        matched_by_name_only += 1
                    else:
                        ambiguous_skips += 1
                        continue

            merged_channels = merge_channel_lists(
                before_channels,
                collect_channels_from_events(matched_candidates),
            )
            event["channels"] = merged_channels

            matched_events += 1
            if len(merged_channels) > len(before_channels):
                channels_added += len(merged_channels) - len(before_channels)

    enrichment = fanzo_payload.setdefault("channel_enrichment", {})
    enrichment["merged_at"] = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    enrichment["sources"] = ["fanzo.com", "wheresthematch.com"]
    enrichment["match_mode"] = "canonical-name-with-time-disambiguation"
    enrichment["matched_events"] = matched_events
    enrichment["channels_added_from_witm"] = channels_added
    enrichment["matched_by_name_and_time"] = matched_by_name_and_time
    enrichment["matched_by_name_only"] = matched_by_name_only
    enrichment["ambiguous_match_keys_skipped"] = ambiguous_skips

    return fanzo_payload, {
        "matched_events": matched_events,
        "channels_added": channels_added,
        "matched_by_name_and_time": matched_by_name_and_time,
        "matched_by_name_only": matched_by_name_only,
        "ambiguous_skips": ambiguous_skips,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge FANZO weekly schedule with Where's The Match channels."
    )
    parser.add_argument("--fanzo", default="weekly_schedule.json", help="Path to FANZO schedule JSON")
    parser.add_argument(
        "--witm",
        default="weekly_schedule_witm.json",
        help="Path to Where's The Match schedule JSON",
    )
    parser.add_argument(
        "--output",
        default="weekly_schedule.json",
        help="Path for merged output JSON",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        fanzo_payload = load_json(args.fanzo)
    except Exception as exc:
        print(f"Failed to load FANZO schedule '{args.fanzo}': {exc}", file=sys.stderr)
        return 1

    try:
        witm_payload = load_json(args.witm)
    except Exception as exc:
        print(f"Failed to load Where's The Match schedule '{args.witm}': {exc}", file=sys.stderr)
        return 1

    try:
        merged_payload, stats = merge_payloads(fanzo_payload, witm_payload)
    except Exception as exc:
        print(f"Merge failed: {exc}", file=sys.stderr)
        return 1

    try:
        save_json(args.output, merged_payload)
    except Exception as exc:
        print(f"Failed to save merged schedule to '{args.output}': {exc}", file=sys.stderr)
        return 1

    print(f"Merged schedule written to {args.output}")
    print(f"  Matched events: {stats['matched_events']}")
    print(f"  Channels added from Where's The Match: {stats['channels_added']}")
    print(f"  Matched by name+time: {stats['matched_by_name_and_time']}")
    print(f"  Matched by name only: {stats['matched_by_name_only']}")
    print(f"  Ambiguous keys skipped: {stats['ambiguous_skips']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
