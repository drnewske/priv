#!/usr/bin/env python3
"""
Merge FANZO non-soccer schedule with WITM enrichment.

Exact match key:
  - Same day
  - Same canonical event name
  - Same clock time (HH:MM) when available, else deterministic name-only fallback

WITM is used to reinforce FANZO with:
  - Additional channels
  - Sport/competition logos
"""

from __future__ import annotations

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

NON_BROADCAST_WORD_RE = re.compile(
    r"\b(app|website|web\s*site|youtube|radio)\b",
    re.IGNORECASE,
)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)


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
    return WHITESPACE_RE.sub(" ", value.strip())


def normalize_key_text(value: object) -> str:
    return normalize_text(value).lower()


def canonical_event_name(value: object) -> str:
    text = normalize_key_text(value)
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = text.replace("(w)", " women ").replace("(m)", " men ")
    text = VS_TOKEN_RE.sub(" vs ", text)
    text = YEAR_TOKEN_RE.sub(" ", text)
    text = NON_WORD_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def parse_iso_to_clock(value: object) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
        return parsed.strftime("%H:%M")
    except ValueError:
        pass
    match = re.search(r"\b(\d{2}):(\d{2})\b", text)
    if not match:
        return None
    return f"{match.group(1)}:{match.group(2)}"


def normalize_time_key(_day_date: str, event: Dict) -> Optional[str]:
    iso_clock = parse_iso_to_clock(event.get("start_time_iso"))
    if iso_clock:
        return iso_clock

    raw_time = normalize_text(event.get("time"))
    if not raw_time:
        return None
    try:
        parsed = dt.datetime.strptime(raw_time, "%H:%M")
    except ValueError:
        return None
    return parsed.strftime("%H:%M")


def build_name_key(day_date: str, event: Dict) -> Optional[Tuple[str, str]]:
    name_key = canonical_event_name(event.get("name"))
    if not name_key:
        return None
    return (day_date, name_key)


def is_usable_channel_name(name: str) -> bool:
    cleaned = normalize_text(name)
    if not cleaned:
        return False
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    return True


def clean_channels(channels: object) -> List[str]:
    if not isinstance(channels, list):
        return []
    cleaned: List[str] = []
    seen = set()
    for item in channels:
        if not isinstance(item, str):
            continue
        channel = normalize_text(item)
        if not channel:
            continue
        if not is_usable_channel_name(channel):
            continue
        key = channel.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(channel)
    return cleaned


def merge_channel_lists(fanzo_channels: object, witm_channels: object) -> List[str]:
    merged = clean_channels(fanzo_channels)
    seen = {name.casefold() for name in merged}
    for channel in clean_channels(witm_channels):
        key = channel.casefold()
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
    merged: List[str] = []
    seen = set()
    for event in events:
        for channel in clean_channels(event.get("channels")):
            key = channel.casefold()
            if key in seen:
                continue
            seen.add(key)
            merged.append(channel)
    return merged


def pick_first_logo(events: List[Dict], *keys: str) -> Optional[str]:
    for event in events:
        if not isinstance(event, dict):
            continue
        for key in keys:
            value = normalize_text(event.get(key))
            if value:
                return value
    return None


def build_sport_logo_map(witm_schedule: List[Dict]) -> Dict[str, str]:
    by_sport: Dict[str, str] = {}
    for day in witm_schedule:
        for event in day.get("events", []) if isinstance(day.get("events"), list) else []:
            if not isinstance(event, dict):
                continue
            sport_key = normalize_key_text(event.get("sport"))
            if not sport_key or sport_key in by_sport:
                continue
            logo = normalize_text(event.get("sport_logo")) or normalize_text(event.get("competition_logo"))
            if logo:
                by_sport[sport_key] = logo
    return by_sport


def merge_payloads(fanzo_payload: Dict, witm_payload: Dict) -> Tuple[Dict, Dict[str, int]]:
    fanzo_schedule = fanzo_payload.get("schedule")
    witm_schedule = witm_payload.get("schedule")
    if not isinstance(fanzo_schedule, list):
        raise ValueError("Invalid FANZO payload: missing schedule array.")
    if not isinstance(witm_schedule, list):
        raise ValueError("Invalid WITM payload: missing schedule array.")

    witm_by_name, witm_by_name_time = build_witm_lookup(witm_schedule)
    witm_sport_logos = build_sport_logo_map(witm_schedule)

    matched_events = 0
    channels_added = 0
    logos_enriched = 0
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

            event["channels"] = clean_channels(event.get("channels"))

            name_key = build_name_key(day_date, event)
            if not name_key:
                continue

            candidates_by_name = witm_by_name.get(name_key, [])
            if not candidates_by_name:
                sport_key = normalize_key_text(event.get("sport"))
                if sport_key and not normalize_text(event.get("sport_logo")) and sport_key in witm_sport_logos:
                    event["sport_logo"] = witm_sport_logos[sport_key]
                    logos_enriched += 1
                continue

            before_channels = clean_channels(event.get("channels"))
            fanzo_clock = normalize_time_key(day_date, event)

            matched_candidates: List[Dict] = []
            if fanzo_clock:
                matched_candidates = witm_by_name_time.get((name_key[0], name_key[1], fanzo_clock), [])
                if matched_candidates:
                    matched_by_name_and_time += 1

            if not matched_candidates:
                if len(candidates_by_name) == 1:
                    matched_candidates = candidates_by_name
                    matched_by_name_only += 1
                else:
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

            merged_channels = merge_channel_lists(before_channels, collect_channels_from_events(matched_candidates))
            event["channels"] = merged_channels

            # Fill logo fields from WITM when missing.
            before_logo = normalize_text(event.get("sport_logo"))
            before_comp_logo = normalize_text(event.get("competition_logo"))
            sport_logo = pick_first_logo(matched_candidates, "sport_logo", "competition_logo")
            comp_logo = pick_first_logo(matched_candidates, "competition_logo", "sport_logo")
            if not before_logo and sport_logo:
                event["sport_logo"] = sport_logo
                logos_enriched += 1
            if not before_comp_logo and comp_logo:
                event["competition_logo"] = comp_logo

            matched_events += 1
            if len(merged_channels) > len(before_channels):
                channels_added += len(merged_channels) - len(before_channels)

    enrichment = fanzo_payload.setdefault("channel_enrichment", {})
    enrichment["merged_at"] = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    enrichment["sources"] = ["fanzo.com", "wheresthematch.com"]
    enrichment["match_mode"] = "canonical-name-with-time-disambiguation"
    enrichment["matched_events"] = matched_events
    enrichment["channels_added_from_witm"] = channels_added
    enrichment["logos_added_from_witm"] = logos_enriched
    enrichment["matched_by_name_and_time"] = matched_by_name_and_time
    enrichment["matched_by_name_only"] = matched_by_name_only
    enrichment["ambiguous_match_keys_skipped"] = ambiguous_skips

    return fanzo_payload, {
        "matched_events": matched_events,
        "channels_added": channels_added,
        "logos_enriched": logos_enriched,
        "matched_by_name_and_time": matched_by_name_and_time,
        "matched_by_name_only": matched_by_name_only,
        "ambiguous_skips": ambiguous_skips,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge FANZO schedule with WITM channels/logos.")
    parser.add_argument("--fanzo", default="weekly_schedule_fanzo.json", help="Path to FANZO schedule JSON.")
    parser.add_argument("--witm", default="weekly_schedule_witm.json", help="Path to WITM schedule JSON.")
    parser.add_argument(
        "--output",
        default="weekly_schedule_fanzo_enriched.json",
        help="Path for merged FANZO+WITM JSON.",
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
        print(f"Failed to load WITM schedule '{args.witm}': {exc}", file=sys.stderr)
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

    print(f"[MERGE] Wrote {args.output}")
    print(f"  Matched events: {stats['matched_events']}")
    print(f"  Channels added from WITM: {stats['channels_added']}")
    print(f"  Logos added from WITM: {stats['logos_enriched']}")
    print(f"  Matched by name+time: {stats['matched_by_name_and_time']}")
    print(f"  Matched by name only: {stats['matched_by_name_only']}")
    print(f"  Ambiguous keys skipped: {stats['ambiguous_skips']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
