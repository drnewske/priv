#!/usr/bin/env python3
"""Map schedule channels to channel IDs and apply final per-event geo cap."""

import argparse
import json
import os
import re
import time
from typing import Dict, Optional

from channel_name_placeholders import is_placeholder_channel_name
from channel_selection import (
    index_channel_candidates,
    load_geo_rules,
    select_mapped_event_channels,
    split_mapped_channel_entry,
)

# Configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEDULE_FILE = os.path.join(SCRIPT_DIR, "weekly_schedule.json")
SCHEDULE_FILE_FALLBACK = os.path.join(SCRIPT_DIR, "weekly_schedule_mapped.json")
CHANNELS_FILE = os.path.join(SCRIPT_DIR, "channels.json")
MAP_FILE = os.path.join(SCRIPT_DIR, "channel_map.json")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "e104f869d64e3d41256d5398.json")
DEFAULT_GEO_RULES_FILE = os.path.join(SCRIPT_DIR, "channel_geo_rules.json")

NON_BROADCAST_WORD_RE = re.compile(r"\b(app|website|web\s*site|youtube|radio)\b", re.IGNORECASE)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)


def load_json(filepath: str):
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def save_json(filepath: str, data) -> None:
    with open(filepath, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)


def is_usable_channel_name(name: str) -> bool:
    cleaned = " ".join(str(name or "").strip().split())
    if not cleaned:
        return False
    if is_placeholder_channel_name(cleaned):
        return False
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    return True


def build_exact_lookup(iptv_channels: Dict):
    """Build exact and case-insensitive lookup maps for channel names."""
    name_to_id = {}
    name_to_id_lower = {}
    id_to_channel = {}
    for name, payload in iptv_channels.items():
        if not isinstance(payload, dict):
            continue
        cid = payload.get("id")
        if not isinstance(cid, int):
            continue
        name_to_id[name] = cid
        lower = name.lower()
        if lower not in name_to_id_lower:
            name_to_id_lower[lower] = cid
        id_to_channel[cid] = payload
    return name_to_id, name_to_id_lower, id_to_channel


def resolve_channel_id(
    channel_name: str,
    saved_map: Dict,
    name_to_id: Dict,
    name_to_id_lower: Dict,
    id_to_channel: Dict,
) -> Optional[int]:
    cid = None

    if channel_name in saved_map:
        mapped_value = saved_map[channel_name]
        if isinstance(mapped_value, int):
            if mapped_value in id_to_channel:
                cid = mapped_value
        elif isinstance(mapped_value, str):
            if mapped_value in name_to_id:
                cid = name_to_id[mapped_value]
                saved_map[channel_name] = cid

    if cid is None:
        cid = name_to_id.get(channel_name)
    if cid is None:
        cid = name_to_id_lower.get(channel_name.lower())

    if isinstance(cid, int):
        saved_map[channel_name] = cid
        return cid
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map schedule channels and enforce per-event geo cap.")
    parser.add_argument("--schedule-file", default=SCHEDULE_FILE, help="Primary schedule file path.")
    parser.add_argument(
        "--schedule-fallback",
        default=SCHEDULE_FILE_FALLBACK,
        help="Fallback schedule file path if primary is missing/empty.",
    )
    parser.add_argument("--channels-file", default=CHANNELS_FILE, help="channels.json file path.")
    parser.add_argument("--map-file", default=MAP_FILE, help="channel_map.json file path.")
    parser.add_argument("--output-file", default=OUTPUT_FILE, help="Final mapped output file path.")
    parser.add_argument(
        "--geo-rules-file",
        default=DEFAULT_GEO_RULES_FILE,
        help="Path to channel geo rules JSON (default: aongewach/channel_geo_rules.json).",
    )
    return parser.parse_args()


def map_channels(args: argparse.Namespace) -> int:
    print("Loading data...")
    schedule_data = load_json(args.schedule_file)
    schedule_source = args.schedule_file
    if not schedule_data:
        schedule_data = load_json(args.schedule_fallback)
        schedule_source = args.schedule_fallback
    channels_db = load_json(args.channels_file)
    saved_map = load_json(args.map_file)
    geo_rules = load_geo_rules(args.geo_rules_file)

    if not schedule_data or not channels_db:
        print("Missing input files.")
        return 1
    print(f"Using schedule file: {schedule_source}")

    iptv_channels = channels_db.get("channels", {})
    print("Building exact lookup...")
    t0 = time.time()
    name_to_id, name_to_id_lower, id_to_channel = build_exact_lookup(iptv_channels)
    print(f"Lookup built in {time.time() - t0:.2f}s")

    total_resolved = 0
    unique_channels_mapped = 0
    processed_channels = set()
    total_channel_entries = 0
    unresolved_entries = 0
    events_capped = 0
    events_with_mapped_candidates = 0
    selected_uk = 0
    selected_us = 0
    selected_other = 0
    selected_other_preferred = 0
    selected_total = 0

    print("Mapping channels...")
    t0 = time.time()
    days = schedule_data.get("schedule", [])

    for day_index, day in enumerate(days, start=1):
        day_label = day.get("date", f"day-{day_index}")
        day_events = day.get("events", [])
        print(f"  > Day {day_index}/{len(days)} ({day_label}) - {len(day_events)} events")

        for event in day_events:
            event_channels = event.get("channels", [])
            resolved_entries = []

            for raw_entry in event_channels:
                channel_name, mapped_suffix = split_mapped_channel_entry(raw_entry)
                if not channel_name:
                    continue
                total_channel_entries += 1
                if not is_usable_channel_name(channel_name):
                    unresolved_entries += 1
                    continue

                cid = resolve_channel_id(
                    channel_name=channel_name,
                    saved_map=saved_map,
                    name_to_id=name_to_id,
                    name_to_id_lower=name_to_id_lower,
                    id_to_channel=id_to_channel,
                )

                if cid is None and mapped_suffix and mapped_suffix != "null":
                    # Respect an already-mapped integer suffix if present and valid in channels DB.
                    try:
                        parsed_id = int(mapped_suffix)
                    except (TypeError, ValueError):
                        parsed_id = None
                    if isinstance(parsed_id, int) and parsed_id in id_to_channel:
                        cid = parsed_id
                        saved_map[channel_name] = parsed_id

                if isinstance(cid, int):
                    total_resolved += 1
                    if channel_name not in processed_channels:
                        unique_channels_mapped += 1
                        processed_channels.add(channel_name)
                    resolved_entries.append(
                        {
                            "name": channel_name,
                            "id": cid,
                            "raw": f"{channel_name}, {cid}",
                        }
                    )
                else:
                    unresolved_entries += 1

            candidate_index = index_channel_candidates(event.get("channel_candidates", []))

            selected_entries, selection_stats = select_mapped_event_channels(
                mapped_entries=resolved_entries,
                rules=geo_rules,
                candidate_index=candidate_index,
            )
            if len(resolved_entries) > 0:
                events_with_mapped_candidates += 1
            if len(selected_entries) < len(resolved_entries):
                events_capped += 1

            event["channels"] = [entry["raw"] for entry in selected_entries]
            # Keep final mapped payload simple: drop internal enrichment metadata.
            if "channel_candidates" in event:
                del event["channel_candidates"]
            if "channel_country_groups" in event:
                del event["channel_country_groups"]
            if "mapped_channels" in event:
                del event["mapped_channels"]

            selected_total += int(selection_stats.get("selected_total", 0))
            selected_uk += int(selection_stats.get("selected_uk", 0))
            selected_us += int(selection_stats.get("selected_us", 0))
            selected_other += int(selection_stats.get("selected_other", 0))
            selected_other_preferred += int(selection_stats.get("selected_other_preferred", 0))

    print(f"Mapping + capping finished in {time.time() - t0:.2f}s")

    save_json(args.output_file, schedule_data)
    save_json(args.map_file, saved_map)

    print(f"Done. Resolved {total_resolved} channel entries to stable channel IDs.")
    print(f"Unresolved channel entries (excluded from final): {unresolved_entries}/{total_channel_entries}")
    print(f"Learned {unique_channels_mapped} new ID mappings.")
    print(f"Events with mapped channel candidates: {events_with_mapped_candidates}")
    print(f"Events trimmed by geo cap: {events_capped}")
    print(
        "Final selected distribution: "
        f"total={selected_total}, uk={selected_uk}, us={selected_us}, "
        f"other={selected_other}, other_preferred={selected_other_preferred}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(map_channels(parse_args()))
