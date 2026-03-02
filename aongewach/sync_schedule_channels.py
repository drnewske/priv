#!/usr/bin/env python3
"""
Sync schedule channel names into channels.json with stable IDs.

This script does not test streams. It ensures every channel present in the
schedule exists in channels.json, reusing existing IDs when available and
assigning deterministic IDs for new channels.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import zlib
from typing import Dict, List, Set

from channel_name_placeholders import is_placeholder_channel_name


NON_BROADCAST_WORD_RE = re.compile(r"\b(app|website|web\s*site|youtube|radio)\b", re.IGNORECASE)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)
NOT_TELEVISED_RE = re.compile(
    r"\b(not\s+televised|not\s+on\s+tv|no\s+tv|no\s+broadcast)\b",
    re.IGNORECASE,
)


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def load_json(path: str):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def save_json(path: str, payload) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def is_usable_channel_name(name: str) -> bool:
    cleaned = normalize_text(name)
    if not cleaned:
        return False
    if is_placeholder_channel_name(cleaned):
        return False
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    if NOT_TELEVISED_RE.search(cleaned):
        return False
    return True


def stable_channel_id(name: str) -> int:
    return zlib.adler32(name.encode("utf-8")) & 0xFFFFFFFF


def collect_schedule_channels(schedule_payload: Dict) -> List[str]:
    unique: List[str] = []
    seen: Set[str] = set()
    for day in schedule_payload.get("schedule", []) if isinstance(schedule_payload.get("schedule"), list) else []:
        for event in day.get("events", []) if isinstance(day.get("events"), list) else []:
            channels = event.get("channels", []) if isinstance(event, dict) else []
            if not isinstance(channels, list):
                continue
            for raw in channels:
                text = normalize_text(raw)
                if not is_usable_channel_name(text):
                    continue
                key = text.casefold()
                if key in seen:
                    continue
                seen.add(key)
                unique.append(text)
    return unique


def sync_channels(schedule_payload: Dict, channels_db: Dict) -> Dict:
    if not isinstance(channels_db, dict):
        channels_db = {}
    channels_node = channels_db.get("channels")
    if not isinstance(channels_node, dict):
        channels_node = {}
        channels_db["channels"] = channels_node

    existing_by_lower = {name.casefold(): name for name in channels_node.keys()}
    added = 0
    assigned_missing_ids = 0

    # Backfill IDs for existing channels missing an ID.
    for display_name, payload in channels_node.items():
        if not isinstance(payload, dict):
            channels_node[display_name] = {"id": stable_channel_id(display_name), "logo": None, "qualities": {}}
            assigned_missing_ids += 1
            continue
        if not isinstance(payload.get("id"), int):
            payload["id"] = stable_channel_id(display_name)
            assigned_missing_ids += 1
        if "qualities" not in payload:
            payload["qualities"] = {}
        if "logo" not in payload:
            payload["logo"] = None

    for channel_name in collect_schedule_channels(schedule_payload):
        key = channel_name.casefold()
        if key in existing_by_lower:
            canonical = existing_by_lower[key]
            node = channels_node.get(canonical)
            if isinstance(node, dict) and not isinstance(node.get("id"), int):
                node["id"] = stable_channel_id(canonical)
                assigned_missing_ids += 1
            continue

        channels_node[channel_name] = {
            "id": stable_channel_id(channel_name),
            "logo": None,
            "qualities": {},
        }
        existing_by_lower[key] = channel_name
        added += 1

    metadata = channels_db.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        channels_db["metadata"] = metadata
    metadata["updated_at"] = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metadata["unique_channels"] = len(channels_node)
    metadata["schedule_channel_sync"] = {
        "added_channels": added,
        "assigned_missing_ids": assigned_missing_ids,
    }

    return channels_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync schedule channels into channels.json with stable IDs.")
    parser.add_argument("--schedule", default="weekly_schedule.json", help="Input schedule file path.")
    parser.add_argument("--channels", default="channels.json", help="Input/output channels file path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    schedule_payload = load_json(args.schedule)
    if not isinstance(schedule_payload.get("schedule"), list):
        print(f"Invalid schedule payload: {args.schedule}")
        return 1

    channels_payload = load_json(args.channels)
    merged = sync_channels(schedule_payload, channels_payload)
    save_json(args.channels, merged)

    sync_meta = merged.get("metadata", {}).get("schedule_channel_sync", {})
    print(
        f"[SYNC] Wrote {args.channels} | added={int(sync_meta.get('added_channels', 0))} "
        f"id_backfilled={int(sync_meta.get('assigned_missing_ids', 0))} "
        f"total={int(merged.get('metadata', {}).get('unique_channels', 0))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
