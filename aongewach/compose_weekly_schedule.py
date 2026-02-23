#!/usr/bin/env python3
"""
Compose final weekly schedule:
  - Soccer/football events from LiveSportTV only
  - Non-soccer events from FANZO(+WITM enrichment) only
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
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def save_json(path: str, payload: Dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def normalize_text(value: object) -> str:
    return WHITESPACE_RE.sub(" ", str(value or "").strip())


def normalize_key_text(value: object) -> str:
    return normalize_text(value).casefold()


def is_soccer_sport(sport: object) -> bool:
    key = normalize_key_text(sport)
    if not key:
        return False
    explicit_non_soccer = (
        "american football",
        "australian rules",
        "gaelic football",
        "nfl",
    )
    if any(token in key for token in explicit_non_soccer):
        return False
    return key == "soccer" or key == "football" or "soccer" in key


def is_livesporttv_soccer_event(event: Dict) -> bool:
    if is_soccer_sport(event.get("sport")):
        return True
    match_url = normalize_key_text(event.get("match_url"))
    if "/soccer/" in match_url:
        return True
    sport_id = normalize_key_text(event.get("sport_id"))
    return sport_id == "1"


def clean_channels(channels: object) -> List[str]:
    if not isinstance(channels, list):
        return []
    out: List[str] = []
    seen = set()
    for item in channels:
        if not isinstance(item, str):
            continue
        channel = normalize_text(item)
        if not channel:
            continue
        if NON_BROADCAST_WORD_RE.search(channel):
            continue
        if DOMAIN_RE.search(channel):
            continue
        key = channel.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(channel)
    return out


def normalize_event(event: Dict) -> Optional[Dict]:
    if not isinstance(event, dict):
        return None
    normalized = dict(event)
    normalized["channels"] = clean_channels(event.get("channels"))
    if not normalized["channels"]:
        return None
    return normalized


def day_name(date_iso: str) -> str:
    try:
        return dt.datetime.strptime(date_iso, "%Y-%m-%d").strftime("%A")
    except ValueError:
        return ""


def day_index(payload: Dict) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    for day in payload.get("schedule", []) if isinstance(payload.get("schedule"), list) else []:
        if not isinstance(day, dict):
            continue
        date_iso = normalize_text(day.get("date"))
        if not date_iso:
            continue
        out[date_iso] = day
    return out


def event_key(event: Dict) -> Tuple[str, str, str]:
    return (
        normalize_key_text(event.get("name")),
        normalize_key_text(event.get("start_time_iso") or event.get("time")),
        normalize_key_text(event.get("sport")),
    )


def sort_events(events: List[Dict]) -> List[Dict]:
    def _key(event: Dict) -> Tuple[str, str]:
        return (
            normalize_text(event.get("start_time_iso") or event.get("time")),
            normalize_key_text(event.get("name")),
        )

    return sorted(events, key=_key)


def compose_payload(livesporttv: Dict, fanzo_witm: Dict) -> Dict:
    lsv_by_date = day_index(livesporttv)
    fw_by_date = day_index(fanzo_witm)
    all_dates = sorted(set(lsv_by_date.keys()) | set(fw_by_date.keys()))

    schedule: List[Dict] = []
    soccer_count = 0
    nonsoccer_count = 0

    for date_iso in all_dates:
        merged_events: List[Dict] = []
        seen = set()

        lsv_events = lsv_by_date.get(date_iso, {}).get("events", [])
        for raw_event in lsv_events if isinstance(lsv_events, list) else []:
            if not isinstance(raw_event, dict):
                continue
            if not is_livesporttv_soccer_event(raw_event):
                continue
            event = normalize_event(raw_event)
            if not event:
                continue
            key = event_key(event)
            if key in seen:
                continue
            seen.add(key)
            merged_events.append(event)
            soccer_count += 1

        fw_events = fw_by_date.get(date_iso, {}).get("events", [])
        for raw_event in fw_events if isinstance(fw_events, list) else []:
            if not isinstance(raw_event, dict):
                continue
            if is_soccer_sport(raw_event.get("sport")):
                continue
            event = normalize_event(raw_event)
            if not event:
                continue
            key = event_key(event)
            if key in seen:
                continue
            seen.add(key)
            merged_events.append(event)
            nonsoccer_count += 1

        schedule.append(
            {
                "date": date_iso,
                "day": day_name(date_iso) or normalize_text(lsv_by_date.get(date_iso, {}).get("day")) or day_name(date_iso),
                "events": sort_events(merged_events),
            }
        )

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "composed:livesporttv-soccer+fanzo-witm-non-soccer",
        "schedule": schedule,
        "composition": {
            "soccer_from": "livesporttv.com",
            "non_soccer_from": "fanzo.com+wheresthematch.com",
            "soccer_events": soccer_count,
            "non_soccer_events": nonsoccer_count,
            "days": len(schedule),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compose final weekly schedule from LSTV + FANZO/WITM.")
    parser.add_argument(
        "--livesporttv",
        default="weekly_schedule_livesporttv.json",
        help="Input LiveSportTV schedule JSON.",
    )
    parser.add_argument(
        "--fanzo-witm",
        default="weekly_schedule_fanzo_enriched.json",
        help="Input FANZO+WITM merged schedule JSON.",
    )
    parser.add_argument("--output", default="weekly_schedule.json", help="Output composed schedule JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    livesporttv = load_json(args.livesporttv)
    fanzo_witm = load_json(args.fanzo_witm)

    if not isinstance(livesporttv.get("schedule"), list):
        print(f"Invalid LiveSportTV payload: {args.livesporttv}", file=sys.stderr)
        return 1
    if not isinstance(fanzo_witm.get("schedule"), list):
        print(f"Invalid FANZO/WITM payload: {args.fanzo_witm}", file=sys.stderr)
        return 1

    payload = compose_payload(livesporttv, fanzo_witm)
    save_json(args.output, payload)

    comp = payload.get("composition", {})
    print(
        f"[COMPOSE] Wrote {args.output} | soccer={comp.get('soccer_events', 0)} "
        f"non-soccer={comp.get('non_soccer_events', 0)} days={comp.get('days', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
