#!/usr/bin/env python3
"""
Scrape schedule from Where's The Match (WITM).

Default behavior is non-soccer only (soccer/football excluded).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.wheresthematch.com"
SCHEDULE_URL_TEMPLATE = BASE_URL + "/live-sport-on-tv/?showdatestart={date}"

MATCH_SPLIT_RE = re.compile(r"\s+(?:v|vs|-)\s+", re.IGNORECASE)
NON_BROADCAST_WORD_RE = re.compile(
    r"\b(app|website|web\s*site|youtube|radio)\b",
    re.IGNORECASE,
)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def parse_start_date(raw: Optional[str]) -> dt.date:
    if not raw:
        return dt.datetime.now(dt.timezone.utc).date()
    return dt.datetime.strptime(raw.strip(), "%Y-%m-%d").date()


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_site_url(raw: object) -> str:
    text = normalize_text(raw)
    if not text:
        return ""
    return urljoin(BASE_URL + "/", text)


def is_soccer_sport(sport: object) -> bool:
    key = normalize_text(sport).casefold()
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


def split_match_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    parts = MATCH_SPLIT_RE.split(normalize_text(name), maxsplit=1)
    if len(parts) != 2:
        return None, None
    home = parts[0].strip() or None
    away = parts[1].strip() or None
    return home, away


def is_usable_channel_name(name: str) -> bool:
    cleaned = normalize_text(name)
    if not cleaned:
        return False
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    return True


def parse_iso_datetime(value: object) -> Optional[dt.datetime]:
    text = normalize_text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def format_iso_z(value: object) -> Optional[str]:
    parsed = parse_iso_datetime(value)
    if not parsed:
        return None
    return parsed.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def dedupe_strings(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values:
        value = normalize_text(raw)
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def extract_event(row, non_soccer_only: bool = True) -> Optional[Dict]:
    name = ""
    name_elem = row.find(attrs={"itemprop": "name"})
    if name_elem:
        name = normalize_text(name_elem.get("content"))
    if not name:
        fixture_span = row.find("span", class_="fixture")
        if fixture_span:
            name = normalize_text(fixture_span.get_text(" ", strip=True))
    if not name:
        return None

    start_iso_raw = ""
    start_date_elem = row.find(attrs={"itemprop": "startDate"})
    if start_date_elem:
        start_iso_raw = normalize_text(start_date_elem.get("content"))
    start_iso = format_iso_z(start_iso_raw)
    start_dt = parse_iso_datetime(start_iso_raw)
    local_time = start_dt.strftime("%H:%M") if start_dt else ""

    competition = ""
    competition_logo = ""
    sport_name = ""
    sport_logo = ""
    comp_td = row.find("td", class_="competition-name")
    if comp_td:
        comp_link = comp_td.find("a")
        if comp_link:
            competition = normalize_text(comp_link.get_text(" ", strip=True))
        img = comp_td.find("img")
        if img:
            competition_logo = normalize_site_url(img.get("data-src") or img.get("src"))
            sport_logo = competition_logo
            alt = normalize_text(img.get("alt"))
            if alt:
                sport_name = normalize_text(alt.replace("Sport icon", ""))

    if non_soccer_only and is_soccer_sport(sport_name):
        return None

    channels_raw: List[str] = []
    channel_td = row.find("td", class_="channel-details")
    if channel_td:
        for img in channel_td.find_all("img", class_="channel"):
            label = normalize_text(img.get("title") or img.get("alt"))
            label = normalize_text(label.replace(" logo", ""))
            if not label:
                continue
            if not is_usable_channel_name(label):
                continue
            channels_raw.append(label)

    channels = dedupe_strings(channels_raw)
    if not channels:
        return None

    home_team, away_team = split_match_name(name)
    return {
        "name": name,
        "start_time_iso": start_iso,
        "time": local_time,
        "sport": sport_name,
        "competition": competition,
        "competition_logo": competition_logo or None,
        "sport_logo": sport_logo or None,
        "channels": channels,
        "home_team": home_team,
        "away_team": away_team,
        "home_team_id": None,
        "away_team_id": None,
        "home_team_logo": None,
        "away_team_logo": None,
    }


def scrape_date(target_date: dt.date, non_soccer_only: bool = True) -> List[Dict]:
    date_key = target_date.strftime("%Y%m%d")
    url = SCHEDULE_URL_TEMPLATE.format(date=date_key)
    print(f"  > WITM scraping {target_date.isoformat()}...", flush=True)

    try:
        response = requests.get(url, headers={"User-Agent": UA}, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"    x WITM error {target_date.isoformat()}: {exc}", flush=True)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.find_all(
        "tr",
        attrs={
            "itemscope": True,
            "itemtype": re.compile(r"schema\.org/BroadcastEvent"),
        },
    )

    events: List[Dict] = []
    for row in rows:
        event = extract_event(row, non_soccer_only=non_soccer_only)
        if event:
            events.append(event)

    print(f"    v WITM kept {len(events)} events.", flush=True)
    return events


def scrape_range(start_date: dt.date, days: int, non_soccer_only: bool = True) -> Dict:
    schedule: List[Dict] = []
    for offset in range(days):
        current_date = start_date + dt.timedelta(days=offset)
        events = scrape_date(current_date, non_soccer_only=non_soccer_only)
        schedule.append(
            {
                "date": current_date.isoformat(),
                "day": current_date.strftime("%A"),
                "events": events,
            }
        )

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "wheresthematch.com",
        "schedule": schedule,
    }


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape schedule from Where's The Match.")
    parser.add_argument("--date", type=str, default=None, help="Start date in YYYY-MM-DD format. Default: today UTC.")
    parser.add_argument("--days", type=int, default=7, help="Number of days to scrape (default: 7).")
    parser.add_argument("--output", type=str, default="weekly_schedule_witm.json", help="Output JSON path.")
    parser.add_argument(
        "--include-soccer",
        action="store_true",
        help="Include soccer/football events (default: excluded).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_cli_args()
    if args.days < 1:
        print("--days must be >= 1", file=sys.stderr)
        return 2

    try:
        start_date = parse_start_date(args.date)
    except ValueError:
        print(f"Invalid --date value: {args.date!r}. Expected YYYY-MM-DD.", file=sys.stderr)
        return 2

    payload = scrape_range(
        start_date=start_date,
        days=args.days,
        non_soccer_only=not args.include_soccer,
    )

    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)

    print(f"[WITM] Wrote {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
