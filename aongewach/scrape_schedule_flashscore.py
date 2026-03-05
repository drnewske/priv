#!/usr/bin/env python3
"""
Scrape soccer schedules and TV channel listings from Flashscore USA.

This script uses the same internal feed as the Flashscore USA web client.
It exports schedule data to JSON and CSV.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


DEFAULT_BASE_PAGE = "https://www.flashscoreusa.com/"
DEFAULT_DAYS = 7
DEFAULT_DAY_START = 0
DEFAULT_TIMEOUT = 30
ROW_DELIMITER = "~"
CELL_DELIMITER = chr(172)  # "¬"
INDEX_DELIMITER = chr(247)  # "÷"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

MAX_CHANNELS_PER_EVENT = 3
CHANNEL_BLOCK_WORD_RE = re.compile(r"(^|\W)(youtube|app)(\W|$)", re.IGNORECASE)

TOP_COMPETITION_EXACT = {
    "england premier league",
    "spain laliga",
    "italy serie a",
    "germany bundesliga",
    "france ligue 1",
    "saudi arabia saudi professional league",
    "usa mls",
    "england fa cup",
    "england efl cup",
    "england carabao cup",
    "germany dfb pokal",
    "spain copa del rey",
    "italy coppa italia",
    "france coupe de france",
}

TOP_COMPETITION_PREFIX = (
    "world world cup",
    "world fifa world cup",
    "europe friendly international",
    "south america friendly international",
    "africa africa cup of nations",
)

TEAM_LOGO_BASE = "https://static.flashscore.com/res/image/data/"
US_CHANNEL_HINTS = (
    "usa",
    "nbc",
    "peacock",
    "espn",
    "fox",
    "cbs",
    "abc",
    "univision",
    "tudn",
    "fubo",
    "paramount",
    "apple tv",
    "mls season pass",
    "usa network",
    "universo",
    "bein sports us",
)


@dataclass
class FlashscoreConfig:
    host: str
    project_id: int
    project_type_id: int
    lang_web: str
    feed_sign: str
    timezone_hour: int


def iso_z_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def parse_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def normalize_key(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    return " ".join(text.lower().split())


def is_top_competition(name: str) -> bool:
    key = normalize_key(name)
    if not key:
        return False
    if key in TOP_COMPETITION_EXACT:
        return True
    return any(key.startswith(prefix) for prefix in TOP_COMPETITION_PREFIX)


def normalize_channel_url(raw: str) -> str:
    url = normalize_text(raw)
    if not url:
        return ""
    if url.startswith("ttps://"):
        url = "h" + url
    elif url.startswith("//"):
        url = "https:" + url
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", url):
        return url
    return urljoin(DEFAULT_BASE_PAGE, url)


def is_allowed_channel_name(name: str) -> bool:
    cleaned = normalize_text(name)
    if not cleaned:
        return False
    return CHANNEL_BLOCK_WORD_RE.search(cleaned) is None


def is_us_channel(name: str) -> bool:
    key = normalize_key(name)
    if not key:
        return False
    return any(hint in key for hint in US_CHANNEL_HINTS)


def normalize_logo_url(raw: object) -> str:
    value = normalize_text(raw)
    if not value:
        return ""
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", value):
        return value
    if value.startswith("/"):
        return urljoin(DEFAULT_BASE_PAGE, value)
    return urljoin(TEAM_LOGO_BASE, value)


def extract_core_script_url(base_page: str, html: str) -> str:
    match = re.search(r'<script[^>]+src="([^"]*core_[^"]+\.js)"', html, re.IGNORECASE)
    if not match:
        raise RuntimeError("Could not locate Flashscore core JS bundle URL.")
    return urljoin(base_page, match.group(1))


def extract_default_tz(html: str) -> int:
    match = re.search(r"default_tz\s*=\s*(-?\d+)", html)
    return int(match.group(1)) if match else -5


def extract_cjs_config(core_js: str) -> Dict:
    marker = "cjs._config = "
    marker_pos = core_js.find(marker)
    if marker_pos < 0:
        raise RuntimeError("Could not locate cjs._config payload in core JS.")

    start = core_js.find("{", marker_pos)
    if start < 0:
        raise RuntimeError("Could not parse cjs._config object start.")

    depth = 0
    in_string = False
    escaped = False
    end = -1
    for i in range(start, len(core_js)):
        ch = core_js[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end < 0:
        raise RuntimeError("Could not parse cjs._config object end.")

    return json.loads(core_js[start:end])


def extract_feed_sign(core_js: str, config: Dict) -> str:
    app = config.get("app", {})
    from_config = normalize_text(app.get("feed_sign"))
    if from_config:
        return from_config

    match = re.search(r"var\s+feed_sign\s*=\s*'([^']+)'", core_js)
    if not match:
        raise RuntimeError("Could not find feed_sign token.")
    return normalize_text(match.group(1))


def resolve_flashscore_config(session: requests.Session, base_page: str, timeout: int) -> FlashscoreConfig:
    page_resp = session.get(base_page, timeout=timeout)
    page_resp.raise_for_status()
    html = page_resp.text

    core_url = extract_core_script_url(base_page, html)
    core_resp = session.get(core_url, timeout=timeout)
    core_resp.raise_for_status()
    core_js = core_resp.text

    cfg = extract_cjs_config(core_js)
    app = cfg.get("app", {})
    project = app.get("project", {})
    project_type = app.get("project_type", {})
    lang = app.get("lang", {})
    feed_resolver = app.get("feed_resolver", {})

    host = normalize_text(feed_resolver.get("default_url")) or "https://global.flashscore.ninja"
    project_id = parse_int(project.get("id"), 130)
    project_type_id = parse_int(project_type.get("id"), 1)
    lang_web = normalize_text(lang.get("web")) or "en-usa"
    feed_sign = extract_feed_sign(core_js, cfg)
    timezone_hour = extract_default_tz(html)

    return FlashscoreConfig(
        host=host.rstrip("/"),
        project_id=project_id,
        project_type_id=project_type_id,
        lang_web=lang_web,
        feed_sign=feed_sign,
        timezone_hour=timezone_hour,
    )


def parse_row_to_map(row: str) -> Dict[str, str]:
    mapped: Dict[str, str] = {}
    for cell in row.split(CELL_DELIMITER):
        if INDEX_DELIMITER not in cell:
            continue
        key, value = cell.split(INDEX_DELIMITER, 1)
        if key:
            mapped[key] = value
    return mapped


def parse_channel_payload(raw: str) -> Tuple[List[str], List[Dict[str, object]], List[Dict[str, object]]]:
    if not raw:
        return [], [], []

    try:
        payload = json.loads(raw)
    except Exception:
        return [], [], []

    channels: List[str] = []
    channel_links: List[Dict[str, object]] = []
    highlights: List[Dict[str, object]] = []
    seen_channels = set()
    candidates: List[Dict[str, object]] = []

    for item in payload.get("1", []):
        if not isinstance(item, dict):
            continue
        name = normalize_text(item.get("BN"))
        if not is_allowed_channel_name(name):
            continue
        url = normalize_channel_url(item.get("BU"))
        tv_id = item.get("TVI")
        key = name.casefold()
        if not name or key in seen_channels:
            continue
        seen_channels.add(key)
        candidates.append(
            {
                "name": name,
                "url": url,
                "tv_id": tv_id,
            }
        )

    usa_candidates = [item for item in candidates if is_us_channel(str(item.get("name", "")))]
    selected = usa_candidates if usa_candidates else candidates
    channel_links = selected[:MAX_CHANNELS_PER_EVENT]
    channels = [normalize_text(item.get("name")) for item in channel_links if normalize_text(item.get("name"))]

    for item in payload.get("HP", []):
        if not isinstance(item, dict):
            continue
        highlights.append(
            {
                "provider_id": item.get("HPI"),
                "provider_name": normalize_text(item.get("HPN")),
                "provider_ref": normalize_text(item.get("HPR")),
            }
        )

    return channels, channel_links, highlights


def to_new_york_iso(ts: int) -> str:
    dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    if ZoneInfo is None:
        return dt_utc.isoformat().replace("+00:00", "Z")
    return dt_utc.astimezone(ZoneInfo("America/New_York")).isoformat()


def parse_feed_events(feed_text: str, day_offset: int) -> List[Dict[str, object]]:
    events: List[Dict[str, object]] = []
    current_league: Dict[str, str] = {}

    for raw_row in feed_text.split(ROW_DELIMITER):
        row = parse_row_to_map(raw_row)
        if not row:
            continue

        if "ZA" in row:
            current_league = {
                "competition": normalize_text(row.get("ZA")),
                "country": normalize_text(row.get("ZY")),
                "competition_id": normalize_text(row.get("ZEE")),
                "country_id": normalize_text(row.get("ZB")),
                "competition_url": normalize_text(row.get("ZL")),
            }

        if "AA" not in row:
            continue

        start_ts = parse_int(row.get("AD"), 0)
        channels, channel_links, highlight_providers = parse_channel_payload(row.get("AL", ""))

        event = {
            "day_offset": day_offset,
            "event_id": normalize_text(row.get("AA")),
            "start_timestamp_utc": start_ts,
            "start_time_utc": (
                dt.datetime.fromtimestamp(start_ts, tz=dt.timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
                if start_ts > 0
                else ""
            ),
            "start_time_new_york": to_new_york_iso(start_ts) if start_ts > 0 else "",
            "home_team": normalize_text(row.get("AE")),
            "away_team": normalize_text(row.get("AF")),
            "home_team_short": normalize_text(row.get("WM")),
            "away_team_short": normalize_text(row.get("WN")),
            "home_team_logo": normalize_logo_url(row.get("OA")),
            "away_team_logo": normalize_logo_url(row.get("OB")),
            "home_team_slug": normalize_text(row.get("WU")),
            "away_team_slug": normalize_text(row.get("WV")),
            "status_code": normalize_text(row.get("AN")),
            "home_score": normalize_text(row.get("AW")),
            "away_score": normalize_text(row.get("AX")),
            "channels": channels,
            "channel_links": channel_links,
            "highlight_providers": highlight_providers,
            "has_channels": bool(channels),
            "event_url": f"https://www.flashscoreusa.com/match/{normalize_text(row.get('AA'))}/#/match-summary",
            "competition": current_league.get("competition", ""),
            "country": current_league.get("country", ""),
            "competition_id": current_league.get("competition_id", ""),
            "competition_url": current_league.get("competition_url", ""),
            "country_id": current_league.get("country_id", ""),
        }
        events.append(event)

    return events


def to_slim_events(events: List[Dict[str, object]]) -> List[Dict[str, object]]:
    slim: List[Dict[str, object]] = []
    for event in events:
        start_utc = normalize_text(event.get("start_time_utc"))
        start_date = ""
        start_time = ""
        if start_utc:
            try:
                utc_dt = dt.datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
                utc_dt = utc_dt.astimezone(dt.timezone.utc)
                start_date = utc_dt.date().isoformat()
                start_time = utc_dt.strftime("%H:%M")
            except Exception:
                pass

        channels = []
        for item in event.get("channel_links") or []:
            if not isinstance(item, dict):
                continue
            name = normalize_text(item.get("name"))
            url = normalize_channel_url(item.get("url"))
            if not name:
                continue
            channels.append({"name": name, "url": url})

        slim.append(
            {
                "home_team": normalize_text(event.get("home_team")),
                "away_team": normalize_text(event.get("away_team")),
                "home_team_logo": normalize_logo_url(event.get("home_team_logo")),
                "away_team_logo": normalize_logo_url(event.get("away_team_logo")),
                "competition": normalize_text(event.get("competition")),
                "country": normalize_text(event.get("country")),
                "competition_id": normalize_text(event.get("competition_id")),
                "competition_url": normalize_channel_url(str(event.get("competition_url") or "")),
                "start_date": start_date,
                "start_time": start_time,
                "start_time_utc": start_utc,
                "channels": channels,
            }
        )
    return slim


def fetch_feed_text(
    session: requests.Session,
    cfg: FlashscoreConfig,
    day_offset: int,
    timeout: int,
) -> str:
    feed_name = f"f_1_{day_offset}_{cfg.timezone_hour}_{cfg.lang_web}_{cfg.project_type_id}"
    url = f"{cfg.host}/{cfg.project_id}/x/feed/{feed_name}"
    response = session.get(
        url,
        headers={
            "x-fsign": cfg.feed_sign,
            "user-agent": UA,
            "referer": DEFAULT_BASE_PAGE,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def build_csv_rows(events: List[Dict[str, object]]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for event in events:
        links = event.get("channels") or []
        names: List[str] = []
        urls: List[str] = []
        for item in links:
            if not isinstance(item, dict):
                continue
            name = normalize_text(item.get("name"))
            url = normalize_channel_url(item.get("url"))
            if name:
                names.append(name)
            if url:
                urls.append(url)

        rows.append(
            {
                "start_date": event.get("start_date"),
                "start_time": event.get("start_time"),
                "start_time_utc": event.get("start_time_utc"),
                "competition": event.get("competition"),
                "country": event.get("country"),
                "home_team": event.get("home_team"),
                "away_team": event.get("away_team"),
                "home_team_logo": event.get("home_team_logo"),
                "away_team_logo": event.get("away_team_logo"),
                "channels": " | ".join(names),
                "channel_urls": " | ".join(urls),
            }
        )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Flashscore USA soccer schedules and channels.")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="How many day offsets to fetch.")
    parser.add_argument(
        "--day-start",
        type=int,
        default=DEFAULT_DAY_START,
        help="Starting day offset. 0=today, 1=tomorrow, -1=yesterday.",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--include-non-top-competitions",
        action="store_true",
        help="Disable top-flight competition filtering.",
    )
    parser.add_argument(
        "--output-json",
        default="aongewach/flashscore_soccer_schedule_channels.json",
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--output-csv",
        default="aongewach/flashscore_soccer_schedule_channels.csv",
        help="Output CSV file path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    session = requests.Session()
    session.headers.update({"user-agent": UA})

    cfg = resolve_flashscore_config(session, DEFAULT_BASE_PAGE, args.timeout)
    top_only = not args.include_non_top_competitions

    all_events: List[Dict[str, object]] = []
    day_summaries: List[Dict[str, object]] = []

    for day_offset in range(args.day_start, args.day_start + max(0, args.days)):
        feed_text = fetch_feed_text(session, cfg, day_offset, args.timeout)
        raw_events = parse_feed_events(feed_text, day_offset)
        if top_only:
            events = [event for event in raw_events if is_top_competition(str(event.get("competition", "")))]
        else:
            events = raw_events
        with_channels = sum(1 for event in events if event.get("has_channels"))

        day_summaries.append(
            {
                "day_offset": day_offset,
                "events_before_competition_filter": len(raw_events),
                "events": len(events),
                "events_with_channels": with_channels,
            }
        )
        all_events.extend(events)

    slim_events = to_slim_events(all_events)
    slim_with_channels = sum(1 for event in slim_events if event.get("channels"))

    payload = {
        "generated_at": iso_z_now(),
        "source": "flashscoreusa.com",
        "config": {
            "host": cfg.host,
            "project_id": cfg.project_id,
            "project_type_id": cfg.project_type_id,
            "lang_web": cfg.lang_web,
            "timezone_hour": cfg.timezone_hour,
        },
        "summary": {
            "days_requested": args.days,
            "day_start": args.day_start,
            "top_competitions_only": top_only,
            "max_channels_per_event": MAX_CHANNELS_PER_EVENT,
            "total_events": len(slim_events),
            "total_events_with_channels": slim_with_channels,
        },
        "day_summaries": day_summaries,
        "events": slim_events,
    }

    with open(args.output_json, "w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)

    csv_rows = build_csv_rows(slim_events)
    csv_fieldnames = [
        "start_date",
        "start_time",
        "start_time_utc",
        "competition",
        "country",
        "home_team",
        "away_team",
        "home_team_logo",
        "away_team_logo",
        "channels",
        "channel_urls",
    ]
    with open(args.output_csv, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)

    print(
        "Saved {} events ({} with channels) to {} and {}.".format(
            payload["summary"]["total_events"],
            payload["summary"]["total_events_with_channels"],
            args.output_json,
            args.output_csv,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
