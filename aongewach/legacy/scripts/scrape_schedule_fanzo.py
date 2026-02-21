#!/usr/bin/env python3
"""
Scrape weekly sports schedule from FANZO TV Guide API.

This script mirrors the legacy weekly output structure used by the pipeline:
{
  "generated_at": "...",
  "source": "fanzo.com",
  "schedule": [
    {"date": "YYYY-MM-DD", "day": "Monday", "events": [...]}
  ]
}

It also enriches events with home/away team names, IDs, and logos when available.
"""

import argparse
import base64
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import sys
import time
import uuid
from typing import Dict, List, Optional, Tuple

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


API_ENDPOINT = "https://www-service.fanzo.com/sports/fixtures"
JWT_SECRET = os.getenv("FANZO_JWT_SECRET", "eahjhettffd_aea335232__gsdfds21")
JWT_DID = os.getenv("FANZO_DEVICE_ID", "7ec16ce4-21f8-42f0-9fef-21cd6cff51eb")
DEFAULT_UID = int(os.getenv("FANZO_UID", "2"))
PAGE_LIMIT = 100

MATCH_SPLIT_RE = re.compile(r"\s+(?:v|vs|-)\s+", re.IGNORECASE)
NON_BROADCAST_WORD_RE = re.compile(r"\b(app|website|web\s*site|youtube)\b", re.IGNORECASE)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)


def b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def get_timezone(name: str) -> dt.tzinfo:
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return dt.timezone.utc


def parse_iso_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def format_iso_z(value: Optional[str]) -> Optional[str]:
    parsed = parse_iso_datetime(value)
    if not parsed:
        return value
    parsed_utc = parsed.astimezone(dt.timezone.utc).replace(microsecond=0)
    return parsed_utc.isoformat().replace("+00:00", "Z")


def split_match_name(name: str) -> Optional[Tuple[str, str]]:
    parts = MATCH_SPLIT_RE.split(name, maxsplit=1)
    if len(parts) != 2:
        return None
    left = parts[0].strip()
    right = parts[1].strip()
    if not left or not right:
        return None
    return left, right


def is_usable_channel_name(name: str) -> bool:
    cleaned = (name or "").strip()
    if not cleaned:
        return False
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    return True


def build_jwt(uid: int = DEFAULT_UID) -> Tuple[str, int]:
    now_ms = int(time.time() * 1000)
    exp_ms = now_ms + (10 * 60 * 1000)

    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iat": now_ms,
        "nbf": now_ms,
        "exp": exp_ms,
        "jti": str(uuid.uuid4()),
        "did": JWT_DID,
        "uid": uid,
    }

    header_part = b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_part = b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    message = f"{header_part}.{payload_part}".encode("utf-8")
    signature = hmac.new(JWT_SECRET.encode("utf-8"), message, hashlib.sha256).digest()
    jwt_token = f"{header_part}.{payload_part}.{b64url_encode(signature)}"
    return jwt_token, exp_ms


class FanzoClient:
    def __init__(self, locale: str = "en", uid: int = DEFAULT_UID):
        self.locale = locale
        self.uid = uid
        self.session = requests.Session()
        self.jwt_token: Optional[str] = None
        self.jwt_expiry_ms = 0

    def _get_token(self, force_refresh: bool = False) -> str:
        now_ms = int(time.time() * 1000)
        if (
            force_refresh
            or self.jwt_token is None
            or now_ms >= (self.jwt_expiry_ms - 30_000)
        ):
            self.jwt_token, self.jwt_expiry_ms = build_jwt(self.uid)
        return self.jwt_token

    def _headers(self, force_refresh: bool = False) -> Dict[str, str]:
        token = self._get_token(force_refresh=force_refresh)
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {token}",
            "locale": self.locale,
            "origin": "https://www.fanzo.com",
            "referer": "https://www.fanzo.com/",
            "user-agent": "Mozilla/5.0",
        }

    def _fetch_page(self, params: Dict[str, object], retry: bool = True) -> Dict:
        response = self.session.get(
            API_ENDPOINT,
            params=params,
            headers=self._headers(force_refresh=False),
            timeout=30,
        )
        if response.status_code == 401 and retry:
            response = self.session.get(
                API_ENDPOINT,
                params=params,
                headers=self._headers(force_refresh=True),
                timeout=30,
            )
        response.raise_for_status()
        return response.json()

    def fetch_day(self, target_date: dt.date, tz: dt.tzinfo, limit: int = PAGE_LIMIT) -> List[Dict]:
        local_midnight = dt.datetime.combine(target_date, dt.time.min, tzinfo=tz)
        local_date_utc = local_midnight.isoformat(timespec="seconds")

        events: List[Dict] = []
        offset = 0
        page = 1

        while True:
            params = {
                "limit": limit,
                "offset": offset,
                "otherSports": 0,
                "localDateUtc": local_date_utc,
            }
            payload = self._fetch_page(params)
            batch = payload.get("result") or []
            if not isinstance(batch, list):
                break

            events.extend(batch)

            if len(batch) < limit:
                break

            offset += limit
            page += 1
            if page > 50:  # hard stop safety
                break

        return events


def pick_team_logo(team: Dict) -> Optional[str]:
    for key in ("logo", "image", "secondaryLogo"):
        value = team.get(key)
        if value:
            return value
    return None


def normalize_team_id(value: object) -> Optional[int]:
    if isinstance(value, int) and value > 0:
        return value
    return None


def extract_team_data(raw_event: Dict, fallback_name: str) -> Dict[str, object]:
    teams = raw_event.get("teams") or []
    teams_by_id = {}
    for team in teams:
        team_id = team.get("id")
        if isinstance(team_id, int):
            teams_by_id[team_id] = team

    home_team_raw = teams_by_id.get(raw_event.get("team1"))
    away_team_raw = teams_by_id.get(raw_event.get("team2"))

    if home_team_raw is None and len(teams) >= 1:
        home_team_raw = teams[0]
    if away_team_raw is None and len(teams) >= 2:
        away_team_raw = teams[1]

    home_name = (home_team_raw or {}).get("name")
    away_name = (away_team_raw or {}).get("name")
    home_id = normalize_team_id((home_team_raw or {}).get("id")) or normalize_team_id(
        raw_event.get("team1")
    )
    away_id = normalize_team_id((away_team_raw or {}).get("id")) or normalize_team_id(
        raw_event.get("team2")
    )
    home_logo = pick_team_logo(home_team_raw or {}) if home_team_raw else None
    away_logo = pick_team_logo(away_team_raw or {}) if away_team_raw else None

    split_names = split_match_name(fallback_name)
    if split_names:
        split_home, split_away = split_names
        if not home_name:
            home_name = split_home
        if not away_name:
            away_name = split_away

        # FANZO occasionally emits duplicated team rows for a head-to-head event.
        # If the title clearly has two sides, prefer the split names and drop bad duplicate IDs.
        if (
            home_name
            and away_name
            and home_name.strip().lower() == away_name.strip().lower()
            and split_home.strip().lower() != split_away.strip().lower()
        ):
            home_name = split_home
            away_name = split_away
            if home_id == away_id:
                home_id = None
                away_id = None
                if home_logo == away_logo:
                    home_logo = None
                    away_logo = None

    return {
        "home_team": home_name,
        "away_team": away_name,
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home_team_logo": home_logo,
        "away_team_logo": away_logo,
    }


def transform_event(raw_event: Dict) -> Optional[Dict]:
    name = (raw_event.get("name") or "").strip()
    if not name:
        return None

    channels = []
    seen = set()
    for channel in raw_event.get("channels") or []:
        if not isinstance(channel, dict):
            continue
        channel_name = (channel.get("name") or "").strip()
        if not channel_name:
            continue
        if not is_usable_channel_name(channel_name):
            continue
        key = channel_name.lower()
        if key in seen:
            continue
        seen.add(key)
        channels.append(channel_name)

    if not channels:
        return None

    start_iso_raw = raw_event.get("startTimeUtc") or raw_event.get("startTime")
    start_dt = parse_iso_datetime(start_iso_raw)
    time_str = start_dt.strftime("%H:%M") if start_dt else ""

    competition = raw_event.get("competition") or {}
    sport = raw_event.get("sport") or {}

    event = {
        "name": name,
        "start_time_iso": format_iso_z(start_iso_raw),
        "time": time_str,
        "sport": (sport.get("name") or "").strip(),
        "competition": (competition.get("name") or "").strip(),
        "competition_logo": competition.get("competitionLogo"),
        "channels": channels,
    }
    event.update(extract_team_data(raw_event, name))
    return event


def scrape_date(client: FanzoClient, target_date: dt.date, tz: dt.tzinfo) -> List[Dict]:
    formatted_date = target_date.strftime("%Y-%m-%d")
    print(f"  > Scraping {formatted_date}...", flush=True)
    try:
        raw_events = client.fetch_day(target_date, tz)
    except requests.RequestException as exc:
        print(f"    x Error fetching {formatted_date}: {exc}", flush=True)
        return []

    transformed = []
    for raw_event in raw_events:
        event = transform_event(raw_event)
        if event:
            transformed.append(event)

    print(
        f"    v Found {len(transformed)} events (kept after channel/name filtering).",
        flush=True,
    )
    return transformed


def scrape_week(client: FanzoClient, tz: dt.tzinfo, reference_date: Optional[dt.date] = None) -> Dict:
    if reference_date is None:
        reference_date = dt.datetime.now(tz).date()

    start_of_week = reference_date - dt.timedelta(days=reference_date.weekday())
    print(f"Scraping weekly schedule starting Monday {start_of_week}...", flush=True)

    schedule = []
    for idx in range(7):
        current_date = start_of_week + dt.timedelta(days=idx)
        events = scrape_date(client, current_date, tz)
        schedule.append(
            {
                "date": current_date.strftime("%Y-%m-%d"),
                "day": current_date.strftime("%A"),
                "events": events,
            }
        )

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "fanzo.com",
        "schedule": schedule,
    }


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape weekly schedule from FANZO TV guide.")
    parser.add_argument(
        "--date",
        type=str,
        help="Single date in YYYYMMDD format. If omitted, scrapes current week (Mon-Sun).",
    )
    parser.add_argument(
        "--timezone",
        type=str,
        default="Europe/London",
        help="IANA timezone used to build FANZO localDateUtc (default: Europe/London).",
    )
    parser.add_argument(
        "--locale",
        type=str,
        default="en",
        help="FANZO locale header (default: en).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_cli_args()
    tz = get_timezone(args.timezone)
    client = FanzoClient(locale=args.locale, uid=DEFAULT_UID)

    if args.date:
        try:
            target_date = dt.datetime.strptime(args.date, "%Y%m%d").date()
        except ValueError:
            print("Invalid --date format. Use YYYYMMDD.", file=sys.stderr)
            return 1

        events = scrape_date(client, target_date, tz)
        output_file = f"schedule_{target_date.strftime('%Y-%m-%d')}.json"
        payload = {
            "date": target_date.strftime("%Y-%m-%d"),
            "source": "fanzo.com",
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "events": events,
        }
        with open(output_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        print(f"Saved {output_file}", flush=True)
        return 0

    weekly_payload = scrape_week(client, tz)
    output_file = "weekly_schedule.json"
    with open(output_file, "w", encoding="utf-8") as handle:
        json.dump(weekly_payload, handle, indent=2, ensure_ascii=False)
    print(f"\nSuccessfully saved weekly schedule to {output_file}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
