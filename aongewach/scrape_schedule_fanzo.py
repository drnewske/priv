#!/usr/bin/env python3
"""
Scrape FANZO schedule for a date range.

Default behavior is non-soccer only (soccer/football excluded).
"""

from __future__ import annotations

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
from channel_name_placeholders import is_placeholder_channel_name

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
NON_BROADCAST_WORD_RE = re.compile(
    r"\b(app|website|web\s*site|youtube|radio)\b",
    re.IGNORECASE,
)
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


def parse_start_date(raw: Optional[str]) -> dt.date:
    if not raw:
        return dt.datetime.now(dt.timezone.utc).date()
    return dt.datetime.strptime(raw.strip(), "%Y-%m-%d").date()


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


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


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


def is_usable_channel_name(name: str) -> bool:
    cleaned = (name or "").strip()
    if not cleaned:
        return False
    if is_placeholder_channel_name(cleaned):
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
            if page > 50:
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
    home_id = normalize_team_id((home_team_raw or {}).get("id")) or normalize_team_id(raw_event.get("team1"))
    away_id = normalize_team_id((away_team_raw or {}).get("id")) or normalize_team_id(raw_event.get("team2"))
    home_logo = pick_team_logo(home_team_raw or {}) if home_team_raw else None
    away_logo = pick_team_logo(away_team_raw or {}) if away_team_raw else None

    split_names = split_match_name(fallback_name)
    if split_names:
        split_home, split_away = split_names
        if not home_name:
            home_name = split_home
        if not away_name:
            away_name = split_away

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


def transform_event(raw_event: Dict, non_soccer_only: bool = True) -> Optional[Dict]:
    name = (raw_event.get("name") or "").strip()
    if not name:
        return None

    competition = raw_event.get("competition") or {}
    sport = raw_event.get("sport") or {}
    sport_name = normalize_text(sport.get("name"))
    if non_soccer_only and is_soccer_sport(sport_name):
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

    event = {
        "name": name,
        "start_time_iso": format_iso_z(start_iso_raw),
        "time": time_str,
        "sport": sport_name,
        "competition": (competition.get("name") or "").strip(),
        "competition_logo": competition.get("competitionLogo"),
        "sport_logo": None,
        "channels": channels,
    }
    event.update(extract_team_data(raw_event, name))
    return event


def scrape_date(
    client: FanzoClient,
    target_date: dt.date,
    tz: dt.tzinfo,
    non_soccer_only: bool = True,
) -> List[Dict]:
    formatted_date = target_date.strftime("%Y-%m-%d")
    print(f"  > FANZO scraping {formatted_date}...", flush=True)
    try:
        raw_events = client.fetch_day(target_date, tz)
    except requests.RequestException as exc:
        print(f"    x FANZO error {formatted_date}: {exc}", flush=True)
        return []

    transformed = []
    for raw_event in raw_events:
        event = transform_event(raw_event, non_soccer_only=non_soccer_only)
        if event:
            transformed.append(event)

    print(f"    v FANZO kept {len(transformed)} events.", flush=True)
    return transformed


def scrape_range(
    client: FanzoClient,
    start_date: dt.date,
    days: int,
    tz: dt.tzinfo,
    non_soccer_only: bool = True,
) -> Dict:
    schedule = []
    for offset in range(days):
        current_date = start_date + dt.timedelta(days=offset)
        events = scrape_date(client, current_date, tz, non_soccer_only=non_soccer_only)
        schedule.append(
            {
                "date": current_date.strftime("%Y-%m-%d"),
                "day": current_date.strftime("%A"),
                "events": events,
            }
        )

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "fanzo.com",
        "schedule": schedule,
    }


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape schedule from FANZO TV guide.")
    parser.add_argument("--date", type=str, default=None, help="Start date in YYYY-MM-DD format. Default: today UTC.")
    parser.add_argument("--days", type=int, default=7, help="Number of days to scrape (default: 7).")
    parser.add_argument("--timezone", type=str, default="Europe/London", help="IANA timezone for FANZO localDateUtc.")
    parser.add_argument("--locale", type=str, default="en", help="FANZO locale header.")
    parser.add_argument("--output", type=str, default="weekly_schedule_fanzo.json", help="Output JSON path.")
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

    tz = get_timezone(args.timezone)
    client = FanzoClient(locale=args.locale, uid=DEFAULT_UID)
    payload = scrape_range(
        client=client,
        start_date=start_date,
        days=args.days,
        tz=tz,
        non_soccer_only=not args.include_soccer,
    )

    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)

    print(f"[FANZO] Wrote {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
