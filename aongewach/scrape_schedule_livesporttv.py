#!/usr/bin/env python3
"""
Scrape sports TV guide data from LiveSportTV.

Output shape mirrors the weekly schedule payload used in this repository:
{
  "generated_at": "...",
  "source": "livesporttv.com",
  "schedule": [
    {"date": "YYYY-MM-DD", "day": "Monday", "events": [...]}
  ]
}

This scraper uses the same endpoints as the site frontend:
1) /schedules/{date}/
2) /data-today
3) /api/collapsible/tournament/
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, unquote, urljoin, urlparse

import cloudscraper
from bs4 import BeautifulSoup, Tag
from bs4 import FeatureNotFound

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


BASE_URL = "https://www.livesporttv.com"
SCHEDULE_PATH = "/schedules/{date}/"
DATA_TODAY_ENDPOINT = f"{BASE_URL}/data-today"
TOURNAMENT_ENDPOINT = f"{BASE_URL}/api/collapsible/tournament/"

NON_BROADCAST_WORD_RE = re.compile(r"\b(app|website|web\s*site|youtube)\b", re.IGNORECASE)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)
VERSION_RE = re.compile(r"version:\s*'([^']+)'")
TIME_ZONE_RE = re.compile(r"time_zone:\s*'([^']+)'")
ISO_CODE_RE = re.compile(r"iso_code:\s*'([^']+)'")
LOCALE_RE = re.compile(r"locale:\s*'([^']+)'")
MATCH_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{4,64}$")
LOGO_PLACEHOLDER_RE = re.compile(r"(?:^|[/_-])(no-logo|default)(?:[._/-]|$)", re.IGNORECASE)
TEAM_LOGO_WIDTH_RE = re.compile(r"/resize/width/20(?=/uploads/teams/)", re.IGNORECASE)

_PARSER = "lxml"


def parse_html(html: str) -> BeautifulSoup:
    global _PARSER
    try:
        return BeautifulSoup(html, _PARSER)
    except FeatureNotFound:
        _PARSER = "html.parser"
        return BeautifulSoup(html, _PARSER)


@dataclass(frozen=True)
class TournamentRequest:
    request_id: str
    create_time: str
    expire_time: str
    order_by: str
    time_zone: str
    iso_code: str
    source_sport_key: str

    def params(self) -> Dict[str, str]:
        payload = {
            "request_id": self.request_id,
            "create_time": self.create_time,
            "expire_time": self.expire_time,
            "order_by": self.order_by,
            "time_zone": self.time_zone,
        }
        if self.iso_code:
            payload["iso_code"] = self.iso_code
        return payload


class LiveSportTVClient:
    def __init__(self, timeout: int = 45, retries: int = 4, backoff_seconds: float = 1.75):
        self.timeout = timeout
        self.retries = max(1, retries)
        self.backoff_seconds = max(0.0, backoff_seconds)
        self.session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )

    def _get_text(self, url: str, params: Optional[Dict[str, str]] = None) -> str:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except Exception as exc:
                last_exc = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.backoff_seconds * attempt)
        if last_exc is None:
            raise RuntimeError(f"Request failed for {url}")
        raise RuntimeError(f"Request failed for {url}: {last_exc}") from last_exc

    def get_schedule_html(self, target_date: dt.date) -> str:
        url = BASE_URL + SCHEDULE_PATH.format(date=target_date.isoformat())
        return self._get_text(url)

    def get_data_today(self, params: Dict[str, str]) -> Dict[str, List[str]]:
        body = self._get_text(DATA_TODAY_ENDPOINT, params=params)
        parsed = json.loads(body)
        if not isinstance(parsed, dict):
            raise ValueError("Unexpected /data-today payload; expected object")
        typed: Dict[str, List[str]] = {}
        for key, value in parsed.items():
            if isinstance(value, list):
                typed[str(key)] = [item for item in value if isinstance(item, str)]
        return typed

    def get_tournament(self, request: TournamentRequest) -> Dict:
        body = self._get_text(TOURNAMENT_ENDPOINT, params=request.params())
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("Unexpected tournament payload; expected object")
        return payload


def get_timezone(name: str) -> dt.tzinfo:
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return dt.timezone.utc


def iso_z_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def extract_script_config(html: str, default_locale: str = "en") -> Dict[str, str]:
    locale = default_locale
    version = "537"
    time_zone = "UTC"
    iso_code = ""

    html_lang_match = re.search(r"<html[^>]+lang=\"([^\"]+)\"", html, re.IGNORECASE)
    if html_lang_match:
        locale = html_lang_match.group(1).strip() or locale

    version_match = VERSION_RE.search(html)
    if version_match:
        version = version_match.group(1).strip() or version

    time_zone_match = TIME_ZONE_RE.search(html)
    if time_zone_match:
        time_zone = time_zone_match.group(1).strip() or time_zone

    iso_code_match = ISO_CODE_RE.search(html)
    if iso_code_match:
        iso_code = iso_code_match.group(1).strip()

    locale_match = LOCALE_RE.search(html)
    if locale_match:
        locale = locale_match.group(1).strip() or locale

    return {
        "locale": locale,
        "version": version,
        "time_zone": time_zone,
        "iso_code": iso_code,
    }


def is_usable_channel_name(name: str, keep_noisy_channels: bool = False) -> bool:
    cleaned = (name or "").strip()
    if not cleaned:
        return False
    if keep_noisy_channels:
        return True
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    return True


def dedupe_strings(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for raw in values:
        value = (raw or "").strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def normalize_whitespace(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_identity_text(value: object) -> str:
    return normalize_whitespace(value).casefold()


def normalize_logo_url(raw: object) -> str:
    normalized = normalize_site_url(raw)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    if not parsed.path:
        return normalized
    patched_path = TEAM_LOGO_WIDTH_RE.sub("/resize/width/40", parsed.path)
    if patched_path == parsed.path:
        return normalized
    return parsed._replace(path=patched_path).geturl().rstrip("/")


def is_special_competition_event(competition: object, country: object, sport: object) -> bool:
    comp = normalize_identity_text(competition)
    country_key = normalize_identity_text(country)
    sport_key = normalize_identity_text(sport)

    if not comp:
        return False

    # UEFA top competitions
    if "champions league" in comp:
        if "afc champions league" in comp or "caf champions league" in comp:
            return False
        if "uefa" in comp or country_key in {"international", "europe"}:
            return True
    if "europa league" in comp:
        if "uefa" in comp or country_key in {"international", "europe"}:
            return True
    if "conference league" in comp:
        if "uefa" in comp or country_key in {"international", "europe"}:
            return True

    # England top comps
    if "premier league" in comp and country_key == "england":
        return True
    if "carabao cup" in comp or ("efl cup" in comp and country_key == "england"):
        return True
    if "fa cup" in comp and country_key == "england":
        return True

    # Spain / Germany / France top leagues/cups
    if ("la liga" in comp or "laliga" in comp) and country_key == "spain":
        if "2" not in comp and "hypermotion" not in comp:
            return True
    if "bundesliga" in comp and country_key == "germany":
        if "2. bundesliga" not in comp and "2 bundesliga" not in comp:
            return True
    if ("dfb-pokal" in comp or "dfb pokal" in comp) and country_key == "germany":
        return True
    if "ligue 1" in comp and country_key == "france":
        return True

    # Top NBA competitions
    if "nba" in comp and sport_key == "basketball":
        if "g league" not in comp:
            return True

    return False


def normalize_site_url(raw: object, keep_fragment: bool = False) -> str:
    url = str(raw or "").strip()
    if not url:
        return ""
    if url.startswith("//"):
        url = f"https:{url}"
    elif url.startswith("/"):
        url = urljoin(BASE_URL, url)
    elif not urlparse(url).scheme:
        url = urljoin(BASE_URL + "/", url)

    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"}:
        parsed = parsed._replace(scheme="https")
    if parsed.path:
        encoded_path = quote(unquote(parsed.path), safe="/:@&+$,;=-_.!~*'()")
        parsed = parsed._replace(path=encoded_path)
    if not keep_fragment:
        parsed = parsed._replace(fragment="")
    normalized = parsed.geturl().rstrip("/")
    return normalized


def canonicalize_match_url(raw: object) -> str:
    normalized = normalize_site_url(raw, keep_fragment=True)
    if not normalized:
        return ""

    parsed = urlparse(normalized)
    path_parts = [part for part in parsed.path.split("/") if part]
    fragment = normalize_whitespace(parsed.fragment)
    token = fragment if MATCH_TOKEN_RE.match(fragment) else ""

    if not token and path_parts:
        last = normalize_whitespace(path_parts[-1])
        if MATCH_TOKEN_RE.match(last):
            token = last

    if token and path_parts:
        path_parts[-1] = token
        new_path = "/" + "/".join(path_parts)
    else:
        new_path = parsed.path

    canonical = parsed._replace(path=new_path, fragment="")
    return canonical.geturl().rstrip("/")


def extract_match_identity_token(event: Dict) -> str:
    key = normalize_whitespace(event.get("match_key"))
    if key:
        return normalize_identity_text(key)

    match_url = canonicalize_match_url(event.get("match_url"))
    if not match_url:
        return ""

    parsed = urlparse(match_url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        return ""
    tail = normalize_whitespace(path_parts[-1])
    if MATCH_TOKEN_RE.match(tail):
        return normalize_identity_text(tail)
    return ""


def is_usable_logo_url(raw: object) -> bool:
    normalized = normalize_logo_url(raw)
    if not normalized:
        return False
    if LOGO_PLACEHOLDER_RE.search(normalized):
        return False
    parsed = urlparse(normalized)
    filename = unquote(parsed.path).rsplit("/", 1)[-1].strip()
    if not filename or "." not in filename:
        return False
    return True


def logo_quality_score(raw: object) -> int:
    url = normalize_logo_url(raw)
    if not url:
        return -100
    lowered = url.lower()
    score = 0
    if "/resize/" in lowered:
        score += 4
    if "?" in lowered:
        score += 1
    if "/uploads/" in lowered and "/resize/" not in lowered:
        score -= 1
    if LOGO_PLACEHOLDER_RE.search(lowered):
        score -= 5
    return score


def choose_preferred_logo(existing: object, incoming: object) -> Optional[str]:
    existing_url = normalize_logo_url(existing)
    incoming_url = normalize_logo_url(incoming)

    if not existing_url and not incoming_url:
        return None
    if not existing_url:
        return incoming_url if is_usable_logo_url(incoming_url) else None
    if not incoming_url:
        return existing_url if is_usable_logo_url(existing_url) else None

    existing_score = logo_quality_score(existing_url)
    incoming_score = logo_quality_score(incoming_url)
    chosen = incoming_url if incoming_score > existing_score else existing_url
    if not is_usable_logo_url(chosen):
        fallback = existing_url if chosen == incoming_url else incoming_url
        return fallback if is_usable_logo_url(fallback) else None
    return chosen


def parse_srcset_first_url(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    first = text.split(",", 1)[0].strip()
    if not first:
        return ""
    return first.split(" ", 1)[0].strip()


def extract_logo_from_node(node: Optional[Tag]) -> Optional[str]:
    if node is None:
        return None
    if not isinstance(node, Tag):
        return None

    candidate_attrs = (
        "src",
        "data-src",
        "data-original",
        "data-lazy-src",
        "data-srcset",
        "srcset",
    )
    for attr in candidate_attrs:
        raw = node.get(attr)
        if not raw:
            continue
        candidate = parse_srcset_first_url(raw) if "srcset" in attr else str(raw).strip()
        if candidate.startswith("data:"):
            continue
        normalized = normalize_logo_url(candidate)
        if normalized:
            return normalized
    return None


def select_match_node(container: Tag) -> Optional[Tag]:
    if not isinstance(container, Tag):
        return None
    if container.has_attr("data-match"):
        return container
    return container.select_one("[data-match]")


def iter_unique_match_nodes(soup: BeautifulSoup) -> Iterable[Tag]:
    seen = set()
    for node in soup.select("[data-match]"):
        if not isinstance(node, Tag):
            continue
        key = normalize_whitespace(node.get("data-match") or node.get("id"))
        if not key:
            continue
        lowered = key.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        yield node


def merge_duplicate_events(existing: Dict, incoming: Dict) -> Dict:
    merged = dict(existing)

    merged_channels = dedupe_strings(
        list(existing.get("channels") or []) + list(incoming.get("channels") or [])
    )
    if merged_channels:
        merged["channels"] = merged_channels

    merged["home_team_logo"] = choose_preferred_logo(
        existing.get("home_team_logo"), incoming.get("home_team_logo")
    )
    merged["away_team_logo"] = choose_preferred_logo(
        existing.get("away_team_logo"), incoming.get("away_team_logo")
    )
    merged["special"] = bool(existing.get("special")) or bool(incoming.get("special"))

    for field in (
        "name",
        "start_time_iso",
        "time",
        "sport",
        "competition",
        "country",
        "home_team",
        "away_team",
        "home_team_id",
        "away_team_id",
        "status",
        "score_home",
        "score_away",
        "match_key",
        "match_fx_id",
        "competition_url",
        "sport_id",
    ):
        if normalize_whitespace(merged.get(field)):
            continue
        merged[field] = incoming.get(field)

    existing_match_url = canonicalize_match_url(existing.get("match_url"))
    incoming_match_url = canonicalize_match_url(incoming.get("match_url"))
    if existing_match_url:
        merged["match_url"] = existing_match_url
    elif incoming_match_url:
        merged["match_url"] = incoming_match_url
    else:
        merged["match_url"] = ""

    return merged


def parse_channels_from_match_payload(
    tv_listings: Dict,
    match_node: Optional[Tag],
    keep_noisy_channels: bool = False,
) -> List[str]:
    candidates: List[str] = []

    raw_values = tv_listings.get("value")
    if isinstance(raw_values, str) and raw_values.strip():
        text = raw_values.strip()
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                candidates.extend([str(item) for item in parsed if isinstance(item, (str, int, float))])
        except Exception:
            pass

    html_values = tv_listings.get("html")
    if isinstance(html_values, str) and html_values.strip():
        soup = parse_html(html_values)
        for anchor in soup.select("a"):
            text = anchor.get_text(" ", strip=True)
            if text:
                candidates.append(text)

    if match_node is not None:
        for anchor in match_node.select(".match__channels a, .match_channels a"):
            text = anchor.get_text(" ", strip=True)
            if text:
                candidates.append(text)

    channels = []
    for channel in dedupe_strings(candidates):
        if is_usable_channel_name(channel, keep_noisy_channels=keep_noisy_channels):
            channels.append(channel)
    return channels


def build_event_name(home_name: str, away_name: str) -> str:
    home = (home_name or "").strip()
    away = (away_name or "").strip()
    if home and away:
        return f"{home} v {away}"
    return home or away


def parse_match_datetime(time_text: Optional[str], tz_name: str) -> Tuple[Optional[str], str]:
    _ = tz_name
    if not time_text:
        return None, ""
    cleaned = time_text.strip()
    if not cleaned:
        return None, ""

    parsed: Optional[dt.datetime] = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = dt.datetime.strptime(cleaned, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        return None, ""

    # LiveSportTV's data-time behaves as UTC clock time for schedule payloads.
    utc_dt = parsed.replace(tzinfo=dt.timezone.utc)
    iso = utc_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return iso, parsed.strftime("%H:%M")


def infer_sport_from_url(url: str) -> str:
    if not url:
        return ""
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    parts = path.split("/")
    if not parts:
        return ""
    return parts[0].replace("-", " ")


def normalize_sport_name(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    text = text.replace(".", "")
    return " ".join(token.capitalize() for token in text.split())


def parse_li_match_event(
    li: Tag,
    default_tz: str,
    sport_by_id: Dict[str, str],
    keep_noisy_channels: bool = False,
) -> Optional[Dict]:
    if not isinstance(li, Tag):
        return None

    match_node = select_match_node(li)
    if match_node is None:
        return None

    data_match = normalize_whitespace(match_node.get("data-match") or match_node.get("id"))
    detail_anchor = match_node.select_one(".matches-status a.link_matchs_pjax, a.link_matchs_pjax")
    detail_url = canonicalize_match_url(detail_anchor.get("href") if detail_anchor else "")

    home_name_node = match_node.select_one(".match__home__name, .match_home_name")
    away_name_node = match_node.select_one(".match__away__name, .match_away_name")
    home_name = home_name_node.get_text(" ", strip=True) if home_name_node else ""
    away_name = away_name_node.get_text(" ", strip=True) if away_name_node else ""

    if not home_name and not away_name:
        return None

    event_name = build_event_name(home_name, away_name)
    status_node = match_node.select_one(".matches-status")
    status_text = status_node.get_text(" ", strip=True) if status_node else ""

    home_logo_node = match_node.select_one(".match__home__image img, .match_home_img img")
    away_logo_node = match_node.select_one(".match__away__image img, .match_away_img img")
    home_logo = choose_preferred_logo(None, extract_logo_from_node(home_logo_node))
    away_logo = choose_preferred_logo(None, extract_logo_from_node(away_logo_node))

    home_score_node = match_node.select_one(".matches-home")
    away_score_node = match_node.select_one(".matches-away")
    home_score = home_score_node.get_text(" ", strip=True) if home_score_node else ""
    away_score = away_score_node.get_text(" ", strip=True) if away_score_node else ""

    channels: List[str] = []
    for anchor in match_node.select(".match__channels a, .match_channels a"):
        text = anchor.get_text(" ", strip=True)
        if text and is_usable_channel_name(text, keep_noisy_channels=keep_noisy_channels):
            channels.append(text)
    channels = dedupe_strings(channels)
    if not channels:
        return None

    match_iso, match_time = parse_match_datetime(match_node.get("data-time"), default_tz)
    sport_id = normalize_whitespace(match_node.get("data-sport"))
    sport_name = sport_by_id.get(sport_id, "")
    if not sport_name:
        sport_name = infer_sport_from_url(detail_url)
    sport_name = normalize_sport_name(sport_name)

    competition = normalize_whitespace(match_node.get("data-comp"))
    country = normalize_whitespace(match_node.get("data-country"))
    special = is_special_competition_event(competition, country, sport_name)

    event = {
        "name": event_name,
        "start_time_iso": match_iso,
        "time": match_time,
        "sport": sport_name,
        "competition": competition,
        "country": country,
        "special": special,
        "channels": channels,
        "home_team": home_name or None,
        "away_team": away_name or None,
        "home_team_id": None,
        "away_team_id": None,
        "home_team_logo": home_logo,
        "away_team_logo": away_logo,
        "status": status_text,
        "score_home": home_score or None,
        "score_away": away_score or None,
        "match_key": data_match or None,
        "match_url": detail_url,
        "competition_url": normalize_site_url(match_node.get("data-comp_link")),
        "sport_id": sport_id or None,
    }
    return event


def parse_match_payload_event(
    payload: Dict,
    fallback_tz: str,
    sport_by_id: Dict[str, str],
    keep_noisy_channels: bool = False,
) -> Optional[Dict]:
    if not isinstance(payload, dict):
        return None

    match_node: Optional[Tag] = None
    html = payload.get("html")
    if isinstance(html, str) and html.strip():
        soup = parse_html(html)
        match_node = select_match_node(soup)

    # Keep API enrichment page-aligned: skip rows without a visible match node.
    if match_node is None:
        return None

    tv_listings = payload.get("tv_listings")
    tv_listings_dict = tv_listings if isinstance(tv_listings, dict) else {}
    channels = parse_channels_from_match_payload(
        tv_listings_dict, match_node, keep_noisy_channels=keep_noisy_channels
    )
    if not channels:
        return None

    match = payload.get("match") if isinstance(payload.get("match"), dict) else {}
    home = payload.get("home") if isinstance(payload.get("home"), dict) else {}
    away = payload.get("away") if isinstance(payload.get("away"), dict) else {}
    score = payload.get("score") if isinstance(payload.get("score"), dict) else {}

    home_name = ""
    away_name = ""
    node = match_node.select_one(".match__home__name, .match_home_name")
    if node:
        home_name = node.get_text(" ", strip=True)
    node = match_node.select_one(".match__away__name, .match_away_name")
    if node:
        away_name = node.get_text(" ", strip=True)

    if not home_name:
        home_name = normalize_whitespace(home.get("name"))
    if not away_name:
        away_name = normalize_whitespace(away.get("name"))

    if not home_name and not away_name:
        return None

    event_name = build_event_name(home_name, away_name)

    data_time = match_node.get("data-time")
    start_iso, local_time = parse_match_datetime(data_time, fallback_tz)

    status_text = ""
    status_node = match_node.select_one(".matches-status")
    if status_node:
        status_text = status_node.get_text(" ", strip=True)
    if not status_text:
        status = match.get("status")
        if isinstance(status, dict):
            status_text = normalize_whitespace(status.get("value"))

    score_home = None
    score_away = None
    if isinstance(score.get("home"), dict):
        score_home = score["home"].get("value")
    if isinstance(score.get("away"), dict):
        score_away = score["away"].get("value")
    if score_home in (None, ""):
        score_node = match_node.select_one(".matches-home")
        if score_node:
            score_home = score_node.get_text(" ", strip=True)
    if score_away in (None, ""):
        score_node = match_node.select_one(".matches-away")
        if score_node:
            score_away = score_node.get_text(" ", strip=True)

    sport_id = normalize_whitespace(match_node.get("data-sport"))
    match_url = normalize_whitespace(match.get("url"))
    if not match_url:
        anchor = match_node.select_one("a.link_matchs_pjax")
        if anchor and anchor.get("href"):
            match_url = normalize_whitespace(anchor.get("href"))
    match_url = canonicalize_match_url(match_url)

    sport_name = sport_by_id.get(sport_id, "")
    if not sport_name:
        sport_name = infer_sport_from_url(match_url)
    sport_name = normalize_sport_name(sport_name)

    home_logo = choose_preferred_logo(
        extract_logo_from_node(match_node.select_one(".match__home__image img, .match_home_img img")),
        home.get("image"),
    )
    home_logo = choose_preferred_logo(home_logo, home.get("logo"))
    home_logo = choose_preferred_logo(home_logo, home.get("image_url"))

    away_logo = choose_preferred_logo(
        extract_logo_from_node(match_node.select_one(".match__away__image img, .match_away_img img")),
        away.get("image"),
    )
    away_logo = choose_preferred_logo(away_logo, away.get("logo"))
    away_logo = choose_preferred_logo(away_logo, away.get("image_url"))

    competition = normalize_whitespace(match_node.get("data-comp"))
    country = normalize_whitespace(match_node.get("data-country"))
    special = is_special_competition_event(competition, country, sport_name)
    competition_url = normalize_site_url(match_node.get("data-comp_link"))
    match_key = normalize_whitespace(match_node.get("data-match") or match_node.get("id"))

    if not match_key:
        raw_key = match.get("key")
        if raw_key:
            match_key = normalize_whitespace(raw_key)

    return {
        "name": event_name,
        "start_time_iso": start_iso,
        "time": local_time,
        "sport": sport_name,
        "competition": competition,
        "country": country,
        "special": special,
        "channels": channels,
        "home_team": home_name or None,
        "away_team": away_name or None,
        "home_team_id": None,
        "away_team_id": None,
        "home_team_logo": home_logo,
        "away_team_logo": away_logo,
        "status": status_text,
        "score_home": str(score_home).strip() if score_home not in (None, "") else None,
        "score_away": str(score_away).strip() if score_away not in (None, "") else None,
        "match_key": match_key or None,
        "match_fx_id": match.get("fx_id"),
        "match_url": match_url,
        "competition_url": competition_url,
        "sport_id": sport_id or None,
    }


def event_dedupe_key(event: Dict) -> Tuple[str, ...]:
    start_iso = normalize_identity_text(event.get("start_time_iso"))
    sport = normalize_identity_text(event.get("sport"))

    token = extract_match_identity_token(event)
    if token:
        return ("match", token, start_iso)

    home = normalize_identity_text(event.get("home_team"))
    away = normalize_identity_text(event.get("away_team"))
    if home or away:
        return ("teams", home, away, start_iso, sport)

    name = normalize_identity_text(event.get("name"))
    competition = normalize_identity_text(event.get("competition"))
    return ("name", name, start_iso, sport, competition)


def sort_events(events: List[Dict]) -> List[Dict]:
    def _sort_key(event: Dict) -> Tuple[str, str]:
        start_iso = str(event.get("start_time_iso") or "")
        name = str(event.get("name") or "")
        return (start_iso, name.lower())

    return sorted(events, key=_sort_key)


def apply_competition_special_labels(events: List[Dict]) -> List[Dict]:
    """Apply `special` as a competition-level label and propagate to all its events."""
    by_competition: Dict[Tuple[str, str, str], bool] = {}

    for event in events:
        key = (
            normalize_identity_text(event.get("competition")),
            normalize_identity_text(event.get("country")),
            normalize_identity_text(event.get("sport")),
        )
        if key not in by_competition:
            by_competition[key] = is_special_competition_event(
                event.get("competition"), event.get("country"), event.get("sport")
            )

    for event in events:
        key = (
            normalize_identity_text(event.get("competition")),
            normalize_identity_text(event.get("country")),
            normalize_identity_text(event.get("sport")),
        )
        event["special"] = bool(by_competition.get(key, False))

    return events


def extract_sport_map(soup: BeautifulSoup) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for block in soup.select("div[id^='list-sport-'][data-multisite_sport]"):
        sport_id = (block.get("data-multisite_sport") or "").strip()
        sport_name = (block.get("data-sport_name") or "").strip()
        if sport_id and sport_name:
            mapping[sport_id] = sport_name
    return mapping


def extract_comp_ignore_ids(soup: BeautifulSoup) -> List[str]:
    values = []
    for node in soup.select("input.comp_ignore"):
        value = (node.get("value") or "").strip()
        if value:
            values.append(value)
    return dedupe_strings(values)


def extract_tournaments_from_soup(
    soup: BeautifulSoup, default_iso_code: str, source_sport_key: str = ""
) -> Dict[str, TournamentRequest]:
    tournaments: Dict[str, TournamentRequest] = {}
    selector = "li[data-request_id][data-create_time][data-expire_time][data-order_by][data-time_zone]"
    for li in soup.select(selector):
        request_id = (li.get("data-request_id") or "").strip()
        create_time = (li.get("data-create_time") or "").strip()
        expire_time = (li.get("data-expire_time") or "").strip()
        order_by = (li.get("data-order_by") or "").strip()
        time_zone = (li.get("data-time_zone") or "").strip()
        iso_code = (li.get("data-iso_code") or default_iso_code or "").strip()
        if not request_id or not create_time or not expire_time or not order_by or not time_zone:
            continue
        if request_id in tournaments:
            continue
        tournaments[request_id] = TournamentRequest(
            request_id=request_id,
            create_time=create_time,
            expire_time=expire_time,
            order_by=order_by,
            time_zone=time_zone,
            iso_code=iso_code,
            source_sport_key=source_sport_key,
        )
    return tournaments


def merge_tournaments(dest: Dict[str, TournamentRequest], src: Dict[str, TournamentRequest]) -> None:
    for request_id, tournament in src.items():
        if request_id not in dest:
            dest[request_id] = tournament


def parse_data_today_tournaments(
    payload: Dict[str, List[str]], default_iso_code: str
) -> Dict[str, TournamentRequest]:
    tournaments: Dict[str, TournamentRequest] = {}
    for sport_key, chunks in payload.items():
        if not chunks:
            continue
        html = "".join(chunks)
        soup = parse_html(html)
        parsed = extract_tournaments_from_soup(
            soup, default_iso_code=default_iso_code, source_sport_key=sport_key
        )
        merge_tournaments(tournaments, parsed)
    return tournaments


def scrape_one_date(
    client: LiveSportTVClient,
    target_date: dt.date,
    include_data_today: bool,
    max_pages: int,
    max_tournaments: Optional[int],
    keep_noisy_channels: bool,
    html_override: Optional[str] = None,
) -> Tuple[List[Dict], Dict[str, int]]:
    html = html_override if html_override is not None else client.get_schedule_html(target_date)
    soup = parse_html(html)

    config = extract_script_config(html)
    sport_by_id = extract_sport_map(soup)
    comp_ignore_ids = extract_comp_ignore_ids(soup)
    comp_ignore = ",".join(comp_ignore_ids) if comp_ignore_ids else "[]"

    tournaments = extract_tournaments_from_soup(soup, default_iso_code=config["iso_code"])
    initial_matches: List[Dict] = []
    for li in iter_unique_match_nodes(soup):
        event = parse_li_match_event(
            li,
            default_tz=config["time_zone"],
            sport_by_id=sport_by_id,
            keep_noisy_channels=keep_noisy_channels,
        )
        if event:
            initial_matches.append(event)

    if include_data_today and html_override is None:
        for page in range(1, max(1, max_pages) + 1):
            params = {
                "date_today": target_date.isoformat(),
                "locale": config["locale"],
                "version": config["version"],
                "time_zone": config["time_zone"],
                "iso_code": config["iso_code"],
                "comp_ignore": comp_ignore,
                "page": str(page),
            }
            payload = client.get_data_today(params)
            if not payload:
                continue
            parsed = parse_data_today_tournaments(payload, default_iso_code=config["iso_code"])
            merge_tournaments(tournaments, parsed)

    api_events: List[Dict] = []
    success = 0
    failed = 0
    tournament_items: List[TournamentRequest] = []
    if include_data_today:
        tournament_items = list(tournaments.values())
        if max_tournaments is not None and max_tournaments > 0:
            tournament_items = tournament_items[:max_tournaments]

        for tournament in tournament_items:
            try:
                payload = client.get_tournament(tournament)
            except Exception:
                failed += 1
                continue

            matches = payload.get("matches")
            if not isinstance(matches, list):
                continue

            for raw_match in matches:
                event = parse_match_payload_event(
                    raw_match if isinstance(raw_match, dict) else {},
                    fallback_tz=tournament.time_zone,
                    sport_by_id=sport_by_id,
                    keep_noisy_channels=keep_noisy_channels,
                )
                if event:
                    api_events.append(event)
            success += 1

    merged_events: Dict[Tuple[str, ...], Dict] = {}
    for event in initial_matches + api_events:
        key = event_dedupe_key(event)
        if key not in merged_events:
            merged_events[key] = event
            continue
        merged_events[key] = merge_duplicate_events(merged_events[key], event)

    events = sort_events(list(merged_events.values()))
    events = apply_competition_special_labels(events)

    return events, {
        "initial_matches": len(initial_matches),
        "api_matches": len(api_events),
        "tournaments_total": len(tournament_items),
        "tournaments_success": success,
        "tournaments_failed": failed,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape daily or multi-day TV guide from LiveSportTV.")
    parser.add_argument(
        "--date",
        default=None,
        help="Start date in YYYY-MM-DD format. Default: today in UTC.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to scrape, starting from --date (default: 1).",
    )
    parser.add_argument(
        "--output",
        default="weekly_schedule_livesporttv.json",
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=2,
        help="How many /data-today pages to request per day (default: 2, same as site script).",
    )
    parser.add_argument(
        "--max-tournaments",
        type=int,
        default=None,
        help="Optional hard cap for tournament API calls per day (debug/partial runs).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=45,
        help="HTTP timeout in seconds (default: 45).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Retry attempts per request (default: 4).",
    )
    parser.add_argument(
        "--backoff",
        type=float,
        default=1.75,
        help="Linear backoff multiplier in seconds (default: 1.75).",
    )
    parser.add_argument(
        "--no-data-today",
        action="store_true",
        help="Disable /data-today + tournament API enrichment and parse only schedule HTML.",
    )
    parser.add_argument(
        "--keep-noisy-channels",
        action="store_true",
        help="Keep channels that look like app/website/domain names (default: filtered out).",
    )
    parser.add_argument(
        "--html-file",
        default=None,
        help="Parse a local schedules HTML file instead of requesting LiveSportTV (single-day only).",
    )
    return parser.parse_args()


def parse_start_date(raw: Optional[str]) -> dt.date:
    if not raw:
        return dt.datetime.now(dt.timezone.utc).date()
    return dt.datetime.strptime(raw.strip(), "%Y-%m-%d").date()


def main() -> int:
    args = parse_args()

    if args.days < 1:
        print("--days must be >= 1", file=sys.stderr)
        return 2

    try:
        start_date = parse_start_date(args.date)
    except ValueError:
        print(f"Invalid --date value: {args.date!r}. Expected YYYY-MM-DD.", file=sys.stderr)
        return 2

    html_override: Optional[str] = None
    if args.html_file:
        if args.days != 1:
            print("--html-file supports only --days 1.", file=sys.stderr)
            return 2
        try:
            with open(args.html_file, "r", encoding="utf-8") as handle:
                html_override = handle.read()
        except OSError as exc:
            print(f"Failed to read --html-file '{args.html_file}': {exc}", file=sys.stderr)
            return 1

    client = LiveSportTVClient(timeout=args.timeout, retries=args.retries, backoff_seconds=args.backoff)

    schedule: List[Dict] = []
    aggregate_stats = {
        "initial_matches": 0,
        "api_matches": 0,
        "tournaments_total": 0,
        "tournaments_success": 0,
        "tournaments_failed": 0,
    }

    for offset in range(args.days):
        target_date = start_date + dt.timedelta(days=offset)
        print(f"[LiveSportTV] Scraping {target_date.isoformat()} ...")
        try:
            events, stats = scrape_one_date(
                client=client,
                target_date=target_date,
                include_data_today=not args.no_data_today,
                max_pages=args.max_pages,
                max_tournaments=args.max_tournaments,
                keep_noisy_channels=args.keep_noisy_channels,
                html_override=html_override if offset == 0 else None,
            )
        except Exception as exc:
            print(f"[LiveSportTV] Failed on {target_date.isoformat()}: {exc}", file=sys.stderr)
            return 1

        for key in aggregate_stats:
            aggregate_stats[key] += stats.get(key, 0)

        schedule.append(
            {
                "date": target_date.isoformat(),
                "day": target_date.strftime("%A"),
                "events": events,
            }
        )
        print(
            f"[LiveSportTV] {target_date.isoformat()} -> {len(events)} events "
            f"(initial={stats['initial_matches']}, api={stats['api_matches']}, "
            f"tournaments={stats['tournaments_success']}/{stats['tournaments_total']})"
        )

    payload = {
        "generated_at": iso_z_now(),
        "source": "livesporttv.com",
        "schedule": schedule,
        "extraction": {
            "mode": "schedule+data-today+tournament-api" if not args.no_data_today else "schedule-only",
            "max_pages": max(1, args.max_pages),
            "stats": aggregate_stats,
        },
    }

    try:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
    except OSError as exc:
        print(f"Failed to save output to '{args.output}': {exc}", file=sys.stderr)
        return 1

    print(f"[LiveSportTV] Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
