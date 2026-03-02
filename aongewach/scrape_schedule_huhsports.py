#!/usr/bin/env python3
"""
Scrape HuhSports TV Guide and extract teams, team logos, and TV network names.
"""

from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import json
import os
import random
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


DEFAULT_URL = "https://www.huhsports.com/tv-guide"
DEFAULT_OUTPUT = "weekly_schedule_huhsports.json"
DEFAULT_TIMEOUT = 30
DEFAULT_DAYS = 7
DEFAULT_LOGO_PREFIX = "https://flwvkgyqubvqipqkkqmo.supabase.co/storage/v1/object/public"
DEFAULT_HTTP_RETRIES = 6
DEFAULT_HTTP_BACKOFF_SECONDS = 2.0
DEFAULT_HTTP_MAX_BACKOFF_SECONDS = 90.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 1.0
DEFAULT_MAX_PROBE_REQUESTS = 14
DEFAULT_STOP_PROBING_AFTER_RATE_LIMITS = 2
DEFAULT_PROXY_MODE = "round_robin"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

FLIGHT_CHUNK_RE = re.compile(
    r"self\.__next_f\.push\(\[1,\"((?:[^\"\\]|\\.)*)\"\]\)",
    re.DOTALL,
)
INITIAL_LEAGUES_KEY = "\"initialLeagues\":"
TEAM_LOGO_URL_RE = re.compile(
    r"(https://[^\"'\s]+/storage/v1/object/public)/icons/team/(\d+)\.(png|webp|jpg|jpeg|svg)",
    re.IGNORECASE,
)
URL_CREDENTIAL_RE = re.compile(r"(https?://)([^/@\s]+)@", re.IGNORECASE)


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def sanitize_error_text(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return URL_CREDENTIAL_RE.sub(r"\1***@", text)


def normalize_proxy_url(raw: object) -> str:
    text = normalize_text(raw)
    if not text:
        return ""

    if "://" in text:
        parsed = urlsplit(text)
        if parsed.scheme and parsed.netloc:
            return text
        return ""

    if text.count(":") == 3:
        host, port, username, password = text.split(":", 3)
        if host and port and username and password:
            return f"http://{username}:{password}@{host}:{port}"
        return ""

    if "@" in text:
        return f"http://{text}"

    if text.count(":") == 1:
        host, port = text.split(":", 1)
        if host and port:
            return f"http://{host}:{port}"
    return ""


def _read_proxy_tokens_from_text(raw: str) -> List[str]:
    tokens: List[str] = []
    if not raw:
        return tokens
    normalized_lines = raw.replace(",", "\n").replace(";", "\n").splitlines()
    for line in normalized_lines:
        token = normalize_text(line)
        if not token or token.startswith("#"):
            continue
        tokens.append(token)
    return tokens


def load_proxy_pool(proxy_file: str, proxy_list: str) -> List[str]:
    pool: List[str] = []
    seen = set()

    for token in _read_proxy_tokens_from_text(proxy_list):
        proxy_url = normalize_proxy_url(token)
        if not proxy_url:
            continue
        key = proxy_url.casefold()
        if key in seen:
            continue
        seen.add(key)
        pool.append(proxy_url)

    file_path = normalize_text(proxy_file)
    if file_path and os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                text = handle.read()
        except Exception:
            text = ""
        for token in _read_proxy_tokens_from_text(text):
            proxy_url = normalize_proxy_url(token)
            if not proxy_url:
                continue
            key = proxy_url.casefold()
            if key in seen:
                continue
            seen.add(key)
            pool.append(proxy_url)

    return pool


def proxy_label(proxy_url: str) -> str:
    parsed = urlsplit(proxy_url)
    host = parsed.hostname or "proxy"
    port = parsed.port
    if port:
        return f"{host}:{port}"
    return host


def choose_proxy(session: requests.Session, proxy_pool: List[str], proxy_mode: str) -> Optional[str]:
    if not proxy_pool:
        return None
    mode = normalize_text(proxy_mode).lower()
    if mode == "random":
        return random.choice(proxy_pool)

    idx = int(getattr(session, "_proxy_index", 0))
    selected = proxy_pool[idx % len(proxy_pool)]
    setattr(session, "_proxy_index", idx + 1)
    return selected


def parse_start_date(raw: Optional[str]) -> dt.date:
    if not raw:
        return dt.datetime.now(dt.timezone.utc).date()
    return dt.datetime.strptime(raw.strip(), "%Y-%m-%d").date()


def parse_positive_int(value: object) -> Optional[int]:
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            parsed = int(text)
            if parsed > 0:
                return parsed
    return None


def dedupe_strings(values: Iterable[object]) -> List[str]:
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


def parse_int_or_none(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text and (text.isdigit() or (text.startswith("-") and text[1:].isdigit())):
            return int(text)
    return None


def build_url_with_query(base_url: str, extra_params: Dict[str, str]) -> str:
    parsed = urlsplit(base_url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_map = {key: value for key, value in query_pairs}
    for key, value in extra_params.items():
        query_map[key] = str(value)
    query = urlencode(query_map, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def build_probe_urls_for_date(base_url: str, target_date: dt.date) -> List[str]:
    iso_date = target_date.isoformat()
    compact_date = target_date.strftime("%Y%m%d")
    probe_sets = [
        {"date": iso_date},
        {"showdatestart": compact_date},
        {"showDateStart": compact_date},
        {"startDate": iso_date, "endDate": iso_date},
        {"from": iso_date, "to": iso_date},
        {"day": iso_date},
    ]
    return dedupe_strings(build_url_with_query(base_url, params) for params in probe_sets)


def extract_balanced_json(text: str, start_index: int) -> str:
    if start_index < 0 or start_index >= len(text):
        raise ValueError("Invalid JSON start index.")

    opening = text[start_index]
    if opening not in "[{":
        raise ValueError("Expected JSON array/object start.")
    closing = "]" if opening == "[" else "}"

    depth = 0
    in_string = False
    escaped = False

    for idx in range(start_index, len(text)):
        ch = text[idx]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == "\"":
                in_string = False
            continue

        if ch == "\"":
            in_string = True
            continue
        if ch == opening:
            depth += 1
            continue
        if ch == closing:
            depth -= 1
            if depth == 0:
                return text[start_index : idx + 1]

    raise ValueError("Could not find balanced JSON segment.")


def decode_next_flight_chunks(html: str) -> List[str]:
    decoded: List[str] = []
    for match in FLIGHT_CHUNK_RE.finditer(html):
        raw_chunk = match.group(1)
        try:
            decoded_chunk = json.loads("\"" + raw_chunk + "\"")
        except json.JSONDecodeError:
            continue
        decoded.append(decoded_chunk)
    return decoded


def extract_initial_leagues(decoded_chunks: Iterable[str]) -> List[Dict]:
    for chunk in decoded_chunks:
        key_index = chunk.find(INITIAL_LEAGUES_KEY)
        if key_index < 0:
            continue

        json_start = chunk.find("[", key_index)
        if json_start < 0:
            continue

        try:
            leagues_text = extract_balanced_json(chunk, json_start)
            parsed = json.loads(leagues_text)
        except (ValueError, json.JSONDecodeError):
            continue

        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]

    return []


def extract_logo_map(html: str) -> Tuple[Dict[int, str], str]:
    logos_by_team_id: Dict[int, str] = {}
    logo_prefix = DEFAULT_LOGO_PREFIX

    for match in TEAM_LOGO_URL_RE.finditer(html):
        prefix, team_id_text, extension = match.groups()
        logo_prefix = prefix
        team_id = parse_positive_int(team_id_text)
        if team_id is None:
            continue
        logos_by_team_id[team_id] = f"{prefix}/icons/team/{team_id}.{extension.lower()}"

    return logos_by_team_id, logo_prefix


def team_logo_url(team_id: Optional[int], logos_by_team_id: Dict[int, str], logo_prefix: str) -> Optional[str]:
    if not team_id:
        return None
    if team_id in logos_by_team_id:
        return logos_by_team_id[team_id]
    return f"{logo_prefix}/icons/team/{team_id}.png"


def normalize_channel_payload(raw_channels: object) -> Tuple[List[Dict[str, object]], List[str]]:
    channel_rows: List[Dict[str, object]] = []
    channel_names: List[str] = []

    if not isinstance(raw_channels, list):
        return channel_rows, channel_names

    for raw_channel in raw_channels:
        if isinstance(raw_channel, dict):
            name = normalize_text(raw_channel.get("name"))
            if not name:
                continue
            channel_rows.append(
                {
                    "id": raw_channel.get("id"),
                    "name": name,
                    "country_code": normalize_text(raw_channel.get("country_code")) or None,
                }
            )
            channel_names.append(name)
            continue

        value = normalize_text(raw_channel)
        if not value:
            continue
        channel_rows.append({"id": None, "name": value, "country_code": None})
        channel_names.append(value)

    return channel_rows, dedupe_strings(channel_names)


def extract_matches(leagues: List[Dict], logos_by_team_id: Dict[int, str], logo_prefix: str) -> List[Dict]:
    matches: List[Dict] = []

    for league in leagues:
        league_id = parse_positive_int(league.get("id"))
        league_name = normalize_text(league.get("name")) or None
        league_slug = normalize_text(league.get("slug")) or None

        raw_matches = league.get("matches")
        if not isinstance(raw_matches, list):
            continue

        for raw_match in raw_matches:
            if not isinstance(raw_match, dict):
                continue

            home_team = normalize_text(raw_match.get("homeTeam")) or None
            away_team = normalize_text(raw_match.get("awayTeam")) or None
            home_team_id = parse_positive_int(raw_match.get("team_home_id"))
            away_team_id = parse_positive_int(raw_match.get("team_away_id"))
            channel_rows, tv_names = normalize_channel_payload(raw_match.get("channels"))

            matches.append(
                {
                    "id": raw_match.get("id"),
                    "match_id": raw_match.get("match_id"),
                    "date": normalize_text(raw_match.get("date")) or None,
                    "time": normalize_text(raw_match.get("time")) or None,
                    "start_time": raw_match.get("start_time"),
                    "league_id": league_id,
                    "league": league_name,
                    "league_slug": league_slug,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "home_team_logo": team_logo_url(home_team_id, logos_by_team_id, logo_prefix),
                    "away_team_logo": team_logo_url(away_team_id, logos_by_team_id, logo_prefix),
                    "tv_names": tv_names,
                    "channels": channel_rows,
                }
            )

    return matches


def clone_match(match: Dict) -> Dict:
    return {
        **match,
        "tv_names": list(match.get("tv_names") or []),
        "channels": [
            {
                "id": channel.get("id"),
                "name": normalize_text(channel.get("name")),
                "country_code": normalize_text(channel.get("country_code")) or None,
            }
            for channel in (match.get("channels") or [])
            if isinstance(channel, dict) and normalize_text(channel.get("name"))
        ],
    }


def merge_channels(existing: List[Dict], incoming: List[Dict]) -> List[Dict]:
    out: List[Dict] = []
    by_name_lower: Dict[str, Dict] = {}

    for raw_channel in list(existing) + list(incoming):
        if not isinstance(raw_channel, dict):
            continue
        name = normalize_text(raw_channel.get("name"))
        if not name:
            continue
        key = name.casefold()
        channel_id = raw_channel.get("id")
        country_code = normalize_text(raw_channel.get("country_code")) or None

        if key not in by_name_lower:
            node = {"id": channel_id, "name": name, "country_code": country_code}
            by_name_lower[key] = node
            out.append(node)
            continue

        node = by_name_lower[key]
        if node.get("id") is None and channel_id is not None:
            node["id"] = channel_id
        if not node.get("country_code") and country_code:
            node["country_code"] = country_code

    return out


def match_identity_key(match: Dict) -> str:
    for field in ("match_id", "id"):
        value = normalize_text(match.get(field))
        if value:
            return f"{field}:{value}"

    return "|".join(
        [
            normalize_text(match.get("date")),
            normalize_text(match.get("time")),
            normalize_text(match.get("home_team")),
            normalize_text(match.get("away_team")),
            normalize_text(match.get("league_id")),
            normalize_text(match.get("league")),
        ]
    )


def merge_match(existing: Dict, incoming: Dict) -> Dict:
    merged = clone_match(existing)

    scalar_fields = [
        "id",
        "match_id",
        "date",
        "time",
        "start_time",
        "league_id",
        "league",
        "league_slug",
        "home_team",
        "away_team",
        "home_team_id",
        "away_team_id",
        "home_team_logo",
        "away_team_logo",
    ]
    for field in scalar_fields:
        existing_value = merged.get(field)
        incoming_value = incoming.get(field)
        if (existing_value is None or normalize_text(existing_value) == "") and incoming_value not in (None, ""):
            merged[field] = incoming_value

    merged["tv_names"] = dedupe_strings(list(merged.get("tv_names") or []) + list(incoming.get("tv_names") or []))
    merged["channels"] = merge_channels(
        list(merged.get("channels") or []),
        list(incoming.get("channels") or []),
    )
    return merged


def merge_match_list(matches: List[Dict]) -> List[Dict]:
    merged_map: Dict[str, Dict] = {}
    for raw_match in matches:
        if not isinstance(raw_match, dict):
            continue
        key = match_identity_key(raw_match)
        if key not in merged_map:
            merged_map[key] = clone_match(raw_match)
            continue
        merged_map[key] = merge_match(merged_map[key], raw_match)
    return list(merged_map.values())


def extract_match_dates(matches: Iterable[Dict]) -> Set[str]:
    return {
        normalize_text(match.get("date"))
        for match in matches
        if normalize_text(match.get("date"))
    }


def sort_matches(matches: List[Dict]) -> List[Dict]:
    def key_fn(match: Dict) -> Tuple[str, int, str, str, str]:
        date_key = normalize_text(match.get("date")) or "9999-99-99"
        start_time_raw = parse_int_or_none(match.get("start_time"))
        start_time_key = start_time_raw if start_time_raw is not None else 2_147_483_647
        time_key = normalize_text(match.get("time")) or "99:99"
        league_key = normalize_text(match.get("league")) or ""
        home_key = normalize_text(match.get("home_team")) or ""
        return (date_key, start_time_key, time_key, league_key, home_key)

    return sorted(matches, key=key_fn)


def unique_league_count(matches: Iterable[Dict]) -> int:
    leagues: Set[str] = set()
    for match in matches:
        league_id = parse_positive_int(match.get("league_id"))
        league_slug = normalize_text(match.get("league_slug"))
        league_name = normalize_text(match.get("league"))
        if league_id:
            leagues.add(f"id:{league_id}")
        elif league_slug:
            leagues.add(f"slug:{league_slug.casefold()}")
        elif league_name:
            leagues.add(f"name:{league_name.casefold()}")
    return len(leagues)


class FetchError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


def parse_retry_after_seconds(value: object) -> Optional[float]:
    text = normalize_text(value)
    if not text:
        return None
    if text.isdigit():
        seconds = int(text)
        return max(0.0, float(seconds))
    try:
        target = email.utils.parsedate_to_datetime(text)
    except Exception:
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(dt.timezone.utc)
    delay = (target - now).total_seconds()
    return max(0.0, delay)


def _enforce_request_spacing(session: requests.Session, min_interval_seconds: float) -> None:
    if min_interval_seconds <= 0:
        return
    last_ts = float(getattr(session, "_last_request_monotonic", 0.0))
    now = time.monotonic()
    wait_for = (last_ts + min_interval_seconds) - now
    if wait_for > 0:
        time.sleep(wait_for)


def _mark_request_time(session: requests.Session) -> None:
    setattr(session, "_last_request_monotonic", time.monotonic())


def fetch_url(
    session: requests.Session,
    url: str,
    timeout: int,
    retries: int,
    backoff_seconds: float,
    max_backoff_seconds: float,
    min_interval_seconds: float,
    proxy_pool: List[str],
    proxy_mode: str,
) -> requests.Response:
    max_attempts = max(1, int(retries))

    for attempt in range(1, max_attempts + 1):
        _enforce_request_spacing(session, min_interval_seconds)
        proxy_url = choose_proxy(session, proxy_pool, proxy_mode)
        request_kwargs = {}
        if proxy_url:
            request_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}

        try:
            response = session.get(url, timeout=max(1, timeout), **request_kwargs)
        except requests.RequestException as exc:
            _mark_request_time(session)
            if attempt >= max_attempts:
                raise FetchError(f"Request error for {url}: {sanitize_error_text(exc)}") from exc
            delay = min(max_backoff_seconds, backoff_seconds * (2 ** (attempt - 1))) + random.uniform(0.15, 0.85)
            proxy_suffix = f" via {proxy_label(proxy_url)}" if proxy_url else ""
            print(
                f"[HuhSports] request error on attempt {attempt}/{max_attempts} for {url}{proxy_suffix}; retrying in {delay:.1f}s...",
                flush=True,
            )
            time.sleep(delay)
            continue

        _mark_request_time(session)
        status = int(response.status_code)
        if 200 <= status < 300:
            return response

        retryable = status in {429, 500, 502, 503, 504}
        if retryable and attempt < max_attempts:
            retry_after = parse_retry_after_seconds(response.headers.get("Retry-After"))
            if retry_after is not None:
                delay = min(max_backoff_seconds, retry_after)
            else:
                delay = min(max_backoff_seconds, backoff_seconds * (2 ** (attempt - 1))) + random.uniform(0.15, 0.85)
            proxy_suffix = f" via {proxy_label(proxy_url)}" if proxy_url else ""
            print(
                f"[HuhSports] HTTP {status} on attempt {attempt}/{max_attempts} for {url}{proxy_suffix}; retrying in {delay:.1f}s...",
                flush=True,
            )
            time.sleep(delay)
            continue

        raise FetchError(f"HTTP {status} for {url}", status_code=status)

    raise FetchError(f"Exhausted retries for {url}")


def scrape_page(
    session: requests.Session,
    url: str,
    timeout: int,
    retries: int,
    backoff_seconds: float,
    max_backoff_seconds: float,
    min_interval_seconds: float,
    proxy_pool: List[str],
    proxy_mode: str,
) -> Tuple[List[Dict], List[Dict], str]:
    response = fetch_url(
        session=session,
        url=url,
        timeout=timeout,
        retries=retries,
        backoff_seconds=backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
        min_interval_seconds=min_interval_seconds,
        proxy_pool=proxy_pool,
        proxy_mode=proxy_mode,
    )
    html = response.text

    decoded_chunks = decode_next_flight_chunks(html)
    if not decoded_chunks:
        raise RuntimeError("Could not decode Next.js payload chunks from page.")

    leagues = extract_initial_leagues(decoded_chunks)
    if not leagues:
        raise RuntimeError("Could not locate `initialLeagues` payload in page content.")

    logos_by_team_id, logo_prefix = extract_logo_map(html)
    matches = extract_matches(leagues, logos_by_team_id, logo_prefix)
    return leagues, matches, response.url


def build_payload(
    url: str,
    matches: List[Dict],
    requested_start_date: dt.date,
    requested_days: int,
    exposure_detected: bool,
    base_available_dates: Set[str],
    probe_attempted_urls: List[str],
    probe_hits: Dict[str, Optional[str]],
) -> Dict[str, object]:
    unique_tv_names = dedupe_strings(
        channel_name
        for match in matches
        for channel_name in match.get("tv_names", [])
    )
    unique_teams = dedupe_strings(
        team_name
        for match in matches
        for team_name in (match.get("home_team"), match.get("away_team"))
    )
    matches_with_channels = sum(1 for match in matches if match.get("tv_names"))
    available_dates = sorted(extract_match_dates(matches))
    requested_end_date = requested_start_date + dt.timedelta(days=max(1, requested_days) - 1)
    hit_dates = sorted([date for date, hit_url in probe_hits.items() if hit_url])
    missed_dates = sorted([date for date, hit_url in probe_hits.items() if not hit_url])

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "huhsports.com/tv-guide",
        "url": url,
        "league_count": unique_league_count(matches),
        "match_count": len(matches),
        "match_count_with_channels": matches_with_channels,
        "unique_team_count": len(unique_teams),
        "unique_tv_name_count": len(unique_tv_names),
        "available_dates": available_dates,
        "requested_window": {
            "start_date": requested_start_date.isoformat(),
            "end_date": requested_end_date.isoformat(),
            "days": max(1, requested_days),
        },
        "date_probe": {
            "week_window_exposed": exposure_detected,
            "base_available_dates": sorted(base_available_dates),
            "urls_attempted_count": len(probe_attempted_urls),
            "urls_attempted": probe_attempted_urls,
            "requested_dates_found": hit_dates,
            "requested_dates_not_found": missed_dates,
        },
        "matches": matches,
    }


def build_empty_fallback_payload(
    url: str,
    requested_start_date: dt.date,
    requested_days: int,
    attempted_urls: List[str],
    fallback_source: str,
    fallback_reason: str,
) -> Dict[str, object]:
    requested_dates = [requested_start_date + dt.timedelta(days=offset) for offset in range(max(1, requested_days))]
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "huhsports.com/tv-guide",
        "url": url,
        "league_count": 0,
        "match_count": 0,
        "match_count_with_channels": 0,
        "unique_team_count": 0,
        "unique_tv_name_count": 0,
        "available_dates": [],
        "requested_window": {
            "start_date": requested_start_date.isoformat(),
            "end_date": (requested_start_date + dt.timedelta(days=max(1, requested_days) - 1)).isoformat(),
            "days": max(1, requested_days),
        },
        "date_probe": {
            "week_window_exposed": False,
            "base_available_dates": [],
            "urls_attempted_count": len(attempted_urls),
            "urls_attempted": attempted_urls,
            "requested_dates_found": [],
            "requested_dates_not_found": [date.isoformat() for date in requested_dates],
            "probe_requests_used": 0,
            "probe_rate_limit_hits": 0,
            "probe_truncated": True,
            "fallback_used": True,
            "fallback_mode": "empty",
            "fallback_source": fallback_source,
            "fallback_reason": fallback_reason,
        },
        "matches": [],
    }


def load_fallback_payload(path: str) -> Optional[Dict[str, object]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if not isinstance(payload.get("matches"), list):
        return None
    return payload


def persist_payload(path: str, payload: Dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape teams, team logos, and TV names from HuhSports TV Guide."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="TV guide URL.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON path.")
    parser.add_argument(
        "--proxy-file",
        default=os.getenv("HUHSPORTS_PROXY_FILE", ""),
        help="Optional proxy file path (one proxy per line). Supports ip:port:user:pass format.",
    )
    parser.add_argument(
        "--proxy-list",
        default=os.getenv("HUHSPORTS_PROXY_LIST", ""),
        help="Optional inline proxy list (env-friendly; newline/comma/semicolon separated).",
    )
    parser.add_argument(
        "--proxy-mode",
        choices=["round_robin", "random"],
        default=os.getenv("HUHSPORTS_PROXY_MODE", DEFAULT_PROXY_MODE),
        help="Proxy rotation strategy when multiple proxies are supplied.",
    )
    parser.add_argument(
        "--fallback-file",
        default="",
        help="Fallback JSON path to use when HuhSports is temporarily unavailable (default: output path).",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--http-retries",
        type=int,
        default=DEFAULT_HTTP_RETRIES,
        help="Total HTTP attempts per request (retries with backoff for 429/5xx).",
    )
    parser.add_argument(
        "--http-backoff-seconds",
        type=float,
        default=DEFAULT_HTTP_BACKOFF_SECONDS,
        help="Base exponential backoff seconds.",
    )
    parser.add_argument(
        "--http-max-backoff-seconds",
        type=float,
        default=DEFAULT_HTTP_MAX_BACKOFF_SECONDS,
        help="Max backoff cap in seconds.",
    )
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
        help="Minimum spacing between HTTP requests to reduce rate-limit pressure.",
    )
    parser.add_argument(
        "--max-probe-requests",
        type=int,
        default=DEFAULT_MAX_PROBE_REQUESTS,
        help="Maximum date-probe requests after base page fetch.",
    )
    parser.add_argument(
        "--stop-probing-after-rate-limits",
        type=int,
        default=DEFAULT_STOP_PROBING_AFTER_RATE_LIMITS,
        help="Stop additional probe calls after this many 429 probe failures.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for week probing in YYYY-MM-DD (default: today UTC).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="Number of days to probe for date-specific schedule exposure (default: 7).",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=5,
        help="Print the first N extracted matches for quick local validation.",
    )
    return parser.parse_args()


def run(
    url: str,
    output: str,
    timeout: int,
    start_date: dt.date,
    days: int,
    sample: int,
    proxy_file: str,
    proxy_list: str,
    proxy_mode: str,
    fallback_file: str,
    http_retries: int,
    http_backoff_seconds: float,
    http_max_backoff_seconds: float,
    min_request_interval_seconds: float,
    max_probe_requests: int,
    stop_probing_after_rate_limits: int,
) -> int:
    days = max(1, int(days))
    http_retries = max(1, int(http_retries))
    http_backoff_seconds = max(0.25, float(http_backoff_seconds))
    http_max_backoff_seconds = max(1.0, float(http_max_backoff_seconds))
    min_request_interval_seconds = max(0.0, float(min_request_interval_seconds))
    max_probe_requests = max(0, int(max_probe_requests))
    stop_probing_after_rate_limits = max(0, int(stop_probing_after_rate_limits))
    proxy_mode = normalize_text(proxy_mode).lower() or DEFAULT_PROXY_MODE

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
    )

    fallback_path = normalize_text(fallback_file) or output
    proxy_pool = load_proxy_pool(proxy_file=proxy_file, proxy_list=proxy_list)
    if proxy_pool:
        print(f"[HuhSports] Using {len(proxy_pool)} proxies (mode={proxy_mode}).", flush=True)

    try:
        base_leagues, base_matches, final_base_url = scrape_page(
            session=session,
            url=url,
            timeout=timeout,
            retries=http_retries,
            backoff_seconds=http_backoff_seconds,
            max_backoff_seconds=http_max_backoff_seconds,
            min_interval_seconds=min_request_interval_seconds,
            proxy_pool=proxy_pool,
            proxy_mode=proxy_mode,
        )
    except Exception as exc:
        # Fail-open for transient fetch/rate-limit issues so the wider pipeline can proceed.
        recoverable = isinstance(exc, (FetchError, requests.RequestException))
        if not recoverable:
            raise

        fallback_payload = load_fallback_payload(fallback_path)
        if fallback_payload is not None:
            fallback_payload["generated_at"] = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace(
                "+00:00", "Z"
            )
            fallback_probe = fallback_payload.get("date_probe")
            if not isinstance(fallback_probe, dict):
                fallback_probe = {}
                fallback_payload["date_probe"] = fallback_probe
            fallback_probe["fallback_used"] = True
            fallback_probe["fallback_mode"] = "cached"
            fallback_probe["fallback_source"] = fallback_path
            fallback_probe["fallback_reason"] = sanitize_error_text(exc)
            persist_payload(output, fallback_payload)
            print(
                f"[HuhSports] base scrape unavailable ({sanitize_error_text(exc)}). Reused fallback data from {fallback_path} and wrote {output}.",
                flush=True,
            )
            return 0

        empty_payload = build_empty_fallback_payload(
            url=url,
            requested_start_date=start_date,
            requested_days=days,
            attempted_urls=[url],
            fallback_source=fallback_path,
            fallback_reason=sanitize_error_text(exc),
        )
        persist_payload(output, empty_payload)
        print(
            f"[HuhSports] base scrape unavailable ({sanitize_error_text(exc)}). No cached fallback found; wrote empty fallback payload to {output}.",
            flush=True,
        )
        return 0

    combined_matches: List[Dict] = list(base_matches)
    base_available_dates = extract_match_dates(base_matches)

    requested_dates = [start_date + dt.timedelta(days=offset) for offset in range(days)]
    requested_date_set = {date.isoformat() for date in requested_dates}

    attempted_urls: List[str] = [url]
    attempted_url_set: Set[str] = {url}
    probe_hits: Dict[str, Optional[str]] = {}
    probe_requests_used = 0
    probe_rate_limit_hits = 0
    stop_probing = False

    for target_date in requested_dates:
        target_iso = target_date.isoformat()
        hit_url: Optional[str] = None

        if stop_probing:
            probe_hits[target_iso] = None
            continue

        for probe_url in build_probe_urls_for_date(url, target_date):
            if probe_requests_used >= max_probe_requests:
                stop_probing = True
                break
            if probe_url in attempted_url_set:
                continue
            attempted_url_set.add(probe_url)
            attempted_urls.append(probe_url)
            probe_requests_used += 1

            try:
                _, probe_matches, _ = scrape_page(
                    session=session,
                    url=probe_url,
                    timeout=timeout,
                    retries=http_retries,
                    backoff_seconds=http_backoff_seconds,
                    max_backoff_seconds=http_max_backoff_seconds,
                    min_interval_seconds=min_request_interval_seconds,
                    proxy_pool=proxy_pool,
                    proxy_mode=proxy_mode,
                )
            except Exception as exc:
                status_code = getattr(exc, "status_code", None)
                if status_code == 429:
                    probe_rate_limit_hits += 1
                    if stop_probing_after_rate_limits > 0 and probe_rate_limit_hits >= stop_probing_after_rate_limits:
                        print(
                            "[HuhSports] probe rate-limited repeatedly; stopping extra probe requests for this run.",
                            flush=True,
                        )
                        stop_probing = True
                        break
                continue

            combined_matches.extend(probe_matches)
            probe_dates = extract_match_dates(probe_matches)
            if target_iso in probe_dates:
                hit_url = probe_url
                break

        probe_hits[target_iso] = hit_url

    merged_matches = sort_matches(merge_match_list(combined_matches))
    merged_dates = extract_match_dates(merged_matches)

    exposure_detected = bool((requested_date_set - base_available_dates) & merged_dates)
    if exposure_detected:
        final_matches = [match for match in merged_matches if normalize_text(match.get("date")) in requested_date_set]
    else:
        final_matches = merged_matches

    payload = build_payload(
        url=final_base_url,
        matches=final_matches,
        requested_start_date=start_date,
        requested_days=days,
        exposure_detected=exposure_detected,
        base_available_dates=base_available_dates,
        probe_attempted_urls=attempted_urls,
        probe_hits=probe_hits,
    )
    probe_meta = payload.get("date_probe")
    if not isinstance(probe_meta, dict):
        probe_meta = {}
        payload["date_probe"] = probe_meta
    probe_meta["probe_requests_used"] = probe_requests_used
    probe_meta["probe_rate_limit_hits"] = probe_rate_limit_hits
    probe_meta["probe_truncated"] = stop_probing
    probe_meta["fallback_used"] = False

    persist_payload(output, payload)

    unique_tv_count = int(payload.get("unique_tv_name_count", 0))
    unique_team_count = int(payload.get("unique_team_count", 0))
    print(f"Scraped {len(base_leagues)} base leagues from {final_base_url}")
    print(f"Window probe: {start_date.isoformat()} -> {(start_date + dt.timedelta(days=days - 1)).isoformat()} ({days} days)")
    print(
        "Date-specific week exposure: "
        + ("yes (returning requested window)" if exposure_detected else "no (returning all available)")
    )
    print(f"Total matches returned: {len(final_matches)}")
    print(f"Unique teams: {unique_team_count} | Unique TV names: {unique_tv_count}")
    print(f"Wrote output to: {output}")

    if sample > 0 and final_matches:
        print("\nSample matches:")
        for match in final_matches[:sample]:
            tv_text = ", ".join(match.get("tv_names") or []) or "(no channels)"
            print(
                "  - "
                f"{match.get('date')} {match.get('time')} | "
                f"{match.get('home_team')} vs {match.get('away_team')} | "
                f"TV: {tv_text}"
            )

    return 0


def main() -> int:
    args = parse_cli_args()
    try:
        start_date = parse_start_date(args.start_date)
        return run(
            url=args.url,
            output=args.output,
            timeout=args.timeout,
            start_date=start_date,
            days=max(1, int(args.days)),
            sample=max(0, int(args.sample)),
            proxy_file=args.proxy_file,
            proxy_list=args.proxy_list,
            proxy_mode=args.proxy_mode,
            fallback_file=args.fallback_file,
            http_retries=max(1, int(args.http_retries)),
            http_backoff_seconds=max(0.25, float(args.http_backoff_seconds)),
            http_max_backoff_seconds=max(1.0, float(args.http_max_backoff_seconds)),
            min_request_interval_seconds=max(0.0, float(args.min_request_interval_seconds)),
            max_probe_requests=max(0, int(args.max_probe_requests)),
            stop_probing_after_rate_limits=max(0, int(args.stop_probing_after_rate_limits)),
        )
    except ValueError as exc:
        print(f"Invalid argument: {exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Scrape failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
