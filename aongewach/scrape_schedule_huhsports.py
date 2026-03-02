#!/usr/bin/env python3
"""
Scrape HuhSports TV Guide and extract teams, team logos, and TV network names.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


DEFAULT_URL = "https://www.huhsports.com/tv-guide"
DEFAULT_OUTPUT = "weekly_schedule_huhsports.json"
DEFAULT_TIMEOUT = 30
DEFAULT_DAYS = 7
DEFAULT_LOGO_PREFIX = "https://flwvkgyqubvqipqkkqmo.supabase.co/storage/v1/object/public"

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


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


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


def scrape_page(session: requests.Session, url: str, timeout: int) -> Tuple[List[Dict], List[Dict], str]:
    response = session.get(url, headers={"User-Agent": UA}, timeout=max(1, timeout))
    response.raise_for_status()
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


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape teams, team logos, and TV names from HuhSports TV Guide."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="TV guide URL.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON path.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds.")
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


def run(url: str, output: str, timeout: int, start_date: dt.date, days: int, sample: int) -> int:
    days = max(1, int(days))
    session = requests.Session()

    base_leagues, base_matches, final_base_url = scrape_page(session, url, timeout)
    combined_matches: List[Dict] = list(base_matches)
    base_available_dates = extract_match_dates(base_matches)

    requested_dates = [start_date + dt.timedelta(days=offset) for offset in range(days)]
    requested_date_set = {date.isoformat() for date in requested_dates}

    attempted_urls: List[str] = [url]
    attempted_url_set: Set[str] = {url}
    probe_hits: Dict[str, Optional[str]] = {}

    for target_date in requested_dates:
        target_iso = target_date.isoformat()
        hit_url: Optional[str] = None

        for probe_url in build_probe_urls_for_date(url, target_date):
            if probe_url in attempted_url_set:
                continue
            attempted_url_set.add(probe_url)
            attempted_urls.append(probe_url)

            try:
                _, probe_matches, _ = scrape_page(session, probe_url, timeout)
            except Exception:
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

    with open(output, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

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
