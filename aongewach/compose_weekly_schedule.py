#!/usr/bin/env python3
"""
Compose final weekly schedule from:
  - FANZO(+WITM enrichment) as primary source
  - Secondary football source (Flashscore/HuhSports) as additional football source

Rules:
  - Keep FANZO team data when a FANZO/secondary football event matches.
  - If FANZO football teams are placeholders (e.g. TBC), recover teams from event name when possible; otherwise replace from secondary match data.
  - If FANZO football event has no usable channels (e.g. "Not Televised"), keep only when matched to secondary source.
  - Merge overlapping channels (union) and keep unique events from both sources.
  - Emit a normalized, source-agnostic schema.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import unicodedata
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional, Set, Tuple

from channel_filters import (
    is_usable_channel_name as is_usable_broadcast_channel_name,
    normalize_channel_name,
    select_regional_channel_names,
)
from channel_name_placeholders import is_placeholder_channel_name


WHITESPACE_RE = re.compile(r"\s+")
VS_TOKEN_RE = re.compile(r"\s+(?:v|vs)\s+", re.IGNORECASE)
NON_WORD_RE = re.compile(r"[^a-z0-9\s]")
YEAR_TOKEN_RE = re.compile(r"\b20\d{2}\b")

NOT_TELEVISED_RE = re.compile(
    r"\b(not\s+televised|not\s+on\s+tv|no\s+tv|no\s+broadcast|broadcast\s+tbc)\b",
    re.IGNORECASE,
)
TEAM_PLACEHOLDER_RE = re.compile(
    r"\b(tbc|tba|to\s+be\s+confirmed|to\s+be\s+announced|unknown|n/?a)\b",
    re.IGNORECASE,
)

TEAM_STOP_WORDS = {
    "fc",
    "cf",
    "sc",
    "ac",
    "afc",
    "club",
    "the",
    "de",
    "cd",
    "fk",
    "sk",
    "sv",
    "ss",
}
TEAM_ABBREVIATION_MAP = {
    "utd": "united",
    "st": "saint",
}
WITM_FOOTBALL_COMPETITION_LOGO_URL = "https://www.wheresthematch.com/images/sports/football.gif"
DEFAULT_LIVESPORTTV_PATH = "weekly_schedule_livesporttv.json"
DEFAULT_FLASHSCORE_SPORT_ASSETS_PATH = "flashscore_sport_assets.json"
DEFAULT_TEAM_LOGO_REGISTRY_PATH = "priv-boring-hole.json"
TEAM_LOGO_REGISTRY_SCHEMA_VERSION = "fanzo-team-logo-registry-v1"
GENERIC_TEAM_LOGO_PATTERNS = (
    "/images/team.png",
    "images/team.png?v=",
)
LOW_QUALITY_TEAM_LOGO_PATTERNS = (
    "flashscore.com/res/image",
)

TEAM_TOKEN_EXPANSIONS = {
    "utd": "united",
    "st": "saint",
    "ste": "sainte",
    "mt": "mount",
    "man": "manchester",
}
TEAM_ALIAS_EXPANSIONS = {
    "man city": ("manchester city",),
    "manchester city": ("man city",),
    "man utd": ("manchester united",),
    "manchester united": ("man utd",),
    "wolves": ("wolverhampton", "wolverhampton wanderers"),
    "wolverhampton": ("wolves", "wolverhampton wanderers"),
    "wolverhampton wanderers": ("wolves", "wolverhampton"),
    "spurs": ("tottenham", "tottenham hotspur"),
    "tottenham": ("spurs", "tottenham hotspur"),
    "tottenham hotspur": ("spurs", "tottenham"),
    "psg": ("paris saint germain", "paris sg"),
    "paris sg": ("psg", "paris saint germain"),
    "paris saint germain": ("psg", "paris sg"),
    "mgladbach": ("monchengladbach", "borussia monchengladbach", "gladbach"),
    "gladbach": ("monchengladbach", "borussia monchengladbach", "mgladbach"),
    "monchengladbach": ("mgladbach", "gladbach", "borussia monchengladbach"),
    "borussia monchengladbach": ("monchengladbach", "gladbach", "mgladbach"),
    "dundee utd": ("dundee united",),
    "dundee united": ("dundee utd",),
    "oxford utd": ("oxford united",),
    "oxford united": ("oxford utd",),
}
COMPETITION_ALIAS_EXPANSIONS = {
    "fa cup": ("english fa cup",),
    "english fa cup": ("fa cup",),
    "efl cup": ("english efl cup", "carabao cup"),
    "english efl cup": ("efl cup", "carabao cup"),
    "carabao cup": ("efl cup", "english efl cup"),
    "bundesliga": ("german bundesliga",),
    "german bundesliga": ("bundesliga",),
    "laliga": ("la liga", "spanish la liga"),
    "la liga": ("laliga", "spanish la liga"),
    "spanish la liga": ("laliga", "la liga"),
    "ligue 1": ("french ligue 1",),
    "french ligue 1": ("ligue 1",),
    "serie a": ("italian serie a",),
    "italian serie a": ("serie a",),
    "coupe de france": ("french coupe de france",),
    "french coupe de france": ("coupe de france",),
    "premier league": ("english premier league",),
    "english premier league": ("premier league",),
    "saudi professional league": ("saudi pro league",),
    "saudi pro league": ("saudi professional league",),
}

NORMALIZED_EVENT_FIELDS = [
    "name",
    "start_time_iso",
    "time",
    "sport",
    "competition",
    "competition_logo",
    "sport_logo",
    "channels",
    "home_team",
    "away_team",
    "home_team_id",
    "away_team_id",
    "home_team_logo",
    "away_team_logo",
]


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


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


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


def format_iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_epoch_seconds(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if text and (text.isdigit() or (text.startswith("-") and text[1:].isdigit())):
            return int(text)
    return None


def parse_hhmm(value: object) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None
    match = re.search(r"\b(\d{2}):(\d{2})\b", text)
    if not match:
        return None
    return f"{match.group(1)}:{match.group(2)}"


def derive_start_time_iso(raw_event: Dict) -> Optional[str]:
    parsed_iso = parse_iso_datetime(raw_event.get("start_time_iso") or raw_event.get("start_time_utc"))
    if parsed_iso:
        return format_iso_z(parsed_iso)

    epoch_seconds = parse_epoch_seconds(raw_event.get("start_time"))
    if epoch_seconds is not None:
        parsed_epoch = dt.datetime.fromtimestamp(epoch_seconds, tz=dt.timezone.utc)
        return format_iso_z(parsed_epoch)

    return None


def derive_time(raw_event: Dict, start_time_iso: Optional[str]) -> Optional[str]:
    time_hhmm = parse_hhmm(raw_event.get("time"))
    if time_hhmm:
        return time_hhmm

    parsed_iso = parse_iso_datetime(start_time_iso)
    if parsed_iso:
        return parsed_iso.astimezone(dt.timezone.utc).strftime("%H:%M")
    return None


def is_football_sport(sport: object) -> bool:
    key = normalize_key_text(sport)
    if not key:
        return False
    explicit_non_football = (
        "american football",
        "australian rules",
        "gaelic football",
        "nfl",
    )
    if any(token in key for token in explicit_non_football):
        return False
    return key == "soccer" or key == "football" or "soccer" in key or "football" in key


def is_usable_channel_name(name: str) -> bool:
    cleaned = normalize_text(name)
    if not cleaned:
        return False
    if NOT_TELEVISED_RE.search(cleaned):
        return False
    return is_usable_broadcast_channel_name(cleaned, placeholder_checker=is_placeholder_channel_name)


def clean_channels(channels: object) -> List[str]:
    if not isinstance(channels, list):
        return []
    out: List[str] = []
    seen = set()
    for item in channels:
        if isinstance(item, dict):
            raw_value = normalize_text(item.get("name"))
        else:
            raw_value = normalize_text(item)
        if not raw_value:
            continue
        if not is_usable_channel_name(raw_value):
            continue
        value = normalize_channel_name(raw_value)
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def channels_from_raw(raw_event: Dict) -> List[str]:
    channels = clean_channels(raw_event.get("channels"))
    if channels:
        return channels
    return clean_channels(raw_event.get("tv_names"))


def to_int_or_none(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text and (text.isdigit() or (text.startswith("-") and text[1:].isdigit())):
            return int(text)
    return None


def split_match_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    parts = re.split(r"\s+(?:v|vs)\s+", normalize_text(name), maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return None, None
    left = normalize_text(parts[0]) or None
    right = normalize_text(parts[1]) or None
    return left, right


def canonical_event_name(value: object) -> str:
    text = strip_accents(normalize_key_text(value))
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = text.replace("(w)", " women ").replace("(m)", " men ")
    text = VS_TOKEN_RE.sub(" vs ", text)
    text = YEAR_TOKEN_RE.sub(" ", text)
    text = NON_WORD_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def canonical_team_name(value: object) -> str:
    text = strip_accents(normalize_key_text(value))
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = NON_WORD_RE.sub(" ", text)
    tokens: List[str] = []
    for token in WHITESPACE_RE.sub(" ", text).strip().split(" "):
        if not token:
            continue
        mapped = TEAM_ABBREVIATION_MAP.get(token, token)
        if mapped in TEAM_STOP_WORDS:
            continue
        tokens.append(mapped)
    return " ".join(tokens)


def tokenize_name(value: str) -> List[str]:
    return [token for token in value.split(" ") if token]


def acronym_from_tokens(tokens: Iterable[str]) -> str:
    letters = [token[0] for token in tokens if token]
    return "".join(letters)


def add_alias_variants(
    out: Set[str],
    value: object,
    alias_map: Dict[str, Tuple[str, ...]],
    *,
    canonicalizer,
    token_expansions: Optional[Dict[str, str]] = None,
) -> None:
    raw = normalize_text(value)
    if not raw:
        return

    queue = [raw, raw.replace("-", " "), raw.replace("&", " and ")]
    seen: Set[str] = set()

    while queue:
        candidate = normalize_text(queue.pop(0))
        if not candidate:
            continue
        key = canonicalizer(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        out.add(key)

        tokens = tokenize_name(key)
        if tokens:
            expanded_tokens = [token_expansions.get(token, token) for token in tokens] if token_expansions else tokens
            expanded_key = " ".join(expanded_tokens).strip()
            if expanded_key and expanded_key not in seen:
                queue.append(expanded_key)

            compact_key = "".join(tokens)
            if compact_key:
                out.add(compact_key)

            initials = acronym_from_tokens(tokens)
            if len(initials) >= 2:
                out.add(initials)

            if len(tokens) > 1:
                without_single_letter = " ".join(token for token in tokens if len(token) > 1)
                if without_single_letter and without_single_letter not in seen:
                    queue.append(without_single_letter)

                if canonicalizer is canonical_team_name and tokens[0] == "borussia":
                    queue.append(" ".join(tokens[1:]))

        for extra in alias_map.get(key, ()):
            if extra:
                queue.append(extra)


def add_registry_alias_variants(
    out: Set[str],
    value: object,
    alias_map: Dict[str, Tuple[str, ...]],
    *,
    canonicalizer,
    token_expansions: Optional[Dict[str, str]] = None,
) -> None:
    raw = normalize_text(value)
    if not raw:
        return

    queue = [raw, raw.replace("-", " "), raw.replace("&", " and ")]
    seen: Set[str] = set()

    while queue:
        candidate = normalize_text(queue.pop(0))
        if not candidate:
            continue
        key = canonicalizer(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        out.add(key)

        tokens = tokenize_name(key)
        if tokens:
            expanded_tokens = [token_expansions.get(token, token) for token in tokens] if token_expansions else tokens
            expanded_key = " ".join(expanded_tokens).strip()
            if expanded_key and expanded_key not in seen:
                queue.append(expanded_key)

            if len(tokens) > 1:
                without_single_letter = " ".join(token for token in tokens if len(token) > 1)
                if without_single_letter and without_single_letter not in seen:
                    queue.append(without_single_letter)

                if canonicalizer is canonical_team_name and tokens[0] == "borussia":
                    queue.append(" ".join(tokens[1:]))

        for extra in alias_map.get(key, ()):
            if extra:
                queue.append(extra)


def unique_text_values(values: Iterable[object]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        text = normalize_text(value)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def unique_int_values(values: Iterable[object]) -> List[int]:
    out: List[int] = []
    seen: Set[int] = set()
    for value in values:
        parsed = to_int_or_none(value)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        out.append(parsed)
    return out


def build_team_alias_inputs(raw_event: Dict, side: str) -> List[str]:
    primary_key = canonical_team_name(raw_event.get(f"{side}_team"))
    values: List[object] = [raw_event.get(f"{side}_team")]

    short_name = normalize_text(raw_event.get(f"{side}_team_short"))
    short_key = canonical_team_name(short_name)
    if short_name and (short_key == primary_key or len(short_key) >= 4):
        values.append(short_name)

    values.append(raw_event.get(f"{side}_team_slug"))
    return unique_text_values(values)


def filter_registry_alias_values(primary_name: object, values: Iterable[object]) -> List[str]:
    primary_key = canonical_team_name(primary_name)
    filtered: List[str] = []
    for text in unique_text_values(values):
        key = canonical_team_name(text)
        if not key:
            continue
        if key == primary_key or " " in key or len(key) >= 5:
            filtered.append(text)
    return filtered


def build_team_aliases(raw_event: Dict, side: str) -> List[str]:
    aliases: Set[str] = set()
    for field in (
        f"{side}_team",
        f"{side}_team_short",
        f"{side}_team_slug",
    ):
        add_alias_variants(
            aliases,
            raw_event.get(field),
            TEAM_ALIAS_EXPANSIONS,
            canonicalizer=canonical_team_name,
            token_expansions=TEAM_TOKEN_EXPANSIONS,
        )
    return sorted(aliases)


def build_competition_aliases(raw_event: Dict) -> List[str]:
    aliases: Set[str] = set()
    for field in ("competition", "competition_full", "league"):
        raw = normalize_text(raw_event.get(field))
        if not raw:
            continue
        add_alias_variants(
            aliases,
            raw,
            COMPETITION_ALIAS_EXPANSIONS,
            canonicalizer=canonical_event_name,
        )
    return sorted(aliases)


def variant_similarity(left: str, right: str) -> float:
    left_key = normalize_text(left)
    right_key = normalize_text(right)
    if not left_key or not right_key:
        return 0.0
    if left_key == right_key:
        return 1.0

    sequence = similarity(left_key, right_key)
    containment = 0.0
    shorter, longer = sorted((left_key, right_key), key=len)
    if len(shorter) >= 4 and shorter in longer:
        containment = 0.92 if len(shorter) >= 6 else 0.84

    left_tokens = set(tokenize_name(left_key))
    right_tokens = set(tokenize_name(right_key))
    intersection = left_tokens & right_tokens
    token_score = 0.0
    if intersection:
        shorter_tokens = left_tokens if len(left_tokens) <= len(right_tokens) else right_tokens
        smaller_is_subset = intersection == shorter_tokens
        if smaller_is_subset:
            if len(shorter_tokens) >= 2:
                token_score = 0.94
            elif len(shorter_tokens) == 1:
                token = next(iter(shorter_tokens))
                token_score = 0.90 if len(token) >= 8 else 0.65
        else:
            token_score = len(intersection) / max(1, min(len(left_tokens), len(right_tokens)))

    return max(sequence, containment, token_score)


def best_variant_similarity(left_variants: Iterable[str], right_variants: Iterable[str]) -> float:
    left = [value for value in left_variants if normalize_text(value)]
    right = [value for value in right_variants if normalize_text(value)]
    if not left or not right:
        return 0.0

    best = 0.0
    for left_value in left:
        for right_value in right:
            score = variant_similarity(left_value, right_value)
            if score > best:
                best = score
    return best


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def is_placeholder_team_name(value: object) -> bool:
    team_name = normalize_text(value)
    if not team_name:
        return True
    if TEAM_PLACEHOLDER_RE.search(team_name):
        return True
    # Reuse placeholder detector for TBC/TBA style tokens.
    return bool(is_placeholder_channel_name(team_name))


def has_valid_teams(event: Dict) -> bool:
    return not is_placeholder_team_name(event.get("home_team")) and not is_placeholder_team_name(event.get("away_team"))


def compose_name(home_team: Optional[str], away_team: Optional[str], fallback_name: object) -> Optional[str]:
    home = normalize_text(home_team)
    away = normalize_text(away_team)
    if home and away:
        return f"{home} v {away}"
    fallback = normalize_text(fallback_name)
    return fallback or None


def normalize_event(
    raw_event: Dict,
    source: str,
    force_sport: Optional[str] = None,
    allow_empty_channels: bool = False,
) -> Optional[Dict]:
    if not isinstance(raw_event, dict):
        return None

    channels = channels_from_raw(raw_event)
    source_key = normalize_key_text(source)
    if "flashscore" in source_key:
        channels = select_regional_channel_names(channels, max_channels=4, include_uk=True)
    if not allow_empty_channels and not channels:
        return None

    home_team = normalize_text(raw_event.get("home_team")) or None
    away_team = normalize_text(raw_event.get("away_team")) or None

    name = compose_name(home_team, away_team, raw_event.get("name"))
    if not name:
        return None

    split_home, split_away = split_match_name(name)
    if (home_team is None or is_placeholder_team_name(home_team)) and split_home and not is_placeholder_team_name(split_home):
        home_team = split_home
    if (away_team is None or is_placeholder_team_name(away_team)) and split_away and not is_placeholder_team_name(split_away):
        away_team = split_away

    start_time_iso = derive_start_time_iso(raw_event)
    normalized = {
        "name": name,
        "start_time_iso": start_time_iso,
        "time": derive_time(raw_event, start_time_iso),
        "sport": normalize_text(force_sport or raw_event.get("sport")) or None,
        "competition": normalize_text(raw_event.get("competition") or raw_event.get("league")) or None,
        "competition_logo": normalize_text(raw_event.get("competition_logo")) or None,
        "sport_logo": normalize_text(raw_event.get("sport_logo")) or None,
        "channels": channels,
        "home_team": home_team,
        "away_team": away_team,
        "home_team_id": to_int_or_none(raw_event.get("home_team_id")),
        "away_team_id": to_int_or_none(raw_event.get("away_team_id")),
        "home_team_logo": normalize_text(raw_event.get("home_team_logo")) or None,
        "away_team_logo": normalize_text(raw_event.get("away_team_logo")) or None,
        "_date": normalize_text(raw_event.get("date")) or None,
        "_source": source,
        "_home_team_alias_inputs": build_team_alias_inputs(raw_event, "home"),
        "_away_team_alias_inputs": build_team_alias_inputs(raw_event, "away"),
        "_home_team_aliases": build_team_aliases(raw_event, "home"),
        "_away_team_aliases": build_team_aliases(raw_event, "away"),
        "_competition_aliases": build_competition_aliases(raw_event),
    }
    return normalized


def event_date(day_date: str, event: Dict) -> str:
    event_level_date = normalize_text(event.get("_date"))
    return event_level_date or day_date


def event_datetime_utc(day_date: str, event: Dict) -> Optional[dt.datetime]:
    parsed_iso = parse_iso_datetime(event.get("start_time_iso"))
    if parsed_iso:
        return parsed_iso.astimezone(dt.timezone.utc)

    hhmm = parse_hhmm(event.get("time"))
    if not hhmm:
        return None
    try:
        base_date = dt.datetime.strptime(day_date, "%Y-%m-%d").date()
        hour, minute = [int(part) for part in hhmm.split(":", 1)]
    except ValueError:
        return None
    return dt.datetime(base_date.year, base_date.month, base_date.day, hour, minute, tzinfo=dt.timezone.utc)


def time_score(day_date: str, left: Dict, right: Dict) -> Tuple[float, Optional[float]]:
    left_dt = event_datetime_utc(day_date, left)
    right_dt = event_datetime_utc(day_date, right)
    if left_dt is None or right_dt is None:
        return 0.0, None

    diff_minutes = abs((left_dt - right_dt).total_seconds()) / 60.0
    if diff_minutes == 0:
        return 1.0, diff_minutes
    if diff_minutes <= 10:
        return 0.92, diff_minutes
    if diff_minutes <= 30:
        return 0.82, diff_minutes
    if diff_minutes <= 60:
        return 0.65, diff_minutes
    return 0.0, diff_minutes


def team_pair_scores(fanzo_event: Dict, huh_event: Dict) -> Tuple[float, float]:
    f_home = fanzo_event.get("_home_team_aliases") or [canonical_team_name(fanzo_event.get("home_team"))]
    f_away = fanzo_event.get("_away_team_aliases") or [canonical_team_name(fanzo_event.get("away_team"))]
    h_home = huh_event.get("_home_team_aliases") or [canonical_team_name(huh_event.get("home_team"))]
    h_away = huh_event.get("_away_team_aliases") or [canonical_team_name(huh_event.get("away_team"))]

    direct_home = best_variant_similarity(f_home, h_home)
    direct_away = best_variant_similarity(f_away, h_away)
    direct_avg = (direct_home + direct_away) / 2.0
    direct_min = min(direct_home, direct_away)

    swap_home = best_variant_similarity(f_home, h_away)
    swap_away = best_variant_similarity(f_away, h_home)
    swap_avg = (swap_home + swap_away) / 2.0
    swap_min = min(swap_home, swap_away)

    if swap_avg > direct_avg:
        return swap_avg, swap_min
    return direct_avg, direct_min


def event_match_features(day_date: str, fanzo_event: Dict, huh_event: Dict) -> Dict[str, float]:
    team_avg, team_min = team_pair_scores(fanzo_event, huh_event)
    name_score = similarity(
        canonical_event_name(fanzo_event.get("name")),
        canonical_event_name(huh_event.get("name")),
    )
    competition_score = best_variant_similarity(
        fanzo_event.get("_competition_aliases") or [canonical_event_name(fanzo_event.get("competition"))],
        huh_event.get("_competition_aliases") or [canonical_event_name(huh_event.get("competition"))],
    )
    ts, diff_minutes = time_score(day_date, fanzo_event, huh_event)

    teams_valid = has_valid_teams(fanzo_event) and has_valid_teams(huh_event)
    if teams_valid:
        confidence = (0.65 * team_avg) + (0.20 * name_score) + (0.15 * ts)
    else:
        confidence = (0.55 * name_score) + (0.25 * competition_score) + (0.20 * ts)

    return {
        "confidence": confidence,
        "team_avg": team_avg,
        "team_min": team_min,
        "name_score": name_score,
        "competition_score": competition_score,
        "time_score": ts,
        "time_diff_minutes": -1.0 if diff_minutes is None else diff_minutes,
        "teams_valid": 1.0 if teams_valid else 0.0,
    }


def is_acceptable_match(features: Dict[str, float], fanzo_event: Dict) -> bool:
    teams_valid = bool(int(features["teams_valid"]))

    if teams_valid:
        if features["team_min"] >= 0.80 and features["time_score"] >= 0.82 and features["confidence"] >= 0.80:
            return True
        return features["confidence"] >= 0.90 and features["time_score"] >= 0.65

    # Placeholder-team FANZO events: require stronger name/time agreement.
    return features["confidence"] >= 0.85 and features["time_score"] >= 0.82


def find_best_huh_match(
    day_date: str,
    fanzo_event: Dict,
    huh_events: List[Dict],
    used_indices: Set[int],
) -> Tuple[Optional[int], Optional[Dict[str, float]]]:
    best_index: Optional[int] = None
    best_features: Optional[Dict[str, float]] = None

    for index, candidate in enumerate(huh_events):
        if index in used_indices:
            continue
        if event_date(day_date, candidate) != day_date:
            continue
        if not is_football_sport(candidate.get("sport")):
            continue

        features = event_match_features(day_date, fanzo_event, candidate)
        if best_features is None or features["confidence"] > best_features["confidence"]:
            best_index = index
            best_features = features

    if best_index is None or best_features is None:
        return None, None
    if not is_acceptable_match(best_features, fanzo_event):
        return None, None
    return best_index, best_features


def date_index_from_schedule_payload(payload: Dict, source_name: str) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    for day in payload.get("schedule", []) if isinstance(payload.get("schedule"), list) else []:
        if not isinstance(day, dict):
            continue
        date_iso = normalize_text(day.get("date"))
        if not date_iso:
            continue
        node = out.setdefault(date_iso, {"date": date_iso, "day": day_name(date_iso), "events": []})
        for raw_event in day.get("events", []) if isinstance(day.get("events"), list) else []:
            if not isinstance(raw_event, dict):
                continue
            event = normalize_event(raw_event, source=source_name, allow_empty_channels=True)
            if event:
                node["events"].append(event)
    return out


def merge_channels(first: Iterable[str], second: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in list(first) + list(second):
        value = normalize_channel_name(raw)
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def choose_value(primary: object, secondary: object) -> object:
    if primary is None:
        return secondary
    if isinstance(primary, str) and not normalize_text(primary):
        return secondary
    return primary


def merge_event(primary: Dict, secondary: Dict, keep_primary_teams: bool) -> Dict:
    merged = dict(primary)
    primary_channels = clean_channels(primary.get("channels", []))
    secondary_channels = clean_channels(secondary.get("channels", []))

    primary_source = normalize_key_text(primary.get("_source"))
    secondary_source = normalize_key_text(secondary.get("_source"))
    if primary_source == "fanzo" and "flashscore" in secondary_source:
        # If FANZO matched Flashscore for football, keep FANZO channels and
        # add only US/ZA/ME channels from Flashscore (no UK duplicate).
        secondary_channels = select_regional_channel_names(secondary_channels, max_channels=4, include_uk=False)
        merged["channels"] = merge_channels(primary_channels, secondary_channels)
    else:
        merged["channels"] = merge_channels(primary_channels, secondary_channels)

    # Non-team metadata: prefer primary when available.
    for field in ("start_time_iso", "time", "sport", "competition", "competition_logo", "sport_logo"):
        merged[field] = choose_value(primary.get(field), secondary.get(field))

    if keep_primary_teams and has_valid_teams(primary):
        for field in ("home_team", "away_team", "home_team_id", "away_team_id", "home_team_logo", "away_team_logo"):
            merged[field] = choose_value(primary.get(field), secondary.get(field))
    else:
        for field in ("home_team", "away_team", "home_team_id", "away_team_id", "home_team_logo", "away_team_logo"):
            merged[field] = choose_value(secondary.get(field), primary.get(field))

    return merged


def event_sort_key(event: Dict) -> Tuple[str, str]:
    iso = normalize_text(event.get("start_time_iso"))
    if iso:
        return (iso, canonical_event_name(event.get("name")))
    return (normalize_text(event.get("time")) or "99:99", canonical_event_name(event.get("name")))


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


def _event_date_iso(raw_event: Dict) -> str:
    direct = normalize_text(raw_event.get("date"))
    if direct:
        return direct

    start_date = normalize_text(raw_event.get("start_date"))
    if start_date:
        return start_date

    parsed_iso = parse_iso_datetime(raw_event.get("start_time_iso") or raw_event.get("start_time_utc"))
    if parsed_iso:
        return parsed_iso.astimezone(dt.timezone.utc).date().isoformat()

    return ""


def date_index_from_football_source(payload: Dict) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    source_name = normalize_text(payload.get("source")) or "football-secondary"

    for raw_match in payload.get("matches", []) if isinstance(payload.get("matches"), list) else []:
        if not isinstance(raw_match, dict):
            continue
        date_iso = _event_date_iso(raw_match)
        if not date_iso:
            continue
        node = out.setdefault(date_iso, {"date": date_iso, "day": day_name(date_iso), "events": []})
        event = normalize_event(raw_match, source=source_name, force_sport="Football")
        if event:
            node["events"].append(event)

    for raw_event in payload.get("events", []) if isinstance(payload.get("events"), list) else []:
        if not isinstance(raw_event, dict):
            continue
        date_iso = _event_date_iso(raw_event)
        if not date_iso:
            continue
        node = out.setdefault(date_iso, {"date": date_iso, "day": day_name(date_iso), "events": []})
        event = normalize_event(raw_event, source=source_name, force_sport="Football")
        if event:
            node["events"].append(event)
    return out


def build_football_logo_maps(fanzo_payload: Dict) -> Tuple[Dict[str, str], str]:
    competition_logo_by_key: Dict[str, str] = {}
    football_fallback_logo: str = WITM_FOOTBALL_COMPETITION_LOGO_URL

    for day in fanzo_payload.get("schedule", []) if isinstance(fanzo_payload.get("schedule"), list) else []:
        for raw_event in day.get("events", []) if isinstance(day.get("events"), list) else []:
            if not isinstance(raw_event, dict):
                continue
            if not is_football_sport(raw_event.get("sport")):
                continue

            competition = normalize_text(raw_event.get("competition") or raw_event.get("league"))
            competition_key = canonical_event_name(competition)
            competition_logo = normalize_text(raw_event.get("competition_logo")) or None
            sport_logo = normalize_text(raw_event.get("sport_logo")) or None

            chosen_logo = competition_logo or sport_logo
            if competition_key and chosen_logo and competition_key not in competition_logo_by_key:
                competition_logo_by_key[competition_key] = chosen_logo

    return competition_logo_by_key, football_fallback_logo


def enrich_football_logos(event: Dict, competition_logo_map: Dict[str, str], fallback_logo: str) -> None:
    if not is_football_sport(event.get("sport")):
        return

    competition_key = canonical_event_name(event.get("competition"))
    mapped_logo = competition_logo_map.get(competition_key) if competition_key else None

    if not normalize_text(event.get("competition_logo")) and mapped_logo:
        event["competition_logo"] = mapped_logo
    if not normalize_text(event.get("sport_logo")):
        event["sport_logo"] = mapped_logo or fallback_logo


def is_generic_team_logo(value: object) -> bool:
    url = normalize_key_text(value)
    if not url:
        return True
    return any(pattern in url for pattern in GENERIC_TEAM_LOGO_PATTERNS)


def is_usable_team_logo(value: object) -> bool:
    return not is_generic_team_logo(value)


def needs_team_logo_upgrade(value: object) -> bool:
    url = normalize_key_text(value)
    if not url:
        return True
    if is_generic_team_logo(url):
        return True
    return any(pattern in url for pattern in LOW_QUALITY_TEAM_LOGO_PATTERNS)


def empty_team_logo_registry_payload() -> Dict:
    return {
        "generated_at": None,
        "source": "fanzo.com",
        "schema_version": TEAM_LOGO_REGISTRY_SCHEMA_VERSION,
        "teams": [],
    }


def create_team_logo_registry_state(payload: Optional[Dict]) -> Dict[str, object]:
    raw_payload = payload if isinstance(payload, dict) else {}
    metadata = empty_team_logo_registry_payload()
    raw_source = normalize_text(raw_payload.get("source"))
    if raw_source:
        metadata["source"] = raw_source
    raw_generated_at = normalize_text(raw_payload.get("generated_at"))
    if raw_generated_at:
        metadata["generated_at"] = raw_generated_at

    state: Dict[str, object] = {
        "metadata": metadata,
        "entries": {},
        "alias_index": {},
        "entry_variants": {},
        "dirty": True,
    }

    raw_teams = raw_payload.get("teams")
    if isinstance(raw_teams, dict):
        raw_teams = list(raw_teams.values())
    if not isinstance(raw_teams, list):
        raw_teams = []

    for raw_entry in raw_teams:
        if not isinstance(raw_entry, dict):
            continue
        upsert_team_logo_registry_entry(
            state,
            team_name=raw_entry.get("name") or raw_entry.get("team") or raw_entry.get("team_key"),
            logo=raw_entry.get("logo"),
            fanzo_ids=raw_entry.get("fanzo_ids") if isinstance(raw_entry.get("fanzo_ids"), list) else [],
            alias_values=raw_entry.get("aliases") if isinstance(raw_entry.get("aliases"), list) else [],
            source_values=raw_entry.get("sources") if isinstance(raw_entry.get("sources"), list) else [],
            preferred_key=raw_entry.get("team_key"),
            track_stats=False,
        )

    return state


def serialize_team_logo_registry_state(state: Dict[str, object]) -> Dict:
    metadata = state.get("metadata") if isinstance(state.get("metadata"), dict) else empty_team_logo_registry_payload()
    entries = state.get("entries") if isinstance(state.get("entries"), dict) else {}
    teams: List[Dict] = []
    for team_key in sorted(entries.keys()):
        entry = entries[team_key]
        if not isinstance(entry, dict):
            continue
        team_name = normalize_text(entry.get("name")) or team_key
        teams.append(
            {
                "team_key": team_key,
                "name": team_name,
                "logo": normalize_text(entry.get("logo")) or None,
                "fanzo_ids": unique_int_values(entry.get("fanzo_ids") or []),
                "aliases": unique_text_values(entry.get("aliases") or [team_name]),
                "sources": unique_text_values(entry.get("sources") or []),
            }
        )

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": normalize_text(metadata.get("source")) or "fanzo.com",
        "schema_version": TEAM_LOGO_REGISTRY_SCHEMA_VERSION,
        "teams": teams,
    }


def sync_team_logo_registry_payload(payload: Dict, state: Dict[str, object]) -> None:
    serialized = serialize_team_logo_registry_state(state)
    payload.clear()
    payload.update(serialized)


def team_logo_registry_entry_variants(entry: Dict) -> Set[str]:
    variants: Set[str] = set()
    for value in [entry.get("name")] + list(entry.get("aliases") or []):
        add_registry_alias_variants(
            variants,
            value,
            TEAM_ALIAS_EXPANSIONS,
            canonicalizer=canonical_team_name,
            token_expansions=TEAM_TOKEN_EXPANSIONS,
        )
    return variants


def build_team_query_variants(values: Iterable[object]) -> Set[str]:
    variants: Set[str] = set()
    for value in unique_text_values(values):
        add_registry_alias_variants(
            variants,
            value,
            TEAM_ALIAS_EXPANSIONS,
            canonicalizer=canonical_team_name,
            token_expansions=TEAM_TOKEN_EXPANSIONS,
        )
    return variants


def rebuild_team_logo_registry_lookup(state: Dict[str, object]) -> None:
    entries = state.get("entries") if isinstance(state.get("entries"), dict) else {}
    alias_index: Dict[str, Set[str]] = {}
    entry_variants: Dict[str, Set[str]] = {}

    for team_key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        variants = team_logo_registry_entry_variants(entry)
        if team_key:
            variants.add(team_key)
        entry_variants[team_key] = variants
        for variant in variants:
            alias_index.setdefault(variant, set()).add(team_key)

    state["alias_index"] = alias_index
    state["entry_variants"] = entry_variants
    state["dirty"] = False


def find_team_logo_registry_key_by_variants(
    state: Dict[str, object],
    query_variants: Set[str],
) -> Optional[str]:
    if not query_variants:
        return None
    if state.get("dirty"):
        rebuild_team_logo_registry_lookup(state)

    alias_index = state.get("alias_index") if isinstance(state.get("alias_index"), dict) else {}
    entry_variants = state.get("entry_variants") if isinstance(state.get("entry_variants"), dict) else {}

    candidate_keys: Set[str] = set()
    for variant in query_variants:
        candidate_keys.update(alias_index.get(variant, set()))
    if not candidate_keys:
        return None

    best_key: Optional[str] = None
    best_hits = -1
    best_similarity = 0.0
    for candidate_key in sorted(candidate_keys):
        candidate_variants = entry_variants.get(candidate_key) or set()
        exact_hits = len(query_variants & candidate_variants)
        similarity_score = best_variant_similarity(query_variants, candidate_variants)
        if exact_hits > best_hits or (exact_hits == best_hits and similarity_score > best_similarity):
            best_key = candidate_key
            best_hits = exact_hits
            best_similarity = similarity_score

    if best_key is None or best_hits <= 0:
        return None
    return best_key


def derive_team_logo_registry_key(
    *,
    team_name: object,
    alias_values: Iterable[object] = (),
    preferred_key: object = None,
) -> str:
    variants = build_team_query_variants([preferred_key, team_name, *list(alias_values or [])])
    if not variants:
        return canonical_team_name(preferred_key or team_name)

    word_variants = [variant for variant in variants if " " in variant]
    candidates = word_variants or [variant for variant in variants if len(variant) >= 4] or list(variants)
    return max(candidates, key=lambda value: (len(tokenize_name(value)), len(value), value))


def upsert_team_logo_registry_entry(
    state: Dict[str, object],
    *,
    team_name: object,
    logo: object = None,
    fanzo_ids: Iterable[object] = (),
    alias_values: Iterable[object] = (),
    source_values: Iterable[object] = (),
    preferred_key: object = None,
    track_stats: bool = True,
) -> Dict[str, int]:
    entries = state.get("entries") if isinstance(state.get("entries"), dict) else {}
    name = normalize_text(team_name) or None
    normalized_logo = normalize_text(logo) or None
    normalized_aliases = filter_registry_alias_values(name, [name, *list(alias_values or [])])
    normalized_sources = unique_text_values(source_values or [])
    normalized_fanzo_ids = unique_int_values(fanzo_ids or [])
    direct_key = canonical_team_name(preferred_key)
    if direct_key and direct_key in entries:
        team_key = direct_key
    else:
        query_variants = build_team_query_variants([preferred_key, name, *normalized_aliases])
        matched_key = find_team_logo_registry_key_by_variants(state, query_variants)
        team_key = matched_key or derive_team_logo_registry_key(
            team_name=name,
            alias_values=normalized_aliases,
            preferred_key=preferred_key,
        )
    if not team_key:
        return {"entries_added": 0, "entries_updated": 0, "aliases_added": 0}

    stats = {"entries_added": 0, "entries_updated": 0, "aliases_added": 0}
    changed = False

    existing = entries.get(team_key)
    if existing is None:
        entries[team_key] = {
            "team_key": team_key,
            "name": name or (normalized_aliases[0] if normalized_aliases else team_key),
            "logo": normalized_logo,
            "fanzo_ids": normalized_fanzo_ids,
            "aliases": normalized_aliases or ([name] if name else []),
            "sources": normalized_sources,
        }
        state["dirty"] = True
        if track_stats:
            stats["entries_added"] = 1
        return stats

    existing_name = normalize_text(existing.get("name")) or None
    if name and (not existing_name or len(name) > len(existing_name)):
        existing["name"] = name
        changed = True

    existing_logo = normalize_text(existing.get("logo")) or None
    if normalized_logo and (
        not existing_logo or (needs_team_logo_upgrade(existing_logo) and not needs_team_logo_upgrade(normalized_logo))
    ):
        existing["logo"] = normalized_logo
        changed = True

    merged_fanzo_ids = unique_int_values(list(existing.get("fanzo_ids") or []) + normalized_fanzo_ids)
    if merged_fanzo_ids != list(existing.get("fanzo_ids") or []):
        existing["fanzo_ids"] = merged_fanzo_ids
        changed = True

    prior_aliases = unique_text_values(existing.get("aliases") or [])
    merged_aliases = unique_text_values(prior_aliases + normalized_aliases)
    aliases_added = max(0, len(merged_aliases) - len(prior_aliases))
    if merged_aliases != prior_aliases:
        existing["aliases"] = merged_aliases
        changed = True

    prior_sources = unique_text_values(existing.get("sources") or [])
    merged_sources = unique_text_values(prior_sources + normalized_sources)
    if merged_sources != prior_sources:
        existing["sources"] = merged_sources
        changed = True

    if changed:
        state["dirty"] = True
        if track_stats:
            stats["entries_updated"] = 1
            stats["aliases_added"] = aliases_added
    return stats


def team_logo_registry_query_values(event: Dict, side: str) -> List[str]:
    return filter_registry_alias_values(
        event.get(f"{side}_team"),
        [
            event.get(f"{side}_team"),
            *(event.get(f"_{side}_team_alias_inputs") or []),
        ],
    )


def find_team_logo_registry_match(
    state: Dict[str, object],
    *,
    event: Dict,
    side: str,
) -> Optional[Tuple[str, Dict]]:
    entries = state.get("entries") if isinstance(state.get("entries"), dict) else {}
    query_variants = build_team_query_variants(team_logo_registry_query_values(event, side))
    if not query_variants:
        return None
    best_key = find_team_logo_registry_key_by_variants(state, query_variants)
    if best_key is None:
        return None
    entry = entries.get(best_key)
    if not isinstance(entry, dict):
        return None
    return best_key, entry


def seed_team_logo_registry_from_fanzo_payload(
    fanzo_payload: Dict,
    state: Dict[str, object],
) -> Dict[str, int]:
    stats = {"entries_added": 0, "entries_updated": 0, "aliases_added": 0}
    for day in fanzo_payload.get("schedule", []) if isinstance(fanzo_payload.get("schedule"), list) else []:
        if not isinstance(day, dict):
            continue
        for raw_event in day.get("events", []) if isinstance(day.get("events"), list) else []:
            if not isinstance(raw_event, dict):
                continue
            if not is_football_sport(raw_event.get("sport")):
                continue
            event = normalize_event(raw_event, source="fanzo", allow_empty_channels=True)
            if not event or not has_valid_teams(event):
                continue
            for side in ("home", "away"):
                team_name = event.get(f"{side}_team")
                if is_placeholder_team_name(team_name):
                    continue
                result = upsert_team_logo_registry_entry(
                    state,
                    team_name=team_name,
                    logo=event.get(f"{side}_team_logo"),
                    fanzo_ids=[event.get(f"{side}_team_id")],
                    alias_values=event.get(f"_{side}_team_alias_inputs") or [team_name],
                    source_values=["fanzo"],
                )
                for key in stats:
                    stats[key] += result.get(key, 0)
    return stats


def add_secondary_aliases_to_team_logo_registry(
    state: Dict[str, object],
    *,
    primary_event: Dict,
    secondary_event: Dict,
    secondary_source_name: str,
) -> Dict[str, int]:
    stats = {"entries_added": 0, "entries_updated": 0, "aliases_added": 0}
    if not has_valid_teams(primary_event):
        return stats

    for side in ("home", "away"):
        team_name = primary_event.get(f"{side}_team")
        if is_placeholder_team_name(team_name):
            continue
        result = upsert_team_logo_registry_entry(
            state,
            team_name=team_name,
            logo=primary_event.get(f"{side}_team_logo"),
            fanzo_ids=[primary_event.get(f"{side}_team_id")],
            alias_values=[
                *(primary_event.get(f"_{side}_team_alias_inputs") or []),
                *(secondary_event.get(f"_{side}_team_alias_inputs") or []),
            ],
            source_values=["fanzo", secondary_source_name],
        )
        for key in stats:
            stats[key] += result.get(key, 0)
    return stats


def apply_team_logo_registry(
    event: Dict,
    state: Dict[str, object],
    *,
    secondary_source_name: str,
) -> Tuple[bool, Dict[str, int]]:
    updated = False
    stats = {"entries_added": 0, "entries_updated": 0, "aliases_added": 0}
    for side in ("home", "away"):
        logo_field = f"{side}_team_logo"
        current_logo = normalize_text(event.get(logo_field)) or None
        if not needs_team_logo_upgrade(current_logo):
            continue

        matched = find_team_logo_registry_match(state, event=event, side=side)
        if matched is None:
            continue
        team_key, entry = matched
        candidate_logo = normalize_text(entry.get("logo")) or None
        if candidate_logo and is_usable_team_logo(candidate_logo) and candidate_logo != current_logo:
            event[logo_field] = candidate_logo
            updated = True

        result = upsert_team_logo_registry_entry(
            state,
            team_name=entry.get("name") or event.get(f"{side}_team"),
            logo=entry.get("logo"),
            fanzo_ids=entry.get("fanzo_ids") or [],
            alias_values=team_logo_registry_query_values(event, side),
            source_values=["fanzo", secondary_source_name],
            preferred_key=team_key,
        )
        for key in stats:
            stats[key] += result.get(key, 0)

    return updated, stats


def is_acceptable_livesporttv_logo_match(features: Dict[str, float]) -> bool:
    if features["time_score"] < 0.82:
        return False
    if features["team_min"] >= 0.80 and features["competition_score"] >= 0.75:
        return True
    return features["confidence"] >= 0.88 and features["team_min"] >= 0.70


def find_best_livesporttv_match(
    day_date: str,
    football_event: Dict,
    livesporttv_events: List[Dict],
    used_indices: Set[int],
) -> Tuple[Optional[int], Optional[Dict[str, float]]]:
    best_index: Optional[int] = None
    best_features: Optional[Dict[str, float]] = None

    for index, candidate in enumerate(livesporttv_events):
        if index in used_indices:
            continue
        if event_date(day_date, candidate) != day_date:
            continue
        if not is_football_sport(candidate.get("sport")):
            continue
        if not (is_usable_team_logo(candidate.get("home_team_logo")) or is_usable_team_logo(candidate.get("away_team_logo"))):
            continue

        features = event_match_features(day_date, football_event, candidate)
        if best_features is None or features["confidence"] > best_features["confidence"]:
            best_index = index
            best_features = features

    if best_index is None or best_features is None:
        return None, None
    if not is_acceptable_livesporttv_logo_match(best_features):
        return None, None
    return best_index, best_features


def apply_livesporttv_team_logos(event: Dict, livesporttv_event: Dict) -> bool:
    updated = False
    for field in ("home_team_logo", "away_team_logo"):
        current_logo = normalize_text(event.get(field)) or None
        if not needs_team_logo_upgrade(current_logo):
            continue
        candidate = normalize_text(livesporttv_event.get(field)) or None
        if candidate and is_usable_team_logo(candidate) and event.get(field) != candidate:
            event[field] = candidate
            updated = True
    return updated


def dedupe_events(events: List[Dict]) -> List[Dict]:
    merged_by_key: Dict[Tuple[str, str, str, str, str], Dict] = {}

    for event in sorted(events, key=event_sort_key):
        key = (
            canonical_event_name(event.get("name")),
            parse_hhmm(event.get("time")) or "",
            canonical_event_name(event.get("sport")),
            canonical_team_name(event.get("home_team")),
            canonical_team_name(event.get("away_team")),
        )
        if key not in merged_by_key:
            merged_by_key[key] = dict(event)
            continue

        current = merged_by_key[key]
        keep_primary_teams = has_valid_teams(current)
        merged_by_key[key] = merge_event(current, event, keep_primary_teams=keep_primary_teams)

    return list(merged_by_key.values())


def strip_internal_fields(event: Dict) -> Dict:
    return {field: event.get(field) for field in NORMALIZED_EVENT_FIELDS}


def fanzo_event_requires_secondary_match(event: Dict) -> bool:
    if not is_football_sport(event.get("sport")):
        return False
    if not event.get("channels"):
        return True
    return not has_valid_teams(event)


def compose_payload(
    fanzo_payload: Dict,
    football_secondary_payload: Dict,
    secondary_source: Optional[str] = None,
    livesporttv_payload: Optional[Dict] = None,
    sport_assets_payload: Optional[Dict] = None,
    team_logo_registry: Optional[Dict] = None,
) -> Dict:
    fanzo_by_date = day_index(fanzo_payload)
    secondary_by_date = date_index_from_football_source(football_secondary_payload)
    livesporttv_by_date = date_index_from_schedule_payload(
        livesporttv_payload or {},
        source_name=normalize_text((livesporttv_payload or {}).get("source")) or "livesporttv.com",
    )
    all_dates = sorted(set(fanzo_by_date.keys()) | set(secondary_by_date.keys()))
    football_logo_map, football_fallback_logo = build_football_logo_maps(fanzo_payload)
    secondary_source_name = normalize_text(secondary_source) or normalize_text(
        football_secondary_payload.get("source")
    ) or "football-secondary"
    team_logo_registry_state = create_team_logo_registry_state(team_logo_registry)
    registry_seed_stats = seed_team_logo_registry_from_fanzo_payload(fanzo_payload, team_logo_registry_state)

    schedule: List[Dict] = []
    merged_football_events = 0
    fanzo_only_events = 0
    secondary_only_events = 0
    fanzo_football_dropped_no_match = 0
    football_secondary_registry_logo_upgrades = 0
    football_secondary_logo_upgrades = 0

    for date_iso in all_dates:
        fanzo_raw_events = fanzo_by_date.get(date_iso, {}).get("events", [])
        secondary_raw_events = secondary_by_date.get(date_iso, {}).get("events", [])
        livesporttv_events = livesporttv_by_date.get(date_iso, {}).get("events", [])

        fanzo_events: List[Dict] = []
        for raw_event in fanzo_raw_events if isinstance(fanzo_raw_events, list) else []:
            allow_empty = is_football_sport(raw_event.get("sport"))
            event = normalize_event(raw_event, source="fanzo", allow_empty_channels=allow_empty)
            if not event:
                continue
            enrich_football_logos(event, football_logo_map, football_fallback_logo)
            fanzo_events.append(event)

        secondary_events: List[Dict] = []
        for raw_event in secondary_raw_events if isinstance(secondary_raw_events, list) else []:
            enrich_football_logos(raw_event, football_logo_map, football_fallback_logo)
            secondary_events.append(raw_event)

        used_secondary_indices: Set[int] = set()
        used_livesporttv_indices: Set[int] = set()
        day_events: List[Dict] = []

        for fanzo_event in fanzo_events:
            if not is_football_sport(fanzo_event.get("sport")):
                if fanzo_event.get("channels"):
                    day_events.append(fanzo_event)
                    fanzo_only_events += 1
                continue

            match_index, _features = find_best_huh_match(
                day_date=date_iso,
                fanzo_event=fanzo_event,
                huh_events=secondary_events,
                used_indices=used_secondary_indices,
            )

            if match_index is not None:
                used_secondary_indices.add(match_index)
                huh_event = secondary_events[match_index]
                alias_stats = add_secondary_aliases_to_team_logo_registry(
                    team_logo_registry_state,
                    primary_event=fanzo_event,
                    secondary_event=huh_event,
                    secondary_source_name=secondary_source_name,
                )
                for key in registry_seed_stats:
                    registry_seed_stats[key] += alias_stats.get(key, 0)
                keep_primary_teams = has_valid_teams(fanzo_event)
                merged = merge_event(fanzo_event, huh_event, keep_primary_teams=keep_primary_teams)
                enrich_football_logos(merged, football_logo_map, football_fallback_logo)
                if merged.get("channels") and has_valid_teams(merged):
                    day_events.append(merged)
                    merged_football_events += 1
                else:
                    fanzo_football_dropped_no_match += 1
                continue

            if fanzo_event_requires_secondary_match(fanzo_event):
                fanzo_football_dropped_no_match += 1
                continue

            if fanzo_event.get("channels") and has_valid_teams(fanzo_event):
                day_events.append(fanzo_event)
                fanzo_only_events += 1
            else:
                fanzo_football_dropped_no_match += 1

        for idx, huh_event in enumerate(secondary_events):
            if idx in used_secondary_indices:
                continue
            if not is_football_sport(huh_event.get("sport")):
                continue
            if not huh_event.get("channels"):
                continue
            if not has_valid_teams(huh_event):
                continue
            registry_updated, registry_alias_stats = apply_team_logo_registry(
                huh_event,
                team_logo_registry_state,
                secondary_source_name=secondary_source_name,
            )
            for key in registry_seed_stats:
                registry_seed_stats[key] += registry_alias_stats.get(key, 0)
            if registry_updated:
                football_secondary_registry_logo_upgrades += 1
            livesporttv_index, _logo_features = find_best_livesporttv_match(
                day_date=date_iso,
                football_event=huh_event,
                livesporttv_events=livesporttv_events,
                used_indices=used_livesporttv_indices,
            )
            if livesporttv_index is not None:
                used_livesporttv_indices.add(livesporttv_index)
                if apply_livesporttv_team_logos(huh_event, livesporttv_events[livesporttv_index]):
                    football_secondary_logo_upgrades += 1
            day_events.append(huh_event)
            secondary_only_events += 1

        deduped_day_events = dedupe_events(day_events)
        normalized_day_events = [strip_internal_fields(event) for event in sorted(deduped_day_events, key=event_sort_key)]

        schedule.append(
            {
                "date": date_iso,
                "day": day_name(date_iso) or normalize_text(fanzo_by_date.get(date_iso, {}).get("day")) or day_name(date_iso),
                "events": normalized_day_events,
            }
        )

    output = {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "composed:fanzo+football-secondary",
        "schema_version": "fanzo-football-secondary-v4",
        "schema_fields": list(NORMALIZED_EVENT_FIELDS),
        "schedule": schedule,
        "composition": {
            "fanzo_primary_source": "fanzo.com",
            "football_secondary_source": secondary_source_name,
            "fanzo_events_kept": fanzo_only_events,
            "football_secondary_unique_events_added": secondary_only_events,
            "huhsports_unique_events_added": secondary_only_events,
            "football_events_merged": merged_football_events,
            "fanzo_football_events_dropped_missing_secondary_match": fanzo_football_dropped_no_match,
            "fanzo_football_events_dropped_missing_huh_match": fanzo_football_dropped_no_match,
            "football_secondary_events_logo_enriched_from_registry": football_secondary_registry_logo_upgrades,
            "football_secondary_events_logo_enriched_from_livesporttv": football_secondary_logo_upgrades,
            "team_logo_registry_entries_added": registry_seed_stats["entries_added"],
            "team_logo_registry_entries_updated": registry_seed_stats["entries_updated"],
            "team_logo_registry_aliases_added": registry_seed_stats["aliases_added"],
            "days": len(schedule),
        },
    }
    if isinstance(sport_assets_payload, dict) and sport_assets_payload:
        output["sport_assets"] = sport_assets_payload
    if isinstance(team_logo_registry, dict):
        sync_team_logo_registry_payload(team_logo_registry, team_logo_registry_state)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compose normalized schedule from FANZO(+WITM) + secondary football source."
    )
    parser.add_argument(
        "--fanzo-witm",
        default="weekly_schedule_fanzo_enriched.json",
        help="Input FANZO(+WITM enrichment) JSON.",
    )
    parser.add_argument(
        "--football-secondary",
        default="weekly_schedule_flashscore.json",
        help="Input secondary football schedule JSON (Flashscore or HuhSports shape).",
    )
    parser.add_argument("--huhsports", default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--livesporttv",
        default=DEFAULT_LIVESPORTTV_PATH,
        help="Optional LiveSportTV schedule JSON for sharper fallback football team logos.",
    )
    parser.add_argument(
        "--flashscore-sport-assets",
        default=DEFAULT_FLASHSCORE_SPORT_ASSETS_PATH,
        help="Static Flashscore sport SVG payload to embed in final schedule JSON when available.",
    )
    parser.add_argument(
        "--team-logo-registry",
        default=DEFAULT_TEAM_LOGO_REGISTRY_PATH,
        help="Persistent Fanzo-first football team logo registry JSON.",
    )
    parser.add_argument("--output", default="weekly_schedule.json", help="Output composed schedule JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fanzo_payload = load_json(args.fanzo_witm)
    football_secondary_path = args.huhsports or args.football_secondary
    football_secondary_payload = load_json(football_secondary_path)
    livesporttv_payload = load_json(args.livesporttv)
    sport_assets_payload = load_json(args.flashscore_sport_assets)
    team_logo_registry = load_json(args.team_logo_registry)

    if not isinstance(fanzo_payload.get("schedule"), list):
        print(f"Invalid FANZO payload: {args.fanzo_witm}", file=sys.stderr)
        return 1
    if not isinstance(football_secondary_payload.get("matches"), list) and not isinstance(
        football_secondary_payload.get("events"), list
    ):
        print(f"Invalid secondary football payload: {football_secondary_path}", file=sys.stderr)
        return 1

    payload = compose_payload(
        fanzo_payload,
        football_secondary_payload,
        secondary_source=normalize_text(football_secondary_payload.get("source")),
        livesporttv_payload=livesporttv_payload,
        sport_assets_payload=sport_assets_payload,
        team_logo_registry=team_logo_registry,
    )
    save_json(args.output, payload)
    save_json(args.team_logo_registry, team_logo_registry)

    comp = payload.get("composition", {})
    print(
        f"[COMPOSE] Wrote {args.output} | fanzo_kept={comp.get('fanzo_events_kept', 0)} "
        f"secondary_unique={comp.get('football_secondary_unique_events_added', 0)} "
        f"football_merged={comp.get('football_events_merged', 0)} "
        f"registry_logo_upgrades={comp.get('football_secondary_events_logo_enriched_from_registry', 0)} "
        f"logo_upgrades={comp.get('football_secondary_events_logo_enriched_from_livesporttv', 0)} "
        f"registry_entries_added={comp.get('team_logo_registry_entries_added', 0)} "
        f"registry_aliases_added={comp.get('team_logo_registry_aliases_added', 0)} "
        f"dropped_missing_secondary={comp.get('fanzo_football_events_dropped_missing_secondary_match', 0)} "
        f"days={comp.get('days', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
