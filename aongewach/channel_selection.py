#!/usr/bin/env python3
"""Shared channel geo selection helpers for scraper and mapper."""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from typing import Dict, Iterable, List, Optional, Tuple

VALID_BUCKETS = ("uk", "us", "other")
BUCKET_PRIORITY = ("uk", "us", "other")

DEFAULT_GEO_RULES: Dict = {
    "max_event_channels": 5,
    "max_per_bucket": {
        "uk": 2,
        "us": 2,
    },
    "classification": {
        "uk": {
            "exact": [
                "Sky Sports Main Event",
                "Sky Sports Premier League",
                "Sky Sports Football",
                "TNT Sports",
                "Sky Go UK",
                "BBC iPlayer",
                "ITVX",
                "Premier Sports 1",
                "Premier Sports 2",
                "DAZN UK",
            ],
            "keywords": [
                "sky sports",
                "tnt sports",
                "bt sport",
                "bbc",
                "itv",
                "premier sports",
                "sky go uk",
                "dazn uk",
            ],
        },
        "us": {
            "exact": [
                "Fanatiz USA",
                "DAZN USA",
                "beIN SPORTS CONNECT U.S.A.",
                "Peacock",
                "Paramount+",
                "ESPN Deportes USA",
            ],
            "keywords": [
                " usa",
                "u.s.a",
                "united states",
                "espn deportes usa",
                "fox deportes",
                "cbs sports",
                "nbc sports",
                "peacock",
                "paramount+",
                "dazn usa",
                "fanatiz usa",
            ],
        },
        "preferred_other": {
            "exact": [
                "DStv Now",
                "GOtv",
                "MBC Shahid",
                "MBC Action",
            ],
            "keywords": [
                "supersport",
                "dstv",
                "gotv",
                "sabc",
                "saudi",
                "ksa",
                "ssc",
                "mbc",
                "shahid",
                "arabia",
            ],
        },
    },
    "country_groups": {
        "uk": [
            "United Kingdom",
            "UK",
            "England",
            "Scotland",
            "Wales",
            "Northern Ireland",
            "Great Britain",
        ],
        "us": [
            "United States",
            "United States of America",
            "USA",
            "U.S.A.",
        ],
        "preferred_other": [
            "South Africa",
            "Saudi Arabia",
        ],
        "watch": [
            "Nigeria",
            "Ghana",
            "United Kingdom",
            "United States",
            "South Africa",
            "Saudi Arabia",
        ],
    },
    "match_country_enrichment": {
        "enabled": True,
        "include_live_tab": True,
        "include_all_international": False,
        "countries": [
            "Nigeria",
            "Ghana",
            "United Kingdom",
            "United States",
            "South Africa",
            "Saudi Arabia",
        ],
        "max_events_per_day": 0,
    },
    "geo_profiles": [
        {
            "name": "default",
            "enabled": True,
            "primary": True,
            "bucket_hint": "",
            "preferred_other": False,
            "schedule_params": {},
            "tournament_overrides": {},
        },
        {
            "name": "uk",
            "enabled": True,
            "primary": False,
            "bucket_hint": "uk",
            "preferred_other": False,
            "schedule_params": {"iso_code": "235"},
            "tournament_overrides": {"iso_code": "235"},
        },
        {
            "name": "us",
            "enabled": True,
            "primary": False,
            "bucket_hint": "us",
            "preferred_other": False,
            "schedule_params": {"iso_code": "233"},
            "tournament_overrides": {"iso_code": "233"},
        },
        {
            "name": "za",
            "enabled": True,
            "primary": False,
            "bucket_hint": "other",
            "preferred_other": True,
            "schedule_params": {"iso_code": "147"},
            "tournament_overrides": {"iso_code": "147"},
        },
        {
            "name": "saudi",
            "enabled": True,
            "primary": False,
            "bucket_hint": "other",
            "preferred_other": True,
            "schedule_params": {"iso_code": "163"},
            "tournament_overrides": {"iso_code": "163"},
        },
    ],
}


def _normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_key(value: object) -> str:
    return _normalize_text(value).casefold()


def _dedupe_casefold(values: Iterable[str]) -> List[str]:
    output: List[str] = []
    seen = set()
    for raw in values:
        value = _normalize_text(raw)
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _deep_merge_dict(dst: Dict, src: Dict) -> Dict:
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            _deep_merge_dict(dst[key], value)
        else:
            dst[key] = value
    return dst


def load_geo_rules(path: Optional[str]) -> Dict:
    """Load geo rules file and merge over defaults."""
    rules = deepcopy(DEFAULT_GEO_RULES)
    if not path:
        return rules
    if not os.path.exists(path):
        return rules
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        if isinstance(loaded, dict):
            rules = _deep_merge_dict(rules, loaded)
    except Exception:
        # Fallback to defaults on malformed config.
        return deepcopy(DEFAULT_GEO_RULES)
    return rules


def split_mapped_channel_entry(raw: object) -> Tuple[str, Optional[str]]:
    """
    Split either:
      - "Channel Name"
      - "Channel Name, 123"
      - "Channel Name, null"
    Returns: (channel_name, mapped_suffix_or_none)
    """
    text = _normalize_text(raw)
    if not text:
        return "", None

    if "," not in text:
        return text, None

    head, tail = text.rsplit(",", 1)
    tail_norm = _normalize_text(tail).lower()
    if tail_norm == "null" or re.fullmatch(r"-?\d+", tail_norm):
        return _normalize_text(head), tail_norm
    return text, None


def dedupe_channel_names(channels: Iterable[object]) -> List[str]:
    names = []
    for raw in channels:
        name, _ = split_mapped_channel_entry(raw)
        if name:
            names.append(name)
    return _dedupe_casefold(names)


def build_channel_candidates(
    channels: Iterable[object],
    profile_name: str = "",
    bucket_hint: str = "",
    preferred_other: bool = False,
    countries_by_name: Optional[Dict[str, Iterable[str]]] = None,
) -> List[Dict]:
    profile_name = _normalize_text(profile_name)
    hint = _normalize_text(bucket_hint).lower()
    if hint not in VALID_BUCKETS:
        hint = ""

    candidates = []
    for channel_name in dedupe_channel_names(channels):
        countries: List[str] = []
        if isinstance(countries_by_name, dict):
            raw_values = countries_by_name.get(channel_name)
            if raw_values is None:
                raw_values = countries_by_name.get(channel_name.casefold())
            if isinstance(raw_values, (list, tuple, set)):
                countries = _dedupe_casefold([_normalize_text(item) for item in raw_values])

        candidate = {
            "name": channel_name,
            "profiles": [profile_name] if profile_name else [],
            "bucket_hints": [hint] if hint else [],
            "preferred_other": bool(preferred_other),
            "countries": countries,
        }
        candidates.append(candidate)
    return candidates


def _normalize_candidate(raw: object) -> Optional[Dict]:
    if not isinstance(raw, dict):
        return None
    name = _normalize_text(raw.get("name"))
    if not name:
        return None

    profiles = []
    for value in raw.get("profiles", []) if isinstance(raw.get("profiles"), list) else []:
        text = _normalize_text(value)
        if text:
            profiles.append(text)
    profiles = _dedupe_casefold(profiles)

    hints = []
    for value in raw.get("bucket_hints", []) if isinstance(raw.get("bucket_hints"), list) else []:
        text = _normalize_text(value).lower()
        if text in VALID_BUCKETS:
            hints.append(text)
    hints = _dedupe_casefold(hints)

    countries = []
    for value in raw.get("countries", []) if isinstance(raw.get("countries"), list) else []:
        text = _normalize_text(value)
        if text:
            countries.append(text)
    countries = _dedupe_casefold(countries)

    return {
        "name": name,
        "profiles": profiles,
        "bucket_hints": hints,
        "preferred_other": bool(raw.get("preferred_other")),
        "countries": countries,
    }


def merge_channel_candidates(existing: Iterable[object], incoming: Iterable[object]) -> List[Dict]:
    merged: Dict[str, Dict] = {}
    ordered_keys: List[str] = []

    def _ingest(item: object) -> None:
        candidate = _normalize_candidate(item)
        if candidate is None:
            return
        key = _normalize_key(candidate["name"])
        if key not in merged:
            merged[key] = {
                "name": candidate["name"],
                "profiles": list(candidate["profiles"]),
                "bucket_hints": list(candidate["bucket_hints"]),
                "preferred_other": bool(candidate["preferred_other"]),
                "countries": list(candidate["countries"]),
            }
            ordered_keys.append(key)
            return

        node = merged[key]
        node["profiles"] = _dedupe_casefold(list(node["profiles"]) + list(candidate["profiles"]))
        node["bucket_hints"] = _dedupe_casefold(list(node["bucket_hints"]) + list(candidate["bucket_hints"]))
        node["preferred_other"] = bool(node["preferred_other"] or candidate["preferred_other"])
        node["countries"] = _dedupe_casefold(list(node.get("countries", [])) + list(candidate["countries"]))

    for item in existing:
        _ingest(item)
    for item in incoming:
        _ingest(item)

    return [merged[key] for key in ordered_keys]


def index_channel_candidates(candidates: Iterable[object]) -> Dict[str, Dict]:
    merged = merge_channel_candidates([], candidates)
    return {_normalize_key(item["name"]): item for item in merged}


def _name_matches_rules(name: str, exact_values: List[str], keyword_values: List[str]) -> bool:
    key = _normalize_key(name)
    if not key:
        return False

    for exact in exact_values:
        if key == _normalize_key(exact):
            return True

    lowered_name = f" {key} "
    for keyword in keyword_values:
        token = _normalize_key(keyword)
        if not token:
            continue
        if token in lowered_name:
            return True
    return False


def _country_matches(candidate: Optional[Dict], groups: Iterable[str]) -> bool:
    if not isinstance(candidate, dict):
        return False
    raw_countries = candidate.get("countries")
    if not isinstance(raw_countries, list) or not raw_countries:
        return False

    normalized_candidate = {_normalize_key(value) for value in raw_countries if _normalize_text(value)}
    normalized_groups = {_normalize_key(value) for value in groups if _normalize_text(value)}
    if not normalized_candidate or not normalized_groups:
        return False
    return any(value in normalized_groups for value in normalized_candidate)


def classify_channel_bucket(name: str, rules: Dict, candidate: Optional[Dict] = None) -> str:
    if candidate:
        hints = candidate.get("bucket_hints") if isinstance(candidate, dict) else None
        if isinstance(hints, list):
            ordered_hints = [str(item).lower() for item in hints if str(item).lower() in VALID_BUCKETS]
            hint_set = set(ordered_hints)
            # Use metadata-first only when hint is unambiguous.
            if len(hint_set) == 1:
                return next(iter(hint_set))

    country_groups = rules.get("country_groups", {}) if isinstance(rules, dict) else {}
    if isinstance(country_groups, dict) and candidate:
        uk_countries = country_groups.get("uk", []) if isinstance(country_groups.get("uk"), list) else []
        us_countries = country_groups.get("us", []) if isinstance(country_groups.get("us"), list) else []
        in_uk = _country_matches(candidate, uk_countries)
        in_us = _country_matches(candidate, us_countries)
        if in_uk and not in_us:
            return "uk"
        if in_us and not in_uk:
            return "us"
        if in_uk and in_us:
            return "other"

    classification = rules.get("classification", {}) if isinstance(rules, dict) else {}

    uk = classification.get("uk", {}) if isinstance(classification.get("uk"), dict) else {}
    us = classification.get("us", {}) if isinstance(classification.get("us"), dict) else {}

    if _name_matches_rules(
        name,
        uk.get("exact", []) if isinstance(uk.get("exact"), list) else [],
        uk.get("keywords", []) if isinstance(uk.get("keywords"), list) else [],
    ):
        return "uk"

    if _name_matches_rules(
        name,
        us.get("exact", []) if isinstance(us.get("exact"), list) else [],
        us.get("keywords", []) if isinstance(us.get("keywords"), list) else [],
    ):
        return "us"

    return "other"


def is_preferred_other_channel(name: str, rules: Dict, candidate: Optional[Dict] = None) -> bool:
    if candidate and bool(candidate.get("preferred_other")):
        return True

    country_groups = rules.get("country_groups", {}) if isinstance(rules, dict) else {}
    if isinstance(country_groups, dict) and candidate:
        pref_countries = (
            country_groups.get("preferred_other", [])
            if isinstance(country_groups.get("preferred_other"), list)
            else []
        )
        if _country_matches(candidate, pref_countries):
            return True

    classification = rules.get("classification", {}) if isinstance(rules, dict) else {}
    pref = (
        classification.get("preferred_other", {})
        if isinstance(classification.get("preferred_other"), dict)
        else {}
    )
    return _name_matches_rules(
        name,
        pref.get("exact", []) if isinstance(pref.get("exact"), list) else [],
        pref.get("keywords", []) if isinstance(pref.get("keywords"), list) else [],
    )


def select_mapped_event_channels(
    mapped_entries: List[Dict],
    rules: Dict,
    candidate_index: Optional[Dict[str, Dict]] = None,
) -> Tuple[List[Dict], Dict[str, int]]:
    """
    Select mapped channels for one event using UK/US/Others quota logic.

    mapped_entries item shape:
      {"name": str, "id": int, "raw": "Name, 123"}
    """
    candidate_index = candidate_index or {}
    max_total = max(1, int(rules.get("max_event_channels", 5)))
    max_per_bucket = rules.get("max_per_bucket", {}) if isinstance(rules.get("max_per_bucket"), dict) else {}
    uk_limit = max(0, int(max_per_bucket.get("uk", 2)))
    us_limit = max(0, int(max_per_bucket.get("us", 2)))

    deduped: List[Dict] = []
    seen_names = set()
    for entry in mapped_entries:
        if not isinstance(entry, dict):
            continue
        name = _normalize_text(entry.get("name"))
        channel_id = entry.get("id")
        if not name or not isinstance(channel_id, int):
            continue
        key = _normalize_key(name)
        if key in seen_names:
            continue
        seen_names.add(key)
        deduped.append(
            {
                "name": name,
                "id": channel_id,
                "raw": _normalize_text(entry.get("raw")) or f"{name}, {channel_id}",
            }
        )

    uk_rows: List[Dict] = []
    us_rows: List[Dict] = []
    other_pref_rows: List[Dict] = []
    other_rows: List[Dict] = []

    for row in deduped:
        candidate = candidate_index.get(_normalize_key(row["name"]))
        bucket = classify_channel_bucket(row["name"], rules, candidate=candidate)
        preferred = False
        if bucket == "other":
            preferred = is_preferred_other_channel(row["name"], rules, candidate=candidate)

        if bucket == "uk":
            uk_rows.append(row)
        elif bucket == "us":
            us_rows.append(row)
        elif preferred:
            other_pref_rows.append(row)
        else:
            other_rows.append(row)

    selected: List[Dict] = []
    selected.extend(uk_rows[:uk_limit])
    selected.extend(us_rows[:us_limit])

    remaining = max_total - len(selected)
    if remaining > 0:
        selected.extend(other_pref_rows[:remaining])
        remaining = max_total - len(selected)
    if remaining > 0:
        selected.extend(other_rows[:remaining])

    stats = {
        "selected_total": len(selected),
        "selected_uk": sum(1 for row in selected if row in uk_rows),
        "selected_us": sum(1 for row in selected if row in us_rows),
        "selected_other": sum(1 for row in selected if row in other_pref_rows or row in other_rows),
        "selected_other_preferred": sum(1 for row in selected if row in other_pref_rows),
        "candidates_mapped": len(deduped),
    }
    return selected, stats


def get_active_geo_profiles(rules: Dict) -> List[Dict]:
    raw_profiles = rules.get("geo_profiles", []) if isinstance(rules, dict) else []
    profiles: List[Dict] = []
    for raw in raw_profiles if isinstance(raw_profiles, list) else []:
        if not isinstance(raw, dict):
            continue
        if not bool(raw.get("enabled", True)):
            continue

        name = _normalize_text(raw.get("name"))
        if not name:
            continue

        bucket_hint = _normalize_text(raw.get("bucket_hint")).lower()
        if bucket_hint not in VALID_BUCKETS:
            bucket_hint = ""

        schedule_params = raw.get("schedule_params", {})
        if not isinstance(schedule_params, dict):
            schedule_params = {}
        schedule_params = {
            _normalize_text(k): _normalize_text(v)
            for k, v in schedule_params.items()
            if _normalize_text(k) and _normalize_text(v)
        }

        tournament_overrides = raw.get("tournament_overrides", {})
        if not isinstance(tournament_overrides, dict):
            tournament_overrides = {}
        tournament_overrides = {
            _normalize_text(k): _normalize_text(v)
            for k, v in tournament_overrides.items()
            if _normalize_text(k) and _normalize_text(v)
        }

        profiles.append(
            {
                "name": name,
                "enabled": True,
                "primary": bool(raw.get("primary")),
                "bucket_hint": bucket_hint,
                "preferred_other": bool(raw.get("preferred_other")),
                "schedule_params": schedule_params,
                "tournament_overrides": tournament_overrides,
            }
        )

    if not profiles:
        return [
            {
                "name": "default",
                "enabled": True,
                "primary": True,
                "bucket_hint": "",
                "preferred_other": False,
                "schedule_params": {},
                "tournament_overrides": {},
            }
        ]

    if not any(profile.get("primary") for profile in profiles):
        profiles[0]["primary"] = True

    return profiles
