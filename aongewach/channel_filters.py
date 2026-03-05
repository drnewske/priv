#!/usr/bin/env python3
"""
Shared channel filtering and regional selection helpers.
"""

from __future__ import annotations

import re
from typing import Callable, Dict, Iterable, List, Optional, Sequence


NON_BROADCAST_WORD_RE = re.compile(
    r"\b(app|website|web\s*site|youtube|radio)\b",
    re.IGNORECASE,
)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)
NOT_TELEVISED_RE = re.compile(
    r"\b(not\s+televised|not\s+on\s+tv|no\s+tv|no\s+broadcast|broadcast\s+tbc)\b",
    re.IGNORECASE,
)
PAREN_CONTENT_RE = re.compile(r"\(([^)]+)\)")
TRAILING_PAREN_RE = re.compile(r"(?:\s*\([^()]*\)\s*)+$")
DAZN_LINEAR_RE = re.compile(r"\bdazn\s+\d+\b", re.IGNORECASE)

STREAMING_PREFIXES = (
    "espn plus",
    "fanatiz",
    "fubo",
    "paramount",
    "peacock",
    "tod",
    "vix",
    "onefootball",
    "disney",
    "discovery",
    "hbo max",
    "apple tv",
    "bein connect",
    "sky go",
    "now tv",
    "dstv now",
    "stc tv",
    "thmanyah",
    "mbc shahid",
    "jaco",
    "astro go",
    "tcs go",
    "canal goat",
    "ligue 1",
    "viaplay",
    "tv2 play",
    "molotov tv",
    "qq sports",
    "bbc i player",
    "bbc iplayer",
    "mtv katsomo",
    "palace tv",
    "zhibo8",
    "dgo",
    "vivo play",
    "zapping",
)
STREAMING_CONTAINS = (
    "prime video",
    "mls season pass",
    "fox one",
)
STREAMING_EXACT = {
    "dazn",
    "espn plus",
    "fox",
}

REGION_US = "us"
REGION_ZA = "za"
REGION_UK = "uk"
REGION_ME = "me"

US_TAGS = {"usa", "us"}
ZA_TAGS = {"rsa", "za", "afr", "nga"}
UK_TAGS = {"gbr", "uk", "irl"}
ME_TAGS = {"ara", "mena", "uae", "ksa", "sau", "qat", "qatar"}


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_key(value: object) -> str:
    text = normalize_text(value).casefold()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def normalize_channel_name(name: object) -> str:
    text = normalize_text(name)
    if not text:
        return ""
    stripped = TRAILING_PAREN_RE.sub("", text).strip()
    return normalize_text(stripped or text)


def _tag_tokens(name: str) -> List[str]:
    out: List[str] = []
    for part in PAREN_CONTENT_RE.findall(name):
        key = normalize_key(part)
        if not key:
            continue
        out.extend(key.split())
    return out


def is_streaming_service_channel(name: str) -> bool:
    key = normalize_key(name)
    if not key:
        return False

    if "espn+" in name.casefold() or key.startswith("espn plus"):
        return True

    if key.startswith("dazn") and DAZN_LINEAR_RE.search(name) is None:
        return True

    if key in STREAMING_EXACT:
        return True
    if any(key.startswith(prefix) for prefix in STREAMING_PREFIXES):
        return True
    if any(token in key for token in STREAMING_CONTAINS):
        return True
    if " play " in f" {key} ":
        return True
    if " connect " in f" {key} ":
        return True
    if " player " in f" {key} ":
        return True
    return False


def is_usable_channel_name(
    name: str,
    placeholder_checker: Optional[Callable[[str], bool]] = None,
) -> bool:
    cleaned = normalize_text(name)
    if not cleaned:
        return False
    if placeholder_checker and placeholder_checker(cleaned):
        return False
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    if NOT_TELEVISED_RE.search(cleaned):
        return False
    if is_streaming_service_channel(cleaned):
        return False
    return True


def detect_channel_region(name: str) -> Optional[str]:
    key = normalize_key(name)
    if not key:
        return None

    tags = set(_tag_tokens(name))
    if tags & US_TAGS:
        return REGION_US
    if tags & ZA_TAGS:
        return REGION_ZA
    if tags & UK_TAGS:
        return REGION_UK
    if tags & ME_TAGS:
        return REGION_ME

    if " bein sports mena " in f" {key} ":
        return REGION_ME

    us_keywords = (
        "nbc",
        "cbs",
        "abc",
        "espn 2",
        "espn deportes",
        "fox deportes",
        "fox soccer plus",
        "univision",
        "telemundo",
        "tudn",
        "msg",
    )
    if any(token in key for token in us_keywords):
        return REGION_US

    if "supersport" in key:
        return REGION_ZA

    uk_keywords = (
        "sky sports",
        "tnt sports",
        "bbc ",
        "itv",
        "premier sports",
    )
    if "ukr" not in key and any(token in key for token in uk_keywords):
        return REGION_UK

    me_keywords = (
        "bein sports mena",
        "mbc action",
        "ssc ",
        "alkass",
    )
    if any(token in key for token in me_keywords):
        return REGION_ME

    return None


def _dedupe_channel_dicts(channels: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    seen = set()
    for row in channels:
        raw_name = normalize_text(row.get("raw_name") or row.get("name"))
        name = normalize_channel_name(row.get("name") or raw_name)
        if not name:
            name = normalize_channel_name(raw_name)
        if not raw_name or not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "name": name,
                "raw_name": raw_name,
                "url": normalize_text(row.get("url")),
                "tv_id": row.get("tv_id"),
            }
        )
    return out


def select_regional_channel_dicts(
    channels: Sequence[Dict[str, object]],
    max_channels: int = 4,
    include_uk: bool = True,
) -> List[Dict[str, object]]:
    if max_channels < 1:
        return []

    deduped = _dedupe_channel_dicts(channels)
    if not deduped:
        return []

    buckets = {
        REGION_US: [],
        REGION_ZA: [],
        REGION_UK: [],
        REGION_ME: [],
        "other": [],
    }

    for idx, row in enumerate(deduped):
        region = detect_channel_region(str(row.get("raw_name") or row.get("name", "")))
        bucket_key = region if region in buckets else "other"
        buckets[bucket_key].append((idx, row))

    # Prefer SuperSport/CANAL+ options for African slot.
    buckets[REGION_ZA].sort(
        key=lambda item: (
            0
            if "supersport" in normalize_key(item[1].get("name"))
            else 1
            if "canal" in normalize_key(item[1].get("name"))
            else 2,
            item[0],
        )
    )

    # Prefer beIN channels for the Middle East slot.
    buckets[REGION_ME].sort(
        key=lambda item: (0 if "bein" in normalize_key(item[1].get("name")) else 1, item[0])
    )

    region_order = [REGION_US, REGION_ZA]
    if include_uk:
        region_order.append(REGION_UK)
    region_order.append(REGION_ME)

    selected: List[Dict[str, object]] = []
    seen = set()

    for region in region_order:
        for _, row in buckets[region]:
            name_key = normalize_text(row.get("name")).casefold()
            if not name_key or name_key in seen:
                continue
            selected.append(row)
            seen.add(name_key)
            break
        if len(selected) >= max_channels:
            return selected[:max_channels]

    leftovers: List[tuple[int, Dict[str, object]]] = []
    leftovers.extend(buckets[REGION_US][1:])
    leftovers.extend(buckets[REGION_ZA][1:])
    if include_uk:
        leftovers.extend(buckets[REGION_UK][1:])
    leftovers.extend(buckets[REGION_ME][1:])
    leftovers.extend(buckets["other"])

    leftovers.sort(key=lambda item: item[0])
    for _, row in leftovers:
        if len(selected) >= max_channels:
            break
        if not include_uk and detect_channel_region(str(row.get("raw_name") or row.get("name", ""))) == REGION_UK:
            continue
        name_key = normalize_text(row.get("name")).casefold()
        if not name_key or name_key in seen:
            continue
        selected.append(row)
        seen.add(name_key)

    return selected[:max_channels]


def select_regional_channel_names(
    channels: Iterable[str],
    max_channels: int = 4,
    include_uk: bool = True,
) -> List[str]:
    mapped = [{"name": normalize_text(name), "url": "", "tv_id": None} for name in channels]
    selected = select_regional_channel_dicts(mapped, max_channels=max_channels, include_uk=include_uk)
    return [normalize_text(item.get("name")) for item in selected if normalize_text(item.get("name"))]
