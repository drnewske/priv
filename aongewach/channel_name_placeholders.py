#!/usr/bin/env python3
"""Shared detection of placeholder channel labels."""

from __future__ import annotations

import re


WHITESPACE_RE = re.compile(r"\s+")
PLACEHOLDER_EXACT_RE = re.compile(
    r"^(?:"
    r"tba|tbc|tbd|"
    r"n/?a|none|null|unknown|"
    r"unannounced|"
    r"to be announced|to be confirmed|"
    r"not available|"
    r"no channel(?:s)?(?: available)?|"
    r"no broadcaster(?:s)?(?: available)?|"
    r"channel(?:s)?\s+(?:tba|tbc|tbd)|"
    r"-+"
    r")$",
    re.IGNORECASE,
)
PLACEHOLDER_SUFFIX_RE = re.compile(r"^.+\s+(?:tba|tbc|tbd)$", re.IGNORECASE)


def normalize_channel_name(value: object) -> str:
    return WHITESPACE_RE.sub(" ", str(value or "").strip())


def is_placeholder_channel_name(value: object) -> bool:
    cleaned = normalize_channel_name(value)
    if not cleaned:
        return False

    probe = cleaned.strip(" \t\r\n,;|.")
    if PLACEHOLDER_EXACT_RE.fullmatch(probe):
        return True
    if PLACEHOLDER_SUFFIX_RE.fullmatch(probe):
        return True
    return False
