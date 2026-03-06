#!/usr/bin/env python3
"""
Freeze Flashscore header sport SVG icons into a static JSON payload.

The script is intentionally one-time by default:
  - if the output JSON already exists with sport entries, it exits without re-fetching
  - pass --refresh to force a new fetch
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import re
import xml.etree.ElementTree as ET
from copy import deepcopy
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote, urljoin

import requests


DEFAULT_BASE_PAGE = "https://www.flashscore.com/"
DEFAULT_OUTPUT = "aongewach/flashscore_sport_assets.json"
DEFAULT_TIMEOUT = 30
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

def iso_z_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_key(value: object) -> str:
    text = normalize_text(value).casefold()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json(path: Path, payload: Dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


class SportMenuParser(HTMLParser):
    def __init__(self, base_page: str) -> None:
        super().__init__()
        self.base_page = base_page
        self.items: List[Dict[str, str]] = []
        self._seen = set()
        self._current_item: Dict[str, str] | None = None
        self._capture_label = False

    def handle_starttag(self, tag: str, attrs) -> None:
        attributes = {key: value for key, value in attrs}
        classes = normalize_text(attributes.get("class")).split(" ")

        if tag == "a":
            sport_id = normalize_text(attributes.get("data-sport-id"))
            if sport_id:
                self._current_item = {
                    "sport_id": sport_id,
                    "href": urljoin(self.base_page, normalize_text(attributes.get("href"))),
                    "label": "",
                    "sprite_url": "",
                    "symbol_id": "",
                }
            return

        if self._current_item is None:
            return

        if tag == "use":
            href = normalize_text(attributes.get("xlink:href") or attributes.get("href"))
            if "#" in href:
                sprite_path, symbol_id = href.split("#", 1)
                self._current_item["sprite_url"] = urljoin(self.base_page, sprite_path)
                self._current_item["symbol_id"] = normalize_text(symbol_id)
            return

        if tag == "div" and any(cls in {"menuTop__text", "menuMinority__text"} for cls in classes):
            self._capture_label = True

    def handle_data(self, data: str) -> None:
        if self._capture_label and self._current_item is not None:
            label = normalize_text(data)
            if label:
                self._current_item["label"] = label

    def handle_endtag(self, tag: str) -> None:
        if tag == "div":
            self._capture_label = False
            return

        if tag == "a" and self._current_item is not None:
            symbol_id = normalize_text(self._current_item.get("symbol_id"))
            label = normalize_text(self._current_item.get("label"))
            if symbol_id and label:
                key = normalize_key(symbol_id)
                if key not in self._seen:
                    self._seen.add(key)
                    self._current_item["key"] = key
                    self.items.append(dict(self._current_item))
            self._current_item = None
            self._capture_label = False


def parse_menu_sports(html: str, base_page: str) -> List[Dict[str, str]]:
    parser = SportMenuParser(base_page)
    parser.feed(html)
    return parser.items


def fetch_symbol_sprite(session: requests.Session, sprite_url: str, timeout: int) -> Dict[str, ET.Element]:
    response = session.get(sprite_url, timeout=timeout)
    response.raise_for_status()
    root = ET.fromstring(response.text)

    symbol_map: Dict[str, ET.Element] = {}
    ns = {"svg": "http://www.w3.org/2000/svg"}
    for symbol in root.findall("svg:symbol", ns):
        symbol_id = normalize_text(symbol.attrib.get("id"))
        if symbol_id:
            symbol_map[symbol_id] = symbol
    return symbol_map


def build_standalone_svg(symbol: ET.Element) -> str:
    svg_root = ET.Element(
        "svg",
        {
            "xmlns": "http://www.w3.org/2000/svg",
            "viewBox": symbol.attrib.get("viewBox", "0 0 20 20"),
            "fill": symbol.attrib.get("fill", "currentColor"),
        },
    )

    for child in list(symbol):
        svg_root.append(deepcopy(child))

    return ET.tostring(svg_root, encoding="unicode")


def svg_to_data_uri(svg_markup: str) -> str:
    return "data:image/svg+xml;utf8," + quote(svg_markup, safe=":/#?&=+,;%@[]!$'()*")


def svg_to_base64_data_uri(svg_markup: str) -> str:
    encoded = base64.b64encode(svg_markup.encode("utf-8")).decode("ascii")
    return "data:image/svg+xml;base64," + encoded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Freeze Flashscore header sport SVG icons into static JSON.")
    parser.add_argument("--base-page", default=DEFAULT_BASE_PAGE, help="Flashscore base page to inspect.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON path.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds.")
    parser.add_argument("--refresh", action="store_true", help="Force a re-fetch even when output already exists.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)

    existing = load_json(output_path)
    if not args.refresh and isinstance(existing.get("sports"), dict) and existing["sports"]:
        print(f"Using existing static sport asset payload: {output_path}")
        return 0

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    page_response = session.get(args.base_page, timeout=args.timeout)
    page_response.raise_for_status()

    menu_items = parse_menu_sports(page_response.text, args.base_page)
    if not menu_items:
        raise RuntimeError("No Flashscore header sport icons were found in the page markup.")

    sprite_cache: Dict[str, Dict[str, ET.Element]] = {}
    sports: Dict[str, Dict[str, str]] = {}

    for item in menu_items:
        sprite_url = item["sprite_url"]
        if sprite_url not in sprite_cache:
            sprite_cache[sprite_url] = fetch_symbol_sprite(session, sprite_url, args.timeout)
        symbol_map = sprite_cache[sprite_url]
        symbol = symbol_map.get(item["symbol_id"])
        if symbol is None:
            continue

        svg_markup = build_standalone_svg(symbol)
        sports[item["key"]] = {
            "sport_id": item["sport_id"],
            "label": item["label"],
            "href": item["href"],
            "sprite_url": sprite_url,
            "symbol_id": item["symbol_id"],
            "svg": svg_markup,
            "svg_data_uri": svg_to_data_uri(svg_markup),
            "svg_data_uri_base64": svg_to_base64_data_uri(svg_markup),
        }

    payload = {
        "generated_at": iso_z_now(),
        "source": "flashscore.com",
        "base_page": args.base_page,
        "sports": sports,
    }
    save_json(output_path, payload)

    print(f"Saved {len(sports)} static Flashscore sport icons to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
