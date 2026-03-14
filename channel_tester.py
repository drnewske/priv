
import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
import sys
from urllib.request import urlopen

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import shutil
from fuzzywuzzy import fuzz

MATCHES_URL = (
    "https://raw.githubusercontent.com/drnewske/"
    "areallybadideabuttidonthateitijusthateitsomuch/"
    "refs/heads/main/matches.json"
)
DEFAULT_SCHEDULE_PATH = "aongewach/e104f869d64e3d41256d5398.json"
DEFAULT_OUTPUT_PATH = "eventultra.json"

TIME_WINDOW_SECONDS = 2 * 60 * 60
MIN_TEAM_RATIO = 80
MIN_AVG_RATIO = 85

STREAM_URL_RE = re.compile(r"\.m3u8(\?|$)|\.m3u(\?|$)", re.IGNORECASE)
M3U_MIME_HINTS = ("mpegurl", "m3u8", "m3u")

def normalize_team_name(name):
    cleaned = re.sub(r"[^a-z0-9\s]", " ", name.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def safe_console(text):
    if text is None:
        return ""
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        return str(text).encode(encoding, errors="replace").decode(encoding, errors="replace")
    except LookupError:
        return str(text).encode("utf-8", errors="replace").decode("utf-8", errors="replace")


def is_stream_url(url):
    if not url:
        return False
    if STREAM_URL_RE.search(url):
        return True
    lower = url.lower()
    return "m3u8" in lower or "m3u" in lower


def dedupe_keep_order(items):
    seen = set()
    deduped = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def get_stream_urls_from_log(logs):
    stream_urls = []
    for entry in logs:
        try:
            log = json.loads(entry["message"])["message"]
        except (KeyError, ValueError, TypeError):
            continue

        method = log.get("method")
        params = log.get("params", {})

        if method == "Network.requestWillBeSent":
            url = params.get("request", {}).get("url", "")
            if is_stream_url(url):
                stream_urls.append(url)
        elif method == "Network.responseReceived":
            response = params.get("response", {})
            url = response.get("url", "")
            mime = (response.get("mimeType") or "").lower()
            if is_stream_url(url) or any(hint in mime for hint in M3U_MIME_HINTS):
                stream_urls.append(url)

    return dedupe_keep_order(stream_urls)


def load_json(source):
    if source.startswith("http://") or source.startswith("https://"):
        with urlopen(source) as response:
            payload = response.read().decode("utf-8", errors="ignore")
        return json.loads(payload)
    with open(source, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_iso_naive(value):
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def find_browser_binary():
    if os.name == "nt":
        chrome_paths = [
            "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
            "C:\\Program Files\\Chromium\\Application\\chrome.exe",
            "C:\\Program Files (x86)\\Chromium\\Application\\chrome.exe",
        ]
        for path in chrome_paths:
            if os.path.exists(path):
                return "chrome", path

        edge_paths = [
            "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
            "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
        ]
        for path in edge_paths:
            if os.path.exists(path):
                return "edge", path

    if sys.platform == "darwin":
        mac_chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        mac_edge = "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
        if os.path.exists(mac_chrome):
            return "chrome", mac_chrome
        if os.path.exists(mac_edge):
            return "edge", mac_edge

    chrome_bins = [
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
    ]
    for name in chrome_bins:
        path = shutil.which(name)
        if path:
            return "chrome", path

    edge_bins = [
        "microsoft-edge",
        "microsoft-edge-stable",
    ]
    for name in edge_bins:
        path = shutil.which(name)
        if path:
            return "edge", path

    return None, None


def infer_browser_type(driver_binary, browser_binary):
    if driver_binary:
        lower = driver_binary.lower()
        if "msedgedriver" in lower:
            return "edge"
        if "chromedriver" in lower:
            return "chrome"
    if browser_binary:
        lower = browser_binary.lower()
        if "msedge" in lower:
            return "edge"
        if "chrome" in lower or "chromium" in lower:
            return "chrome"
    return None


def create_driver(headful, browser_type=None, browser_binary=None, driver_binary=None):
    browser = browser_type or infer_browser_type(driver_binary, browser_binary)
    binary_path = browser_binary

    if not browser or not binary_path:
        detected_browser, detected_binary = find_browser_binary()
        if not browser:
            browser = detected_browser
        if not binary_path:
            binary_path = detected_binary

    if not browser:
        raise RuntimeError(
            "No Chrome/Chromium/Edge binary found. Install Chrome or Edge, or provide --browser."
        )

    if browser == "chrome":
        options = ChromeOptions()
        if not headful:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-sync")
        options.add_argument("--metrics-recording-only")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--remote-debugging-port=9222")
        if sys.platform.startswith("linux"):
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-setuid-sandbox")
            options.add_argument("--user-data-dir=/tmp/selenium")
        options.add_argument("--log-level=3")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        if binary_path:
            options.binary_location = binary_path
        if driver_binary:
            service = ChromeService(driver_binary)
        else:
            service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        options = EdgeOptions()
        if not headful:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-sync")
        options.add_argument("--metrics-recording-only")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--remote-debugging-port=9222")
        if sys.platform.startswith("linux"):
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-setuid-sandbox")
            options.add_argument("--user-data-dir=/tmp/selenium")
        options.add_argument("--log-level=3")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        if binary_path:
            options.binary_location = binary_path
        if driver_binary:
            service = EdgeService(driver_binary)
        else:
            service = EdgeService(EdgeChromiumDriverManager().install())
        driver = webdriver.Edge(service=service, options=options)

    print(f"Using {browser} at {binary_path}")
    return driver

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matches", default=MATCHES_URL, help="URL or path to matches.json")
    parser.add_argument("--schedule", default=DEFAULT_SCHEDULE_PATH, help="Path to weekly_schedule.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Output path for eventultra.json")
    parser.add_argument("--sleep", type=float, default=5.0, help="Seconds to wait per page load")
    parser.add_argument("--headful", action="store_true", help="Show browser window")
    parser.add_argument("--browser", help="Path to browser binary (chrome.exe or msedge.exe)")
    parser.add_argument("--driver", help="Path to driver binary (chromedriver.exe or msedgedriver.exe)")
    parser.add_argument("--browser-type", choices=["chrome", "edge"], help="Force browser type")
    parser.add_argument("--max-matches", type=int, help="Process only the first N matches (debug)")
    parser.add_argument("--max-links", type=int, help="Process only the first N links (debug)")
    args = parser.parse_args()

    driver = create_driver(
        args.headful,
        browser_type=args.browser_type,
        browser_binary=args.browser,
        driver_binary=args.driver,
    )

    weekly_schedule = load_json(args.schedule)
    matches = load_json(args.matches)

    event_ultra_map = {}

    link_cache = {}
    processed_links = 0
    processed_matches = 0

    for match in matches:
        if not match.get("links"):
            continue
        if args.max_matches and processed_matches >= args.max_matches:
            break

        print(f"Processing match: {safe_console(match.get('title'))}")
        stream_urls = []

        for link in match["links"]:
            url = link.get("url")
            if not url:
                continue
            if args.max_links and processed_links >= args.max_links:
                break

            if url in link_cache:
                stream_urls.extend(link_cache[url])
                continue
            try:
                driver.get(url)
                time.sleep(args.sleep)
                logs = driver.get_log("performance")
                found_urls = get_stream_urls_from_log(logs)
                link_cache[url] = found_urls

                if found_urls:
                    stream_urls.extend(found_urls)
                    print(f"Found {len(found_urls)} stream URLs in {url}")
                else:
                    print(f"No stream URLs found in {url}")
            except Exception as e:
                print(f"Error processing {url}: {e}")
            processed_links += 1

        if args.max_links and processed_links >= args.max_links:
            print("Reached --max-links limit; stopping early.")
            break

        stream_urls = dedupe_keep_order(stream_urls)
        if not stream_urls:
            continue

        match_kickoff = parse_iso_naive(match.get("kickOff", ""))
        if not match_kickoff:
            print(f"Could not parse kickoff time: {match.get('kickOff')}")
            continue

        team1_name = normalize_team_name(match.get("team1", {}).get("name", ""))
        team2_name = normalize_team_name(match.get("team2", {}).get("name", ""))
        if not team1_name or not team2_name:
            continue

        for day in weekly_schedule.get("schedule", []):
            for event in day.get("events", []):
                event_start_time = parse_iso_naive(event.get("start_time_iso", ""))
                if not event_start_time:
                    continue

                if abs((match_kickoff - event_start_time).total_seconds()) > TIME_WINDOW_SECONDS:
                    continue

                event_home_team = normalize_team_name(event.get("home_team", ""))
                event_away_team = normalize_team_name(event.get("away_team", ""))
                if not event_home_team or not event_away_team:
                    continue

                ratio_home = fuzz.token_set_ratio(team1_name, event_home_team)
                ratio_away = fuzz.token_set_ratio(team2_name, event_away_team)
                avg_direct = (ratio_home + ratio_away) / 2

                ratio_home_swap = fuzz.token_set_ratio(team1_name, event_away_team)
                ratio_away_swap = fuzz.token_set_ratio(team2_name, event_home_team)
                avg_swap = (ratio_home_swap + ratio_away_swap) / 2

                if avg_direct >= MIN_AVG_RATIO and ratio_home >= MIN_TEAM_RATIO and ratio_away >= MIN_TEAM_RATIO:
                    match_key = (event.get("name"), event.get("start_time_iso"))
                elif avg_swap >= MIN_AVG_RATIO and ratio_home_swap >= MIN_TEAM_RATIO and ratio_away_swap >= MIN_TEAM_RATIO:
                    match_key = (event.get("name"), event.get("start_time_iso"))
                else:
                    continue

                print(
                    f"Match found for {safe_console(match.get('title'))} -> "
                    f"{safe_console(event.get('name'))}"
                )
                existing = event_ultra_map.get(match_key)
                if not existing:
                    new_event = {k: v for k, v in event.items() if k != "channels"}
                    new_event["streams"] = []
                    event_ultra_map[match_key] = new_event
                    existing = new_event

                start_index = len(existing["streams"])
                for i, url in enumerate(stream_urls):
                    existing["streams"].append({"url": url, "label": f"STR{start_index + i + 1}"})

        processed_matches += 1

    driver.quit()

    event_ultra = list(event_ultra_map.values())
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(event_ultra, f, indent=2)

    print(f"Processing complete. `{args.output}` created.")

if __name__ == '__main__':
    main()
