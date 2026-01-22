import json
import requests
import datetime
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
JSON_FILE = "lovestory.json"
LOG_FILE = "log.txt"
ICON_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"
BASE_URL = "https://ninoiptv.com/"
MAX_PROFILES = 100

# Validation settings
MIN_PLAYLIST_SIZE = 100  # Minimum bytes for valid playlist
PLAYLIST_TIMEOUT = 12    # Seconds to wait for playlist download

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive"
}

def log_to_file(message):
    """Saves a summary to the permanent log.txt file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {message}\n")

def parse_m3u_streams(content):
    """Extract stream URLs from M3U playlist content."""
    streams = []
    lines = content.split('\n')
    
    for line in lines:
        line = line.strip()
        # Skip comments and empty lines
        if line and not line.startswith('#'):
            # Basic validation that it looks like a URL
            if line.startswith('http://') or line.startswith('https://'):
                streams.append(line)
    
    return streams

def validate_playlist(url):
    """
    Lightweight playlist validation for GitHub Actions:
    1. Check if playlist URL is accessible
    2. Verify it returns actual M3U content
    3. Count streams found
    Returns (is_valid, reason, stream_count)
    """
    try:
        print(f"  → Fetching playlist from {url[:60]}...")
        
        # Download the playlist (lightweight - just text content)
        response = requests.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
        
        # Check HTTP status
        if response.status_code >= 400:
            print(f"  ✗ HTTP {response.status_code}")
            return False, f"HTTP {response.status_code}", 0
        
        # Check content size
        content_length = len(response.content)
        if content_length < MIN_PLAYLIST_SIZE:
            print(f"  ✗ Playlist too small ({content_length} bytes)")
            return False, "empty/invalid", 0
        
        # Check if it's actually M3U content
        content = response.text
        
        # Must have M3U header or stream URLs
        has_m3u_header = '#EXTM3U' in content
        has_extinf = '#EXTINF' in content
        
        if not (has_m3u_header or has_extinf):
            print(f"  ✗ Not M3U format (missing headers)")
            return False, "not M3U format", 0
        
        # Parse streams to count them
        streams = parse_m3u_streams(content)
        stream_count = len(streams)
        
        if stream_count == 0:
            print(f"  ✗ No streams found in playlist")
            return False, "no streams", 0
        
        # Success - we have a valid M3U with streams
        print(f"  ✓ VALID M3U ({stream_count} streams, {content_length} bytes)")
        return True, f"valid ({stream_count} streams)", stream_count
        
    except requests.exceptions.Timeout:
        print(f"  ✗ Timeout")
        return False, "timeout", 0
    except Exception as e:
        print(f"  ✗ Error: {str(e)[:30]}")
        return False, "error", 0

def get_domain(url):
    return urlparse(url).netloc

def scrape_nino():
    """Finds the latest article and extracts all unique links."""
    print(f"\n--- STEP 1: ACCESSING {BASE_URL} ---")
    try:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        latest_article_tag = soup.find('h2', class_='entry-title')
        if not latest_article_tag:
            print("FAILED: Could not find any articles on homepage.")
            return []
        
        article_url = latest_article_tag.find('a')['href']
        print(f"Latest article identified: {article_url}")
        
        r_art = requests.get(article_url, headers=HEADERS, timeout=15)
        soup_art = BeautifulSoup(r_art.text, 'html.parser')
        
        found_links = []
        for a in soup_art.find_all('a', href=True):
            href = a['href']
            if "get.php?username=" in href:
                found_links.append(href)
        
        unique_scraped = list(dict.fromkeys(found_links))
        print(f"Scraped {len(unique_scraped)} unique playlist links")
        return unique_scraped

    except Exception as e:
        print(f"CRITICAL ERROR during scrape: {e}")
        return []

def main():
    print("═══════════════════════════════════════════════════")
    print("   O.R CONTENT MANAGER - ENHANCED VALIDATOR")
    print("═══════════════════════════════════════════════════\n")
    
    # Load JSON data
    try:
        with open(JSON_FILE, 'r') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"featured_content": []}
    
    original_list = data.get("featured_content", [])
    alive_list = []
    seen_domains = set()
    
    removed_count = 0

    print(f"\n--- STEP 2: VALIDATING EXISTING {len(original_list)} PROFILES ---")
    for idx, item in enumerate(original_list, 1):
        url = item.get("url")
        domain = get_domain(url)
        
        print(f"\n[{idx}/{len(original_list)}] Testing: {item.get('name', 'Unknown')}")
        
        is_valid, reason, stream_count = validate_playlist(url)
        
        if is_valid and domain not in seen_domains:
            alive_list.append(item)
            seen_domains.add(domain)
        else:
            removed_count += 1
            removal_reason = reason if not is_valid else 'duplicate domain'
            print(f"  ⚠ REMOVED: {removal_reason}")

    slots_available = MAX_PROFILES - len(alive_list)
    print(f"\n{'─' * 50}")
    print(f"Validation complete: {len(alive_list)} valid | {removed_count} removed")
    print(f"Slots available: {slots_available}")
    print(f"{'─' * 50}")

    # Scrape for new content if we have room
    if slots_available > 0:
        print(f"\n--- STEP 3: SEARCHING FOR NEW CONTENT ---")
        new_potential_links = scrape_nino()
        
        for idx, link in enumerate(new_potential_links, 1):
            if len(alive_list) >= MAX_PROFILES:
                print("\n⚠ REACHED LIMIT: Stopping additions at 100.")
                break
            
            domain = get_domain(link)
            if domain in seen_domains:
                print(f"\n[New {idx}/{len(new_potential_links)}] Skipping (domain exists): {link[:50]}...")
                continue
            
            print(f"\n[New {idx}/{len(new_potential_links)}] Validating new playlist...")
            is_valid, reason, stream_count = validate_playlist(link)
            
            if is_valid:
                print(f"  ✓ ADDED to collection")
                alive_list.append({
                    "name": "", 
                    "logo_url": ICON_URL, 
                    "type": "m3u",
                    "url": link, 
                    "server_url": None, 
                    "id": ""
                })
                seen_domains.add(domain)

    print(f"\n--- STEP 4: RE-INDEXING NAMES AND IDs ---")
    for i, item in enumerate(alive_list, start=1):
        num_name = str(i).zfill(3)
        num_id = str(i).zfill(2)
        
        item["name"] = f"O.R {num_name}"
        item["id"] = f"featured_m3u_{num_id}"
        item["logo_url"] = ICON_URL
    
    print(f"Indexed {len(alive_list)} profiles")
    
    # Save files
    data["featured_content"] = alive_list
    with open(JSON_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    summary_msg = f"Sync Complete. Valid Profiles: {len(alive_list)} | Removed: {removed_count} | Validated with stream testing"
    log_to_file(summary_msg)
    
    print(f"\n{'═' * 50}")
    print(f"✓ FINISHED: {summary_msg}")
    print(f"{'═' * 50}\n")

if __name__ == "__main__":
    main()
