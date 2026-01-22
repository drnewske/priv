import json
import requests
import datetime
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# --- CONFIGURATION ---
JSON_FILE = "lovestory.json"
LOG_FILE = "log.txt"
ICON_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"
BASE_URL = "https://ninoiptv.com/"
MAX_PROFILES = 100

# Validation settings
MIN_PLAYLIST_SIZE = 100  # Minimum bytes for valid playlist
PLAYLIST_TIMEOUT = 12    # Seconds to wait for playlist download
DAYS_TO_SCRAPE = 7       # Scrape articles from last 7 days
MAX_ARTICLES = 10        # Maximum articles to scrape

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
        response = requests.get(url, headers=HEADERS, timeout=PLAYLIST_TIMEOUT, allow_redirects=True)
        
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

def extract_date_from_title(title):
    """
    Extract date from article titles like:
    'Latest Free M3U IPTV Playlists | Updated Daily – 21-01-2026 | V2'
    Returns datetime object or None
    """
    # Match patterns like: 21-01-2026, 19-01-2026, etc.
    date_pattern = r'(\d{2})-(\d{2})-(\d{4})'
    match = re.search(date_pattern, title)
    
    if match:
        day, month, year = match.groups()
        try:
            return datetime.datetime(int(year), int(month), int(day))
        except ValueError:
            return None
    return None

def scrape_nino():
    """
    Enhanced scraper that:
    1. Finds articles from the last 7 days
    2. Scrapes both regular and V2 versions
    3. Extracts all unique playlist links
    """
    print(f"\n--- STEP 1: ACCESSING {BASE_URL} ---")
    all_links = []
    
    try:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Get current date
        now = datetime.datetime.now()
        cutoff_date = now - datetime.timedelta(days=DAYS_TO_SCRAPE)
        
        print(f"Looking for articles from {cutoff_date.strftime('%d-%m-%Y')} to {now.strftime('%d-%m-%Y')}")
        
        # Find all article titles
        articles = soup.find_all('h2', class_='entry-title')
        recent_articles = []
        
        for article_tag in articles[:MAX_ARTICLES]:
            title_link = article_tag.find('a')
            if not title_link:
                continue
                
            title = title_link.get_text(strip=True)
            url = title_link['href']
            
            # Extract date from title
            article_date = extract_date_from_title(title)
            
            if article_date and article_date >= cutoff_date:
                recent_articles.append({
                    'title': title,
                    'url': url,
                    'date': article_date
                })
                print(f"  ✓ Found: {title}")
        
        if not recent_articles:
            print("⚠ No recent articles found within date range")
            return []
        
        print(f"\nFound {len(recent_articles)} recent articles to scrape")
        
        # Scrape each article for playlist links
        for idx, article in enumerate(recent_articles, 1):
            print(f"\n--- Scraping Article {idx}/{len(recent_articles)} ---")
            print(f"Title: {article['title']}")
            print(f"URL: {article['url']}")
            
            try:
                r_art = requests.get(article['url'], headers=HEADERS, timeout=15)
                soup_art = BeautifulSoup(r_art.text, 'html.parser')
                
                # Find all links in the article
                article_links = []
                for a in soup_art.find_all('a', href=True):
                    href = a['href']
                    # Only target actual playlist links
                    if "get.php?username=" in href:
                        article_links.append(href)
                
                # Remove duplicates within this article
                unique_article_links = list(dict.fromkeys(article_links))
                all_links.extend(unique_article_links)
                
                print(f"  → Extracted {len(unique_article_links)} playlist links")
                
            except Exception as e:
                print(f"  ✗ Error scraping article: {e}")
                continue
        
        # Remove duplicates across all articles
        unique_all_links = list(dict.fromkeys(all_links))
        
        print(f"\n{'─' * 50}")
        print(f"Total unique playlist links scraped: {len(unique_all_links)}")
        print(f"{'─' * 50}")
        
        return unique_all_links

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
        
        # Track which domains we've successfully added from new scrape
        validated_domains = set()
        
        for idx, link in enumerate(new_potential_links, 1):
            if len(alive_list) >= MAX_PROFILES:
                print("\n⚠ REACHED LIMIT: Stopping additions at 100.")
                break
            
            domain = get_domain(link)
            
            # Skip if we already have a VALID playlist from this domain
            if domain in seen_domains:
                print(f"\n[New {idx}/{len(new_potential_links)}] Skipping (valid domain exists): {link[:50]}...")
                continue
            
            # Skip if we already validated and added this domain in THIS scrape session
            if domain in validated_domains:
                print(f"\n[New {idx}/{len(new_potential_links)}] Skipping (already added from this domain): {link[:50]}...")
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
                validated_domains.add(domain)  # Mark this domain as successfully added
            else:
                print(f"  ✗ REJECTED: {reason} - will test other links from this domain")

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
    
    summary_msg = f"Sync Complete. Valid Profiles: {len(alive_list)} | Removed: {removed_count} | Validated with M3U testing"
    log_to_file(summary_msg)
    
    print(f"\n{'═' * 50}")
    print(f"✓ FINISHED: {summary_msg}")
    print(f"{'═' * 50}\n")

if __name__ == "__main__":
    main()
