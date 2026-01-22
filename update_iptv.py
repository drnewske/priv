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
MIN_PLAYLIST_SIZE = 100
PLAYLIST_TIMEOUT = 12
DAYS_TO_SCRAPE = 7
MAX_ARTICLES = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive"
}

# Game of Thrones themed slot names (100 total - COMPLETE THIS LIST)
GOT_NAMES = [
  "Westeros",
  "Essos",
  "Beyond the Wall",
  "Winterfell",
  "King's Landing",
  "The Wall",
  "Castle Black",
  "Dragonstone",
  "Highgarden",
  "Casterly Rock",
  "The Eyrie",
  "Dorne",
  "Braavos",
  "Meereen",
  "Valyria",
  "White Harbor",
  "The Twins",
  "Riverrun",
  "Harrenhal",
  "Storm's End",
  "Oldtown",
  "The Citadel",
  "The Iron Islands",
  "Pyke",
  "The Reach",
  "The North",
  "The Iron Throne",
  "The Red Keep",
  "The Great Sept of Baelor",
  "Night's Watch",
  "Kingsguard",
  "The Free Cities",
  "The Narrow Sea",
  "Skagos",
  "Bear Island",
  "Sunspear",
  "Astapor",
  "Yunkai",
  "Qarth",
  "Volantis",
  "Slaver's Bay",
  "The Dothraki Sea",
  "The Trident",
  "Godswood",
  "Heart Tree",
  "Weirwood Trees",
  "The Old Gods",
  "The Seven",
  "The Lord of Light",
  "White Walkers",
  "The Long Night",
  "Dragonglass",
  "Valyrian Steel",
  "Wildlings",
  "Giants",
  "Children of the Forest",
  "Faceless Men",
  "The Unsullied",
  "The Dothraki",
  "Dragons",
  "Drogon",
  "Rhaegal",
  "Viserion",
  "Ghost",
  "Nymeria",
  "The Three-Eyed Raven",
  "The Golden Company",
  "The Red Wedding",
  "The Purple Wedding",
  "Blackwater Bay",
  "Hardhome",
  "The Moon Door",
  "The Faith Militant",
  "The High Sparrow",
  "A Song of Ice and Fire",
  "The Targaryen Sigil",
  "The Stark Direwolf",
  "The Lannister Lion",
  "The Crownlands",
  "The Riverlands",
  "The Vale of Arryn",

  "ADELE",
  "Janabi",
  "Tinga"
]


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
        if line and not line.startswith('#'):
            if line.startswith('http://') or line.startswith('https://'):
                streams.append(line)
    
    return streams

def validate_playlist(url):
    """
    Validates playlist and returns detailed info.
    Returns (is_valid, reason, stream_count)
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=PLAYLIST_TIMEOUT, allow_redirects=True)
        
        if response.status_code >= 400:
            return False, f"HTTP {response.status_code}", 0
        
        content_length = len(response.content)
        if content_length < MIN_PLAYLIST_SIZE:
            return False, "empty/invalid", 0
        
        content = response.text
        
        has_m3u_header = '#EXTM3U' in content
        has_extinf = '#EXTINF' in content
        
        if not (has_m3u_header or has_extinf):
            return False, "not M3U format", 0
        
        streams = parse_m3u_streams(content)
        stream_count = len(streams)
        
        if stream_count == 0:
            return False, "no streams", 0
        
        return True, f"valid ({stream_count} streams)", stream_count
        
    except requests.exceptions.Timeout:
        return False, "timeout", 0
    except Exception as e:
        return False, "error", 0

def get_domain(url):
    """Extract clean domain name from URL."""
    domain = urlparse(url).netloc
    # Remove www. and common TLDs for cleaner names
    domain = domain.replace('www.', '')
    return domain

def extract_date_from_title(title):
    """Extract date from article titles."""
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
    """Enhanced scraper for recent articles."""
    print(f"\n--- STEP 1: ACCESSING {BASE_URL} ---")
    all_links = []
    
    try:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        now = datetime.datetime.now()
        cutoff_date = now - datetime.timedelta(days=DAYS_TO_SCRAPE)
        
        print(f"Looking for articles from {cutoff_date.strftime('%d-%m-%Y')} to {now.strftime('%d-%m-%Y')}")
        
        articles = soup.find_all('h2', class_='entry-title')
        recent_articles = []
        
        for article_tag in articles[:MAX_ARTICLES]:
            title_link = article_tag.find('a')
            if not title_link:
                continue
                
            title = title_link.get_text(strip=True)
            url = title_link['href']
            
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
        
        for idx, article in enumerate(recent_articles, 1):
            print(f"\n--- Scraping Article {idx}/{len(recent_articles)} ---")
            print(f"Title: {article['title'][:60]}...")
            
            try:
                r_art = requests.get(article['url'], headers=HEADERS, timeout=15)
                soup_art = BeautifulSoup(r_art.text, 'html.parser')
                
                article_links = []
                for a in soup_art.find_all('a', href=True):
                    href = a['href']
                    if "get.php?username=" in href:
                        article_links.append(href)
                
                unique_article_links = list(dict.fromkeys(article_links))
                all_links.extend(unique_article_links)
                
                print(f"  → Extracted {len(unique_article_links)} playlist links")
                
            except Exception as e:
                print(f"  ✗ Error scraping article: {e}")
                continue
        
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
    print("   O.R CONTENT MANAGER - GAME OF THRONES EDITION")
    print("═══════════════════════════════════════════════════\n")
    
    # Load existing data
    try:
        with open(JSON_FILE, 'r') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"featured_content": []}
    
    existing_slots = data.get("featured_content", [])
    
    # Create slot mapping (preserve existing slot assignments)
    slot_registry = {}  # slot_id -> slot_data
    
    # Load existing slot assignments
    for item in existing_slots:
        slot_id = item.get("slot_id")
        if slot_id is not None and slot_id < len(GOT_NAMES):
            slot_registry[slot_id] = item
    
    print(f"\n--- STEP 2: VALIDATING EXISTING {len(existing_slots)} SLOTS ---")
    
    timestamp_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    validated_domains = set()
    
    # Validate existing slots
    for slot_id, item in list(slot_registry.items()):
        url = item.get("url")
        old_domain = item.get("domain", get_domain(url))
        got_name = GOT_NAMES[slot_id]
        
        print(f"\n[Slot {slot_id + 1:03d}] {got_name}")
        print(f"  Current: {old_domain}")
        print(f"  → Testing playlist...")
        
        is_valid, reason, stream_count = validate_playlist(url)
        
        if is_valid:
            # Update slot with current info
            item["channel_count"] = stream_count
            item["domain"] = old_domain
            item["last_validated"] = timestamp_now
            validated_domains.add(old_domain)
            print(f"  ✓ ALIVE ({stream_count} channels)")
        else:
            print(f"  ✗ DEAD ({reason}) - slot will be reassigned")
            del slot_registry[slot_id]
    
    # Find empty slots
    empty_slots = [i for i in range(MAX_PROFILES) if i not in slot_registry]
    
    print(f"\n{'─' * 50}")
    print(f"Active slots: {len(slot_registry)}")
    print(f"Empty slots: {len(empty_slots)}")
    print(f"{'─' * 50}")
    
    # Scrape for new content to fill empty slots
    if empty_slots:
        print(f"\n--- STEP 3: FILLING {len(empty_slots)} EMPTY SLOTS ---")
        new_potential_links = scrape_nino()
        
        newly_validated_domains = set()
        
        for idx, link in enumerate(new_potential_links, 1):
            if not empty_slots:
                print("\n✓ All slots filled!")
                break
            
            domain = get_domain(link)
            
            # Skip if we already have this domain
            if domain in validated_domains or domain in newly_validated_domains:
                print(f"\n[Link {idx}/{len(new_potential_links)}] Skipping {domain[:40]} (already in use)")
                continue
            
            print(f"\n[Link {idx}/{len(new_potential_links)}] Testing {domain[:40]}...")
            is_valid, reason, stream_count = validate_playlist(link)
            
            if is_valid:
                # Assign to next available slot
                slot_id = empty_slots.pop(0)
                got_name = GOT_NAMES[slot_id]
                
                # Check if this slot had a previous assignment
                old_slot_data = existing_slots[slot_id] if slot_id < len(existing_slots) else None
                old_domain = old_slot_data.get("domain", "None") if old_slot_data else "None"
                
                print(f"  ✓ ASSIGNED to Slot {slot_id + 1:03d} ({got_name})")
                print(f"    {stream_count} channels from {domain}")
                
                # Create change log entry
                change_log = f"Changed from {old_domain} to {domain}" if old_domain != "None" else f"Initial assignment: {domain}"
                
                slot_registry[slot_id] = {
                    "slot_id": slot_id,
                    "name": got_name,
                    "domain": domain,
                    "channel_count": stream_count,
                    "logo_url": ICON_URL,
                    "type": "m3u",
                    "url": link,
                    "server_url": None,
                    "id": f"got_slot_{str(slot_id + 1).zfill(2)}",
                    "last_changed": timestamp_now,
                    "last_validated": timestamp_now,
                    "change_log": change_log
                }
                
                validated_domains.add(domain)
                newly_validated_domains.add(domain)
            else:
                print(f"  ✗ REJECTED: {reason}")
    
    # Build final sorted list
    final_list = []
    for slot_id in range(MAX_PROFILES):
        if slot_id in slot_registry:
            final_list.append(slot_registry[slot_id])
    
    # Save
    data["featured_content"] = final_list
    with open(JSON_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Summary
    total_channels = sum(item.get("channel_count", 0) for item in final_list)
    summary_msg = f"Sync Complete. Active Slots: {len(final_list)}/{MAX_PROFILES} | Total Channels: {total_channels}"
    log_to_file(summary_msg)
    
    print(f"\n{'═' * 50}")
    print(f"✓ FINISHED: {summary_msg}")
    print(f"{'═' * 50}\n")

if __name__ == "__main__":
    main()
