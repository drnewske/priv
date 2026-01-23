import json
import requests
import datetime
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# --- CONFIGURATION ---
JSON_FILE = "lovestory.json"
LOG_FILE = "log.txt"
FALLBACK_ICON = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"
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

# Game of Thrones themed slot names with custom logo URLs
# Format: {"name": "Slot Name", "logo": "URL or None"}
GOT_SLOTS = [
    {"name": "Westeros", "logo": "https://static.digitecgalaxus.ch/im/Files/2/1/1/3/9/7/9/6/game_of_thrones_intro_map_westeros_elastic21.jpeg?impolicy=teaser&resizeWidth=1000&resizeHeight=500"},
    {"name": "Essos", "logo": "https://imgix.bustle.com/uploads/image/2017/7/12/4e391a2f-8663-4cdd-91eb-9102c5f731d7-52be1751932bb099d5d5650593df5807b50fc3fbbee7da6a556bd5d1d339f39a.jpg?w=800&h=532&fit=crop&crop=faces"},
    {"name": "Beyond the Wall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/27/706_Tormund_Beric_Sandor_Jon_Jorah_Gendry.jpg/revision/latest/scale-to-width-down/1000?cb=20170821092659"},
    {"name": "Winterfell", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/08/1x01_Winterfell.jpg/revision/latest?cb=20170813191451"},
    {"name": "King's Landing", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/83/King%27s_Landing_HotD.png/revision/latest/scale-to-width-down/1000?cb=20220805155800"},
    {"name": "The Wall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f5/The_Wall.jpg/revision/latest/scale-to-width-down/1000?cb=20150323200738"},
    {"name": "Castle Black", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7b/Castle_Black.jpg/revision/latest/scale-to-width-down/1000?cb=20110920111941"},
    {"name": "Dragonstone", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a4/Dragonstone-season7-low.png/revision/latest/scale-to-width-down/1000?cb=20170717082952"},
    {"name": "Highgarden", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a2/704_Highgarden.png/revision/latest?cb=20170807030944"},
    {"name": "Casterly Rock", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a8/Casterly-rock.png/revision/latest/scale-to-width-down/1000?cb=20170731025431"},
    {"name": "The Eyrie", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/59/The_Eyrie.jpg/revision/latest?cb=20110615190250"},
    {"name": "Dorne", "logo": None},
    {"name": "Braavos", "logo": None},
    {"name": "Meereen", "logo": None},
    {"name": "Valyria", "logo": None},
    {"name": "White Harbor", "logo": None},
    {"name": "The Twins", "logo": None},
    {"name": "Riverrun", "logo": None},
    {"name": "Harrenhal", "logo": None},
    {"name": "Storm's End", "logo": None},
    {"name": "Oldtown", "logo": None},
    {"name": "The Citadel", "logo": None},
    {"name": "The Iron Islands", "logo": None},
    {"name": "Pyke", "logo": None},
    {"name": "The Reach", "logo": None},
    {"name": "The North", "logo": None},
    {"name": "The Iron Throne", "logo": None},
    {"name": "The Red Keep", "logo": None},
    {"name": "The Great Sept of Baelor", "logo": None},
    {"name": "Night's Watch", "logo": None},
    {"name": "Kingsguard", "logo": None},
    {"name": "The Free Cities", "logo": None},
    {"name": "The Narrow Sea", "logo": None},
    {"name": "Caraxes", "logo": "https://staticg.sportskeeda.com/editor/2022/12/03519-16722910130144-1920.jpg"},
    {"name": "Bear Island", "logo": None},
    {"name": "Sunspear", "logo": None},
    {"name": "Astapor", "logo": None},
    {"name": "Balerion The Black Dread", "logo": "https://staticg.sportskeeda.com/editor/2022/12/62cdf-16722910130058-1920.jpg"},
    {"name": "Qarth", "logo": None},
    {"name": "Daenerys", "logo": "https://www.thepopverse.com/_next/image?url=https%3A%2F%2Fmedia.thepopverse.com%2Fmedia%2Femilia-clarke-game-of-thrones-dothraki-language-i2yd4ljq4zpfvikmv0onmgmvy2.png&w=1280&q=75"},
    {"name": "Slaver's Bay", "logo": None},
    {"name": "The Dothraki Sea", "logo": None},
    {"name": "The Trident", "logo": None},
    {"name": "Godswood", "logo": None},
    {"name": "Heart Tree", "logo": None},
    {"name": "Weirwood Trees", "logo": None},
    {"name": "The Old Gods", "logo": None},
    {"name": "The Seven", "logo": None},
    {"name": "The Lord of Light", "logo": None},
    {"name": "White Walkers", "logo": None},
    {"name": "The Long Night", "logo": None},
    {"name": "Dragonglass", "logo": None},
    {"name": "Valyrian Steel", "logo": None},
    {"name": "Wildlings", "logo": None},
    {"name": "Syrax", "logo": "https://staticg.sportskeeda.com/editor/2022/12/934cb-16722910129818-1920.jpg"},
    {"name": "Children of the Forest", "logo": None},
    {"name": "Faceless Men", "logo": None},
    {"name": "The Unsullied", "logo": None},
    {"name": "The Dothraki", "logo": None},
    {"name": "Dragons", "logo": None},
    {"name": "Drogon", "logo": "https://staticg.sportskeeda.com/editor/2022/12/9eb54-16722910132128-1920.jpg"},
    {"name": "Rhaegal", "logo": "https://staticg.sportskeeda.com/editor/2022/12/ef298-16722910129506-1920.jpg"},
    {"name": "Viserion", "logo": "https://staticg.sportskeeda.com/editor/2022/12/2d488-16722910129839-1920.jpg"},
    {"name": "Ghost", "logo": "https://i.cbc.ca/ais/1.3078255,1431974097000/full/max/0/default.jpg?im=Crop%2Crect%3D%280%2C91%2C400%2C225%29%3BResize%3D860"},
    {"name": "Nymeria", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d3/Nymeria_bites_Joffrey.png/revision/latest/scale-to-width-down/1000?cb=20150404115158"},
    {"name": "The Three-Eyed Raven", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3a/Three-eyed_raven.png/revision/latest?cb=20110622101243"},
    {"name": "The Golden Company", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f4/Golden_Company_S8.jpg/revision/latest/scale-to-width-down/1000?cb=20190408221754"},
    {"name": "The Red Wedding", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a2/Robb_Wind_MHYSA_new_lightened.jpg/revision/latest/scale-to-width-down/1000?cb=20160830004546"},
    {"name": "The Purple Wedding", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4d/Purple_Wedding.png/revision/latest/scale-to-width-down/1000?cb=20150210223603"},
    {"name": "Blackwater Bay", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/41/Wildfire_explosion.jpg/revision/latest/scale-to-width-down/1000?cb=20150328212702"},
    {"name": "Hardhome", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b0/Hardhome_%28episode%29_.jpg/revision/latest/scale-to-width-down/1000?cb=20150601113829"},
    {"name": "The Moon Door", "logo": "https://static.independent.co.uk/s3fs-public/thumbnails/image/2016/04/29/14/gameofthrones-moon-door.jpg?quality=75&width=1368&crop=3%3A2%2Csmart&auto=webp"},
    {"name": "The Faith Militant", "logo": None},
    {"name": "The High Sparrow", "logo": None},
    {"name": "A Song of Ice and Fire", "logo": None},
    {"name": "The Targaryen Sigil", "logo": None},
    {"name": "The Stark Direwolf", "logo": None},
    {"name": "The Lannister Lion", "logo": None},
    {"name": "The Crownlands", "logo": None},
    {"name": "The Riverlands", "logo": None},
    {"name": "The Vale of Arryn", "logo": None},
    {"name": "ADELE", "logo": "https://charts-static.billboard.com/img/2008/02/adele-9x5-344x344.jpg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Tinga", "logo": "https://images.theconversation.com/files/698357/original/file-20251024-66-5tljkb.jpg?ixlib=rb-4.1.0&rect=0%2C340%2C4096%2C2048&q=45&auto=format&w=668&h=334&fit=crop"}
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

def test_logo_url(logo_url):
    """Test if logo URL is reachable. Returns True if accessible."""
    if not logo_url:
        return False
    try:
        response = requests.head(logo_url, headers=HEADERS, timeout=5, allow_redirects=True)
        return response.status_code < 400
    except:
        return False

def get_logo_for_slot(slot_id):
    """Get logo URL for slot, with fallback to default icon."""
    slot_data = GOT_SLOTS[slot_id]
    logo_url = slot_data.get("logo")
    
    # If no custom logo or unreachable, use fallback
    if not logo_url or not test_logo_url(logo_url):
        return FALLBACK_ICON
    
    return logo_url

def get_domain(url):
    """Extract clean domain name from URL."""
    domain = urlparse(url).netloc
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
        if slot_id is not None and slot_id < len(GOT_SLOTS):
            slot_registry[slot_id] = item
    
    print(f"\n--- STEP 2: VALIDATING EXISTING {len(existing_slots)} SLOTS ---")
    
    timestamp_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    validated_domains = set()
    
    # Validate existing slots
    for slot_id, item in list(slot_registry.items()):
        url = item.get("url")
        old_domain = item.get("domain", get_domain(url))
        got_name = GOT_SLOTS[slot_id]["name"]
        
        print(f"\n[Slot {slot_id + 1:03d}] {got_name}")
        print(f"  Current: {old_domain}")
        print(f"  → Testing playlist...")
        
        is_valid, reason, stream_count = validate_playlist(url)
        
        if is_valid:
            # Update slot with current info
            item["slot_id"] = slot_id
            item["name"] = got_name
            item["channel_count"] = stream_count
            item["domain"] = old_domain
            item["logo_url"] = get_logo_for_slot(slot_id)
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
                got_name = GOT_SLOTS[slot_id]["name"]
                
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
                    "logo_url": get_logo_for_slot(slot_id),
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
