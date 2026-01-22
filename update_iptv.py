import json
import requests
import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# --- SETTINGS ---
JSON_FILE = "lovestory.json"
LOG_FILE = "log.txt"
ICON_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"
BASE_URL = "https://ninoiptv.com/"
MAX_PROFILES = 100

# High-quality 2026 Browser Headers to avoid bot detection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive"
}

def log_event(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {message}\n")

def is_link_alive(url):
    try:
        response = requests.head(url, headers=HEADERS, timeout=7, allow_redirects=True)
        return response.status_code < 400
    except:
        return False

def get_domain(url):
    return urlparse(url).netloc

def scrape_latest_nino():
    links = []
    try:
        # 1. Quick visit to home
        r = requests.get(BASE_URL, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        latest_article = soup.find('article')
        if not latest_article: return []
        
        target_url = latest_article.find('a')['href']
        
        # 2. Enter latest article and scrape
        r_art = requests.get(target_url, headers=HEADERS, timeout=10)
        art_soup = BeautifulSoup(r_art.text, 'html.parser')
        for a in art_soup.find_all('a', href=True):
            href = a['href']
            if "get.php?username=" in href:
                links.append(href)
    except Exception as e:
        log_event(f"Scrape Error: {e}")
    return list(set(links))

def main():
    log_event("Starting Sync Process...")
    
    # 1. Load JSON
    try:
        with open(JSON_FILE, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"featured_content": []}

    current_list = data.get("featured_content", [])
    
    # 2. Cleanup Dead Links
    initial_count = len(current_list)
    alive_list = []
    seen_domains = set()
    
    for item in current_list:
        url = item.get("url")
        domain = get_domain(url)
        if domain not in seen_domains and is_link_alive(url):
            alive_list.append(item)
            seen_domains.add(domain)
    
    dead_removed = initial_count - len(alive_list)
    log_event(f"Cleanup: Removed {dead_removed} dead/duplicate links.")

    # 3. Scrape and Add New
    if len(alive_list) < MAX_PROFILES:
        scraped = scrape_latest_nino()
        added_count = 0
        for link in scraped:
            if len(alive_list) >= MAX_PROFILES: break
            
            domain = get_domain(link)
            if domain not in seen_domains:
                if is_link_alive(link):
                    alive_list.append({
                        "name": "", "logo_url": ICON_URL, "type": "m3u",
                        "url": link, "server_url": None, "id": ""
                    })
                    seen_domains.add(domain)
                    added_count += 1
        log_event(f"Scraping: Added {added_count} new unique links.")

    # 4. Strict Sequential Re-indexing (The O.R numbering)
    for i, item in enumerate(alive_list, start=1):
        num_str = str(i).zfill(3)
        item["name"] = f"O.R {num_str}"
        item["id"] = f"featured_m3u_{str(i).zfill(2)}"
        item["logo_url"] = ICON_URL

    # 5. Save and Close
    data["featured_content"] = alive_list
    with open(JSON_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    log_event(f"Success: Final profile count is {len(alive_list)}.")

if __name__ == "__main__":
    main()
