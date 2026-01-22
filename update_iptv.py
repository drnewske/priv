import json
import requests
import datetime
import sys
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# --- CONFIGURATION ---
JSON_FILE = "lovestory.json"
LOG_FILE = "log.txt"
ICON_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"
BASE_URL = "https://ninoiptv.com/"
MAX_PROFILES = 100

# Mimic a real user browser to prevent 403 Forbidden errors
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

def test_link(url):
    """
    Tests the URL and prints the exact status to terminal.
    Returns (True/False, reason)
    """
    try:
        # Use HEAD to save bandwidth and time
        response = requests.head(url, headers=HEADERS, timeout=8, allow_redirects=True)
        status = response.status_code
        if status < 400:
            print(f"tested link {url} result OK ({status})")
            return True, "alive"
        else:
            print(f"tested link {url} result DEAD ({status})")
            return False, f"status {status}"
    except requests.exceptions.Timeout:
        print(f"tested link {url} result TIMEOUT")
        return False, "timeout"
    except Exception as e:
        print(f"tested link {url} result ERROR ({str(e)[:20]})")
        return False, "error"

def get_domain(url):
    return urlparse(url).netloc

def scrape_nino():
    """Finds the latest article and extracts all unique links."""
    print(f"\n--- STEP 1: ACCESSING {BASE_URL} ---")
    try:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find the very first article link
        latest_article_tag = soup.find('h2', class_='entry-title')
        if not latest_article_tag:
            print("FAILED: Could not find any articles on homepage.")
            return []
        
        article_url = latest_article_tag.find('a')['href']
        print(f"Latest article identified: {article_url}")
        
        # Scrape the article content
        r_art = requests.get(article_url, headers=HEADERS, timeout=15)
        soup_art = BeautifulSoup(r_art.text, 'html.parser')
        
        found_links = []
        for a in soup_art.find_all('a', href=True):
            href = a['href']
            # Only target actual playlist links
            if "get.php?username=" in href:
                found_links.append(href)
        
        # Clean the list (unique URLs only)
        unique_scraped = list(dict.fromkeys(found_links))
        print(f"scraped list all links scraped: {unique_scraped}")
        return unique_scraped

    except Exception as e:
        print(f"CRITICAL ERROR during scrape: {e}")
        return []

def main():
    print("O.R CONTENT MANAGER STARTING...")
    
    # Load JSON data
    try:
        with open(JSON_FILE, 'r') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"featured_content": []}
    
    original_list = data.get("featured_content", [])
    alive_list = []
    seen_domains = set()

    print(f"\n--- STEP 2: CLEANING EXISTING {len(original_list)} PROFILES ---")
    for item in original_list:
        url = item.get("url")
        domain = get_domain(url)
        
        is_alive, reason = test_link(url)
        
        if is_alive and domain not in seen_domains:
            alive_list.append(item)
            seen_domains.add(domain)
        else:
            # Explicit removal logging for the terminal
            print(f"cleaned up {url} reason {reason if not is_alive else 'duplicate domain'}")

    # Calculate remaining slots
    slots_available = MAX_PROFILES - len(alive_list)
    print(f"Current valid profiles: {len(alive_list)}. Slots available: {slots_available}")

    # Scrape for new content if we have room
    if slots_available > 0:
        print(f"\n--- STEP 3: SEARCHING FOR NEW CONTENT ---")
        new_potential_links = scrape_nino()
        
        for link in new_potential_links:
            if len(alive_list) >= MAX_PROFILES:
                print("REACHED LIMIT: Stopping additions at 100.")
                break
            
            domain = get_domain(link)
            if domain not in seen_domains:
                is_alive, reason = test_link(link)
                if is_alive:
                    print(f"appending {link}")
                    alive_list.append({
                        "name": "", 
                        "logo_url": ICON_URL, 
                        "type": "m3u",
                        "url": link, 
                        "server_url": None, 
                        "id": ""
                    })
                    seen_domains.add(domain)
            else:
                print(f"skipping {link} result (domain already exists in list)")

    print("\n--- STEP 4: RE-INDEXING NAMES AND IDs ---")
    # This loop ensures strict O.R numbering and icon forcing
    for i, item in enumerate(alive_list, start=1):
        num_name = str(i).zfill(3) # 001, 002...
        num_id = str(i).zfill(2)   # 01, 02...
        
        item["name"] = f"O.R {num_name}"
        item["id"] = f"featured_m3u_{num_id}"
        item["logo_url"] = ICON_URL
    
    # Save files
    data["featured_content"] = alive_list
    with open(JSON_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    summary_msg = f"Sync Complete. Profiles: {len(alive_list)} | Removed: {len(original_list) - len(alive_list) if len(original_list) > 0 else 0}"
    log_to_file(summary_msg)
    print(f"\nFINISHED: {summary_msg}")

if __name__ == "__main__":
    main()
