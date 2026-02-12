import requests
from bs4 import BeautifulSoup
import json
import argparse
import datetime
import re
import sys

def scrape_date(date_obj):
    """
    Scrape schedule for a specific date object. Returns list of events.
    """
    date_str = date_obj.strftime("%Y%m%d")
    formatted_date = date_obj.strftime("%Y-%m-%d")
    
    url = f"https://www.wheresthematch.com/live-sport-on-tv/?showdatestart={date_str}"
    print(f"  > Scraping {formatted_date}...", flush=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"    x Error fetching {formatted_date}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    
    events = []
    rows = soup.find_all('tr', attrs={'itemscope': True, 'itemtype': re.compile(r'schema\.org/BroadcastEvent')})
    
    for row in rows:
        try:
            event_data = {}

            # 1. Event Name
            name_elem = row.find(attrs={"itemprop": "name"})
            if name_elem:
                event_data['name'] = name_elem.get('content', '').strip()
            
            if not event_data.get('name'):
                 fixture_span = row.find('span', class_='fixture')
                 if fixture_span:
                     event_data['name'] = fixture_span.get_text(strip=True)
            
            # 2. Time
            start_date_elem = row.find(attrs={"itemprop": "startDate"})
            if start_date_elem:
                start_iso = start_date_elem.get('content', '')
                event_data['start_time_iso'] = start_iso
                try:
                    # Parse simplified ISO for display time
                    dt_obj = datetime.datetime.fromisoformat(start_iso.replace('Z', '+00:00'))
                    event_data['time'] = dt_obj.strftime("%H:%M")
                except ValueError:
                    event_data['time'] = start_iso
            else:
                time_span = row.find('span', class_='time')
                if time_span:
                    event_data['time'] = time_span.get_text(strip=True)

            # 3. Competition / Sport / Logo
            comp_td = row.find('td', class_='competition-name')
            if comp_td:
                # Competition Logo/Icon
                img = comp_td.find('img')
                if img:
                    # Prioritize data-src (lazy load) over src (placeholder)
                    event_data['competition_logo'] = img.get('data-src') or img.get('src')
                    
                    # Fix relative URLs if any (though usually absolute on this site, good practice)
                    if event_data['competition_logo'] and not event_data['competition_logo'].startswith('http'):
                         event_data['competition_logo'] = "https://www.wheresthematch.com" + event_data['competition_logo']

                    event_data['sport'] = img.get('alt', '').replace('Sport icon', '').strip()
                
                # Competition name
                comp_link = comp_td.find('a')
                if comp_link:
                    event_data['competition'] = comp_link.get_text(strip=True)
                else:
                    event_data['competition'] = comp_td.get_text(" ", strip=True)

            # 4. Channels
            raw_channels = []
            channel_td = row.find('td', class_='channel-details')
            if channel_td:
                channel_imgs = channel_td.find_all('img', class_='channel')
                for img in channel_imgs:
                    channel_name = img.get('title') or img.get('alt')
                    if channel_name:
                        channel_name = channel_name.replace(' logo', '').strip()
                        raw_channels.append(channel_name)
            
            # --- FILTERING LOGIC ---
            # Filter out junk channels
            junk_keywords = ["website", "youtube", "app"]
            # Regex for domains (e.g. channel.net, site.com, something.co.uk)
            domain_regex = re.compile(r'\b[\w-]+\.(com|net|org|co\.[a-z]{2}|io|tv|biz|info|me|eu|us)\b', re.IGNORECASE)
            
            filtered_channels = []
            
            for ch in raw_channels:
                is_junk = False
                ch_lower = ch.lower()
                
                # Scan for keywords
                for kw in junk_keywords:
                    if kw in ch_lower:
                        is_junk = True
                        break
                
                # Scan for domain patterns
                if not is_junk and domain_regex.search(ch):
                    is_junk = True
                
                if not is_junk:
                    filtered_channels.append(ch)

            event_data['channels'] = filtered_channels
            
            # Only add event if it has valid channels (and a name)
            if event_data.get('name') and event_data.get('channels'):
                events.append(event_data)

        except Exception as e:
            continue

    print(f"    v Found {len(events)} events (after filtering).")
    return events

def scrape_week():
    """Scrape the entire current week (Monday to Sunday)."""
    today = datetime.date.today()
    # Find start of current week (Monday)
    start_of_week = today - datetime.timedelta(days=today.weekday())
    
    weekly_schedule = []
    
    print(f"Scraping weekly schedule starting Monday {start_of_week}...")
    
    for i in range(7):
        current_date = start_of_week + datetime.timedelta(days=i)
        day_events = scrape_date(current_date)
        
        weekly_schedule.append({
            "date": current_date.strftime("%Y-%m-%d"),
            "day": current_date.strftime("%A"),
            "events": day_events
        })
    
    # Output file
    output_file = f"weekly_schedule.json"
    
    result = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "wheresthematch.com",
        "schedule": weekly_schedule
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\nSuccessfully saved weekly schedule to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Scrape sports schedule.")
    parser.add_argument("--date", type=str, help="Single date (YYYYMMDD). If omitted, scrapes full week.")
    args = parser.parse_args()
    
    if args.date:
        # Single date mode
        try:
            dt = datetime.datetime.strptime(args.date, "%Y%m%d")
            events = scrape_date(dt)
            output_file = f"schedule_{dt.strftime('%Y-%m-%d')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({"date": args.date, "events": events}, f, indent=2)
            print(f"Saved {output_file}")
        except ValueError:
            print("Invalid date format.")
    else:
        # Weekly mode
        scrape_week()

if __name__ == "__main__":
    main()
