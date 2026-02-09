import requests
from bs4 import BeautifulSoup
import json
import argparse
import datetime
import re
import sys

def scrape_schedule(date_str=None):
    """
    Scrape schedule for a specific date (YYYYMMDD) or today if None.
    """
    if not date_str:
        today = datetime.date.today()
        date_str = today.strftime("%Y%m%d")
        formatted_date = today.strftime("%Y-%m-%d")
    else:
        try:
            dt = datetime.datetime.strptime(date_str, "%Y%m%d")
            formatted_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            print(f"Error: Invalid date format '{date_str}'. Use YYYYMMDD.")
            sys.exit(1)

    url = f"https://www.wheresthematch.com/live-sport-on-tv/?showdatestart={date_str}"
    print(f"Scraping schedule for {formatted_date} from {url}...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    
    events = []
    
    # Locate rows with schema.org BroadcastEvent or similar structure
    # Based on user snippet, rows are <tr> with itemscope itemtype="...BroadcastEvent"
    rows = soup.find_all('tr', attrs={'itemscope': True, 'itemtype': re.compile(r'schema\.org/BroadcastEvent')})
    
    print(f"Found {len(rows)} events.")

    for row in rows:
        try:
            event_data = {}

            # 1. Event Name
            # Try itemprop="name" first, then fixture details
            name_elem = row.find(attrs={"itemprop": "name"})
            if name_elem:
                event_data['name'] = name_elem.get('content', '').strip()
            
            if not event_data.get('name'):
                 # Fallback: Extract from .fixture-details text
                 fixture_span = row.find('span', class_='fixture')
                 if fixture_span:
                     event_data['name'] = fixture_span.get_text(strip=True)
            
            # 2. Time
            start_date_elem = row.find(attrs={"itemprop": "startDate"})
            if start_date_elem:
                start_iso = start_date_elem.get('content', '')
                # Extract time part (e.g., 2026-02-09T05:30:00Z -> 05:30)
                # Keep original ISO for machine readability
                event_data['start_time_iso'] = start_iso
                try:
                    dt_obj = datetime.datetime.fromisoformat(start_iso.replace('Z', '+00:00'))
                    event_data['time'] = dt_obj.strftime("%H:%M")
                except ValueError:
                    event_data['time'] = start_iso
            else:
                # Fallback to .time class
                time_span = row.find('span', class_='time')
                if time_span:
                    event_data['time'] = time_span.get_text(strip=True)

            # 3. Competition / Sport
            comp_td = row.find('td', class_='competition-name')
            if comp_td:
                # Try image alt for sport
                img = comp_td.find('img')
                if img:
                    event_data['sport'] = img.get('alt', '').replace('Sport icon', '').strip()
                
                # Competition name
                comp_link = comp_td.find('a')
                if comp_link:
                    event_data['competition'] = comp_link.get_text(strip=True)
                else:
                    # check for span text if no link
                    event_data['competition'] = comp_td.get_text(" ", strip=True)

            # 4. Channels
            channels = []
            channel_td = row.find('td', class_='channel-details')
            if channel_td:
                # Channels are usually images
                channel_imgs = channel_td.find_all('img', class_='channel')
                for img in channel_imgs:
                    channel_name = img.get('title') or img.get('alt')
                    if channel_name:
                        # Clean up " logo" suffix if present
                        channel_name = channel_name.replace(' logo', '').strip()
                        channels.append(channel_name)
            
            event_data['channels'] = channels
            
            # Add to list if we have at least a name and time
            if event_data.get('name'):
                events.append(event_data)

        except Exception as e:
            print(f"Error parsing row: {e}")
            continue

    # Output file
    output_file = f"schedule_{formatted_date}.json"
    
    result = {
        "date": formatted_date,
        "source": "wheresthematch.com",
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "events": events
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"Successfully saved {len(events)} events to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Scrape sports schedule from wheresthematch.com")
    parser.add_argument("--date", type=str, help="Date to scrape in YYYYMMDD format (e.g., 20260210)")
    args = parser.parse_args()
    
    scrape_schedule(args.date)

if __name__ == "__main__":
    main()
