import requests
import json
from datetime import datetime, time, timezone, timedelta
import os
import urllib3

# Suppress only the InsecureRequestWarning from urllib3 needed for verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
BASE_URL = "https://h5.aoneroom.com"
M3U_FILENAME = "hththbddnmndvhjhhjhhjek.m3u"
JSON_FILENAME = "lovestory.json"
LOG_FILENAME = "update_log.txt"
STATE_FILENAME = "match_state.json"
M3U_URL_BASE = "priv-bc7.pages.dev"
LOGO_URL = "https://i.dailymail.co.uk/1s/2025/08/30/17/101692313-0-image-a-97_1756570796730.jpg"

# --- HEADERS ---
# These headers mimic a real browser request to bypass server-side checks.
# They were captured from a successful request in the browser's developer tools.
HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'origin': 'https://aisports.cc',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://aisports.cc/',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Brave";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'sec-gpc': '1',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
}


# --- HELPER FUNCTIONS ---

def get_utc_timestamps_for_day(target_date):
    """
    Calculates the startTime and endTime for the API call, defining a "day"
    as the 24-hour period from 21:00 UTC on the previous day.
    """
    previous_day = target_date - timedelta(days=1)
    
    # Start time is 21:00:00 UTC on the day before the target date.
    start_dt = datetime.combine(previous_day, time(21, 0), tzinfo=timezone.utc)
    
    # End time is 20:59:59.999 UTC on the target date.
    end_dt = datetime.combine(target_date, time(20, 59, 59, 999999), tzinfo=timezone.utc)
    
    start_timestamp_ms = int(start_dt.timestamp() * 1000)
    end_timestamp_ms = int(end_dt.timestamp() * 1000)
    
    return start_timestamp_ms, end_timestamp_ms

def get_api_data(url, params):
    """Generic function to fetch data from the API using the correct headers."""
    try:
        response = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        log_update(f"API request failed for {url} with params {params}: {err}")
        return None

def get_match_list_for_day(sport_type="football", target_date=None):
    """Fetches a list of all matches for a specific day and sport using the correct timestamp logic."""
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()
        
    start_timestamp, end_timestamp = get_utc_timestamps_for_day(target_date)

    api_url = f"{BASE_URL}/wefeed-h5-bff/live/match-list-v3"
    params = {
        'status': 0,
        'matchType': sport_type,
        'startTime': start_timestamp,
        'endTime': end_timestamp
    }
    
    data = get_api_data(api_url, params)
    if data and data.get('code') == 0:
        match_groups = data.get('data', {}).get('list', [])
        all_matches = []
        if match_groups:
            for group in match_groups:
                if group and 'matchList' in group:
                    all_matches.extend(group['matchList'])
        return all_matches
    return []

def get_match_details(match_id):
    """Fetches full details for a single match ID."""
    api_url = f"{BASE_URL}/wefeed-h5-bff/live/match-detail"
    params = {'id': match_id}
    data = get_api_data(api_url, params)
    return data.get('data') if data and data.get('code') == 0 else None

def log_update(message):
    """Appends a message to the log file with a timestamp."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    with open(LOG_FILENAME, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

def load_state():
    """Loads the previous state of matches from a JSON file."""
    if os.path.exists(STATE_FILENAME):
        with open(STATE_FILENAME, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def save_state(state):
    """Saves the current state of matches to a JSON file."""
    with open(STATE_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)
        
def normalize_url_path(url):
    """Strips protocol and ensures consistent path for comparison."""
    if not url:
        return ""
    return url.replace("https://", "").replace("http://", "")

def update_lovestory_json():
    """Updates the lovestory.json file, ensuring only one entry exists."""
    today_str = datetime.now(timezone.utc).strftime('%d.%m.%Y')
    target_path = f"{M3U_URL_BASE}/{M3U_FILENAME}"
    target_url_with_protocol = f"https://{target_path}"

    try:
        with open(JSON_FILENAME, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"featured_content": []}

    featured_content = data.get("featured_content", [])
    entry_index = -1
    
    for i, item in enumerate(featured_content):
        if normalize_url_path(item.get('url', '')) == target_path:
            entry_index = i
            break
            
    if entry_index != -1:
        entry = featured_content[entry_index]
        if entry.get('name') != today_str or entry.get('url') != target_url_with_protocol:
            log_update(f"Updating existing entry in {JSON_FILENAME}. Name: -> {today_str}, URL -> {target_url_with_protocol}")
            entry['name'] = today_str
            entry['url'] = target_url_with_protocol
    else:
        log_update(f"Adding new M3U entry to {JSON_FILENAME} for the first time.")
        new_entry = {
            "name": today_str,
            "logo_url": LOGO_URL,
            "type": "m3u",
            "url": target_url_with_protocol,
            "server_url": None
        }
        featured_content.insert(0, new_entry)
        
    final_content = []
    found_once = False
    for item in featured_content:
        if normalize_url_path(item.get('url', '')) == target_path:
            if not found_once:
                final_content.append(item)
                found_once = True
        else:
            final_content.append(item)

    for i, item in enumerate(final_content):
        item['id'] = f"featured_m3u_{i+1:02}"
            
    data['featured_content'] = final_content
    with open(JSON_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

# --- MAIN WORKFLOW ---
def main():
    """Main function to run the ETL process."""
    log_update("Workflow started.")
    
    old_state = load_state()
    new_state = {}
    m3u_entries = []
    processed_ids = set()

    target_date = datetime.now(timezone.utc).date()
    log_update(f"Scanning for matches in the 24-hour window ending on {target_date.strftime('%Y-%m-%d')} at 21:00 UTC.")
    
    for sport in ["football", "cricket", "basketball"]:
        match_list = get_match_list_for_day(sport, target_date)
        if not match_list:
            log_update(f"Found 0 matches for {sport.capitalize()}.")
            continue
        
        log_update(f"Found {len(match_list)} matches for {sport.capitalize()}.")

        for match_summary in match_list:
            match_id = match_summary.get('id')
            if not match_id or match_id in processed_ids:
                continue

            # Only get full details if the match is live or finished
            match_status = match_summary.get('status')
            if match_status not in ["MatchIng", "MatchEnded"]:
                continue
                
            details = get_match_details(match_id)
            if not details:
                continue
            
            processed_ids.add(match_id)
            
            team1_name = details.get('team1', {}).get('name', 'N/A')
            team2_name = details.get('team2', {}).get('name', 'N/A')
            logo = details.get('team1', {}).get('avatar', '')
            group = sport.capitalize()
            start_time = int(details.get('startTime', 0))

            all_streams = []
            primary_stream = details.get('playPath')
            if primary_stream and primary_stream.startswith('http'):
                all_streams.append({'title': 'Primary', 'path': primary_stream})
            
            alternative_streams = details.get('playSource', [])
            if alternative_streams:
                all_streams.extend(alternative_streams)
            
            old_streams = old_state.get(match_id, {}).get('streams', [])
            if not old_streams and all_streams:
                log_update(f"STREAM ADDED: Match ID {match_id} ({team1_name} vs {team2_name}) now has streams.")

            new_state[match_id] = {'streams': [s['path'] for s in all_streams]}

            if all_streams:
                # Convert start time to the required UTC format "HHMMHRS UTC"
                utc_dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
                time_str = utc_dt.strftime('%H%M')
                
                base_match_name = f"{team1_name} vs {team2_name} ({time_str}HRS UTC)"

                for i, stream in enumerate(all_streams):
                    match_name = base_match_name
                    if len(all_streams) > 1:
                        match_name += f" - Signal {i+1}"

                    m3u_entries.append({
                        "match": match_name,
                        "logo": logo,
                        "group": group,
                        "stream": stream['path'],
                        "startTime": start_time
                    })

    m3u_entries.sort(key=lambda x: x['startTime'])
    
    playlist_lines = ["#EXTM3U"]
    for entry in m3u_entries:
        extinf = f'#EXTINF:-1 tvg-logo="{entry["logo"]}" group-title="{entry["group"]}",{entry["match"]}'
        playlist_lines.append(extinf)
        playlist_lines.append(entry["stream"])
        
    with open(M3U_FILENAME, 'w', encoding='utf-8') as f:
        f.write("\n".join(playlist_lines))
    log_update(f"Generated {M3U_FILENAME} with {len(m3u_entries)} total streams.")

    save_state(new_state)
    update_lovestory_json()
    
    log_update("Workflow finished successfully.")

if __name__ == "__main__":
    main()

