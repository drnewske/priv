import requests
import json
from datetime import datetime, time, timezone
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

# --- HELPER FUNCTIONS ---

def get_api_data(url, params):
    """Generic function to fetch data from the API."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://aisports.cc/'
    }
    try:
        response = requests.get(url, params=params, headers=headers, verify=False, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        log_update(f"API request failed for {url} with params {params}: {err}")
        return None

def get_match_list_for_day(sport_type="football", target_date=None):
    """Fetches a list of all matches for a specific day and sport."""
    if target_date is None:
        target_date = datetime.now().date()
    start_of_day = int(datetime.combine(target_date, time.min).timestamp() * 1000)
    end_of_day = int(datetime.combine(target_date, time.max).timestamp() * 1000)
    api_url = f"{BASE_URL}/wefeed-h5-bff/live/match-list-v3"
    params = {'status': 0, 'matchType': sport_type, 'startTime': start_of_day, 'endTime': end_of_day}
    
    data = get_api_data(api_url, params)
    if data and data.get('code') == 0:
        match_groups = data.get('data', {}).get('list', [])
        all_matches = []
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

def update_lovestory_json():
    """Updates the lovestory.json file once per day."""
    today_str = datetime.now().strftime('%d.%m.%Y')
    target_url = f"{M3U_URL_BASE}/{M3U_FILENAME}"

    try:
        with open(JSON_FILENAME, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"featured_content": []}

    featured_content = data.get("featured_content", [])
    entry_index = -1
    for i, item in enumerate(featured_content):
        if item.get('url') == target_url:
            entry_index = i
            break
            
    if entry_index != -1:
        # Entry exists, check if the name needs updating
        if featured_content[entry_index].get('name') != today_str:
            log_update(f"Updating date in {JSON_FILENAME} to {today_str}.")
            featured_content[entry_index]['name'] = today_str
        else:
            log_update(f"{JSON_FILENAME} is already up-to-date for today.")
            return # No changes needed
    else:
        # Entry doesn't exist, prepend it and re-index others
        log_update(f"Adding new M3U entry to {JSON_FILENAME} for the first time.")
        new_entry = {
            "name": today_str,
            "logo_url": LOGO_URL,
            "type": "m3u",
            "url": target_url,
            "server_url": None
        }
        featured_content.insert(0, new_entry)
        # Re-index all entries
        for i, item in enumerate(featured_content):
            item['id'] = f"featured_m3u_{i+1:02}"
            
    data['featured_content'] = featured_content
    with open(JSON_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

# --- MAIN WORKFLOW ---

def main():
    """Main function to run the ETL process."""
    log_update("Workflow started.")
    
    old_state = load_state()
    new_state = {}
    processed_matches_for_m3u = []

    today_date = datetime.now().date()
    
    for sport in ["football", "cricket", "basketball"]:
        match_list = get_match_list_for_day(sport, today_date)
        if not match_list:
            continue

        for match_summary in match_list:
            match_id = match_summary.get('id')
            if not match_id:
                continue

            details = get_match_details(match_id)
            if not details:
                continue
            
            stream_url = details.get('playPath')
            
            # --- Logging Logic ---
            old_stream_url = old_state.get(match_id, {}).get('stream')
            if not old_stream_url and stream_url:
                log_update(f"STREAM ADDED: Match ID {match_id} ({details.get('team1', {}).get('name')} vs {details.get('team2', {}).get('name')}) now has a stream URL.")
            
            # --- State and M3U Data Preparation ---
            new_state[match_id] = {'stream': stream_url}
            
            if stream_url and stream_url.startswith('http'):
                processed_matches_for_m3u.append({
                    "match": f"{details.get('team1', {}).get('name', 'N/A')} vs {details.get('team2', {}).get('name', 'N/A')}",
                    "logo": details.get('team1', {}).get('avatar', ''),
                    "group": sport.capitalize(),
                    "stream": stream_url,
                    "startTime": int(details.get('startTime', 0))
                })

    # --- Generate and Write M3U Playlist ---
    playlist_lines = ["#EXTM3U"]
    processed_matches_for_m3u.sort(key=lambda x: x['startTime'])
    for match in processed_matches_for_m3u:
        extinf = f'#EXTINF:-1 tvg-logo="{match["logo"]}" group-title="{match["group"]}",{match["match"]}'
        playlist_lines.append(extinf)
        playlist_lines.append(match["stream"])
        
    with open(M3U_FILENAME, 'w', encoding='utf-8') as f:
        f.write("\n".join(playlist_lines))
    log_update(f"Generated {M3U_FILENAME} with {len(processed_matches_for_m3u)} streamable matches.")

    # --- Update State and JSON File ---
    save_state(new_state)
    update_lovestory_json()
    
    log_update("Workflow finished successfully.")

if __name__ == "__main__":
    main()
