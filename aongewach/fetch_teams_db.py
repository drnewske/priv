import json
import requests
import time
import zlib
import os
import re

# Configuration
SCHEDULE_FILE = 'weekly_schedule.json'
TEAMS_DB_FILE = 'spdb_teams.json'
API_KEY = '3' # TheSportsDB Free Tier Key is '3' (or '1'/'123' sometimes, but '3' is common for free tier testing)
              # Actually, documentation says "3" is for private/patreon, "2" is beta, "1" is free.
              # Let's try '3' as per some docs or fallback to '1'. 
              # EDIT: The most common public free key is '3' or '1'. Let's use '3'.
API_BASE_URL = 'https://www.thesportsdb.com/api/v1/json/3/searchteams.php'

def load_json(filepath):
    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_stable_id(name):
    """
    Generate a stable integer ID from a string using Adler32.
    """
    return zlib.adler32(name.encode('utf-8')) & 0xffffffff

def clean_team_name(name):
    """
    Remove suffixes like U18, U21, Women, etc. to find the 'base' team.
    """
    # Common suffixes to remove (case insensitive)
    suffixes = [
        r'\s+U\d+$',          # U18, U21, U23
        r'\s+Women$',         # Women
        r'\s+Ladies$',        # Ladies
        r'\s+\(W\)$',         # (W)
        r'\s+\(M\)$',         # (M)
        r'\s+Reserves$',      # Reserves
        r'\s+Youth$',         # Youth
        r'\s+II$',            # II (Second team)
        r'\s+B$',             # B (B team)
    ]
    
    cleaned = name
    for suffix in suffixes:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)
    
    return cleaned.strip()

def fetch_team_from_api(team_name):
    """
    Search TheSportsDB for a team.
    """
    try:
        response = requests.get(API_BASE_URL, params={'t': team_name}, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and data.get('teams'):
                return data['teams'][0] # Return first match
    except Exception as e:
        print(f"  Error fetching {team_name}: {e}")
    return None

def main():
    print("Loading schedule...")
    schedule_data = load_json(SCHEDULE_FILE)
    if not schedule_data:
        print("No schedule data found.")
        return

    # Load existing DB
    teams_db = load_json(TEAMS_DB_FILE)
    if not teams_db:
        teams_db = {"teams": {}}
    
    unique_teams = set()
    
    # Extract all teams from schedule
    for day in schedule_data.get('schedule', []):
        for event in day.get('events', []):
            name = event.get('name', '')
            
            # Simple heuristic: Split on " v " or " vs " or " - "
            # Use regex from map_schedule_to_teams.py but simplified if needed.
            # Actually, let's just copy the same regex logic.
            split_match = re.split(r'\s+(?:v|vs|VS|V|-)\s+', name, maxsplit=1)
            
            if len(split_match) == 2:
                home_raw = split_match[0].strip()
                away_raw = split_match[1].strip()
                unique_teams.add(clean_team_name(home_raw))
                unique_teams.add(clean_team_name(away_raw))
    
    print(f"Found {len(unique_teams)} unique teams in schedule.")
    
    # Check against DB
    new_teams_count = 0
    
    for team_name in sorted(unique_teams):
        # Normalization for key lookup (simple constraint)
        # We check if the name *exactly* exists as a key first
        if team_name in teams_db['teams']:
            continue
            
        print(f"Fetching: {team_name}...")
        
        # Rate limiting (TheSportsDB is lenient but let's be safe)
        time.sleep(1.5) 
        
        api_data = fetch_team_from_api(team_name)
        
        if api_data:
            # Use the API's name as the official name, but map the schedule name to it?
            # User wants "unique hash id for each team found". 
            # We will use the *Team Name from Schedule* as the key to ensure we find it again,
            # BUT we will store the API data.
            
            # actually, better to store by API Name to avoid duplicates if schedule has typos?
            # No, schedule has consistent typos usually. 
            # Let's save under the Cleaned Schedule Name so lookup is O(1).
            
            # Generate Stable ID
            # We use the API's strTeam for the ID hash to ensure that if "Man Utd" and "Manchester United" produces same API result, they get same ID?
            # Yes, that's smarter.
            
            api_team_name = api_data['strTeam']
            stable_id = get_stable_id(api_team_name)
            
            entry = {
                "id": stable_id,
                "api_id": api_data['idTeam'],
                "name": api_team_name,
                "logo_url": api_data['strBadge'] or api_data['strLogo'], # Badge is usually better for small icons
                "banner_url": api_data['strBanner'],
                "league": api_data['strLeague'],
                "sport": api_data['strSport'],
                "aliases": [] 
            }
            
            # Save using the SEARCHED name (Schedule Name) as the key
            # so map_schedule_to_teams finds it easily.
            teams_db['teams'][team_name] = entry
            
            print(f"  > Found: {api_team_name} (ID: {stable_id})")
            new_teams_count += 1
            
            # Save periodically
            if new_teams_count % 5 == 0:
                 save_json(TEAMS_DB_FILE, teams_db)
                 
        else:
            print(f"  x Not found in DB.")
            
    save_json(TEAMS_DB_FILE, teams_db)
    print(f"\nDone. Added {new_teams_count} new teams to {TEAMS_DB_FILE}.")

if __name__ == "__main__":
    main()
