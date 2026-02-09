import json
import re
import os
from difflib import get_close_matches

def load_json(filepath):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

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

def find_team_data(team_name, teams_data):
    """
    Find team data for a name.
    """
    cleaned_name = clean_team_name(team_name)
    cleaned_lower = cleaned_name.lower()

    # 1. Exact Key Match
    if cleaned_name in teams_data:
        return teams_data[cleaned_name]

    # Linear search for case-insensitive Key or Alias match
    for team_key, data in teams_data.items():
        # Check Key
        if team_key.lower() == cleaned_lower:
             return data
        
        # Check Aliases
        aliases = data.get('aliases', [])
        if aliases:
            for alias in aliases:
                if alias.lower() == cleaned_lower:
                    return data
    
    return None

def process_schedule():
    print("Loading data...")
    schedule_data = load_json('weekly_schedule.json')
    teams_db = load_json('spdb_teams.json')

    if not schedule_data or not teams_db:
        return

    # Extract the 'teams' dictionary from the DB structure
    teams_map = teams_db.get('teams', {})
    
    print("Mapping teams...")
    mapped_count = 0
    total_events = 0

    for day in schedule_data.get('schedule', []):
        for event in day.get('events', []):
            total_events += 1
            name = event.get('name', '')
            
            # Regex to split separate "Home" and "Away"
            split_match = re.split(r'\s+(?:v|vs|VS|V|-)\s+', name, maxsplit=1)
            
            if len(split_match) == 2:
                home_raw = split_match[0].strip()
                away_raw = split_match[1].strip()
                
                # Store the parsed raw text
                event['home_team'] = home_raw
                event['away_team'] = away_raw
                
                # Find Teams
                home_data = find_team_data(home_raw, teams_map)
                away_data = find_team_data(away_raw, teams_map)
                
                # Initialize with None for consistency
                event['home_team_id'] = None
                event['home_team_logo'] = None
                event['away_team_id'] = None
                event['away_team_logo'] = None

                if home_data:
                    event['home_team_id'] = home_data.get('id')
                    event['home_team_logo'] = home_data.get('logo_url')
                
                if away_data:
                    event['away_team_id'] = away_data.get('id')
                    event['away_team_logo'] = away_data.get('logo_url')
                
                if home_data or away_data:
                    mapped_count += 1

    output_file = 'weekly_schedule_mapped.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(schedule_data, f, indent=2, ensure_ascii=False)
    
    print(f"Done. Processed {total_events} events.")
    print(f"Mapped logos for {mapped_count} events (at least one team found).")
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    process_schedule()
