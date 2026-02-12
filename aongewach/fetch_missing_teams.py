import requests
import json
import os
import time

# List of teams to search for
TARGET_TEAMS = [
    "Paris SG",
    "Chelsea",
    "Al-Nassr",
    "Al-Hilal",
    "Al-Shabab"
]

DB_FILE = "spdb_teams.json"
API_BASE = "https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t="

def load_json(filepath):
    if not os.path.exists(filepath):
        return {"teams": {}, "_index": {}}
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def fetch_and_update():
    db = load_json(DB_FILE)
    teams_db = db.get("teams", {})
    index_db = db.get("_index", {})
    
    updated_count = 0

    for team_name in TARGET_TEAMS:
        print(f"Searching for: {team_name}...")
        try:
            # URL encode the team name
            url = API_BASE + requests.utils.quote(team_name)
            response = requests.get(url)
            data = response.json()
            
            if data and data.get("teams"):
                # Take the first result usually
                team_data = data["teams"][0]
                found_name = team_data["strTeam"]
                
                print(f"  Found: {found_name} (ID: {team_data['idTeam']})")
                
                # Construct our DB entry format
                entry = {
                    "id": int(team_data["idTeam"]),
                    "api_id": team_data["idTeam"],
                    "name": found_name,
                    "short_name": team_data.get("strTeamShort"),
                    "alternates": [team_data.get("strAlternate")] if team_data.get("strAlternate") else [],
                    "keywords": [k.strip() for k in (team_data.get("strKeywords") or "").split(",") if k.strip()],
                    "logo_url": team_data.get("strBadge"),
                    "banner_url": team_data.get("strBanner"),
                    "league": team_data.get("strLeague"),
                    "sport": team_data.get("strSport"),
                    "country": team_data.get("strCountry"),
                    "aliases": [] # Initialize empty aliases list
                }
                
                # If the team we looked for is different from official name, add as alias
                if team_name.lower() != found_name.lower():
                    entry["aliases"].append(team_name.lower())

                # Update DB
                teams_db[found_name] = entry
                
                # Update Index
                # 1. Normal index
                norm_name = found_name.lower().replace(" ", "")
                index_db[norm_name] = found_name
                
                # 2. Alias index
                for alias in entry["aliases"]:
                    norm_alias = alias.lower().replace(" ", "")
                    index_db[norm_alias] = found_name

                updated_count += 1
            else:
                print(f"  No results found for {team_name}")
                
        except Exception as e:
             print(f"  Error fetching {team_name}: {e}")
        
        time.sleep(1) # Be nice to API

    if updated_count > 0:
        db["teams"] = teams_db
        db["_index"] = index_db
        db["_meta"] = db.get("_meta", {})
        db["_meta"]["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        
        save_json(DB_FILE, db)
        print(f"\nSuccessfully added/updated {updated_count} teams in {DB_FILE}.")
    else:
        print("\nNo updates made.")

if __name__ == "__main__":
    fetch_and_update()
