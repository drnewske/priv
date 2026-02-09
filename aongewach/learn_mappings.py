import json
import os

def load_json(filepath):
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def learn():
    schedule_file = 'weekly_schedule_mapped.json'
    teams_file = 'teams.json'
    
    schedule_data = load_json(schedule_file)
    teams_data = load_json(teams_file)
    
    if not schedule_data or not teams_data:
        print("Files not found.")
        return

    teams_map = teams_data.get('teams', {})
    
    # Inverted map for ID lookup: ID -> Team Key
    id_to_key = {}
    for k, v in teams_map.items():
        if 'id' in v:
            id_to_key[v['id']] = k
    
    updated_count = 0
    
    for day in schedule_data.get('schedule', []):
        for event in day.get('events', []):
            
            # Check Home Team
            home_name = event.get('home_team')
            home_id = event.get('home_team_id')
            
            if home_name and home_id:
                # User (or system) has provided an ID.
                # Let's see if we know this name -> ID mapping.
                
                # Look up the team by ID
                team_key = id_to_key.get(home_id)
                if team_key:
                    team_obj = teams_map[team_key]
                    aliases = team_obj.get('aliases', [])
                    
                    # If the name used in schedule isn't the main key AND isn't in aliases
                    if home_name != team_key and home_name.lower() not in [a.lower() for a in aliases]:
                        print(f"Learning: '{home_name}' is alias for '{team_key}' (ID: {home_id})")
                        aliases.append(home_name.lower())
                        team_obj['aliases'] = aliases
                        updated_count += 1
                else:
                    print(f"Warning: ID {home_id} found in schedule but not in teams.json!")

            # Check Away Team (Same Logic)
            away_name = event.get('away_team')
            away_id = event.get('away_team_id')
            
            if away_name and away_id:
                team_key = id_to_key.get(away_id)
                if team_key:
                    team_obj = teams_map[team_key]
                    aliases = team_obj.get('aliases', [])
                    if away_name != team_key and away_name.lower() not in [a.lower() for a in aliases]:
                        print(f"Learning: '{away_name}' is alias for '{team_key}' (ID: {away_id})")
                        aliases.append(away_name.lower())
                        team_obj['aliases'] = aliases
                        updated_count += 1
                else:
                    print(f"Warning: ID {away_id} found in schedule but not in teams.json!")

    if updated_count > 0:
        save_json(teams_file, teams_data)
        print(f"Success! Learned {updated_count} new mappings.")
    else:
        print("No new mappings found to learn.")

if __name__ == "__main__":
    learn()
