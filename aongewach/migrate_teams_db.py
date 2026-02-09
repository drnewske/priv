import json
import hashlib

def migrate():
    input_file = 'teams.json'
    output_file = 'teams.json' # Overwrite

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("teams.json not found!")
        return

    teams = data.get('teams', {})
    print(f"Found {len(teams)} teams. Migrating...")

    # Start ID from 1000 to look nice
    current_id = 1000
    
    for team_name, team_data in teams.items():
        # Generate ID if missing
        if 'id' not in team_data:
            team_data['id'] = current_id
            current_id += 1
        
        # Generate simple logo_id (hash of URL or just ID) if missing
        # Using ID string is simplest for now
        if 'logo_id' not in team_data:
            team_data['logo_id'] = str(team_data['id'])

    # Update metadata
    if 'metadata' not in data:
        data['metadata'] = {}
    data['metadata']['next_team_id'] = current_id

    # Save
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Migration complete. Next ID: {current_id}")

if __name__ == "__main__":
    migrate()
