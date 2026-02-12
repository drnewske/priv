#!/usr/bin/env python3
"""
Map Schedule to Teams — Links schedule events to team IDs and logos.
Uses the new fuzzy_match.TeamMatcher for multi-tier matching.
"""

import json
import re
import os
from fuzzy_match import TeamMatcher, clean_team_name


def load_json(filepath):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def process_schedule():
    print("Loading data...")
    schedule_data = load_json('weekly_schedule.json')

    if not schedule_data:
        return

    # Initialize the matcher
    matcher = TeamMatcher('spdb_teams.json')
    print(f"Loaded {len(matcher.teams)} teams in database.")

    # Load legacy teams for fallback
    print("Loading legacy teams...")
    legacy_db = load_json('teams.json')
    legacy_teams = legacy_db.get('teams', {})
    
    # helper to find in legacy
    def find_in_legacy(name):
        if not name: return None
        # Try direct key
        if name in legacy_teams:
            return legacy_teams[name]
        # Try lower key
        for k, v in legacy_teams.items():
            if k.lower() == name.lower():
                return v
            # Try aliases
            for alias in v.get('aliases', []):
                if alias.lower() == name.lower():
                    return v
        return None

    print("Mapping teams...")
    mapped_count = 0
    partial_count = 0
    legacy_fallback_count = 0
    total_events = 0
    not_found_names = set()

    for day in schedule_data.get('schedule', []):
        for event in day.get('events', []):
            name = event.get('name', '')
            sport = event.get('sport', '')

            # Regex to split "Home v Away"
            split_match = re.split(r'\s+(?:v|vs|VS|V|-)\s+', name, maxsplit=1)

            if len(split_match) == 2:
                total_events += 1
                home_raw = split_match[0].strip()
                away_raw = split_match[1].strip()

                # Store parsed raw names
                event['home_team'] = home_raw
                event['away_team'] = away_raw

                # Initialize with None
                event['home_team_id'] = None
                event['home_team_logo'] = None
                event['away_team_id'] = None
                event['away_team_logo'] = None

                # Helper to process a single team side
                def resolve_team(raw_name):
                    # 1. Try SPDB Matcher
                    match = matcher.find(raw_name, sport)
                    if match:
                        return match, False
                    
                    # 2. Techically not found, try Legacy Fallback
                    legacy_match = find_in_legacy(raw_name)
                    if legacy_match:
                        # We found it in legacy!
                        # Does this legacy team exist in SPDB under its proper name?
                        # This helps us link "PSG" (Legacy) -> "Paris Saint-Germain" (SPDB)
                        legacy_proper_name = legacy_match.get('name') or raw_name
                        spdb_check = matcher.find(legacy_proper_name, sport)
                        
                        if spdb_check:
                            # Yes! The legacy name led us to a valid SPDB entry.
                            # We should learn the original raw_name as an alias for this SPDB team.
                            matcher._learn_alias(spdb_check['name'], raw_name)
                            return spdb_check, False
                        else:
                            # No, SPDB really doesn't have it. Use Legacy data provided it has an ID/Logo.
                            # Create a pseudo-object to return
                            return {
                                'id': legacy_match.get('id'),
                                'logo_url': legacy_match.get('logo_url')
                            }, True
                    
                    # 3. Tier 3: API Fallback (The user's new request)
                    # If still not found, try to fetch from API directly
                    print(f"  Thinking... attempting API fetch for: {raw_name}")
                    api_match = matcher.fetch_and_learn(raw_name)
                    if api_match:
                        return api_match, False

                    return None, False

                # Resolve Home
                home_data, used_legacy_home = resolve_team(home_raw)
                if home_data:
                    event['home_team_id'] = home_data.get('id')
                    event['home_team_logo'] = home_data.get('logo_url')
                    if used_legacy_home: legacy_fallback_count += 1

                # Resolve Away
                away_data, used_legacy_away = resolve_team(away_raw)
                if away_data:
                    event['away_team_id'] = away_data.get('id')
                    event['away_team_logo'] = away_data.get('logo_url')
                    if used_legacy_away: legacy_fallback_count += 1
                
                # Tracking
                if not home_data:
                    not_found_names.add(f"{clean_team_name(home_raw)} ({sport})")
                
                if not away_data:
                    not_found_names.add(f"{clean_team_name(away_raw)} ({sport})")

                if home_data and away_data:
                    mapped_count += 1
                elif home_data or away_data:
                    partial_count += 1

    # Save the mapped schedule
    output_file = 'weekly_schedule_mapped.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(schedule_data, f, indent=2, ensure_ascii=False)

    # Save learned aliases back to database
    matcher.save()

    print(f"\nDone. Processed {total_events} team events.")
    print(f"  Fully matched:    {mapped_count}")
    print(f"  Partially matched: {partial_count}")
    print(f"  Legacy Fallback:   {legacy_fallback_count} (Used legacy DB directly)")
    print(f"  No match:          {total_events - mapped_count - partial_count}")
    print(f"Saved to {output_file}")

    if not_found_names:
        print(f"\nTeams NOT found ({len(not_found_names)}):")
        for n in sorted(not_found_names):
            print(f"  - {n}")


if __name__ == "__main__":
    process_schedule()
