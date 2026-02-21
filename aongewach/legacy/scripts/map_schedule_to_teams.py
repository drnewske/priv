#!/usr/bin/env python3
"""
Map Schedule to Teams — Links schedule events to team IDs and logos.
Uses the new fuzzy_match.TeamMatcher for multi-tier matching.
"""

import json
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent


def load_json(filepath):
    path = Path(filepath)
    if not path.exists():
        print(f"File not found: {filepath}")
        return {}
    try:
        with path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to parse {filepath}: {e}")
        return {}


def process_schedule():
    print("Loading data...")
    schedule_file = SCRIPT_DIR / 'weekly_schedule.json'
    schedule_data = load_json(schedule_file)

    if not schedule_data:
        return

    source = str(schedule_data.get('source', '')).lower()
    if source.startswith(('fanzo', 'livesporttv')):
        print("Schedule source already includes team metadata. Skipping fuzzy/API team mapping.")
        total_events = 0
        for day in schedule_data.get('schedule', []):
            for event in day.get('events', []):
                name = event.get('name', '')
                split_match = re.split(r'\s+(?:v|vs|VS|V|-)\s+', name, maxsplit=1)
                if len(split_match) == 2:
                    total_events += 1
                    home_raw = split_match[0].strip()
                    away_raw = split_match[1].strip()
                    event['home_team'] = event.get('home_team') or home_raw
                    event['away_team'] = event.get('away_team') or away_raw
                    if 'home_team_id' not in event:
                        event['home_team_id'] = None
                    if 'home_team_logo' not in event:
                        event['home_team_logo'] = None
                    if 'away_team_id' not in event:
                        event['away_team_id'] = None
                    if 'away_team_logo' not in event:
                        event['away_team_logo'] = None

        output_file = SCRIPT_DIR / 'weekly_schedule_mapped.json'
        with output_file.open('w', encoding='utf-8') as f:
            json.dump(schedule_data, f, indent=2, ensure_ascii=False)

        print(f"Done. Processed {total_events} team events (FANZO fast path).")
        print(f"Saved to {output_file.name}")
        return

    # Legacy path: load matcher module from repo root where legacy scripts now live.
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))
    from fuzzy_match import TeamMatcher, clean_team_name

    # Initialize the matcher
    matcher = TeamMatcher(str(ROOT_DIR / 'spdb_teams.json'))
    print(f"Loaded {len(matcher.teams)} teams in database.")

    # Load legacy teams for fallback
    print("Loading legacy teams...")
    legacy_db = load_json(ROOT_DIR / 'teams.json')
    legacy_teams = legacy_db.get('teams', {}) if isinstance(legacy_db, dict) else {}
    api_lookup_cache = {}
    
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

                # Preserve pre-seeded values (e.g. FANZO source), only fill missing fields.
                event['home_team'] = event.get('home_team') or home_raw
                event['away_team'] = event.get('away_team') or away_raw
                if 'home_team_id' not in event:
                    event['home_team_id'] = None
                if 'home_team_logo' not in event:
                    event['home_team_logo'] = None
                if 'away_team_id' not in event:
                    event['away_team_id'] = None
                if 'away_team_logo' not in event:
                    event['away_team_logo'] = None

                # Helper to process a single team side
                def resolve_team(raw_name):
                    cache_key = (raw_name.lower().strip(), (sport or '').lower().strip())
                    if cache_key in api_lookup_cache:
                        return api_lookup_cache[cache_key]

                    # 1. Try SPDB Matcher
                    match = matcher.find(raw_name, sport)
                    if match:
                        result = (match, False)
                        api_lookup_cache[cache_key] = result
                        return result
                    
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
                            result = (spdb_check, False)
                            api_lookup_cache[cache_key] = result
                            return result
                        else:
                            # No, SPDB really doesn't have it. Use Legacy data provided it has an ID/Logo.
                            # Create a pseudo-object to return
                            result = ({
                                'id': legacy_match.get('id'),
                                'logo_url': legacy_match.get('logo_url')
                            }, True)
                            api_lookup_cache[cache_key] = result
                            return result
                    
                    # 3. Stop here intentionally: no external API fallback.
                    result = (None, False)
                    api_lookup_cache[cache_key] = result
                    return result

                # Only resolve missing IDs/logos to avoid overwriting preloaded values.
                home_data = None
                away_data = None
                used_legacy_home = False
                used_legacy_away = False

                home_needs_lookup = not event.get('home_team_id') or not event.get('home_team_logo')
                away_needs_lookup = not event.get('away_team_id') or not event.get('away_team_logo')

                if home_needs_lookup:
                    home_data, used_legacy_home = resolve_team(home_raw)
                    if home_data:
                        if not event.get('home_team_id'):
                            event['home_team_id'] = home_data.get('id')
                        if not event.get('home_team_logo'):
                            event['home_team_logo'] = home_data.get('logo_url')
                        if used_legacy_home:
                            legacy_fallback_count += 1

                if away_needs_lookup:
                    away_data, used_legacy_away = resolve_team(away_raw)
                    if away_data:
                        if not event.get('away_team_id'):
                            event['away_team_id'] = away_data.get('id')
                        if not event.get('away_team_logo'):
                            event['away_team_logo'] = away_data.get('logo_url')
                        if used_legacy_away:
                            legacy_fallback_count += 1
                
                # Tracking
                home_mapped = bool(event.get('home_team_id')) or bool(event.get('home_team_logo'))
                away_mapped = bool(event.get('away_team_id')) or bool(event.get('away_team_logo'))

                if not home_mapped:
                    not_found_names.add(f"{clean_team_name(home_raw)} ({sport})")
                
                if not away_mapped:
                    not_found_names.add(f"{clean_team_name(away_raw)} ({sport})")

                if home_mapped and away_mapped:
                    mapped_count += 1
                elif home_mapped or away_mapped:
                    partial_count += 1

    # Save the mapped schedule
    output_file = SCRIPT_DIR / 'weekly_schedule_mapped.json'
    with output_file.open('w', encoding='utf-8') as f:
        json.dump(schedule_data, f, indent=2, ensure_ascii=False)

    # Save learned aliases back to database
    matcher.save()

    print(f"\nDone. Processed {total_events} team events.")
    print(f"  Fully matched:    {mapped_count}")
    print(f"  Partially matched: {partial_count}")
    print(f"  Legacy Fallback:   {legacy_fallback_count} (Used legacy DB directly)")
    print(f"  No match:          {total_events - mapped_count - partial_count}")
    print(f"Saved to {output_file.name}")

    if not_found_names:
        print(f"\nTeams NOT found ({len(not_found_names)}):")
        for n in sorted(not_found_names):
            print(f"  - {n}")


if __name__ == "__main__":
    process_schedule()
