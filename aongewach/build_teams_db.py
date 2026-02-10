#!/usr/bin/env python3
"""
Build Teams DB - Bulk preloader for TheSportsDB.
Uses a two-phase approach:
  Phase 1: Fetch all leagues per sport via search_all_leagues.php, then 
           bulk-fetch teams per league via lookup_all_teams.php
  Phase 2: Fallback per-team search for any schedule teams not matched
"""

import json
import os
import re
import time
import zlib
import requests
from datetime import datetime, timezone

# Configuration
API_KEY = '3'
API_BASE = f'https://www.thesportsdb.com/api/v1/json/{API_KEY}'
TEAMS_DB_FILE = 'spdb_teams.json'
SCHEDULE_FILE = 'weekly_schedule.json'
RATE_LIMIT_DELAY = 2.1  # seconds between API calls (30 req/min = 2s minimum)
MAX_RETRIES = 3

# --- TSDB sport names to search for leagues
SPORTS_TO_FETCH = [
    'Soccer', 'Basketball', 'Ice Hockey', 'American Football',
    'Baseball', 'Rugby', 'Cricket', 'Tennis', 'Motorsport',
    'Handball', 'Volleyball', 'Australian Football',
    'Boxing', 'MMA', 'Golf', 'Cycling', 'Esports',
]


def load_json(filepath):
    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_stable_id(name):
    """Generate a stable integer ID from a string using Adler32."""
    return zlib.adler32(name.encode('utf-8')) & 0xffffffff


def api_call(endpoint, params=None, retries=MAX_RETRIES):
    """Make an API call with retry logic."""
    url = f'{API_BASE}/{endpoint}'
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp.json()
            print(f"  HTTP {resp.status_code} for {endpoint}")
        except Exception as e:
            print(f"  Error: {e}")
            if attempt < retries - 1:
                time.sleep(5)
    return None


def normalize_for_index(name):
    """Normalize a name for index lookup."""
    if not name:
        return ''
    n = re.sub(r'\s+', ' ', name.lower().strip())
    n = re.sub(r'[^\w\s]', '', n)
    return n.strip()


def parse_csv_field(value):
    """Parse a comma-separated string into a list of stripped values."""
    if not value:
        return []
    return [v.strip() for v in value.split(',') if v.strip()]


def build_index(teams_dict):
    """Build reverse-lookup index: normalized_name -> canonical team key."""
    index = {}
    for team_key, data in teams_dict.items():
        # Index the canonical key
        norm_key = normalize_for_index(team_key)
        if norm_key:
            index[norm_key] = team_key

        # Index API name
        api_name = data.get('name', '')
        norm_api = normalize_for_index(api_name)
        if norm_api and norm_api not in index:
            index[norm_api] = team_key

        # Index alternates
        for alt in data.get('alternates', []):
            norm_alt = normalize_for_index(alt)
            if norm_alt and norm_alt not in index:
                index[norm_alt] = team_key

        # Index short name
        short = data.get('short_name', '')
        norm_short = normalize_for_index(short)
        if norm_short and len(norm_short) >= 2 and norm_short not in index:
            index[norm_short] = team_key

        # Index keywords
        for kw in data.get('keywords', []):
            norm_kw = normalize_for_index(kw)
            if norm_kw and len(norm_kw) >= 3 and norm_kw not in index:
                index[norm_kw] = team_key

        # Index learned aliases
        for alias in data.get('aliases', []):
            norm_alias = normalize_for_index(alias)
            if norm_alias and norm_alias not in index:
                index[norm_alias] = team_key

    return index


def make_team_entry(team, existing_aliases=None):
    """Create a standardized team entry from API data."""
    team_name = team.get('strTeam', '').strip()
    if not team_name:
        return None, None

    stable_id = get_stable_id(team_name)
    alternates = parse_csv_field(team.get('strTeamAlternate', ''))
    keywords = parse_csv_field(team.get('strKeywords', ''))
    short_name = (team.get('strTeamShort') or '').strip()
    aliases = existing_aliases or []

    entry = {
        'id': stable_id,
        'api_id': team.get('idTeam', ''),
        'name': team_name,
        'short_name': short_name,
        'alternates': alternates,
        'keywords': keywords,
        'logo_url': team.get('strBadge') or team.get('strLogo') or None,
        'banner_url': team.get('strBanner') or None,
        'league': team.get('strLeague', ''),
        'sport': team.get('strSport', ''),
        'country': team.get('strCountry', ''),
        'aliases': aliases,
    }
    return team_name, entry


def phase1_bulk_fetch(existing_aliases):
    """Phase 1: Fetch leagues per sport, then teams per league."""
    teams_dict = {}

    # Step 1: Collect all league IDs across all sports
    all_leagues = []
    print("\n-- Phase 1: Fetching leagues per sport --")

    for sport in SPORTS_TO_FETCH:
        time.sleep(RATE_LIMIT_DELAY)
        data = api_call('search_all_leagues.php', params={'s': sport})

        # API returns leagues in 'countries' key
        leagues = []
        if data:
            leagues = data.get('countries', []) or []

        # Filter out "_No League" and "_Defunct" entries
        valid = [l for l in leagues if not l.get('strLeague', '').startswith('_')]
        all_leagues.extend(valid)
        print(f"  {sport:25s} -> {len(valid):3d} leagues")

    print(f"\n  Total leagues to fetch teams from: {len(all_leagues)}")

    # Step 2: Fetch teams for each league
    print("\n-- Phase 1: Fetching teams per league --")
    teams_added = 0

    for i, league in enumerate(all_leagues):
        league_id = league.get('idLeague', '')
        league_name = league.get('strLeague', '?')

        time.sleep(RATE_LIMIT_DELAY)
        data = api_call('lookup_all_teams.php', params={'id': league_id})
        api_teams = data.get('teams', []) if data else []

        if not api_teams:
            continue

        league_added = 0
        for team in api_teams:
            team_name = team.get('strTeam', '').strip()
            if not team_name or team_name in teams_dict:
                continue

            aliases = existing_aliases.get(team_name, [])
            name, entry = make_team_entry(team, aliases)
            if name:
                teams_dict[name] = entry
                teams_added += 1
                league_added += 1

        if (i + 1) % 10 == 0 or league_added > 0:
            print(f"  [{i+1}/{len(all_leagues)}] {league_name:40s} +{league_added} teams (total: {teams_added})")

        # Checkpoint save every 30 leagues
        if (i + 1) % 30 == 0:
            print(f"  [CHECKPOINT] Saving {len(teams_dict)} teams...")
            _save_db(teams_dict)

    print(f"\n  Phase 1 complete: {teams_added} teams from {len(all_leagues)} leagues")
    return teams_dict


def phase2_fallback_search(teams_dict, existing_aliases):
    """Phase 2: Search for schedule teams not found in bulk data."""
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        print("  No schedule file found, skipping fallback search.")
        return teams_dict

    # Build index from current teams for quick lookup
    index = build_index(teams_dict)

    # Extract unique team names from schedule
    unique_names = set()
    for day in schedule.get('schedule', []):
        for event in day.get('events', []):
            name = event.get('name', '')
            parts = re.split(r'\s+(?:v|vs|VS|V|-)\s+', name, maxsplit=1)
            if len(parts) == 2:
                unique_names.add(parts[0].strip())
                unique_names.add(parts[1].strip())

    # Filter to names not already in index
    missing = []
    for name in unique_names:
        norm = normalize_for_index(name)
        if norm and norm not in index and name not in teams_dict:
            missing.append(name)

    if not missing:
        print("\n  Phase 2: All schedule teams already covered!")
        return teams_dict

    print(f"\n-- Phase 2: Searching for {len(missing)} unmatched teams --")
    found = 0

    for name in sorted(missing):
        time.sleep(RATE_LIMIT_DELAY)
        data = api_call('searchteams.php', params={'t': name})
        api_teams = data.get('teams', []) if data else []

        if not api_teams:
            continue

        team = api_teams[0]  # Take best match
        team_name = team.get('strTeam', '').strip()
        if not team_name or team_name in teams_dict:
            # Still add an alias from the schedule name -> existing team
            if team_name in teams_dict:
                norm = normalize_for_index(name)
                if norm not in index:
                    index[norm] = team_name
                    aliases = teams_dict[team_name].get('aliases', [])
                    if name.lower() not in [a.lower() for a in aliases]:
                        aliases.append(name.lower())
                        teams_dict[team_name]['aliases'] = aliases
            continue

        aliases = existing_aliases.get(team_name, [])
        _, entry = make_team_entry(team, aliases)
        if entry:
            teams_dict[team_name] = entry
            found += 1
            print(f"  + {name:30s} -> {team_name}")

    print(f"\n  Phase 2 complete: {found} additional teams found via search")
    return teams_dict


def _save_db(teams_dict):
    """Save teams dict with index and metadata."""
    index = build_index(teams_dict)
    db = {
        '_meta': {
            'last_built': datetime.now(timezone.utc).isoformat(),
            'team_count': len(teams_dict),
            'index_entries': len(index),
        },
        'teams': teams_dict,
        '_index': index,
    }
    save_json(TEAMS_DB_FILE, db)
    print(f"  Saved {len(teams_dict)} teams, {len(index)} index entries to {TEAMS_DB_FILE}")


def main():
    print("=" * 60)
    print("BUILD TEAMS DB - Bulk Preloader v2")
    print("=" * 60)

    # Load existing DB to preserve aliases
    existing_db = load_json(TEAMS_DB_FILE)
    existing_teams = existing_db.get('teams', {})
    existing_aliases = {}
    for key, data in existing_teams.items():
        if data.get('aliases'):
            existing_aliases[key] = data['aliases']

    # Phase 1: Bulk fetch via leagues
    teams_dict = phase1_bulk_fetch(existing_aliases)

    # Preserve teams from old DB not found in bulk
    preserved = 0
    for old_key, old_data in existing_teams.items():
        if old_key not in teams_dict:
            teams_dict[old_key] = old_data
            preserved += 1
    if preserved:
        print(f"\n  Preserved {preserved} teams from previous DB")

    # Phase 2: Fallback search for schedule teams
    teams_dict = phase2_fallback_search(teams_dict, existing_aliases)

    # Final save
    _save_db(teams_dict)

    print(f"\n{'=' * 60}")
    print(f"BUILD COMPLETE")
    print(f"  Total teams: {len(teams_dict)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
