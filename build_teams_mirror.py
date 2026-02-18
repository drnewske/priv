#!/usr/bin/env python3
"""
Build Teams DB - Full mirror crawler, free-tier friendly.

Crawls every league + season in TheSportsDB to discover ALL teams.
Fully resumable: Ctrl+C and run again to continue where you left off.

Fixes v2:
  - Only adds genuinely NEW teams (skips existing by name)
  - Always seeds seen_ids from DB on resume (prevents re-resolution)
  - Adaptive rate limiting (backs off on 429, speeds up when clear)
  - Clear stats: new vs skipped vs failed
"""

import json
import os
import re
import time
import zlib
import requests
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY          = '3'
API_BASE         = f'https://www.thesportsdb.com/api/v1/json/{API_KEY}'
_DIR             = os.path.dirname(os.path.abspath(__file__))
TEAMS_DB_FILE    = os.path.join(_DIR, 'spdb_teams.json')
STATE_FILE       = os.path.join(_DIR, 'spdb_build_state.json')
CHECKPOINT_EVERY = 15
MAX_RETRIES      = 3

# Adaptive rate limiting
BASE_DELAY       = 2.5   # minimum delay between requests
_current_delay   = BASE_DELAY
_consecutive_ok  = 0

# Leave empty to grab ALL sports
SPORTS_FILTER = set()

SPORTS_TO_FETCH = [
    'Soccer', 'Basketball', 'Ice Hockey', 'American Football',
    'Baseball', 'Rugby', 'Cricket', 'Tennis', 'Motorsport',
    'Handball', 'Volleyball', 'Australian Football', 'Boxing',
    'MMA', 'Golf', 'Cycling', 'Esports'
]


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_json(path, default=None):
    if not os.path.exists(path):
        return default if default is not None else {}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    try:
        os.replace(tmp, path)
    except OSError:
        if os.path.exists(path):
            os.remove(path)
        os.rename(tmp, path)


# ---------------------------------------------------------------------------
# API with adaptive rate limiting
# ---------------------------------------------------------------------------

def api_call(endpoint, params=None):
    """Single API call with retry on 429."""
    global _current_delay, _consecutive_ok
    url = f'{API_BASE}/{endpoint}'

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=15)

            if resp.status_code == 429:
                # Back off: double the delay, cap at 8s
                _current_delay = min(_current_delay * 1.5, 8.0)
                _consecutive_ok = 0
                wait = 10 + (15 * attempt)  # 10s, 25s, 40s
                print(f'    [429] backing off {wait}s (delay now {_current_delay:.1f}s)')
                time.sleep(wait)
                continue

            if resp.status_code == 200:
                # Success: gradually reduce delay back toward BASE_DELAY
                _consecutive_ok += 1
                if _consecutive_ok > 10 and _current_delay > BASE_DELAY:
                    _current_delay = max(BASE_DELAY, _current_delay - 0.1)

                text = resp.text.strip()
                if not text or text in ('null', '[]', '{}'):
                    return None
                try:
                    return resp.json()
                except ValueError:
                    return None
            return None

        except requests.RequestException as exc:
            print(f'    [ERR attempt {attempt+1}] {exc}')
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
    return None


def throttled(endpoint, params=None):
    """Rate-limited API call."""
    time.sleep(_current_delay)
    return api_call(endpoint, params)


# ---------------------------------------------------------------------------
# Team record helpers
# ---------------------------------------------------------------------------

def stable_id(name):
    return zlib.adler32(name.encode('utf-8')) & 0xFFFFFFFF


def parse_csv(value):
    if not value:
        return []
    return [v.strip() for v in value.split(',') if v.strip()]


def normalize(name):
    if not name:
        return ''
    n = re.sub(r'\s+', ' ', name.lower().strip())
    return re.sub(r'[^\w\s]', '', n).strip()


def make_entry(raw, existing_aliases=None):
    name = (raw.get('strTeam') or '').strip()
    if not name:
        return None, None
    return name, {
        'id':         stable_id(name),
        'api_id':     raw.get('idTeam', ''),
        'name':       name,
        'short_name': (raw.get('strTeamShort') or '').strip(),
        'alternates': parse_csv(raw.get('strTeamAlternate', '')),
        'keywords':   parse_csv(raw.get('strKeywords', '')),
        'logo_url':   raw.get('strBadge') or raw.get('strLogo') or None,
        'banner_url': raw.get('strBanner') or None,
        'league':     raw.get('strLeague', ''),
        'sport':      raw.get('strSport', ''),
        'country':    raw.get('strCountry', ''),
        'aliases':    list(existing_aliases or []),
    }


def build_index(teams):
    index = {}
    def add(n, key):
        if n and n not in index:
            index[n] = key
    for key, d in teams.items():
        add(normalize(key), key)
        add(normalize(d.get('name', '')), key)
        s = normalize(d.get('short_name', ''))
        if len(s) >= 2: add(s, key)
        for v in d.get('alternates', []): add(normalize(v), key)
        for v in d.get('keywords', []):
            n = normalize(v)
            if len(n) >= 3: add(n, key)
        for v in d.get('aliases', []): add(normalize(v), key)
    return index


# ---------------------------------------------------------------------------
# Per-league extraction
# ---------------------------------------------------------------------------

def get_seasons(league_id):
    data = throttled('search_all_seasons.php', {'id': league_id})
    seasons = (data or {}).get('seasons') or []
    raw = [s.get('strSeason', '') for s in seasons if s.get('strSeason')]
    try:
        raw.sort(key=lambda s: s.split('-')[0], reverse=True)
    except Exception:
        pass
    return raw


def get_team_ids_for_season(league_id, season):
    data = throttled('eventsseason.php', {'id': league_id, 's': season})
    events = (data or {}).get('events') or []
    ids = set()
    for ev in events:
        for field in ('idHomeTeam', 'idAwayTeam'):
            tid = (ev.get(field) or '').strip()
            if tid and tid != '0':
                ids.add(tid)
    return ids


def resolve_team(team_id):
    data = throttled('lookupteam.php', {'id': team_id})
    records = (data or {}).get('teams') or []
    return records[0] if records else None


def ids_from_fixtures(league_id):
    """Fallback for leagues with no season data."""
    ids = set()
    for ep in ('eventsnextleague.php', 'eventspastleague.php'):
        data = throttled(ep, {'id': league_id})
        for ev in (data or {}).get('events') or []:
            for field in ('idHomeTeam', 'idAwayTeam'):
                tid = (ev.get(field) or '').strip()
                if tid and tid != '0':
                    ids.add(tid)
    return ids


# ---------------------------------------------------------------------------
# Main build loop
# ---------------------------------------------------------------------------

def build(teams, seen_ids, completed_leagues):
    existing_aliases = {k: v.get('aliases', []) for k, v in teams.items() if v.get('aliases')}

    # --- Fetch leagues ---
    print(f'\n  Scanning {len(SPORTS_TO_FETCH)} sports for leagues...')
    all_leagues = []

    for sport_name in SPORTS_TO_FETCH:
        if SPORTS_FILTER and sport_name not in SPORTS_FILTER:
            continue
        l_data = throttled('search_all_leagues.php', {'s': sport_name})
        sport_leagues = (l_data or {}).get('countries') or []
        valid = [l for l in sport_leagues if not l.get('strLeague', '').startswith('_')]
        all_leagues.extend(valid)
        if valid:
            print(f'    {sport_name}: {len(valid)} leagues')

    # Deduplicate
    details_map = {l['idLeague']: l for l in all_leagues if l.get('idLeague')}
    leagues = list(details_map.values())
    remaining = [lg for lg in leagues if lg.get('idLeague', '') not in completed_leagues]

    print(f'\n  {len(leagues)} leagues total | {len(completed_leagues)} done | {len(remaining)} remaining')
    print(f'  {len(teams):,} teams in DB | {len(seen_ids):,} team IDs seen\n')

    # --- Stats ---
    new_teams = 0
    skipped_existing = 0
    api_calls_saved = 0

    for i, league in enumerate(remaining, 1):
        lid    = league.get('idLeague', '')
        lname  = league.get('strLeague', '?')
        lsport = league.get('strSport', '?')
        if not lid:
            continue

        # Get seasons
        seasons = get_seasons(lid)
        league_team_ids = set()

        if seasons:
            print(f'  [{i:3d}/{len(remaining)}] {lname} ({lsport}) - {len(seasons)} seasons')
            for season in seasons:
                s_ids = get_team_ids_for_season(lid, season)
                league_team_ids |= s_ids
        else:
            print(f'  [{i:3d}/{len(remaining)}] {lname} ({lsport}) - no seasons, checking fixtures')
            league_team_ids = ids_from_fixtures(lid)

        # Only resolve IDs we haven't seen before
        to_resolve = league_team_ids - seen_ids
        already_seen = len(league_team_ids) - len(to_resolve)

        if already_seen:
            api_calls_saved += already_seen

        if to_resolve:
            league_new = 0
            league_skipped = 0

            for tid in to_resolve:
                raw = resolve_team(tid)
                if raw:
                    tname = (raw.get('strTeam') or '').strip()
                    if tname:
                        if tname not in teams:
                            # genuinely new team
                            aliases = existing_aliases.get(tname, [])
                            _, entry = make_entry(raw, aliases)
                            if entry:
                                teams[tname] = entry
                                league_new += 1
                                new_teams += 1
                        else:
                            league_skipped += 1
                            skipped_existing += 1

                # Mark ID as seen regardless
                seen_ids.add(tid)

            status = f'    +{league_new} new'
            if league_skipped:
                status += f' | {league_skipped} already in DB'
            if already_seen:
                status += f' | {already_seen} IDs skipped'
            print(status)
        else:
            if league_team_ids:
                print(f'    all {len(league_team_ids)} teams already known')

        completed_leagues.add(lid)

        if i % CHECKPOINT_EVERY == 0:
            _checkpoint(teams, seen_ids, completed_leagues)
            print(f'    [stats] {new_teams} new | {skipped_existing} skipped | {api_calls_saved} calls saved')

    return new_teams


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _checkpoint(teams, seen_ids, completed_leagues):
    save_json(STATE_FILE, {
        'seen_ids':          list(seen_ids),
        'completed_leagues': list(completed_leagues),
    })
    index = build_index(teams)
    save_json(TEAMS_DB_FILE, {
        '_meta': {
            'last_updated':  datetime.now(timezone.utc).isoformat(),
            'team_count':    len(teams),
            'index_entries': len(index),
        },
        'teams':  teams,
        '_index': index,
    })
    print(f'  [SAVED] {len(teams):,} teams | {len(completed_leagues)} leagues done')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print('=' * 60)
    print('BUILD TEAMS DB  -  Full Mirror v2 (fixed)')
    print('Ctrl+C to stop. Run again to resume.')
    print('=' * 60)

    # Load existing DB
    existing_db = load_json(TEAMS_DB_FILE)
    teams = dict(existing_db.get('teams', {}))

    # Load resume state
    state = load_json(STATE_FILE)
    seen_ids = set(state.get('seen_ids', []))
    completed_leagues = set(state.get('completed_leagues', []))

    # FIX: ALWAYS seed seen_ids from DB teams (not just when state is empty)
    # This prevents re-resolving teams we already have
    db_ids_added = 0
    for k, v in teams.items():
        api_id = str(v.get('api_id', ''))
        if api_id and api_id not in seen_ids:
            seen_ids.add(api_id)
            db_ids_added += 1

    if db_ids_added:
        print(f'  Seeded {db_ids_added} team IDs from existing DB into seen_ids')

    if completed_leagues:
        print(f'  Resuming: {len(completed_leagues)} leagues done, '
              f'{len(teams):,} teams, {len(seen_ids):,} IDs known\n')

    try:
        added = build(teams, seen_ids, completed_leagues)
        _checkpoint(teams, seen_ids, completed_leagues)

        print(f'\n{"=" * 60}')
        print(f'DONE - {len(teams):,} total teams (+{added} new this session)')
        print(f'{"=" * 60}')

    except KeyboardInterrupt:
        print('\n\n[STOPPED] Saving progress...')
        _checkpoint(teams, seen_ids, completed_leagues)
        print('Progress saved. Run again to resume.')


if __name__ == '__main__':
    main()
