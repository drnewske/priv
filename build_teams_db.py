#!/usr/bin/env python3
"""
Build Teams DB - Maximum coverage local mirror, free-tier only.

Approach:
  For every league in TheSportsDB:
    1. Get all seasons via search_all_seasons.php
    2. For each season, get all events via eventsseason.php
    3. Extract every unique home/away team ID
    4. Resolve each unseen team ID via lookupteam.php

This captures every team that has EVER played in any tracked league --
not just teams with upcoming fixtures. Fully resumable: progress is
saved to a state file so you can Ctrl+C and continue later.

Free endpoints used (all work on key '3'):
  all_leagues.php
  search_all_seasons.php?id=<league_id>
  eventsseason.php?id=<league_id>&s=<season>
  lookupteam.php?id=<team_id>

Runtime estimate (free key, ~30 req/min):
  ~1000 leagues x avg 5 seasons = many hours.
  Run overnight with: nohup python build_teams_db.py &
  Or inside screen/tmux. Safe to Ctrl+C and resume anytime.
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
TEAMS_DB_FILE    = 'spdb_teams.json'
STATE_FILE       = 'spdb_build_state.json'
RATE_LIMIT_DELAY = 2.2
MAX_RETRIES      = 3
CHECKPOINT_EVERY = 30

# Leave empty to grab ALL sports, or restrict e.g. {'Soccer', 'Basketball'}
SPORTS_FILTER = set()


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
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def api_call(endpoint, params=None):
    url = f'{API_BASE}/{endpoint}'
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                wait = 20 * (attempt + 1)
                print(f'  [429] rate limited -- sleeping {wait}s ...')
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                text = resp.text.strip()
                if not text or text in ('null', '[]', '{}'):
                    return None
                try:
                    return resp.json()
                except ValueError:
                    return None
            return None
        except requests.RequestException as exc:
            print(f'  [ERR attempt {attempt+1}] {exc}')
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
    return None


def throttled(endpoint, params=None):
    time.sleep(RATE_LIMIT_DELAY)
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
    raw.sort(key=lambda s: s.split('-')[0], reverse=True)
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
    """Minimal fallback for leagues with no season data."""
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

def build(teams, seen_ids, completed_leagues, existing_aliases):
    print('Fetching league list ...')
    data = throttled('all_leagues.php')
    all_leagues = (data or {}).get('leagues') or []

    leagues = [
        lg for lg in all_leagues
        if not lg.get('strLeague', '').startswith('_')
        and (not SPORTS_FILTER or lg.get('strSport', '') in SPORTS_FILTER)
    ]

    remaining = [lg for lg in leagues if lg.get('idLeague', '') not in completed_leagues]

    print(f'  {len(leagues)} leagues total  |  '
          f'{len(completed_leagues)} already done  |  '
          f'{len(remaining)} to process\n')

    added_this_run = 0

    for i, league in enumerate(remaining, 1):
        lid    = league.get('idLeague', '')
        lname  = league.get('strLeague', '?')
        lsport = league.get('strSport', '?')
        if not lid:
            continue

        # Get all seasons for this league
        seasons = get_seasons(lid)

        if seasons:
            # Collect team IDs across all historical seasons
            season_ids = set()
            for season in seasons:
                season_ids |= get_team_ids_for_season(lid, season)
            new_ids = season_ids - seen_ids
        else:
            # No season data -- fall back to next/past fixture endpoints
            new_ids = ids_from_fixtures(lid) - seen_ids

        seen_ids |= new_ids

        # Resolve each new team ID to a full record
        league_new = 0
        for tid in new_ids:
            raw = resolve_team(tid)
            if not raw:
                continue
            tname = (raw.get('strTeam') or '').strip()
            if not tname or tname in teams:
                continue
            _, entry = make_entry(raw, existing_aliases.get(tname, []))
            if entry:
                teams[tname] = entry
                league_new += 1

        added_this_run += league_new
        completed_leagues.add(lid)

        if league_new or i % 20 == 0:
            print(f'  [{i:4d}/{len(remaining)}] '
                  f'[{lsport:20s}] {lname:45s}  '
                  f'+{league_new:3d}  total: {len(teams):,}')

        if i % CHECKPOINT_EVERY == 0:
            _checkpoint(teams, seen_ids, completed_leagues)

    return added_this_run


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
    print(f'  [checkpoint] {len(teams):,} teams  |  '
          f'{len(completed_leagues)} leagues done  ->  {TEAMS_DB_FILE}')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print('=' * 70)
    print('BUILD TEAMS DB  --  full mirror, free-tier only')
    print('Safe to Ctrl+C and resume -- progress is saved automatically.')
    print('=' * 70)

    existing_db      = load_json(TEAMS_DB_FILE)
    teams            = dict(existing_db.get('teams', {}))
    existing_aliases = {k: v['aliases'] for k, v in teams.items() if v.get('aliases')}

    state             = load_json(STATE_FILE)
    seen_ids          = set(state.get('seen_ids', []))
    completed_leagues = set(state.get('completed_leagues', []))

    if completed_leagues:
        print(f'\nResuming: {len(completed_leagues)} leagues done, '
              f'{len(teams):,} teams stored so far.\n')

    added = build(teams, seen_ids, completed_leagues, existing_aliases)

    _checkpoint(teams, seen_ids, completed_leagues)

    print(f'\n{"=" * 70}')
    print(f'DONE  --  {len(teams):,} total teams  (+{added} this run)')
    print(f'{"=" * 70}')


if __name__ == '__main__':
    main()
