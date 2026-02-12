#!/usr/bin/env python3
"""
Fuzzy Match Engine for team name resolution.
Multi-tier matching: exact → index → normalized → fuzzy.
Self-learning aliases for continuous improvement.
"""

import json
import os
import re
import time
import requests
from difflib import SequenceMatcher


# ─── Sport name mapping: schedule uses different names than TSDB
SPORT_MAP = {
    'football': 'Soccer',
    'soccer': 'Soccer',
    'basketball': 'Basketball',
    'ice-hockey': 'Ice Hockey',
    'ice hockey': 'Ice Hockey',
    'rugby': 'Rugby',
    'rugby union': 'Rugby',
    'rugby league': 'Rugby',
    'cricket': 'Cricket',
    'tennis': 'Tennis',
    'golf': 'Golf',
    'american football': 'American Football',
    'nfl': 'American Football',
    'baseball': 'Baseball',
    'motorsport': 'Motorsport',
    'boxing': 'Boxing',
    'mma': 'MMA',
    'handball': 'Handball',
    'volleyball': 'Volleyball',
    'australian football': 'Australian Football',
    'esports': 'Esports',
    'darts': 'Darts',
    'snooker': 'Snooker',
    'cycling': 'Cycling',
}

# ─── Suffixes to strip when cleaning team names
TEAM_SUFFIXES = [
    r'\s+U\d+s?$',       # U18, U21, U23, U18s
    r'\s+Women$',         # Women
    r'\s+Ladies$',        # Ladies
    r'\s+\(W\)$',         # (W)
    r'\s+\(M\)$',         # (M)
    r'\s+Reserves$',      # Reserves
    r'\s+Youth$',         # Youth
    r'\s+II$',            # II (Second team)
    r'\s+B$',             # B (B team)
]

# ─── Common prefixes/suffixes to normalize for fuzzy matching
NOISE_WORDS = {'fc', 'afc', 'sc', 'cf', 'fk', 'bk', 'sk'}


def load_json(filepath):
    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def clean_team_name(name):
    """Remove U18, Women, etc. suffixes to find the 'base' team."""
    cleaned = name
    for suffix in TEAM_SUFFIXES:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def normalize_for_index(name):
    """Normalize a name for index lookup (must match build_teams_db.py)."""
    if not name:
        return ''
    n = re.sub(r'\s+', ' ', name.lower().strip())
    n = re.sub(r'[^\w\s]', '', n)
    return n.strip()


def normalize_deep(name):
    """
    Deeper normalization: remove FC/AFC/SC noise words, strip accents.
    Used for tier-3 matching.
    """
    n = normalize_for_index(name)
    # Remove noise words
    tokens = [t for t in n.split() if t not in NOISE_WORDS]
    return ' '.join(tokens)


def map_sport(schedule_sport):
    """Map schedule sport names to TSDB sport names."""
    if not schedule_sport:
        return None
    return SPORT_MAP.get(schedule_sport.lower(), schedule_sport)


def clean_for_api(name):
    """
    Clean team name specifically for API search.
    Removes: U18/U21/B/C team, standalone FC/Football Club.
    """
    if not name:
        return ""
    
    # 1. Remove generic suffixes (U18, etc.) - reuse existing logic
    cleaned = clean_team_name(name)
    
    # 2. Specific API cleaning rules (Case-insensitive)
    # Remove standalone "FC", "Football Club"
    # Note: \b ensures we don't match "AFC" or "FC" inside a word.
    # We want to match "Arsenal FC" -> "Arsenal", but not "AFC Wimbledon" -> "Wimbledon" (user said unless part of word like afc)
    # Wait, user said "unless fc is part of a word..like afc..so only rid of standalone fc".
    # \bFC\b matches " FC " but NOT "AFC". So \bFC\b is safe.
    
    # Patterns to remove:
    # - u18, u18s, u21, u21s
    # - b team, team b, c team, team c
    # - football club, fc
    
    patterns = [
        r'\b(?:u18s?|u21s?)\b',
        r'\b(?:[bc]\s+team|team\s+[bc])\b',
        r'\b(?:football\s+club|fc)\b'
    ]
    
    for pat in patterns:
        cleaned = re.sub(pat, '', cleaned, flags=re.IGNORECASE)
        
    # Cleanup extra spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


class TeamMatcher:
    """
    Multi-tier team name matcher with self-learning aliases.
    
    Usage:
        matcher = TeamMatcher('spdb_teams.json')
        result = matcher.find('Man Utd', sport='Soccer')
        matcher.save()  # Persist learned aliases
    """

    def __init__(self, db_path='spdb_teams.json'):
        self.db_path = db_path
        self.db = load_json(db_path)
        self.teams = self.db.get('teams', {})
        self.index = self.db.get('_index', {})
        self.learned_count = 0

        # Build sport-filtered sets for fuzzy matching
        self._sport_teams = {}
        for key, data in self.teams.items():
            sport = data.get('sport', '')
            if sport not in self._sport_teams:
                self._sport_teams[sport] = []
            self._sport_teams[sport].append(key)

        # Deep-normalized lookup for tier 3
        self._deep_index = {}
        for key in self.teams:
            deep = normalize_deep(key)
            if deep and deep not in self._deep_index:
                self._deep_index[deep] = key

    def find(self, name, sport=None):
        """
        Find team data for a given name.
        
        Returns: team data dict or None.
        
        Matching tiers:
          1. Exact key match
          2. Index match (alternates, keywords, short names, aliases)
          3. Normalized deep match (strip FC/AFC noise)
          4. Fuzzy match (SequenceMatcher >= 0.85, sport-scoped)
        """
        if not name or not name.strip():
            return None

        cleaned = clean_team_name(name)
        norm = normalize_for_index(cleaned)

        # ── Tier 1: Exact key match
        if cleaned in self.teams:
            return self.teams[cleaned]

        # ── Tier 2: Index match
        if norm in self.index:
            team_key = self.index[norm]
            if team_key in self.teams:
                return self.teams[team_key]

        # Also try the original name (before cleaning) in the index
        orig_norm = normalize_for_index(name)
        if orig_norm != norm and orig_norm in self.index:
            team_key = self.index[orig_norm]
            if team_key in self.teams:
                return self.teams[team_key]

        # ── Tier 3: Deep normalized match
        deep = normalize_deep(cleaned)
        if deep in self._deep_index:
            team_key = self._deep_index[deep]
            if team_key in self.teams:
                # Learn this alias for future instant matching
                self._learn_alias(team_key, cleaned)
                return self.teams[team_key]

        # ── Tier 3.5: Hyphenated match (User request)
        # Try replacing spaces with hyphens and vice versa
        hyphenated = cleaned.replace(' ', '-')
        dehyphenated = cleaned.replace('-', ' ')
        
        for variant in [hyphenated, dehyphenated]:
            if variant == cleaned:
                continue
                
            # Check basic index with variant
            v_norm = normalize_for_index(variant)
            if v_norm in self.index:
                 team_key = self.index[v_norm]
                 if team_key in self.teams:
                     self._learn_alias(team_key, cleaned)
                     return self.teams[team_key]
            
            # Check deep index with variant
            v_deep = normalize_deep(variant)
            if v_deep in self._deep_index:
                team_key = self._deep_index[v_deep]
                if team_key in self.teams:
                    self._learn_alias(team_key, cleaned)
                    return self.teams[team_key]

        # ── Tier 4: Fuzzy match (sport-scoped if possible)
        tsdb_sport = map_sport(sport) if sport else None
        candidates = self._get_candidates(tsdb_sport)

        best_score = 0.0
        best_key = None

        for candidate_key in candidates:
            candidate_norm = normalize_for_index(candidate_key)
            score = SequenceMatcher(None, norm, candidate_norm).ratio()

            if score > best_score:
                best_score = score
                best_key = candidate_key

        # User requested 90% threshold for auto-learning/matching
        THRESHOLD = 0.90
        
        if best_score >= THRESHOLD and best_key:
            # Auto-learn high-confidence matches
            self._learn_alias(best_key, cleaned)
            return self.teams[best_key]
            
        return None

    def _get_candidates(self, tsdb_sport):
        """Get candidate team keys, optionally filtered by sport."""
        if tsdb_sport and tsdb_sport in self._sport_teams:
            candidates = self._sport_teams[tsdb_sport]
            # If very few candidates in this sport, also check all
            if len(candidates) < 10:
                return list(self.teams.keys())
            return candidates
        return list(self.teams.keys())

    def _learn_alias(self, team_key, alias_name):
        """Learn a new alias for a team."""
        if team_key not in self.teams:
            return

        data = self.teams[team_key]
        aliases = data.get('aliases', [])
        norm_alias = alias_name.lower().strip()

        # Don't add if it's already known
        if norm_alias in [a.lower() for a in aliases]:
            return
        if norm_alias == team_key.lower():
            return

        aliases.append(norm_alias)
        data['aliases'] = aliases

        # Also add to live index
        idx_key = normalize_for_index(alias_name)
        if idx_key not in self.index:
            self.index[idx_key] = team_key

        self.learned_count += 1

    def save(self):
        """Save the database with any learned aliases."""
        if self.learned_count > 0:
            self.db['teams'] = self.teams
            self.db['_index'] = self.index
            save_json(self.db_path, self.db)
            print(f"  Saved {self.learned_count} new learned aliases.")

    def fetch_and_learn(self, name):
        """
        Tier 3 Fallback: Query API directly for the name.
        If found, add to DB and save.
        """
        cleaned_api = clean_for_api(name)
        if not cleaned_api:
            return None

        # Strategies to try
        search_terms = [cleaned_api]
        
        # Try hyphenated if spaces exist
        if ' ' in cleaned_api:
            search_terms.append(cleaned_api.replace(' ', '-'))

        API_BASE = "https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t="

        for term in search_terms:
            try:
                # Add delay to respect rate limits
                time.sleep(1.5)
                
                print(f"    API Fetching: {term}...")
                url = API_BASE + requests.utils.quote(term)
                response = requests.get(url, timeout=10)
                
                if response.status_code != 200:
                    print(f"    -> API Error: Status {response.status_code}")
                    continue
                    
                try:
                    data = response.json()
                except ValueError:
                    print(f"    -> API Error: Invalid JSON response (Rate limit?)")
                    continue
                
                if data and data.get("teams"):
                    # Use the first result
                    team_data = data["teams"][0]
                    found_name = team_data["strTeam"]
                    found_id = team_data["idTeam"]
                    
                    # Create entry
                    entry = {
                        "id": int(found_id) if found_id.isdigit() else found_id,
                        "api_id": found_id,
                        "name": found_name,
                        "short_name": team_data.get("strTeamShort") or "",
                        "alternates": [a.strip() for a in (team_data.get("strAlternate") or "").split(",") if a.strip()],
                        "keywords": [k.strip() for k in (team_data.get("strKeywords") or "").split(",") if k.strip()],
                        "logo_url": team_data.get("strTeamBadge"),
                        "banner_url": team_data.get("strTeamBanner"),
                        "league": team_data.get("strLeague") or "_No League",
                        "sport": team_data.get("strSport") or "Unknown",
                        "country": team_data.get("strCountry") or "Unknown",
                        "aliases": [] # Initialize empty
                    }

                    # Add to DB
                    self.teams[found_name] = entry
                    
                    # Index the found name
                    norm_found = normalize_for_index(found_name)
                    self.index[norm_found] = found_name
                    
                    # Index the search term/original name as an alias if different
                    # (This effectively "learns" the mapping)
                    if name != found_name:
                        self._learn_alias(found_name, name)
                    
                    print(f"    -> FOUND: {found_name} (ID: {found_id})")
                    self.learned_count += 1
                    return entry

            except Exception as e:
                print(f"    -> API Error for {term}: {e}")

        return None


if __name__ == "__main__":
    # Quick test
    matcher = TeamMatcher()
    test_names = [
        ("Arsenal", "Football"),
        ("Man Utd", "Football"),
        ("Roma", "Football"),
        ("Sheffield United", "Football"),
        ("Detroit Pistons", "Basketball"),
        ("Orlando Magic", "Basketball"),
        ("Al Hilal", "Football"),
        ("Villarreal", "Football"),
        ("Miami Heat", "Basketball"),
        ("Chelsea U18", "Football"),
        ("Crystal Palace U21", "Football"),
    ]

    print("Testing TeamMatcher:")
    print("-" * 60)
    for name, sport in test_names:
        result = matcher.find(name, sport)
        if result:
            print(f"  ✓ {name:25s} → {result['name']:30s} ({result.get('sport', '?')})")
        else:
            print(f"  ✗ {name:25s} → NOT FOUND")

    matcher.save()
