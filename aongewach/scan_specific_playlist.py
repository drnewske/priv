#!/usr/bin/env python3
"""
Manual Playlist Scanner (Auto-Target Mode)
1. Reads channels.json to find channels with null/missing IDs.
2. Scans a list of provided playlists for these specific channels.
3. Updates channels.json in-place with found streams and generated IDs.
"""

import json
import re
import requests
import os
import zlib
from difflib import SequenceMatcher
from collections import defaultdict

# ─── CONSTANTS ─────────────────────────────────────────────────────────────

# List of Playlist URLs to scan (comma-separated if needed, or just list)
PLAYLIST_URLS = [
    "http://example.com/playlist1.m3u",
    "http://example.com/playlist2.m3u",
]

# OPTIONAL: List of specific channel names to search for (even if they already have IDs)
# This allows you to force-scan for specific channels to find more streams/backups.
EXTRA_TARGETS = [
    # "Sky Sports Main Event",
    # "TNT Sports 1",
]

# ───────────────────────────────────────────────────────────────────────────

CHANNELS_FILE = 'channels.json'

class ChannelNormalizer:
    """Simple normalizer for matching."""
    def extract_quality(self, name):
        name_lower = name.lower()
        if re.search(r'\b(4k|uhd|2160p?)\b', name_lower): return '4K'
        elif re.search(r'\b(fhd|1080p?)\b', name_lower): return 'FHD'
        elif re.search(r'\b(hd|720p?)\b', name_lower): return 'HD'
        elif re.search(r'\b(sd|480p?|360p?)\b', name_lower): return 'SD'
        return 'HD'

def get_channel_id(name):
    """Generate stable ID."""
    return zlib.adler32(name.encode('utf-8')) & 0xffffffff

def load_channels(filepath):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_channels(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_missing_channels(db):
    """Identify channels with null IDs/qualities."""
    missing = []
    if not db or 'channels' not in db:
        return missing
    
    for name, data in db['channels'].items():
        # Condition for "missing": id is None OR qualities is None/Empty
        if data.get('id') is None or not data.get('qualities'):
            missing.append(name)
    
    return missing

def get_all_targets(db):
    """Combine missing channels + EXTRA_TARGETS."""
    targets = set(get_missing_channels(db))
    if 'channels' in db:
        for t in EXTRA_TARGETS:
            if t: targets.add(t)
    return sorted(list(targets))

def scan_single_playlist(url, target_map, db, normalizer):
    """Scan a single playlist URL for targets."""
    print(f"\nFetching playlist: {url}...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        lines = response.text.splitlines()
    except Exception as e:
        print(f"  Error fetching playlist: {e}")
        return 0

    print(f"  Parsing {len(lines)} lines...")
    
    parsed_streams = []
    current_info = {}
    
    for line in lines:
        line = line.strip()
        if not line: continue
        
        if line.startswith('#EXTINF:'):
            current_info = {}
            # Extract Logo
            logo_match = re.search(r'tvg-logo="([^"]*)"', line)
            if logo_match:
                current_info['logo'] = logo_match.group(1)
            
            # Extract Name
            name_match = re.search(r',([^,]*)$', line)
            if name_match:
                current_info['name'] = name_match.group(1).strip()
        
        elif not line.startswith('#'):
            if 'name' in current_info:
                current_info['url'] = line
                parsed_streams.append(current_info)
                current_info = {}

    print(f"  Found {len(parsed_streams)} streams. Searching for {len(target_map)} targets...")
    
    updates = 0
    for stream in parsed_streams:
        stream_name = stream.get('name', '')
        stream_lower = stream_name.lower()
        
        # Check against targets
        matched_target = None
        for t_lower, t_original in target_map.items():
            if t_lower in stream_lower:
                matched_target = t_original
                break
        
        if matched_target:
            quality = normalizer.extract_quality(stream_name)
            url = stream.get('url')
            logo = stream.get('logo')
            
            # Get node
            channel_node = db['channels'].get(matched_target)
            
            # If target came from EXTRA_TARGETS and doesn't exist in DB, create it
            if not channel_node:
                channel_node = {
                    'id': None,
                    'logo': None,
                    'qualities': {}
                }
                db['channels'][matched_target] = channel_node
            
            # Initialize if empty
            if channel_node.get('id') is None:
                channel_node['id'] = get_channel_id(matched_target)
            
            if channel_node.get('qualities') is None:
                channel_node['qualities'] = {}
            
            # Add URL
            if quality not in channel_node['qualities']:
                channel_node['qualities'][quality] = []
            
            if url not in channel_node['qualities'][quality]:
                channel_node['qualities'][quality].append(url)
                channel_node['qualities'][quality].sort()
                
                if not channel_node.get('logo') and logo:
                    channel_node['logo'] = logo
                
                print(f"    [UPDATE] {matched_target} -> Added {quality} stream.")
                updates += 1

    return updates

def scan_playlists():
    print(f"Loading {CHANNELS_FILE}...")
    db = load_channels(CHANNELS_FILE)
    if not db: return

    # Identify targets automatically + manual extras
    targets = get_all_targets(db)
    if not targets:
        print("No targets found (no null channels in DB + no EXTRA_TARGETS defined).")
        return

    print(f"Targeting {len(targets)} channels (Missing + Extras):")
    for t in targets[:10]:
        print(f"  - {t}")
    if len(targets) > 10: print(f"  ... and {len(targets)-10} more.")

    # Prepare lookup
    target_map = {t.lower(): t for t in targets}
    normalizer = ChannelNormalizer()
    total_updates = 0

    # Scan each playlist
    for url in PLAYLIST_URLS:
        if not url or url == "http://example.com/playlist1.m3u":
             continue
        total_updates += scan_single_playlist(url, target_map, db, normalizer)

    if total_updates > 0:
        save_channels(CHANNELS_FILE, db)
        print(f"\nSuccess! Updated {CHANNELS_FILE} with {total_updates} new streams.")
    else:
        print("\nNo updates made. Ensure PLAYLIST_URLS are set and contain the missing channels.")

if __name__ == "__main__":
    scan_playlists()
