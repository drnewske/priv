#!/usr/bin/env python3
"""
Xtream Sports Channel Scanner - Production Version
Fast AND accurate. Scans specific servers for matching channels.
"""

import json
import re
import requests
import argparse
import sys
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from difflib import SequenceMatcher
import concurrent.futures
import time
import zlib
import os

# Configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEDULE_FILE = os.path.join(SCRIPT_DIR, 'weekly_schedule.json')
if not os.path.exists(SCHEDULE_FILE):
    SCHEDULE_FILE = 'weekly_schedule.json'

# Hardcoded Server List
SERVERS = [
    {
        'url': 'http://tv.starsharetv.com:8080/get.php?username=7654321&password=1234567&type=m3u_plus&output=ts',
        'name': 'StarShare TV',
        'type': 'api'
    },
    {
        'url': 'http://abtvab@starshare.net:80/get.php?username=CVCVCVCV&password=CV1CV1CV1&type=m3u&output=mpegts',
        'name': 'StarShare Net',
        'type': 'api'
    },
    {
        'url': 'http://couchguy.club:80/get.php?username=Joshua754&password=Vk8KJG6VqFN3&type=m3u_plus&output=ts',
        'name': 'CouchGuy',
        'type': 'api'
    },
    {
        'url': 'http://Supersonictv.live:8080/get.php?username=Ramsey123&password=Ramsey123&type=m3u_plus&output=ts',
        'name': 'Supersonic TV',
        'type': 'api'
    },
    {
        'url': 'https://raw.githubusercontent.com/a1xmedia/m3u/refs/heads/main/a1x.m3u',
        'name': 'A1XM Public',
        'type': 'direct'
    }
]


class XtreamAPI:
    """Xtream Codes API client."""
    
    def __init__(self, url: str):
        """Parse credentials from M3U URL."""
        parsed = urlparse(url)
        
        # Try query parameters
        params = parse_qs(parsed.query)
        if 'username' in params and 'password' in params:
            self.username = params['username'][0]
            self.password = params['password'][0]
        else:
            # Try path-based format
            parts = [p for p in parsed.path.split('/') if p]
            if len(parts) >= 2:
                self.username = parts[0]
                self.password = parts[1]
            else:
                raise ValueError(f"Cannot parse credentials from URL: {url}")
        
        # Construct Base URL
        # IMPORTANT: Use netloc to preserve Basic Auth (user@host) if present
        scheme = parsed.scheme if parsed.scheme else "http"
        self.base_url = f"{scheme}://{parsed.netloc}"
        self.timeout = 30
    
    def _api_call(self, action: str, **params) -> Optional[List[Dict]]:
        """Make API call."""
        url = f"{self.base_url}/player_api.php?username={self.username}&password={self.password}&action={action}"
        for key, value in params.items():
            url += f"&{key}={value}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            # print(f"API Call Failed ({action}): {e}")
            return None
    
    def get_live_categories(self) -> List[Dict]:
        """Get all live categories."""
        result = self._api_call('get_live_categories')
        return result if result is not None else []
    
    def get_live_streams(self, category_id: Optional[int] = None) -> List[Dict]:
        """Get live streams, optionally filtered by category."""
        params = {'category_id': category_id} if category_id else {}
        result = self._api_call('get_live_streams', **params)
        return result if result is not None else []
    
    def get_stream_url(self, stream_id: int) -> str:
        """Build stream URL."""
        # Xtream codes usually exposes live streams at /live/username/password/stream_id.ts
        return f"{self.base_url}/live/{self.username}/{self.password}/{stream_id}.ts"


class ChannelNormalizer:
    """Normalize and group channel names."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        """Initialize with similarity threshold (0-1)."""
        self.similarity_threshold = similarity_threshold
        
        # Quality patterns
        self.quality_regex = re.compile(
            r'\b(4k|uhd|ultra\s*hd|2160p?|fhd|full\s*hd|1080p?|hd|720p?|sd|480p?|360p?|hevc|h\.?26[45])\b',
            re.IGNORECASE
        )
        
        # Junk patterns (compile once for speed)
        self.junk_patterns = [
            re.compile(r'[#\-_*+=:|~]{2,}'),  # Repeated chars
            re.compile(r'\[(?:vip|hd|fhd|4k|sd|server|backup|link|premium|multi|test|raw|direct|dn)\s*\d*\]', re.I),
            re.compile(r'\((?:vip|hd|fhd|4k|sd|server|backup|link|premium|multi|test|raw|direct|dn)\s*\d*\)', re.I),
            re.compile(r'^\s*[#\-_*+=:|~]+\s*'),  # Leading junk
            re.compile(r'\s*[#\-_*+=:|~]+\s*$'),  # Trailing junk
            re.compile(r'\s*\|+\s*'),  # Pipes
            re.compile(r'\s*-\s*'),     # Hyphens as separators
            re.compile(r'\b(ca|us|uk|de|fr|it|es|pt|br|ar|mx|tr|ru|nl|be|ch|at|pl|ro|bg|hr|rs|ba|mk|si|hu|cz|sk|ua|gr|cy|il|ae|sa|kw|qa|bh|om|lb|jo|eg|ma|dz|tn|ly|sd|sy|iq|ir|pk|in|bd|lk|np|mm|th|vn|la|kh|my|sg|id|ph|cn|tw|hk|mo|kp|kr|jp|au|nz)\s*[:-]\s*', re.I), # Country prefixes
        ]
    
    def extract_quality(self, name: str) -> str:
        """Extract quality tier from channel name."""
        name_lower = name.lower()
        
        if re.search(r'\b(4k|uhd|2160p?)\b', name_lower):
            return '4K'
        elif re.search(r'\b(fhd|1080p?)\b', name_lower):
            return 'FHD'
        elif re.search(r'\b(hd|720p?)\b', name_lower):
            return 'HD'
        elif re.search(r'\b(sd|480p?|360p?)\b', name_lower):
            return 'SD'
        else:
            return 'HD'  # Default
    
    def normalize(self, name: str) -> str:
        """Normalize channel name - remove quality and junk."""
        if not name:
            return ""
        
        # Remove quality indicators
        cleaned = self.quality_regex.sub(' ', name)
        
        # Remove junk patterns
        for pattern in self.junk_patterns:
            cleaned = pattern.sub(' ', cleaned)
        
        # Clean whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Remove empty brackets
        cleaned = re.sub(r'\[\s*\]|\(\s*\)|\{\s*\}', '', cleaned)
        
        # Final trim of special chars
        cleaned = cleaned.strip(' -_#|:;*+=~()[]{}.,\'"\\/')
        
        return cleaned
    
    def similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity between two strings."""
        return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()
    
    def find_match(self, name: str, existing_names: List[str]) -> Optional[str]:
        """Find best match in existing names (if similarity >= threshold)."""
        if not existing_names:
            return None
        
        best_match = None
        best_score = 0.0
        
        for existing in existing_names:
            score = self.similarity(name, existing)
            if score > best_score:
                best_score = score
                best_match = existing
        
        return best_match if best_score >= self.similarity_threshold else None


class SportsScanner:
    """Main scanner class."""
    
    def __init__(self, target_channels: List[str], match_limit: int = 5):
        """Initialize scanner with target channels and limit."""
        # Normalize targets for matching
        self.targets = [t.lower() for t in target_channels if t]
        self.match_limit = match_limit
        
        # Channel storage: {original_target_name: {matches: {quality: set()}, logo: str}}
        self.channels = defaultdict(lambda: {'qualities': defaultdict(set), 'logo': None})
        self.channel_counts = defaultdict(int) # Track count per target
        self.channel_ids = {}
        
        # Stats
        self.stats = {
            'servers_total': 0,
            'servers_success': 0,
            'servers_failed': 0,
            'categories_total': 0,
            'streams_total': 0,
            'channels_added': 0
        }
    
    def _get_channel_id(self, name: str) -> int:
        """Get or create STABLE channel ID (Hash of name)."""
        if name not in self.channel_ids:
            val = zlib.adler32(name.encode('utf-8')) & 0xffffffff
            self.channel_ids[name] = val
        return self.channel_ids[name]

    def _find_target_match(self, stream_name: str) -> Optional[str]:
        """
        Check if stream name contains any target channel (Substring Match).
        This is the inner loop hot-path.
        """
        name_lower = stream_name.lower()
        
        # Simple substring check against all targets
        # Optimizations could be done here if targets list is huge, 
        # but for ~200 targets and ~100k channels it's acceptable (~20M ops).
        for target in self.targets:
             if target in name_lower:
                 return target # Return the matched target string (lower)
        return None

    def _get_display_name(self, target_lower: str) -> str:
        """Recover display name (Title Case)."""
        return target_lower.title()

    def process_streams(self, streams: List[Dict], api_instance: Optional[XtreamAPI] = None):
        """Process a list of streams and match against targets."""
        found_in_batch = 0
        
        for stream in streams:
            stream_name = stream.get('name', '').strip()
            
            if not stream_name:
                continue
            
            # 1. Match Check
            matched_target_lower = self._find_target_match(stream_name)
            if not matched_target_lower:
                    continue
                    
            # 2. Check limit
            if self.channel_counts[matched_target_lower] >= self.match_limit:
                continue
                
            final_name = self._get_display_name(matched_target_lower)
            
            # 3. Extract Details
            # Use 'stream_icon' for API, 'logo' for M3U dict
            stream_logo = stream.get('stream_icon') or stream.get('logo')
            
            quality = ChannelNormalizer(0).extract_quality(stream_name)
            
            # 4. Get URL
            if api_instance:
                stream_id = stream.get('stream_id')
                if not stream_id: continue
                url = api_instance.get_stream_url(stream_id)
            else:
                # Direct M3U url
                url = stream.get('url')
                if not url: continue
            
            # 5. Store
            if url not in self.channels[final_name]['qualities'][quality]:
                self.channels[final_name]['qualities'][quality].add(url)
                found_in_batch += 1
                self.stats['channels_added'] += 1
                self.channel_counts[matched_target_lower] += 1
                
                # Add logo if missing
                if not self.channels[final_name]['logo'] and stream_logo:
                    self.channels[final_name]['logo'] = stream_logo
            
            # Ensure ID exists
            self._get_channel_id(final_name)
            
        return found_in_batch
    
    def scan_direct_m3u(self, url: str) -> Dict:
        """Scan a direct M3U file URL."""
        print(f"  > Starting scan: Direct M3U ({url})...", flush=True)
        result = {
            'success': False,
            'channels_added': 0,
            'error': None
        }
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            lines = response.text.splitlines()
            
            parsed_streams = []
            current_info = {}
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
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
                    # It's a URL
                    if 'name' in current_info:
                        current_info['url'] = line
                        parsed_streams.append(current_info)
                        current_info = {} # Reset
                        
            print(f"    - Parsed {len(parsed_streams)} streams from M3U. Matching...", flush=True)
            self.stats['streams_total'] += len(parsed_streams)
            
            added = self.process_streams(parsed_streams, api_instance=None)
            
            result['success'] = True
            result['channels_added'] = added
            print(f"  v Direct M3U - Done. Added {added} relevant channels.", flush=True)
            
        except Exception as e:
            result['error'] = str(e)
            print(f"  ! Direct M3U - Error: {e}", flush=True)
            
        return result

    def scan_server(self, server: Dict) -> Dict:
        """Scan a single server."""
        if server.get('type') == 'direct':
            return self.scan_direct_m3u(server['url'])

        print(f"  > Starting scan: {server.get('name', 'Unknown')}...", flush=True)
        result = {
            'name': server.get('name', 'Unknown'),
            'success': False,
            'channels_added': 0,
            'error': None
        }
        
        try:
            # Connect to API
            api = XtreamAPI(server['url'])
            
            # STRATEGY: Try to fetch ALL streams first (some providers support this)
            # If that fails or returns empty, fetch all categories and iterate.
            
            all_streams = []
            
            # Method A: Get All Streams
            print(f"    - Attempting to fetch full stream list...", flush=True)
            all_streams = api.get_live_streams(category_id=None)
            
            if all_streams:
                print(f"    - Success. Got {len(all_streams)} streams. Matching...", flush=True)
                self.stats['streams_total'] += len(all_streams)
                added = self.process_streams(all_streams, api_instance=api)
                
                result['success'] = True
                result['channels_added'] = added
                print(f"  v {server.get('name')} - Done. Added {added} channels.", flush=True)
                return result
                
            # Method B: Iterate Categories (Fallback)
            print(f"    - Full list fetch returned empty. Switching to Category Iteration...", flush=True)
            try:
                categories = api.get_live_categories()
            except Exception as e:
                result['error'] = f"Failed to get categories: {e}"
                print(f"  x {server.get('name')} - Error: {e}", flush=True)
                return result

            self.stats['categories_total'] += len(categories)
            print(f"    - Found {len(categories)} categories. Scanning ALL...", flush=True)
            
            total_added = 0
            
            # Scan ALL categories
            for cat in categories:
                cat_id = cat.get('category_id')
                if not cat_id: continue
                
                streams = api.get_live_streams(cat_id)
                if streams:
                    self.stats['streams_total'] += len(streams)
                    # Process batch
                    added = self.process_streams(streams, api_instance=api)
                    total_added += added
            
            result['success'] = True
            result['channels_added'] = total_added
            print(f"  v {server.get('name')} - Done. Added {total_added} channels.", flush=True)
            
        except Exception as e:
            result['error'] = str(e)
            print(f"  ! {server.get('name')} - Error: {e}", flush=True)
        
        return result
    
    def scan_all(self, servers: List[Dict], max_workers: int = 5) -> None:
        """Scan all provided servers."""
        self.stats['servers_total'] = len(servers)
        
        print(f"\n{'='*70}")
        print(f"Scanning {len(servers)} configured servers")
        print(f"{'='*70}\n", flush=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.scan_server, server): server for server in servers}
            
            for future in concurrent.futures.as_completed(futures):
                server = futures[future]
                try:
                    result = future.result()
                    if result['success']:
                        self.stats['servers_success'] += 1
                    else:
                        self.stats['servers_failed'] += 1
                except Exception as e:
                    print(f"Exception scanning {server.get('name')}: {e}")
                    self.stats['servers_failed'] += 1
                    
        print(f"--- Scan complete. ---", flush=True)

    def save(self, output_path: str) -> None:
        """Save results to JSON (Merge with existing)."""
        
        # 1. Load Existing Data to preserve Manual/Previous entries
        existing_data = {}
        if os.path.exists(output_path):
            try:
                with open(output_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load existing {output_path}: {e}")
        
        # Prepare Output Structure
        output = {
            'metadata': {
                'scan_date': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                'stats': self.stats,
                # Unique channels is sum of existing + new (deduplicated)
            },
            'channels': existing_data.get('channels', {})
        }
        
        # 2. Merge Scanned Channels into Output
        # We overwrite with NEW data if found, but keep old data if not found in this scan?
        # User said: "scanner will always include what it finds from the schedule...it should not rewrite the file...it should add to the file"
        # So if we found a channel, we update it. If we didn't find it, we leave it alone (it might be from a manual scan).
        
        for name, data in self.channels.items():
            # If we found streams for this channel
            if data['qualities']: 
                # Create/Update node
                if name not in output['channels']:
                     output['channels'][name] = {
                         'id': self.channel_ids[name],
                         'logo': None,
                         'qualities': {}
                     }
                
                # Update Qualities
                qs = {}
                for quality in ['SD', 'HD', 'FHD', '4K']:
                    if quality in data['qualities']:
                        qs[quality] = sorted(list(data['qualities'][quality]))
                
                output['channels'][name]['qualities'] = qs
                output['channels'][name]['id'] = self.channel_ids[name] # Ensure ID is set
                
                # Update Logo if we have a new one and old is empty
                if data['logo'] and not output['channels'][name].get('logo'):
                    output['channels'][name]['logo'] = data['logo']

        # 3. Add Missing Channels from Schedule (as nulls, ONLY if not already in DB)
        # We don't want to overwrite a valid manual channel with a null just because it wasn't in this specific scan.
        found_names_lower = set(k.lower() for k in output['channels'].keys())
        
        missing_count = 0
        for target in self.targets:
            if target.lower() not in found_names_lower:
                # Truly new/missing
                display_name = target.title()
                output['channels'][display_name] = {
                    'id': None,
                    'logo': None,
                    'qualities': None 
                }
                missing_count += 1

        output['metadata']['unique_channels'] = len(output['channels'])

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        # Console Reporting
        print(f"\n{'='*70}", flush=True)
        print(f"✅ Saved merged results to {output_path}", flush=True)
        print(f"  Total Channels in DB: {len(output['channels'])}", flush=True)
        print(f"  New/Updated in this scan: {len(self.channels)}", flush=True)
        print(f"  Missing (Added as null): {missing_count}", flush=True)
        print(f"{'='*70}\n", flush=True)


def load_target_channels(schedule_file):
    """Load unique channel names from the weekly schedule."""
    try:
        with open(schedule_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        targets = set()
        for day in data.get('schedule', []):
            for event in day.get('events', []):
                for channel in event.get('channels', []):
                    if channel:
                        clean_name = channel.strip()
                        # Specific fix for Laliga TV
                        if clean_name.lower() == 'laligatv':
                            clean_name = 'Laliga Tv'
                        targets.add(clean_name)
        
        print(f"Loaded {len(targets)} unique target channels from schedule.")
        return list(targets)
    except Exception as e:
        print(f"Error loading schedule: {e}")
        return []

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output_file', nargs='?', default='channels.json', help='Output JSON file')
    args = parser.parse_args()
    
    # 1. Load Targets
    targets = load_target_channels(SCHEDULE_FILE)
    if not targets:
        print("No target channels found. Exiting.")
        sys.exit(1)

    # 2. Init Scanner
    scanner = SportsScanner(target_channels=targets, match_limit=5)
    
    # 3. Run Scan
    scanner.scan_all(SERVERS, max_workers=5)
    
    # 4. Save
    scanner.save(args.output_file)


if __name__ == '__main__':
    main()
