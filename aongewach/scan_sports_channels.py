#!/usr/bin/env python3
"""
Xtream Sports Channel Scanner - Production Version
Fast AND accurate. Scans playlists from lovestory.json for matching channels.
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
LOVESTORY_FILE = os.path.join(SCRIPT_DIR, '..', 'lovestory.json')

if not os.path.exists(SCHEDULE_FILE):
    SCHEDULE_FILE = 'weekly_schedule.json'

# Hardcoded Extra Server (Always included)
EXTRA_SERVERS = [
    {
        'url': 'https://raw.githubusercontent.com/a1xmedia/m3u/refs/heads/main/a1x.m3u',
        'name': 'A1XM Public',
        'type': 'direct'
    }
]

def load_servers_from_lovestory(file_path: str) -> List[Dict]:
    """Load servers from lovestory.json featured_content."""
    servers = []
    
    # Add hardcoded extras first
    servers.extend(EXTRA_SERVERS)
    
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found. Using only hardcoded servers.")
        return servers
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        featured = data.get('featured_content', [])
        for item in featured:
            url = item.get('url')
            if not url:
                continue
                
            # Determine type
            # If it ends in .m3u or .m3u8 it might be direct, but the existing code 
            # treats xtream api urls specially.
            # Most entries in lovestory.json seem to be Xtream codes API urls 
            # (e.g. contains username=...&password=...)
            # We'll assume 'api' unless it looks like a direct file list without params, 
            # but the scanner handles both.
            # The existing scanner distinguishes based on 'type' field in the server dict,
            # or we can infer it.
            
            # The item in lovestory.json has a "type": "m3u" field.
            # But the URLs are API urls like .../get.php?username=...
            # The scanner's "scan_server" checks if type == 'direct'.
            # If the URL has username/password params, it's likely an API that returns M3U.
            # The XtreamAPI class handles these URLs.
            
            # Let's map lovestory "type" to our scanner "type".
            # Lovestory "type" is usually "m3u".
            # If the URL looks like a get.php API call, we treat it as 'api' (which downloads m3u or uses player_api)
            # Actually, the previous hardcoded list had 'type': 'api'.
            # Users directive: "replace hardcoded links with data dynamically loaded"
            
            server_type = 'api'
            # If it's a raw github url or doesn't have credentials in query, maybe direct?
            if 'githubusercontent.com' in url or 'raw' in url:
                server_type = 'direct'
            
            servers.append({
                'url': url,
                'name': item.get('name', 'Unknown'),
                'type': server_type
            })
            
        print(f"Loaded {len(servers)} servers from lovestory.json (+ {len(EXTRA_SERVERS)} static).")
        
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        
    return servers

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
                # Fallback for non-standard URLs or lack of creds
                self.username = ""
                self.password = ""
                # print(f"Warning: Cannot parse credentials from URL: {url}")
        
        # Construct Base URL
        # IMPORTANT: Use netloc to preserve Basic Auth (user@host) if present
        scheme = parsed.scheme if parsed.scheme else "http"
        self.base_url = f"{scheme}://{parsed.netloc}"
        self.timeout = 30
    
    def _api_call(self, action: str, **params) -> Optional[List[Dict]]:
        """Make API call."""
        if not self.username or not self.password:
            return None

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
        if not self.username or not self.password:
             return ""
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
        
        if re.search(r'\\b(4k|uhd|2160p?)\\b', name_lower):
            return '4K'
        elif re.search(r'\\b(fhd|1080p?)\\b', name_lower):
            return 'FHD'
        elif re.search(r'\\b(hd|720p?)\\b', name_lower):
            return 'HD'
        elif re.search(r'\\b(sd|480p?|360p?)\\b', name_lower):
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
        cleaned = re.sub(r'\\s+', ' ', cleaned).strip()
        
        # Remove empty brackets
        cleaned = re.sub(r'\\[\\s*\\]|\\(\\s*\\)|\\{\\s*\\}', '', cleaned)
        
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
    
    def __init__(self, target_channels: List[str]):
        """Initialize scanner with target channels."""
        # Normalize targets for matching while preserving stable display names.
        self.target_display_names = {}
        for name in target_channels:
            if not name:
                continue
            cleaned = name.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key not in self.target_display_names:
                self.target_display_names[key] = cleaned
        self.targets = list(self.target_display_names.keys())
        
        # Channel storage: {original_target_name: {qualities: {quality: set()}, logo: str}}
        self.channels = defaultdict(lambda: {'qualities': defaultdict(set), 'logo': None})
        self.channel_ids = {}
        self.normalizer = ChannelNormalizer(0)
        
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
        for target in self.targets:
             if target in name_lower:
                 return target # Return the matched target string (lower)
        return None

    def _get_display_name(self, target_lower: str) -> str:
        """Recover display name using original schedule casing when available."""
        return self.target_display_names.get(target_lower, target_lower.title())

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
            final_name = self._get_display_name(matched_target_lower)
            
            # 2. Extract Details
            # Use 'stream_icon' for API, 'logo' for M3U dict
            stream_logo = stream.get('stream_icon') or stream.get('logo')
            
            quality = self.normalizer.extract_quality(stream_name)
            
            # 3. Get URL
            if api_instance:
                stream_id = stream.get('stream_id')
                if not stream_id: continue
                url = api_instance.get_stream_url(stream_id)
            else:
                # Direct M3U url
                url = stream.get('url')
                if not url: continue
            
            # 4. Store
            if url not in self.channels[final_name]['qualities'][quality]:
                self.channels[final_name]['qualities'][quality].add(url)
                found_in_batch += 1
                self.stats['channels_added'] += 1
                
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
        
        print(f"\\n{'='*70}")
        print(f"Scanning {len(servers)} configured servers")
        print(f"{'='*70}\\n", flush=True)

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
        existing_name_by_lower = {name.lower(): name for name in output['channels'].keys()}
        
        # 2. Merge Scanned Channels into Output
        for name, data in self.channels.items():
            # If we found streams for this channel
            if data['qualities']: 
                canonical_name = existing_name_by_lower.get(name.lower(), name)

                # Create/Update node
                if canonical_name not in output['channels']:
                     output['channels'][canonical_name] = {
                         'id': self._get_channel_id(canonical_name),
                         'logo': None,
                         'qualities': {}
                     }
                     existing_name_by_lower[canonical_name.lower()] = canonical_name
                
                # Update Qualities
                qs = output['channels'][canonical_name].get('qualities') or {}
                
                for quality in ['SD', 'HD', 'FHD', '4K']:
                    new_urls = data['qualities'].get(quality, set())
                    if new_urls:
                        # Merge with existing
                        existing_urls = set(qs.get(quality, []))
                        merged_urls = existing_urls.union(new_urls)
                        qs[quality] = sorted(list(merged_urls))
                
                output['channels'][canonical_name]['qualities'] = qs
                if output['channels'][canonical_name].get('id') is None:
                    output['channels'][canonical_name]['id'] = self._get_channel_id(canonical_name)
                
                # Update Logo if we have a new one and old is empty
                if data['logo'] and not output['channels'][canonical_name].get('logo'):
                    output['channels'][canonical_name]['logo'] = data['logo']

        # 3. Add missing channels from the current schedule as placeholders.
        # Existing channels are never removed.
        found_names_lower = set(existing_name_by_lower.keys())
        
        missing_count = 0
        for target_lower in self.targets:
            if target_lower not in found_names_lower:
                # Truly new/missing
                display_name = self._get_display_name(target_lower)
                output['channels'][display_name] = {
                    'id': None,
                    'logo': None,
                    'qualities': None 
                }
                existing_name_by_lower[target_lower] = display_name
                missing_count += 1

        output['metadata']['unique_channels'] = len(output['channels'])

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        # Console Reporting
        print(f"\\n{'='*70}", flush=True)
        print(f"[OK] Saved merged results to {output_path}", flush=True)
        print(f"  Total Channels in DB: {len(output['channels'])}", flush=True)
        print(f"  New/Updated in this scan: {len(self.channels)}", flush=True)
        print(f"  Missing (Added as null): {missing_count}", flush=True)
        print(f"{'='*70}\\n", flush=True)


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
                        if clean_name.lower() == 'laligatv': clean_name = 'Laliga Tv'
                        targets.add(clean_name)
        
        print(f"Loaded {len(targets)} unique target channels from schedule.")
        return list(targets)
    except Exception as e:
        print(f"Error loading schedule: {e}")
        return []

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output_file', nargs='?', default='channels.json', help='Output JSON file')
    parser.add_argument('--verify', action='store_true', help='Use only first 3 servers for verification')
    args = parser.parse_args()
    
    # 1. Load Targets
    targets = load_target_channels(SCHEDULE_FILE)
    if not targets:
        print("No target channels found. Exiting.")
        sys.exit(1)

    # 2. Load Servers
    servers = load_servers_from_lovestory(LOVESTORY_FILE)
    if args.verify:
         print("VERIFY MODE: Limiting to first 3 servers + Github")
         # We want to keep the git link (usually first in list from load_servers if we put it there, 
         # but actually we prepended it to servers list in load_servers_from_lovestory).
         # So taking the first 4 (1 static + 3 dynamic) roughly matches the "verify with just three links" request.
         # Or strictly following "verify with just three links from the json"
         
         # The servers list starts with EXTRA_SERVERS (1 item).
         # Then appends items from json.
         # We'll stick to 1 (git) + 3 (json) = 4 total.
         servers = servers[:4]
         
    # 3. Init Scanner (unlimited per-channel stream collection)
    scanner = SportsScanner(target_channels=targets)
    
    # 4. Run Scan
    scanner.scan_all(servers, max_workers=5)
    
    # 5. Save
    scanner.save(args.output_file)


if __name__ == '__main__':
    main()

