#!/usr/bin/env python3
"""
Xtream Sports Channel Scanner - Production Version
Fast AND accurate. Uses category filtering + smart fuzzy matching.
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

# Configuration
SCHEDULE_FILE = 'weekly_schedule.json'



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
        
        port = parsed.port or 80
        # Handle cases where scheme is missing or incorrect in specific ways if needed
        scheme = parsed.scheme if parsed.scheme else "http"
        self.base_url = f"{scheme}://{parsed.hostname}:{port}"
        self.timeout = 20
    
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


class SportsFilter:
    """Filter sports categories and channels."""
    
    # Sports category keywords - Updated based on user request
    SPORTS_KEYWORDS = {
        'sport', 'espn', 'fox', 'sky', 'bein', 'dazn', 'dstv', 'supersport',
        'soccer', 'football', 'basketball', 'tennis', 'cricket', 'rugby',
        'golf', 'boxing', 'mma', 'ufc', 'wrestling', 'wwe', 'f1', 'formula',
        'motogp', 'nascar', 'hockey', 'baseball', 'nba', 'nfl', 'nhl', 'mlb',
        'deportes', 'deportivo', 'calcio', 'fussball', 'arena', 'tsn', 'optus',
        'astro', 'star', 'sony', 'ten', 'willow', 'pl', 'premier league',
        'la liga', 'bundesliga', 'serie a', 'ligue 1', 'ucl', 'uefa', 'fifa'
    }
    
    # NON-sports keywords (higher priority)
    EXCLUDE_KEYWORDS = {
        'movie', 'film', 'cinema', 'series', 'drama', 'comedy', 'kids',
        'anime', 'cartoon', 'music', 'news', 'entertainment', 'adult',
        'xxx', '18+', 'porn', 'religion', 'documentary', 'vod'
    }
    
    @classmethod
    def is_sports_category(cls, category_name: str) -> bool:
        """Check if category is sports-related."""
        if not category_name:
            return False
            
        name_lower = category_name.lower()
        
        # Exclude non-sports first
        if any(kw in name_lower for kw in cls.EXCLUDE_KEYWORDS):
            return False
        
        # Check sports keywords
        return any(kw in name_lower for kw in cls.SPORTS_KEYWORDS)


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
        # We map back to the Target Name from the schedule
        self.channels = defaultdict(lambda: {'qualities': defaultdict(set), 'logo': None})
        self.channel_counts = defaultdict(int) # Track count per target
        self.channel_ids = {}
        
        # Stats
        self.stats = {
            'servers_total': 0,
            'servers_success': 0,
            'servers_failed': 0,
            'categories_total': 0,
            'sports_categories': 0,
            'channels_checked': 0,
            'channels_added': 0
        }
    
    def _get_channel_id(self, name: str) -> int:
        """Get or create STABLE channel ID (Hash of name)."""
        if name not in self.channel_ids:
            val = zlib.adler32(name.encode('utf-8')) & 0xffffffff
            self.channel_ids[name] = val
        return self.channel_ids[name]

    def _find_target_match(self, stream_name: str) -> Optional[str]:
        """Check if stream name contains any target channel (Substring Match)."""
        name_lower = stream_name.lower()
        # Strictly look for the target phrase in the stream name
        # We iterate our targets. This could be O(N*M) so rely on optimization if needed
        # but N (targets) is small (~100-200) and M (stream length) is small.
        for target in self.targets:
             if target in name_lower:
                 return target # Return the matched target string (lower)
        return None

    def _get_display_name(self, target_lower: str) -> str:
        """Recover display name (Title Case) - simplified."""
        # In a real impl we might map lower->original, but for now Title Case is fine
        # or we could store the original map.
        # Actually, let's just title case it to look nice, or keep it simple.
        return target_lower.title()

    
    def scan_direct_m3u(self, url: str) -> Dict:
        """Scan a direct M3U file URL."""
        print(f"  > Starting scan: Direct M3U ({url})...", flush=True)
        result = {
            'name': 'Direct M3U',
            'domain': 'direct_m3u',
            'success': False,
            'channels_added': 0,
            'error': None
        }
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            lines = response.text.splitlines()
            
            channels_found = 0
            
            # Simple M3U parser
            current_info = {}
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('#EXTINF:'):
                    # Reset info
                    current_info = {}
                    
                    # Extract Logo
                    logo_match = re.search(r'tvg-logo="([^"]*)"', line)
                    if logo_match:
                        current_info['logo'] = logo_match.group(1)
                    
                    # Extract Name (last part after comma)
                    name_match = re.search(r',([^,]*)$', line)
                    if name_match:
                        current_info['name'] = name_match.group(1).strip()
                
                elif not line.startswith('#'):
                    # It's a URL
                    if 'name' in current_info:
                        stream_name = current_info['name']
                        stream_url = line
                        stream_logo = current_info.get('logo')
                        
                        # Strict Match Check
                        matched_target_lower = self._find_target_match(stream_name)
                        if not matched_target_lower:
                            continue

                        # Check limit
                        if self.channel_counts[matched_target_lower] >= self.match_limit:
                            continue
                        
                        # Use title-cased target as key
                        final_name = self._get_display_name(matched_target_lower)
                        
                        # Get quality (using existing normalizer helper or logic)
                        # We can instantiate normalizer locally or just use regex
                        quality = ChannelNormalizer(0).extract_quality(stream_name)
                        
                        # Add to channels
                        if stream_url not in self.channels[final_name]['qualities'][quality]:
                            self.channels[final_name]['qualities'][quality].add(stream_url)
                            channels_found += 1
                            self.stats['channels_added'] += 1
                            self.channel_counts[matched_target_lower] += 1
                            
                            # Add logo if missing
                            if not self.channels[final_name]['logo'] and stream_logo:
                                self.channels[final_name]['logo'] = stream_logo
                        
                        # Ensure ID exists
                        self._get_channel_id(final_name)
                        
            result['success'] = True
            result['channels_added'] = channels_found
            print(f"  v Direct M3U - Done. Found {channels_found} channels", flush=True)
            
        except Exception as e:
            result['error'] = str(e)
            print(f"  ! Direct M3U - Error: {e}", flush=True)
            
        return result

    def scan_server(self, server: Dict) -> Dict:
        """Scan a single server."""
        if server.get('type') == 'direct':
            return self.scan_direct_m3u(server['url'])

        print(f"  > Starting scan: {server.get('domain', 'Unknown')}...", flush=True)
        result = {
            'name': server.get('name', 'Unknown'),
            'domain': server.get('domain', 'Unknown'),
            'success': False,
            'channels_added': 0,
            'error': None
        }
        
        try:
            # Connect to API
            api = XtreamAPI(server['url'])
            
            # Get categories
            try:
                categories = api.get_live_categories()
            except Exception as e:
                # Catch connection errors early
                print(f"  x {server.get('domain')} - Connection Failed: {e}", flush=True)
                result['error'] = str(e)
                return result

            self.stats['categories_total'] += len(categories)
            
            # Filter sports categories
            sports_categories = [
                cat for cat in categories
                if cat.get('category_name') and 
                   SportsFilter.is_sports_category(cat['category_name'])
            ]
            
            self.stats['sports_categories'] += len(sports_categories)
            
            if not sports_categories:
                result['error'] = 'No sports categories'
                print(f"  x {server.get('domain')} - No sports categories", flush=True)
                return result
            
            print(f"    - Found {len(sports_categories)} sports categories in {server.get('domain')}. Scanning streams...", flush=True)
            
            # Get streams from each sports category
            channels_found = 0
            for i, category in enumerate(sports_categories, 1):
                cat_id = category.get('category_id')
                if not cat_id:
                    continue
                
                try:
                    streams = api.get_live_streams(cat_id)
                except Exception:
                    continue
                    
                self.stats['channels_checked'] += len(streams)
                
                for stream in streams:
                    stream_id = stream.get('stream_id')
                    stream_name = stream.get('name', '').strip()
                    stream_icon = stream.get('stream_icon')
                    
                    if not stream_id or not stream_name:
                        continue
                    
                    # Strict Match Check
                    matched_target_lower = self._find_target_match(stream_name)
                    if not matched_target_lower:
                         continue
                         
                    # Check limit
                    if self.channel_counts[matched_target_lower] >= self.match_limit:
                        continue
                        
                    final_name = self._get_display_name(matched_target_lower)
                    
                    # Get quality
                    quality = ChannelNormalizer(0).extract_quality(stream_name)
                    
                    # Build URL
                    url = api.get_stream_url(stream_id)
                    
                    # Add to channels
                    if url not in self.channels[final_name]['qualities'][quality]:
                        self.channels[final_name]['qualities'][quality].add(url)
                        channels_found += 1
                        self.stats['channels_added'] += 1
                        self.channel_counts[matched_target_lower] += 1
                        
                        # Add logo if missing
                        if not self.channels[final_name]['logo'] and stream_icon:
                            self.channels[final_name]['logo'] = stream_icon
                    
                    # Ensure ID exists
                    self._get_channel_id(final_name)
            
            result['success'] = True
            result['channels_added'] = channels_found
            print(f"  v {server.get('domain')} - Done. Found {channels_found} channels", flush=True)
            
        except Exception as e:
            result['error'] = str(e)
            print(f"  ! {server.get('domain')} - Error: {e}", flush=True)
        
        return result
    
    def scan_selected_servers(self, servers: List[Dict], max_workers: int = 5) -> None:
        """Scan a fixed list of servers."""
        self.stats['servers_total'] = len(servers)
        
        print(f"\n{'='*70}")
        print(f"Scanning {len(servers)} playlists (Priority + Top 20) with max {max_workers} threads")
        print(f"{'='*70}\n", flush=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.scan_server, server): server for server in servers}
            
            for future in concurrent.futures.as_completed(futures):
                server = futures[future]
                try:
                    result = future.result()
                    if result['success'] and result['channels_added'] > 0:
                        self.stats['servers_success'] += 1
                    else:
                        self.stats['servers_failed'] += 1
                except Exception as e:
                    print(f"Exception scanning {server.get('name')}: {e}")
                    self.stats['servers_failed'] += 1
                    
        print(f"--- Scan complete. ---", flush=True)

    def save(self, output_path: str) -> None:
        """Save results to JSON."""
        output = {
            'metadata': {
                'scan_date': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                'servers_total': self.stats['servers_total'],
                'servers_success': self.stats['servers_success'],
                'servers_failed': self.stats['servers_failed'],
                'categories_total': self.stats['categories_total'],
                'sports_categories': self.stats['sports_categories'],
                'channels_checked': self.stats['channels_checked'],
                'unique_channels': len(self.channels),
                'total_links': self.stats['channels_added']
            },
            'channels': {}
        }
        
        # Build output
        for name in sorted(self.channels.keys()):
            channel_data = self.channels[name]
            
            # Sort qualities
            qualities = {}
            for quality in ['SD', 'HD', 'FHD', '4K']:
                if quality in channel_data['qualities']:
                    # Just the URL string list, no source object
                    qualities[quality] = sorted(list(channel_data['qualities'][quality]))
            
            if qualities: # Only add if we have content
                 output['channels'][name] = {
                    'id': self.channel_ids[name],
                    'logo': channel_data['logo'],
                    'qualities': qualities
                }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*70}", flush=True)
        print(f"✅ Saved {len(output['channels'])} unique channels → {output_path}", flush=True)
        print(f"{'='*70}", flush=True)
        print(f"Statistics:", flush=True)
        print(f"  Servers: {self.stats['servers_success']} successful", flush=True)
        print(f"  Categories: {self.stats['sports_categories']} sports categories scanned", flush=True)
        print(f"  Channels: {len(output['channels'])} unique ({self.stats['channels_added']} total links)", flush=True)
        print(f"{'='*70}\n", flush=True)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Scan sports channels from playlists.')
    parser.add_argument('input_file', nargs='?', default='lovestory.json', help='Input JSON file with playlists')
    parser.add_argument('output_file', nargs='?', default='channels.json', help='Output JSON file')
    parser.add_argument('--limit', type=int, help='Limit number of playlists to scan (for testing)')
    parser.add_argument('--domains', type=str, help='Comma-separated list of priority domains to scan')
    return parser.parse_args()


def load_target_channels(schedule_file):
    """Load unique channel names from the weekly schedule."""
    try:
        with open(schedule_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        targets = set()
        for day in data.get('schedule', []):
            for event in day.get('events', []):
                for channel in event.get('channels', []):
                    # We strip and maybe should be careful about splitting?
                    # The user said strict match. Schedule has "Sky Sports Main Event".
                    # We just take the full string.
                    if channel:
                         targets.add(channel.strip())
        
        print(f"Loaded {len(targets)} unique target channels from schedule.")
        return list(targets)
    except Exception as e:
        print(f"Error loading schedule: {e}")
        return []

def main():
    args = parse_arguments()
    
    # Load targets
    targets = load_target_channels(SCHEDULE_FILE)
    if not targets:
        print("No target channels found. Exiting.")
        sys.exit(1)

    try:
        with open(args.input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Input file '{args.input_file}' not found.")
        sys.exit(1)
    
    all_servers = []
    
    # Add external source (User Request)
    all_servers.append({
        'url': 'https://raw.githubusercontent.com/a1xmedia/m3u/refs/heads/main/a1x.m3u',
        'name': 'A1XM Public',
        'domain': 'a1xmedia.github.io',
        'channel_count': 9999999, # HIGHEST priority
        'type': 'direct'
    })
    
    for item in data.get('featured_content', []):
        if item.get('type') == 'm3u' and item.get('url'):
            all_servers.append({
                'url': item['url'],
                'name': item.get('name', 'Unknown'),
                'domain': item.get('domain', 'Unknown'),
                'channel_count': int(item.get('channel_count', 0))
            })
    
    if not all_servers:
        print("No M3U playlists found in input file.")
        sys.exit(1)
        
    print(f"Found {len(all_servers)} total playlists.")
    
    # STRATEGY: 
    # 1. Identify Priorities (External URL)
    # 2. Sort Rest by Channel Count desc
    # 3. Take Top 20 of Rest
    # 4. Combine
    
    priority_url = "a1xmedia.github.io"
    priorities = [s for s in all_servers if s.get('domain') == priority_url]
    others = [s for s in all_servers if s.get('domain') != priority_url]
    
    # Sort others by channel count
    others.sort(key=lambda x: x['channel_count'], reverse=True)
    
    # Take Top 20
    top_20 = others[:20]
    
    final_servers = priorities + top_20
    
    print(f"Selected {len(final_servers)} playlists for scanning:")
    print(f"  - {len(priorities)} priority ({priority_url})")
    print(f"  - {len(top_20)} top playlists (by count)")
    
    if not final_servers:
        print("No servers selected for scanning.")
        sys.exit(0)
    
    # Scan with Strict Targets and Limit
    scanner = SportsScanner(target_channels=targets, match_limit=5)
    
    scanner.scan_selected_servers(final_servers, max_workers=5)
    scanner.save(args.output_file)


if __name__ == '__main__':
    main()
