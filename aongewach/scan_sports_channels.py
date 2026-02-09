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
import zlib  # Added for stable ID hashing


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
    
    def __init__(self, similarity_threshold: float = 0.85):
        """Initialize scanner."""
        self.normalizer = ChannelNormalizer(similarity_threshold)
        
        # Channel storage: {normalized_name: {matches: {quality: set()}, logo: str}}
        self.channels = defaultdict(lambda: {'qualities': defaultdict(set), 'logo': None})
        self.channel_ids = {}
        # self.next_id = 1  # Removed in favor of stable hashing
        
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
    
    def _get_channel_id(self, normalized_name: str) -> int:
        """Get or create STABLE channel ID (Hash of name)."""
        if normalized_name not in self.channel_ids:
            # Use adler32 for deterministic ID
            # & 0xffffffff ensures it's unsigned
            val = zlib.adler32(normalized_name.encode('utf-8')) & 0xffffffff
            self.channel_ids[normalized_name] = val
        return self.channel_ids[normalized_name]
    
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
                    
                    # Extract Group
                    group_match = re.search(r'group-title="([^"]*)"', line)
                    if group_match:
                        current_info['group'] = group_match.group(1)
                    
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
                        
                        # Normalize name
                        normalized = self.normalizer.normalize(stream_name)
                        if not normalized or len(normalized) < 3:
                            continue
                        
                        # Find match
                        existing_names = list(self.channels.keys())
                        matched = self.normalizer.find_match(normalized, existing_names)
                        final_name = matched or normalized
                        
                        # Get quality
                        quality = self.normalizer.extract_quality(stream_name)
                        
                        # Add to channels
                        if stream_url not in self.channels[final_name]['qualities'][quality]:
                            self.channels[final_name]['qualities'][quality].add(stream_url)
                            channels_found += 1
                            self.stats['channels_added'] += 1
                            
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
                    
                    # Normalize name
                    normalized = self.normalizer.normalize(stream_name)
                    if not normalized or len(normalized) < 3: # Skip very short names
                        continue
                    
                    # Find similar channel
                    existing_names = list(self.channels.keys())
                    matched = self.normalizer.find_match(normalized, existing_names)
                    final_name = matched or normalized
                    
                    # Get quality
                    quality = self.normalizer.extract_quality(stream_name)
                    
                    # Build URL
                    url = api.get_stream_url(stream_id)
                    
                    # Add to channels
                    if url not in self.channels[final_name]['qualities'][quality]:
                        self.channels[final_name]['qualities'][quality].add(url)
                        channels_found += 1
                        self.stats['channels_added'] += 1
                        
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
    
    def scan_for_success(self, servers: List[Dict], target_success: int = 20, max_workers: int = 5) -> None:
        """Scan servers until target success count is reached."""
        self.stats['servers_total'] = len(servers)
        
        print(f"\n{'='*70}")
        print(f"Scanning for {target_success} working servers (max {max_workers} parallel)")
        print(f"{'='*70}\n", flush=True)
        
        success_count = 0
        
        # Process in batches to respect the target limit efficiently
        # We don't want to spin up 100 threads if the first 20 work.
        batch_size = max_workers
        
        for i in range(0, len(servers), batch_size):
            if success_count >= target_success:
                print(f"\nReached target of {target_success} successful scans. Stopping.", flush=True)
                break
                
            batch = servers[i : i + batch_size]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {executor.submit(self.scan_server, server): server for server in batch}
                
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result['success'] and result['channels_added'] > 0:
                        success_count += 1
                        self.stats['servers_success'] += 1
                    else:
                        self.stats['servers_failed'] += 1
                        
            print(f"--- Batch complete. Successes so far: {success_count}/{target_success} ---", flush=True)

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


def main():
    args = parse_arguments()
    
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
        'channel_count': 99999, # High priority
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
    
    # 1. Filter by Priority Domains
    priority_domains = set()
    if args.domains:
        priority_domains = {d.strip().lower() for d in args.domains.split(',') if d.strip()}
    
    # Separate into priority and others
    priorities = []
    others = []
    
    for server in all_servers:
        domain = server.get('domain', '').lower()
        if server.get('type') == 'direct' or any(pd in domain for pd in priority_domains):
            priorities.append(server)
        else:
            others.append(server)
            
    # 2. Sort others by channel count (descending)
    others.sort(key=lambda x: x['channel_count'], reverse=True)
    
    # 3. Combine - PRIORITIES FIRST, then the rest of the sorted list
    # We do NOT slice top 20 here anymore, because we want to scan UNTIL we hit 20 successes
    final_servers = priorities + others
    
    # Remove duplicates (based on URL) just in case
    unique_servers = []
    seen_urls = set()
    for s in final_servers:
        # Check against base URL to allow different query params if needed, but here simple str check is fine
        if s['url'] not in seen_urls:
            unique_servers.append(s)
            seen_urls.add(s['url'])
            
    final_servers = unique_servers
    
    # Apply limit if specified (hard limit on total servers scanned, used for testing)
    if args.limit:
        final_servers = final_servers[:args.limit]
        
    print(f"Queued {len(final_servers)} playlists for scanning:")
    print(f"  - {len(priorities)} priority")
    print(f"  - {len(others)} others")
    
    if not final_servers:
        print("No servers selected for scanning.")
        sys.exit(0)
    
    # Scan with 85% similarity threshold (relaxed to group better)
    scanner = SportsScanner(similarity_threshold=0.85)
    
    # Use scan_for_success with target of 20 successful scans
    target_success = 20
    if args.limit:
        target_success = args.limit # If testing with limit, success target matches limit
        
    scanner.scan_for_success(final_servers, target_success=target_success, max_workers=5)
    scanner.save(args.output_file)


if __name__ == '__main__':
    main()
