#!/usr/bin/env python3
"""
Xtream Sports Channel Scanner - Production Version
Fast AND accurate. Uses category filtering + smart fuzzy matching.
"""

import json
import re
import requests
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from difflib import SequenceMatcher
import concurrent.futures
import time


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
        self.base_url = f"http://{parsed.hostname}:{port}"
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
        return f"{self.base_url}/live/{self.username}/{self.password}/{stream_id}.ts"


class SportsFilter:
    """Filter sports categories and channels."""
    
    # Sports category keywords
    SPORTS_KEYWORDS = {
        'sport', 'espn', 'fox sports', 'sky sports', 'bein', 'dazn',
        'soccer', 'football', 'basketball', 'tennis', 'cricket', 'rugby',
        'golf', 'boxing', 'mma', 'ufc', 'wrestling', 'wwe', 'f1', 'formula',
        'motogp', 'nascar', 'hockey', 'baseball', 'nba', 'nfl', 'nhl', 'mlb',
        'deportes', 'deportivo', 'calcio', 'fussball'
    }
    
    # NON-sports keywords (higher priority)
    EXCLUDE_KEYWORDS = {
        'movie', 'film', 'cinema', 'series', 'drama', 'comedy', 'kids',
        'anime', 'cartoon', 'music', 'news', 'entertainment', 'adult',
        'xxx', '18+', 'porn', 'religion', 'documentary'
    }
    
    @classmethod
    def is_sports_category(cls, category_name: str) -> bool:
        """Check if category is sports-related."""
        name_lower = category_name.lower()
        
        # Exclude non-sports first
        if any(kw in name_lower for kw in cls.EXCLUDE_KEYWORDS):
            return False
        
        # Check sports keywords
        return any(kw in name_lower for kw in cls.SPORTS_KEYWORDS)


class ChannelNormalizer:
    """Normalize and group channel names."""
    
    def __init__(self, similarity_threshold: float = 0.90):
        """Initialize with similarity threshold (0-1)."""
        self.similarity_threshold = similarity_threshold
        
        # Quality patterns
        self.quality_regex = re.compile(
            r'\b(4k|uhd|ultra\s*hd|2160p?|fhd|full\s*hd|1080p?|hd|720p?|sd|480p?|360p?|hevc|h\.?26[45])\b',
            re.IGNORECASE
        )
        
        # Junk patterns (compile once for speed)
        self.junk_patterns = [
            re.compile(r'[#\-_*+=:|~]{3,}'),  # Repeated chars
            re.compile(r'\[(?:vip|hd|fhd|4k|sd|server|backup|link|premium|multi|test)\s*\d*\]', re.I),
            re.compile(r'\((?:vip|hd|fhd|4k|sd|server|backup|link|premium|multi|test)\s*\d*\)', re.I),
            re.compile(r'^\s*[#\-_*+=:|~]+\s*'),  # Leading junk
            re.compile(r'\s*[#\-_*+=:|~]+\s*$'),  # Trailing junk
            re.compile(r'\s*\|+\s*'),  # Pipes
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
    
    def __init__(self, similarity_threshold: float = 0.90):
        """Initialize scanner."""
        self.normalizer = ChannelNormalizer(similarity_threshold)
        
        # Channel storage: {normalized_name: {quality: set(urls)}}
        self.channels = defaultdict(lambda: defaultdict(set))
        self.channel_ids = {}
        self.next_id = 1
        
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
        """Get or create channel ID."""
        if normalized_name not in self.channel_ids:
            self.channel_ids[normalized_name] = self.next_id
            self.next_id += 1
        return self.channel_ids[normalized_name]
    
    def scan_server(self, server: Dict) -> Dict:
        """Scan a single server."""
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
            categories = api.get_live_categories()
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
                return result
            
            # Get streams from each sports category
            channels_found = 0
            for category in sports_categories:
                cat_id = category.get('category_id')
                if not cat_id:
                    continue
                
                streams = api.get_live_streams(cat_id)
                self.stats['channels_checked'] += len(streams)
                
                for stream in streams:
                    stream_id = stream.get('stream_id')
                    stream_name = stream.get('name', '').strip()
                    
                    if not stream_id or not stream_name:
                        continue
                    
                    # Normalize name
                    normalized = self.normalizer.normalize(stream_name)
                    if not normalized:
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
                    if url not in self.channels[final_name][quality]:
                        self.channels[final_name][quality].add(url)
                        channels_found += 1
                        self.stats['channels_added'] += 1
                    
                    # Ensure ID exists
                    self._get_channel_id(final_name)
            
            result['success'] = True
            result['channels_added'] = channels_found
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def scan_all(self, servers: List[Dict], max_workers: int = 5) -> None:
        """Scan all servers (with optional parallel processing)."""
        self.stats['servers_total'] = len(servers)
        
        print(f"\n{'='*70}")
        print(f"Scanning {len(servers)} servers (max {max_workers} parallel)")
        print(f"{'='*70}\n")
        
        # Use thread pool for parallel requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.scan_server, server): i 
                      for i, server in enumerate(servers, 1)}
            
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                result = future.result()
                
                status = '✅' if result['success'] else '❌'
                error = f" ({result['error']})" if result['error'] else ''
                
                print(f"[{idx}/{len(servers)}] {status} {result['domain']} - "
                      f"{result['channels_added']} channels{error}")
                
                if result['success']:
                    self.stats['servers_success'] += 1
                else:
                    self.stats['servers_failed'] += 1
    
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
            output['channels'][name] = {
                'id': self.channel_ids[name],
                'qualities': {}
            }
            
            for quality in ['SD', 'HD', 'FHD', '4K']:
                if quality in self.channels[name]:
                    output['channels'][name]['qualities'][quality] = [
                        {'url': url, 'source': 'xtream'}
                        for url in sorted(self.channels[name][quality])
                    ]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*70}")
        print(f"✅ Saved {len(self.channels)} unique channels → {output_path}")
        print(f"{'='*70}")
        print(f"Statistics:")
        print(f"  Servers: {self.stats['servers_success']}/{self.stats['servers_total']} "
              f"({self.stats['servers_failed']} failed)")
        print(f"  Categories: {self.stats['sports_categories']}/{self.stats['categories_total']} sports")
        print(f"  Channels: {len(self.channels)} unique ({self.stats['channels_added']} total links)")
        print(f"{'='*70}\n")


def main():
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'lovestory.json'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'channels.json'
    
    # Load servers
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    servers = []
    for item in data.get('featured_content', []):
        if item.get('type') == 'm3u' and item.get('url'):
            servers.append({
                'url': item['url'],
                'name': item.get('name', 'Unknown'),
                'domain': item.get('domain', 'Unknown')
            })
    
    # Scan with 90% similarity threshold
    scanner = SportsScanner(similarity_threshold=0.90)
    scanner.scan_all(servers, max_workers=5)  # 5 parallel connections
    scanner.save(output_file)


if __name__ == '__main__':
    main()
