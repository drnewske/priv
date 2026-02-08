#!/usr/bin/env python3
"""
Xtream Codes Sports Channel Scanner with Fuzzy Matching
Scans Xtream Codes APIs for sports channels and groups them intelligently.
"""

import json
import re
import requests
from typing import List, Dict, Set, Tuple, Optional
from urllib.parse import urlparse, parse_qs
import time
from collections import defaultdict
from difflib import SequenceMatcher
import hashlib


class XtreamCredentials:
    """Parse and store Xtream Codes credentials."""
    
    def __init__(self, url: str):
        """
        Parse Xtream credentials from M3U URL.
        
        Formats supported:
        - http://domain:port/get.php?username=X&password=Y&type=m3u_plus&output=ts
        - http://domain:port/username/password/get.php?type=m3u_plus
        """
        parsed = urlparse(url)
        self.domain = parsed.hostname
        self.port = parsed.port or 80
        self.base_url = f"http://{self.domain}:{self.port}"
        
        # Try query parameters first
        query_params = parse_qs(parsed.query)
        if 'username' in query_params and 'password' in query_params:
            self.username = query_params['username'][0]
            self.password = query_params['password'][0]
        else:
            # Try path-based format: /username/password/
            path_parts = [p for p in parsed.path.split('/') if p]
            if len(path_parts) >= 2:
                self.username = path_parts[0]
                self.password = path_parts[1]
            else:
                raise ValueError(f"Could not parse credentials from URL: {url}")
    
    def get_api_url(self, action: str, **params) -> str:
        """Build Xtream API URL."""
        param_str = f"username={self.username}&password={self.password}&action={action}"
        for key, value in params.items():
            param_str += f"&{key}={value}"
        return f"{self.base_url}/player_api.php?{param_str}"
    
    def get_stream_url(self, stream_id: int, extension: str = "ts") -> str:
        """Build stream URL."""
        return f"{self.base_url}/live/{self.username}/{self.password}/{stream_id}.{extension}"


class ChannelNormalizer:
    """Normalize and fuzzy-match channel names."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize normalizer.
        
        Args:
            similarity_threshold: Minimum similarity score (0-1) to consider channels the same
        """
        self.similarity_threshold = similarity_threshold
        
        # Common broadcaster patterns (will be used for grouping)
        self.broadcaster_patterns = [
            # Sky Sports
            r'sky\s*sports?',
            # DAZN
            r'dazn',
            # BeIN Sports
            r'be\s*in\s*sports?',
            # ESPN
            r'espn',
            # Canal+
            r'canal\s*\+?\s*(?:sport|deportes?)?',
            # SuperSport (DStv)
            r'super\s*sports?',
            # NBC Sports
            r'nbc\s*sports?',
            # SportTV
            r'sport\s*tv',
            # Eurosport
            r'euro\s*sports?',
            # TNT Sports
            r'tnt\s*sports?',
            # BT Sport
            r'bt\s*sports?',
            # ITV Sport
            r'itv\s*(?:sport)?',
            # BBC Sport
            r'bbc\s*(?:sport)?',
            # Fox Sports
            r'fox\s*sports?',
            # TSN
            r'tsn',
            # Premier Sports
            r'premier\s*sports?',
            # Arena Sport
            r'arena\s*sports?',
            # Eleven Sports
            r'eleven\s*sports?',
            # Setanta Sports
            r'setanta\s*sports?',
            # DirecTV Sports
            r'directv\s*sports?',
            # Movistar Deportes
            r'movistar\s*deportes?',
            # Star Sports
            r'star\s*sports?',
            # Sony Sports
            r'sony\s*(?:six|ten|sports?)',
            # Viaplay
            r'viaplay',
            # Ziggo Sport
            r'ziggo\s*sports?',
        ]
        
        # Quality indicators to remove
        self.quality_patterns = [
            r'\b(?:4k|uhd|ultra\s*hd|2160p?)\b',
            r'\b(?:fhd|full\s*hd|1080p?)\b',
            r'\b(?:hd|720p?)\b',
            r'\b(?:sd|480p?|360p?|240p?)\b',
            r'\b(?:hevc|h\.?264|h\.?265)\b',
            r'\b(?:low|high|medium)\b',
        ]
        
        # Common junk patterns (more comprehensive)
        self.junk_patterns = [
            # Brackets with junk labels (more specific - must be complete brackets)
            r'\[(?:vip|premium|hd|fhd|4k|sd|backup|multi|test|hevc|h\.?264|h\.?265|server\s*\d*|link|stream|source)\]',
            r'\((?:vip|premium|hd|fhd|4k|sd|backup|multi|test|hevc|h\.?264|h\.?265|server\s*\d*|link|stream|source)\)',
            r'\{(?:vip|premium|hd|fhd|4k|sd|backup|multi|test|hevc|h\.?264|h\.?265|server\s*\d*|link|stream|source)\}',
            # Repeated special characters anywhere: ###, ---, ***, etc. (3+ repetitions)
            r'[#\-_*+=:|~]{3,}',
            # Leading junk: any combo of special chars
            r'^[#\-_*+=:|~`!@$%^&(){}\[\]<>.,;\'\"\\\/]+\s*',
            # Trailing junk: same special chars at end
            r'\s*[#\-_*+=:|~`!@$%^&(){}\[\]<>.,;\'\"\\\/]+$',
            # Multiple pipes or separators
            r'\s*\|+\s*',
            # Trailing colons/semicolons
            r'\s*[:;]+\s*$',
            # Leading colons/semicolons
            r'^\s*[:;]+\s*',
        ]
    
    def extract_quality(self, name: str) -> str:
        """Extract quality from channel name."""
        name_lower = name.lower()
        
        if re.search(r'\b(?:4k|uhd|ultra\s*hd|2160p?)\b', name_lower):
            return '4K'
        if re.search(r'\b(?:fhd|full\s*hd|1080p?)\b', name_lower):
            return 'FHD'
        if re.search(r'\b(?:hd|720p?)\b', name_lower):
            return 'HD'
        if re.search(r'\b(?:sd|480p?|360p?|240p?)\b', name_lower):
            return 'SD'
        
        return 'HD'  # Default
    
    def normalize_name(self, name: str) -> str:
        """
        Normalize channel name by removing quality indicators and junk.
        
        Args:
            name: Raw channel name
            
        Returns:
            Cleaned channel name
        """
        if not name:
            return ""
        
        cleaned = name
        
        # First pass: Remove bracket labels specifically
        bracket_patterns = [
            r'\[(?:vip|premium|hd|fhd|4k|sd|backup|multi|test|hevc|h\.?264|h\.?265|server\s*\d*|link|stream|source)\]',
            r'\((?:vip|premium|hd|fhd|4k|sd|backup|multi|test|hevc|h\.?264|h\.?265|server\s*\d*|link|stream|source)\)',
        ]
        for pattern in bracket_patterns:
            cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)
        
        # Remove quality patterns
        for pattern in self.quality_patterns:
            cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)
        
        # Remove junk patterns (apply multiple times for nested junk)
        for _ in range(3):  # Three passes to catch deeply nested patterns
            for pattern in self.junk_patterns:
                cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)
        
        # Clean up whitespace (multiple spaces to single)
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove empty brackets/parentheses/braces (after content removal)
        cleaned = re.sub(r'\[\s*\]|\(\s*\)|\{\s*\}', '', cleaned)
        
        # Remove stray bracket characters that might be left
        cleaned = re.sub(r'(?<!\w)\[|\](?!\w)', ' ', cleaned)  # Standalone brackets
        cleaned = re.sub(r'(?<!\w)\(|\)(?!\w)', ' ', cleaned)  # Standalone parentheses
        
        # Remove any remaining leading/trailing special characters
        cleaned = cleaned.strip(' -_#|:;*+=~`!@$%^&(){}[]<>.,\'"\\/')
        
        # One more whitespace cleanup
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def calculate_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate similarity between two strings.
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            Similarity score between 0 and 1
        """
        # Normalize for comparison
        s1 = str1.lower().strip()
        s2 = str2.lower().strip()
        
        # Exact match
        if s1 == s2:
            return 1.0
        
        # Use SequenceMatcher for fuzzy matching
        return SequenceMatcher(None, s1, s2).ratio()
    
    def find_best_match(self, name: str, existing_names: List[str]) -> Optional[Tuple[str, float]]:
        """
        Find best matching existing name.
        
        Args:
            name: Name to match
            existing_names: List of existing normalized names
            
        Returns:
            Tuple of (best_match, similarity_score) or None
        """
        if not existing_names:
            return None
        
        best_match = None
        best_score = 0.0
        
        for existing in existing_names:
            score = self.calculate_similarity(name, existing)
            if score > best_score:
                best_score = score
                best_match = existing
        
        if best_score >= self.similarity_threshold:
            return (best_match, best_score)
        
        return None


class SportsFilter:
    """Filter sports channels and categories."""
    
    def __init__(self):
        # Sports category keywords
        self.sports_category_keywords = [
            'sport', 'espn', 'fox sports', 'sky sports', 'bein', 'soccer',
            'football', 'basketball', 'tennis', 'cricket', 'rugby', 'golf',
            'boxing', 'mma', 'ufc', 'wrestling', 'wwe', 'formula', 'f1',
            'motogp', 'nascar', 'hockey', 'baseball', 'volleyball',
            'olympics', 'athletics', 'racing', 'deportes', 'deportivo'
        ]
        
        # Non-sports categories to exclude
        self.exclude_category_keywords = [
            'movie', 'film', 'cinema', 'series', 'show', 'drama',
            'comedy', 'action', 'thriller', 'horror', 'documentary',
            'kids', 'anime', 'cartoon', 'music', 'news', 'entertainment',
            'adult', 'xxx', 'porn', '18+', 'religion', 'religious'
        ]
        
        # Sports broadcaster patterns
        self.broadcaster_keywords = [
            'sky sports', 'espn', 'fox sports', 'bein', 'dazn', 'tnt sports',
            'bt sport', 'premier sports', 'supersport', 'super sport',
            'canal+ sport', 'canal sport', 'movistar deportes', 'directv sports',
            'tsn', 'eurosport', 'arena sport', 'eleven sports', 'setanta',
            'sport tv', 'sporttv', 'nbc sports', 'cbs sports', 'itv sport',
            'bbc sport', 'star sports', 'sony sports', 'sony six', 'sony ten',
            'viaplay', 'ziggo sport', 'matchroom', 'ppv', 'golf channel',
            'tennis channel', 'nfl network', 'nba tv', 'mlb network',
            'nhl network', 'fight network', 'racing tv', 'at&t sportsnet',
            'bally sports', 'msg network', 'nesn', 'yes network'
        ]
        
        # League/competition keywords
        self.league_keywords = [
            'premier league', 'epl', 'la liga', 'serie a', 'bundesliga',
            'ligue 1', 'champions league', 'europa league', 'uefa',
            'fifa', 'nba', 'nfl', 'nhl', 'mlb', 'formula 1', 'f1',
            'motogp', 'ufc', 'wwe', 'aew', 'pga', 'lpga', 'cricket',
            'ipl', 'psl', 'big bash'
        ]
    
    def is_sports_category(self, category_name: str) -> bool:
        """Check if category is sports-related."""
        name_lower = category_name.lower()
        
        # Check exclusions first
        if any(keyword in name_lower for keyword in self.exclude_category_keywords):
            return False
        
        # Check sports keywords
        if any(keyword in name_lower for keyword in self.sports_category_keywords):
            return True
        
        return False
    
    def is_sports_channel(self, channel_name: str, category_name: str = '') -> bool:
        """Check if channel is sports-related."""
        name_lower = channel_name.lower()
        
        # Check broadcaster keywords
        if any(keyword in name_lower for keyword in self.broadcaster_keywords):
            return True
        
        # Check league keywords
        if any(keyword in name_lower for keyword in self.league_keywords):
            return True
        
        # If in sports category, likely a sports channel
        if category_name and self.is_sports_category(category_name):
            return True
        
        return False


class XtreamSportsScanner:
    """Scan Xtream Codes servers for sports channels."""
    
    def __init__(self, lovestory_path: str = 'lovestory.json'):
        """Initialize scanner."""
        self.lovestory_path = lovestory_path
        self.normalizer = ChannelNormalizer(similarity_threshold=0.85)
        self.sports_filter = SportsFilter()
        
        # Grouped channels: {normalized_name: {quality: [stream_data]}}
        self.grouped_channels = defaultdict(lambda: defaultdict(list))
        
        # Channel ID mapping
        self.channel_id_map = {}
        self.next_channel_id = 1
        
        self.stats = {
            'servers_scanned': 0,
            'servers_failed': 0,
            'categories_found': 0,
            'sports_categories': 0,
            'total_channels_checked': 0,
            'sports_channels_found': 0,
            'unique_channel_names': 0,
            'total_links_collected': 0
        }
    
    def load_servers(self) -> List[Dict]:
        """Load Xtream server URLs from lovestory.json."""
        with open(self.lovestory_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        servers = []
        for item in data.get('featured_content', []):
            if item.get('type') == 'm3u' and item.get('url'):
                servers.append({
                    'name': item.get('name', 'Unknown'),
                    'url': item['url'],
                    'domain': item.get('domain', 'Unknown'),
                    'id': item.get('id', 'unknown')
                })
        
        return servers
    
    def get_live_categories(self, creds: XtreamCredentials) -> List[Dict]:
        """Fetch live stream categories from Xtream API."""
        url = creds.get_api_url('get_live_categories')
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        return response.json()
    
    def get_live_streams(self, creds: XtreamCredentials, category_id: Optional[int] = None) -> List[Dict]:
        """Fetch live streams from Xtream API."""
        params = {}
        if category_id is not None:
            params['category_id'] = category_id
        
        url = creds.get_api_url('get_live_streams', **params)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        return response.json()
    
    def generate_channel_id(self, normalized_name: str) -> int:
        """Generate or retrieve unique ID for channel."""
        if normalized_name not in self.channel_id_map:
            self.channel_id_map[normalized_name] = self.next_channel_id
            self.next_channel_id += 1
        
        return self.channel_id_map[normalized_name]
    
    def scan_server(self, server_info: Dict):
        """Scan a single Xtream server."""
        print(f"\n📺 Scanning: {server_info['name']} ({server_info['domain']})")
        
        try:
            # Parse credentials
            creds = XtreamCredentials(server_info['url'])
            
            # Get categories
            print("   Fetching categories...")
            categories = self.get_live_categories(creds)
            self.stats['categories_found'] += len(categories)
            
            # Filter sports categories
            sports_categories = []
            for cat in categories:
                cat_name = cat.get('category_name', '')
                cat_id = cat.get('category_id')
                
                if cat_id and cat_name and self.sports_filter.is_sports_category(cat_name):
                    sports_categories.append(cat)
            
            print(f"   Found {len(categories)} categories ({len(sports_categories)} sports)")
            self.stats['sports_categories'] += len(sports_categories)
            
            # Get all live streams (we'll filter ourselves for better coverage)
            print("   Fetching live streams...")
            all_streams = self.get_live_streams(creds)
            self.stats['total_channels_checked'] += len(all_streams)
            
            print(f"   Found {len(all_streams)} total channels")
            
            # Process streams
            sports_count = 0
            links_added = 0
            
            # Get category name mapping
            category_map = {cat['category_id']: cat['category_name'] 
                          for cat in categories if 'category_id' in cat}
            
            for stream in all_streams:
                stream_id = stream.get('stream_id')
                stream_name = stream.get('name', '').strip()
                category_id = stream.get('category_id')
                
                if not stream_id or not stream_name:
                    continue
                
                # Get category name
                category_name = category_map.get(category_id, '')
                
                # Check if sports channel
                if self.sports_filter.is_sports_channel(stream_name, category_name):
                    # Extract quality
                    quality = self.normalizer.extract_quality(stream_name)
                    
                    # Normalize name
                    normalized_name = self.normalizer.normalize_name(stream_name)
                    
                    if not normalized_name:
                        continue
                    
                    # Try fuzzy matching with existing channels
                    existing_names = list(self.grouped_channels.keys())
                    match_result = self.normalizer.find_best_match(normalized_name, existing_names)
                    
                    if match_result:
                        # Use existing normalized name
                        final_name, similarity = match_result
                        if similarity < 1.0:
                            print(f"   🔗 Matched '{normalized_name}' → '{final_name}' ({similarity:.2f})")
                    else:
                        # New channel
                        final_name = normalized_name
                    
                    # Generate unique ID
                    channel_id = self.generate_channel_id(final_name)
                    
                    # Build stream URL
                    stream_url = creds.get_stream_url(stream_id)
                    
                    # Create stream data
                    stream_data = {
                        'url': stream_url,
                        'source': server_info['domain'],
                        'original_name': stream_name,
                        'category': category_name,
                        'stream_id': stream_id
                    }
                    
                    # Check for duplicates
                    existing_urls = [s['url'] for s in self.grouped_channels[final_name][quality]]
                    if stream_url not in existing_urls:
                        self.grouped_channels[final_name][quality].append(stream_data)
                        links_added += 1
                        sports_count += 1
            
            print(f"   ✅ Added {links_added} unique sports channel links")
            self.stats['servers_scanned'] += 1
            self.stats['total_links_collected'] += links_added
            
        except Exception as e:
            print(f"   ❌ Failed: {str(e)}")
            self.stats['servers_failed'] += 1
    
    def scan_all_servers(self, delay: float = 1.0):
        """Scan all servers."""
        servers = self.load_servers()
        print(f"\n{'='*70}")
        print(f"Starting scan of {len(servers)} Xtream servers")
        print(f"{'='*70}")
        
        for i, server in enumerate(servers, 1):
            print(f"\n[{i}/{len(servers)}]", end=' ')
            self.scan_server(server)
            
            if i < len(servers):
                time.sleep(delay)
        
        self.stats['unique_channel_names'] = len(self.grouped_channels)
    
    def save_channels(self, output_path: str = 'channels.json'):
        """Save grouped channels to JSON."""
        channels_output = {}
        
        for channel_name in sorted(self.grouped_channels.keys()):
            channel_id = self.channel_id_map[channel_name]
            qualities = self.grouped_channels[channel_name]
            
            channels_output[channel_name] = {
                'id': channel_id,
                'qualities': {}
            }
            
            # Add qualities in order
            for quality in ['SD', 'HD', 'FHD', '4K']:
                if quality in qualities:
                    channels_output[channel_name]['qualities'][quality] = qualities[quality]
        
        output_data = {
            'metadata': {
                'scan_date': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                'servers_scanned': self.stats['servers_scanned'],
                'servers_failed': self.stats['servers_failed'],
                'categories_found': self.stats['categories_found'],
                'sports_categories': self.stats['sports_categories'],
                'total_channels_checked': self.stats['total_channels_checked'],
                'unique_channel_names': self.stats['unique_channel_names'],
                'total_links_collected': self.stats['total_links_collected']
            },
            'channels': channels_output
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*70}")
        print(f"✅ Saved {self.stats['unique_channel_names']} unique sports channels")
        print(f"   with {self.stats['total_links_collected']} total links to {output_path}")
        print(f"{'='*70}")
        print(f"Statistics:")
        print(f"  - Servers scanned: {self.stats['servers_scanned']}")
        print(f"  - Servers failed: {self.stats['servers_failed']}")
        print(f"  - Categories found: {self.stats['categories_found']}")
        print(f"  - Sports categories: {self.stats['sports_categories']}")
        print(f"  - Total channels checked: {self.stats['total_channels_checked']}")
        print(f"  - Unique channel names: {self.stats['unique_channel_names']}")
        print(f"  - Total links collected: {self.stats['total_links_collected']}")
        print(f"{'='*70}\n")


def main():
    """Main execution."""
    import sys
    
    lovestory_path = sys.argv[1] if len(sys.argv) > 1 else 'lovestory.json'
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'channels.json'
    
    print(f"Input:  {lovestory_path}")
    print(f"Output: {output_path}")
    
    # Show normalization examples
    print(f"\n{'='*70}")
    print("Channel Name Normalization Examples:")
    print(f"{'='*70}")
    
    normalizer = ChannelNormalizer()
    test_cases = [
        "###SKY SPORTS---",
        "***ESPN HD***",
        "---Canal+ Sport FHD 1080p###",
        "___BeIN Sports 4K___",
        "|||DAZN 1|||",
        "===SuperSport Premier League===",
        "+++NBC Sports HD+++",
        "#-#-#Sport TV 1 HD#-#-#",
        "**--**TSN 1080p FHD**--**",
        "[VIP] Sky Sports Football HD [SERVER 1]",
        "(BACKUP) ESPN 2 (HD)",
        "FOX Sports | HD | 1080p",
        "::Arena Sport 1 HD::",
        ";;BT Sport 1;;",
    ]
    
    for test in test_cases:
        normalized = normalizer.normalize_name(test)
        quality = normalizer.extract_quality(test)
        print(f"  '{test}'")
        print(f"    → '{normalized}' ({quality})")
    
    print(f"{'='*70}\n")
    
    scanner = XtreamSportsScanner(lovestory_path)
    scanner.scan_all_servers(delay=0.5)
    scanner.save_channels(output_path)


if __name__ == '__main__':
    main()
