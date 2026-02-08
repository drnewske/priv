#!/usr/bin/env python3
"""
M3U Sports Channel Scanner
Scans multiple M3U playlists from lovestory.json and extracts sports channels.
"""

import json
import re
import requests
from typing import List, Dict, Set
from urllib.parse import urlparse
import time


class M3UParser:
    """Parser for M3U playlist format."""
    
    def __init__(self):
        # Sports channels/networks
        self.sports_keywords = [
            'sport', 'espn', 'fox sports', 'sky sports', 'bein', 'tsn', 'eurosport',
            'dazn', 'arena', 'eleven sports', 'setanta', 'bt sport', 'premier sports',
            'supersport', 'canal+ sport', 'movistar deportes', 'directv sports'
        ]
        
        # Specific leagues and competitions
        self.league_keywords = [
            'premier league', 'la liga', 'serie a', 'bundesliga', 'ligue 1',
            'champions league', 'europa league', 'uefa', 'fifa', 'nba', 'nfl', 
            'nhl', 'mlb', 'formula 1', 'f1', 'motogp', 'ufc', 'wwe', 'aew'
        ]
        
        # VOD file extensions to exclude
        self.vod_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.m4v', '.flv', '.wmv']
        
        # Non-sports group titles to exclude
        self.exclude_groups = [
            'movie', 'film', 'cinema', 'vod', 'series', 'tv show', 'drama',
            'comedy', 'action', 'thriller', 'horror', 'documentary', 'kids',
            'anime', 'cartoon', 'music', 'news', 'entertainment', 'adult'
        ]
    
    def is_sports_channel(self, channel_name: str, group_title: str, stream_url: str) -> bool:
        """
        Determine if a channel is sports-related.
        
        Args:
            channel_name: The channel name
            group_title: The group/category title
            stream_url: The stream URL
            
        Returns:
            True if this appears to be a sports channel
        """
        # Exclude VOD files (movies/series)
        if stream_url:
            url_lower = stream_url.lower()
            if any(url_lower.endswith(ext) for ext in self.vod_extensions):
                return False
        
        # Exclude non-sports groups
        if group_title:
            group_lower = group_title.lower()
            if any(excluded in group_lower for excluded in self.exclude_groups):
                return False
            
            # Group title contains "sport" - very likely a sports channel
            if 'sport' in group_lower:
                return True
        
        # Check channel name for sports keywords
        name_lower = channel_name.lower()
        
        # Check sports networks
        if any(keyword in name_lower for keyword in self.sports_keywords):
            return True
        
        # Check league/competition keywords
        if any(keyword in name_lower for keyword in self.league_keywords):
            return True
        
        return False
    
    def parse_m3u_line(self, line: str) -> Dict:
        """
        Parse an EXTINF line from M3U format.
        
        Format: #EXTINF:-1 tvg-id="..." tvg-name="..." tvg-logo="..." group-title="...",Channel Name
        
        Args:
            line: The EXTINF line to parse
            
        Returns:
            Dictionary with parsed attributes
        """
        result = {
            'tvg_id': None,
            'tvg_name': None,
            'tvg_logo': None,
            'group_title': None,
            'channel_name': None
        }
        
        # Extract tvg-id
        tvg_id_match = re.search(r'tvg-id="([^"]*)"', line)
        if tvg_id_match:
            result['tvg_id'] = tvg_id_match.group(1)
        
        # Extract tvg-name
        tvg_name_match = re.search(r'tvg-name="([^"]*)"', line)
        if tvg_name_match:
            result['tvg_name'] = tvg_name_match.group(1)
        
        # Extract tvg-logo
        tvg_logo_match = re.search(r'tvg-logo="([^"]*)"', line)
        if tvg_logo_match:
            result['tvg_logo'] = tvg_logo_match.group(1)
        
        # Extract group-title
        group_match = re.search(r'group-title="([^"]*)"', line)
        if group_match:
            result['group_title'] = group_match.group(1)
        
        # Extract channel name (after the last comma)
        if ',' in line:
            result['channel_name'] = line.split(',', 1)[1].strip()
        
        return result
    
    def parse_m3u_content(self, content: str) -> List[Dict]:
        """
        Parse M3U playlist content and extract all channels.
        
        Args:
            content: The M3U file content
            
        Returns:
            List of channel dictionaries
        """
        channels = []
        lines = content.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Look for EXTINF lines
            if line.startswith('#EXTINF'):
                channel_info = self.parse_m3u_line(line)
                
                # Next line should be the stream URL
                if i + 1 < len(lines):
                    url_line = lines[i + 1].strip()
                    if url_line and not url_line.startswith('#'):
                        channel_info['stream_url'] = url_line
                        channels.append(channel_info)
                
                i += 2  # Skip to next EXTINF
            else:
                i += 1
        
        return channels


class SportsChannelScanner:
    """Scans M3U playlists for sports channels."""
    
    def __init__(self, lovestory_path: str = 'lovestory.json'):
        """
        Initialize the scanner.
        
        Args:
            lovestory_path: Path to lovestory.json file
        """
        self.lovestory_path = lovestory_path
        self.parser = M3UParser()
        self.sports_channels = []
        self.seen_streams = set()  # For deduplication
        self.stats = {
            'playlists_scanned': 0,
            'playlists_failed': 0,
            'total_channels': 0,
            'sports_channels': 0,
            'duplicates_removed': 0
        }
    
    def load_playlists(self) -> List[Dict]:
        """Load playlist URLs from lovestory.json."""
        with open(self.lovestory_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract playlists from featured_content
        playlists = []
        for item in data.get('featured_content', []):
            if item.get('type') == 'm3u' and item.get('url'):
                playlists.append({
                    'name': item.get('name', 'Unknown'),
                    'url': item['url'],
                    'domain': item.get('domain', 'Unknown'),
                    'id': item.get('id', 'unknown')
                })
        
        return playlists
    
    def fetch_m3u_playlist(self, url: str, timeout: int = 30) -> str:
        """
        Fetch M3U playlist content from URL.
        
        Args:
            url: The playlist URL
            timeout: Request timeout in seconds
            
        Returns:
            Playlist content as string
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        # Handle different encodings
        try:
            return response.content.decode('utf-8')
        except UnicodeDecodeError:
            return response.content.decode('latin-1')
    
    def normalize_stream_url(self, url: str) -> str:
        """
        Normalize stream URL for deduplication.
        Removes query parameters that might vary.
        """
        parsed = urlparse(url)
        # Keep scheme, netloc, and path, ignore query params for comparison
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    def scan_playlist(self, playlist_info: Dict):
        """
        Scan a single playlist for sports channels.
        
        Args:
            playlist_info: Dictionary with playlist metadata
        """
        print(f"\n📺 Scanning: {playlist_info['name']} ({playlist_info['domain']})")
        
        try:
            # Fetch playlist content
            content = self.fetch_m3u_playlist(playlist_info['url'])
            
            # Parse channels
            channels = self.parser.parse_m3u_content(content)
            self.stats['total_channels'] += len(channels)
            
            print(f"   Found {len(channels)} total channels")
            
            # Filter for sports channels
            sports_count = 0
            for channel in channels:
                stream_url = channel.get('stream_url')
                if not stream_url:
                    continue
                
                channel_name = channel.get('channel_name') or channel.get('tvg_name') or 'Unknown'
                group_title = channel.get('group_title', '')
                
                # Check if it's a sports channel (with VOD filtering)
                if self.parser.is_sports_channel(channel_name, group_title, stream_url):
                    # Check for duplicates
                    normalized_url = self.normalize_stream_url(stream_url)
                    
                    if normalized_url not in self.seen_streams:
                        self.seen_streams.add(normalized_url)
                        
                        # Simplified output - just what's needed
                        self.sports_channels.append({
                            'channel_name': channel_name,
                            'stream_url': stream_url,
                            'group_title': group_title,
                            'source_playlist': playlist_info['name']
                        })
                        sports_count += 1
                    else:
                        self.stats['duplicates_removed'] += 1
            
            print(f"   ✅ Found {sports_count} sports channels (unique)")
            self.stats['playlists_scanned'] += 1
            
        except Exception as e:
            print(f"   ❌ Failed: {str(e)}")
            self.stats['playlists_failed'] += 1
    
    def scan_all_playlists(self, delay: float = 1.0):
        """
        Scan all playlists from lovestory.json.
        
        Args:
            delay: Delay between requests in seconds (be nice to servers)
        """
        playlists = self.load_playlists()
        print(f"\n{'='*60}")
        print(f"Starting scan of {len(playlists)} playlists")
        print(f"{'='*60}")
        
        for i, playlist in enumerate(playlists, 1):
            print(f"\n[{i}/{len(playlists)}]", end=' ')
            self.scan_playlist(playlist)
            
            # Be nice to servers
            if i < len(playlists):
                time.sleep(delay)
        
        self.stats['sports_channels'] = len(self.sports_channels)
    
    def save_channels(self, output_path: str = 'channels.json'):
        """
        Save sports channels to JSON file.
        
        Args:
            output_path: Path to save the channels JSON
        """
        output_data = {
            'metadata': {
                'scan_date': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                'playlists_scanned': self.stats['playlists_scanned'],
                'playlists_failed': self.stats['playlists_failed'],
                'total_channels_checked': self.stats['total_channels'],
                'sports_channels_found': self.stats['sports_channels'],
                'duplicates_removed': self.stats['duplicates_removed']
            },
            'channels': self.sports_channels
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*60}")
        print(f"✅ Saved {self.stats['sports_channels']} sports channels to {output_path}")
        print(f"{'='*60}")
        print(f"Statistics:")
        print(f"  - Playlists scanned: {self.stats['playlists_scanned']}")
        print(f"  - Playlists failed: {self.stats['playlists_failed']}")
        print(f"  - Total channels checked: {self.stats['total_channels']}")
        print(f"  - Sports channels found: {self.stats['sports_channels']}")
        print(f"  - Duplicates removed: {self.stats['duplicates_removed']}")
        print(f"{'='*60}\n")


def main():
    """Main execution function."""
    import sys
    
    # Get input/output paths
    lovestory_path = sys.argv[1] if len(sys.argv) > 1 else 'lovestory.json'
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'channels.json'
    
    print(f"Input:  {lovestory_path}")
    print(f"Output: {output_path}")
    
    # Create scanner and run
    scanner = SportsChannelScanner(lovestory_path)
    scanner.scan_all_playlists(delay=0.5)  # 0.5 second delay between requests
    scanner.save_channels(output_path)


if __name__ == '__main__':
    main()
