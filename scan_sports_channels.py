#!/usr/bin/env python3
"""
Channel Search Utility
Browse and search sports channels from channels.json
"""

import json
import sys
from typing import List, Dict, Optional


class ChannelSearcher:
    def __init__(self, channels_json_path: str = 'channels.json'):
        """Load the channels database."""
        with open(channels_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.metadata = data.get('metadata', {})
        self.channels = data.get('channels', [])
        
        print(f"Loaded {len(self.channels)} sports channels")
        print(f"  - Scan date: {self.metadata.get('scan_date', 'Unknown')}")
        print(f"  - Playlists scanned: {self.metadata.get('playlists_scanned', 0)}")
        print(f"  - Duplicates removed: {self.metadata.get('duplicates_removed', 0)}")
        print()
    
    def search(self, query: str) -> List[Dict]:
        """Search for channels matching the query."""
        query_lower = query.lower()
        matches = []
        
        for channel in self.channels:
            channel_name = channel.get('channel_name', '').lower()
            group_title = channel.get('group_title', '').lower()
            
            if query_lower in channel_name or query_lower in group_title:
                matches.append(channel)
        
        return matches
    
    def get_by_source(self, source_name: str) -> List[Dict]:
        """Get all channels from a specific source playlist."""
        return [
            ch for ch in self.channels
            if ch.get('source_playlist', '').lower() == source_name.lower()
        ]
    
    def get_by_group(self, group_name: str) -> List[Dict]:
        """Get all channels from a specific group/category."""
        return [
            ch for ch in self.channels
            if group_name.lower() in ch.get('group_title', '').lower()
        ]
    
    def get_unique_channels(self) -> List[str]:
        """Get list of unique channel names."""
        return sorted(set(ch.get('channel_name', 'Unknown') for ch in self.channels))
    
    def get_unique_sources(self) -> List[str]:
        """Get list of unique source playlists."""
        return sorted(set(ch.get('source_playlist', 'Unknown') for ch in self.channels))
    
    def get_unique_groups(self) -> List[str]:
        """Get list of unique group titles."""
        groups = set()
        for ch in self.channels:
            group = ch.get('group_title')
            if group:
                groups.add(group)
        return sorted(groups)
    
    def display_channel(self, channel: Dict, show_url: bool = False):
        """Pretty print channel information."""
        print(f"\n{'='*70}")
        print(f"Channel: {channel.get('channel_name', 'Unknown')}")
        print(f"{'='*70}")
        
        if channel.get('group_title'):
            print(f"Category:    {channel['group_title']}")
        
        print(f"Source:      {channel.get('source_playlist', 'Unknown')}")
        
        if show_url and channel.get('stream_url'):
            print(f"Stream URL:  {channel['stream_url']}")
        
        print(f"{'='*70}\n")
    
    def interactive_search(self):
        """Run interactive search mode."""
        print("\n" + "="*70)
        print("INTERACTIVE CHANNEL SEARCH")
        print("="*70)
        print("Commands:")
        print("  - Type to search channel names, groups, or TVG names")
        print("  - 'source <name>' - List channels from specific source")
        print("  - 'group <name>' - List channels from specific group")
        print("  - 'sources' - List all unique sources")
        print("  - 'groups' - List all unique groups")
        print("  - 'stats' - Show scan statistics")
        print("  - 'quit' or 'exit' - Exit")
        print("="*70 + "\n")
        
        while True:
            try:
                query = input("Search: ").strip()
                
                if not query:
                    continue
                
                if query.lower() in ['quit', 'exit', 'q']:
                    print("Goodbye!")
                    break
                
                # Parse command
                parts = query.split(maxsplit=1)
                command = parts[0].lower()
                
                if command == 'stats':
                    self.show_stats()
                elif command == 'sources':
                    sources = self.get_unique_sources()
                    print(f"\n📂 {len(sources)} unique sources:\n")
                    for source in sources:
                        count = len(self.get_by_source(source))
                        print(f"  - {source} ({count} channels)")
                    print()
                elif command == 'groups':
                    groups = self.get_unique_groups()
                    print(f"\n📁 {len(groups)} unique groups:\n")
                    for group in groups:
                        count = len(self.get_by_group(group))
                        print(f"  - {group} ({count} channels)")
                    print()
                elif command == 'source' and len(parts) > 1:
                    results = self.get_by_source(parts[1])
                elif command == 'group' and len(parts) > 1:
                    results = self.get_by_group(parts[1])
                else:
                    results = self.search(query)
                
                # Display results
                if command not in ['stats', 'sources', 'groups']:
                    if not results:
                        print(f"❌ No channels found matching '{query}'\n")
                    elif len(results) == 1:
                        self.display_channel(results[0], show_url=True)
                    else:
                        print(f"\n✅ Found {len(results)} channels:\n")
                        for i, ch in enumerate(results[:30], 1):
                            channel_name = ch.get('channel_name', 'Unknown')
                            group = ch.get('group_title', 'N/A')
                            source = ch.get('source_playlist', 'N/A')
                            print(f"{i:3d}. {channel_name:<45} [{group}] from {source}")
                        
                        if len(results) > 30:
                            print(f"\n... and {len(results) - 30} more results")
                        
                        # Option to view details
                        print("\nType a number to view details, or press Enter to continue")
                        choice = input("View channel #: ").strip()
                        if choice.isdigit():
                            idx = int(choice) - 1
                            if 0 <= idx < len(results):
                                self.display_channel(results[idx], show_url=True)
                        print()
                
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}\n")
    
    def show_stats(self):
        """Display statistics about the channels database."""
        print(f"\n{'='*70}")
        print("SCAN STATISTICS")
        print(f"{'='*70}")
        print(f"Scan Date:              {self.metadata.get('scan_date', 'Unknown')}")
        print(f"Playlists Scanned:      {self.metadata.get('playlists_scanned', 0)}")
        print(f"Playlists Failed:       {self.metadata.get('playlists_failed', 0)}")
        print(f"Total Channels Checked: {self.metadata.get('total_channels_checked', 0):,}")
        print(f"Sports Channels Found:  {self.metadata.get('sports_channels_found', 0):,}")
        print(f"Duplicates Removed:     {self.metadata.get('duplicates_removed', 0):,}")
        print(f"\nUnique Channel Names:   {len(self.get_unique_channels())}")
        print(f"Unique Sources:         {len(self.get_unique_sources())}")
        print(f"Unique Groups:          {len(self.get_unique_groups())}")
        print(f"{'='*70}\n")


def main():
    """Main execution function."""
    import os
    
    json_path = 'channels.json'
    if len(sys.argv) > 1:
        json_path = sys.argv[1]
    
    if not os.path.exists(json_path):
        print(f"Error: {json_path} not found!")
        print("Run scan_sports_channels.py first to generate the database.")
        return
    
    # Load and run searcher
    searcher = ChannelSearcher(json_path)
    
    # If command line search provided, do that
    if len(sys.argv) > 2:
        query = ' '.join(sys.argv[2:])
        results = searcher.search(query)
        
        if results:
            print(f"\nFound {len(results)} match(es) for '{query}':\n")
            for channel in results[:10]:
                searcher.display_channel(channel, show_url=True)
            
            if len(results) > 10:
                print(f"... and {len(results) - 10} more results")
        else:
            print(f"No matches found for '{query}'")
    else:
        # Otherwise run interactive mode
        searcher.interactive_search()


if __name__ == '__main__':
    main()
