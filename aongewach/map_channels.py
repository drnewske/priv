import json
import re
import os
import time
from difflib import SequenceMatcher
from collections import defaultdict

# Configuration
SCHEDULE_FILE = 'weekly_schedule_mapped.json'
CHANNELS_FILE = 'channels.json'
MAP_FILE = 'channel_map.json'
OUTPUT_FILE = 'e104f869d64e3d41256d5398.json'
CONFIDENCE_THRESHOLD = 0.85 

def load_json(filepath):
    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def normalize_channel_name(name):
    """
    Cleaner for channel names to improve matching odds.
    """
    if not name:
        return ""
    
    # Lowercase
    name = name.lower()
    
    # Remove junk
    junk = [
        r'uk\s*[:|]\s*', r'us\s*[:|]\s*', 
        r'\b(?:fhd|hd|sd|hevc|h265|50fps|60fps)\b',
        r'\b(?:vip|backup|raw|direct|4k|uhd)\b',
        r'\[.*?\]', 
        r'\(.*?\)'
    ]
    for pattern in junk:
        name = re.sub(pattern, '', name)
    
    # Replacements
    replacements = {
        "sky sports": "sky sp",
        "premier league": "pl",
        "main event": "main ev",
        "football": "fball",
        "bt sport": "tnt sports",
    }
    for k, v in replacements.items():
        name = name.replace(k, v)
    
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    
    # Normalize whitespace
    return " ".join(name.split())

def build_index(iptv_channels):
    """
    Builds an inverted index: Token -> List of (OriginalName, CleanName)
    """
    index = defaultdict(list)
    for original_name, data in iptv_channels.items():
        clean_name = normalize_channel_name(original_name)
        if not clean_name:
            continue
            
        # Store key only to avoid unhashable dicts
        entry = (original_name, clean_name)
        tokens = set(clean_name.split())
        
        for token in tokens:
            if len(token) > 2: # Skip small words
                index[token].append(entry)
                
    return index

def find_best_match_optimized(target_name, index, iptv_channels):
    """
    Find best match using token index.
    """
    target_clean = normalize_channel_name(target_name)
    tokens = [t for t in target_clean.split() if len(t) > 2]
    
    if not tokens:
        return None, 0.0, None
        
    # Get candidates
    candidates = set()
    for token in tokens:
        if token in index:
            for entry in index[token]:
                candidates.add(entry)
    
    if not candidates:
        return None, 0.0, None

    best_score = 0
    best_match = None
    best_match_key = None
    
    for original_name, candidate_clean in candidates:
        # Exact match check
        if target_clean == candidate_clean:
            return original_name, 1.0, iptv_channels[original_name]
            
        ratio = SequenceMatcher(None, target_clean, candidate_clean).ratio()
        
        if target_clean in candidate_clean:
            ratio = max(ratio, 0.9)
            
        if ratio > best_score:
            best_score = ratio
            best_match = iptv_channels[original_name]
            best_match_key = original_name
            
    return best_match_key, best_score, best_match

def map_channels():
    print("Loading data...")
    schedule_data = load_json(SCHEDULE_FILE)
    channels_db = load_json(CHANNELS_FILE)
    saved_map = load_json(MAP_FILE) # Now maps Name -> Channel ID (int) OR Name -> Name (legacy)
    
    if not schedule_data or not channels_db:
        print("Missing input files.")
        return

    iptv_channels = channels_db.get('channels', {})
    
    # Create lookup for ID -> Channel Data
    id_to_channel = {}
    name_to_id = {} # Helper for migration
    
    for k, v in iptv_channels.items():
        if 'id' in v:
            id_to_channel[v['id']] = v
            id_to_channel[v['id']]['name'] = k # Inject name for ref
            name_to_id[k] = v['id']

    print("Building index...")
    t0 = time.time()
    index = build_index(iptv_channels)
    print(f"Index built in {time.time() - t0:.2f}s")
    
    total_matches = 0
    unique_channels_mapped = 0
    processed_channels = set()
    
    print("Mapping channels...")
    t0 = time.time()

    for day in schedule_data.get('schedule', []):
        for event in day.get('events', []):
            event_channels = event.get('channels', [])
            new_channels_list = []
            
            for sched_chan in event_channels:
                cid = None
                
                # 1. Check Saved Map
                if sched_chan in saved_map:
                    val = saved_map[sched_chan]
                    
                    # Case A: Integer ID (Stable Key)
                    if isinstance(val, int):
                        cid = val
                        # Validate ID exists
                        if cid not in id_to_channel:
                             # print(f"Warning: Legacy ID {cid} not found.")
                             cid = None
                    
                    # Case B: String Name (Legacy) -> Mails to ID
                    elif isinstance(val, str):
                        if val in name_to_id:
                            cid = name_to_id[val]
                            # Auto-migrate
                            saved_map[sched_chan] = cid
                        else:
                            cid = None # Name no longer valid
                
                # 2. Find Match if not found
                if not cid:
                    match_key, score, match_data = find_best_match_optimized(sched_chan, index, iptv_channels)
                    if score >= CONFIDENCE_THRESHOLD and match_data:
                        cid = match_data.get('id')
                        # Save new mapping
                        if cid:
                            saved_map[sched_chan] = cid
                            if sched_chan not in processed_channels:
                                unique_channels_mapped += 1
                                processed_channels.add(sched_chan)

                # 3. Format as "Name, ID" or "Name, null"
                if cid:
                    new_channels_list.append(f"{sched_chan}, {cid}")
                    total_matches += 1
                else:
                    new_channels_list.append(f"{sched_chan}, null")

            # Update event with new format
            event['channels'] = new_channels_list
            # Remove legacy field if it exists (though we are building fresh here)
            if 'mapped_channels' in event:
                del event['mapped_channels']

    print(f"Mapping finished in {time.time() - t0:.2f}s")
    
    save_json(OUTPUT_FILE, schedule_data)
    save_json(MAP_FILE, saved_map)
    
    print(f"Done. Mapped {total_matches} events to Stable Channel IDs.")
    print(f"Learned {unique_channels_mapped} new ID mappings.")

if __name__ == "__main__":
    map_channels()
