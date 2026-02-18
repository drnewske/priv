import json
import os
import time

# Configuration
SCHEDULE_FILE = 'weekly_schedule_mapped.json'
CHANNELS_FILE = 'channels.json'
MAP_FILE = 'channel_map.json'
OUTPUT_FILE = 'e104f869d64e3d41256d5398.json'

def load_json(filepath):
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def build_exact_lookup(iptv_channels):
    """Build exact and case-insensitive lookup maps for channel names."""
    name_to_id = {}
    name_to_id_lower = {}
    id_to_channel = {}
    for name, payload in iptv_channels.items():
        if not isinstance(payload, dict):
            continue
        cid = payload.get('id')
        if not isinstance(cid, int):
            continue
        name_to_id[name] = cid
        lower = name.lower()
        if lower not in name_to_id_lower:
            name_to_id_lower[lower] = cid
        id_to_channel[cid] = payload
    return name_to_id, name_to_id_lower, id_to_channel

def map_channels():
    print("Loading data...")
    schedule_data = load_json(SCHEDULE_FILE)
    channels_db = load_json(CHANNELS_FILE)
    saved_map = load_json(MAP_FILE) # Name -> Channel ID (preferred)
    
    if not schedule_data or not channels_db:
        print("Missing input files.")
        return

    iptv_channels = channels_db.get('channels', {})
    print("Building exact lookup...")
    t0 = time.time()
    name_to_id, name_to_id_lower, id_to_channel = build_exact_lookup(iptv_channels)
    print(f"Lookup built in {time.time() - t0:.2f}s")
    
    total_matches = 0
    unique_channels_mapped = 0
    processed_channels = set()
    total_channel_entries = 0
    unresolved_entries = 0
    
    print("Mapping channels...")
    t0 = time.time()
    days = schedule_data.get('schedule', [])

    for day_index, day in enumerate(days, start=1):
        day_label = day.get('date', f"day-{day_index}")
        day_events = day.get('events', [])
        print(f"  > Day {day_index}/{len(days)} ({day_label}) - {len(day_events)} events")
        for event in day.get('events', []):
            event_channels = event.get('channels', [])
            new_channels_list = []
            
            for sched_chan in event_channels:
                total_channel_entries += 1
                cid = None
                channel_name = sched_chan.strip() if isinstance(sched_chan, str) else ""
                if not channel_name:
                    continue
                
                # 1. Check Saved Map
                if channel_name in saved_map:
                    val = saved_map[channel_name]
                    
                    # Case A: Integer ID (Stable Key)
                    if isinstance(val, int):
                        if val in id_to_channel:
                            cid = val
                    
                    # Case B: String Name (Legacy) -> Mails to ID
                    elif isinstance(val, str):
                        if val in name_to_id:
                            cid = name_to_id[val]
                            # Auto-migrate
                            saved_map[channel_name] = cid
                
                # 2. Literal string match (exact first, then case-insensitive)
                if cid is None:
                    cid = name_to_id.get(channel_name)
                if cid is None:
                    cid = name_to_id_lower.get(channel_name.lower())
                if isinstance(cid, int):
                    saved_map[channel_name] = cid
                    if channel_name not in processed_channels:
                        unique_channels_mapped += 1
                        processed_channels.add(channel_name)

                # 3. Format as "Name, ID" or "Name, null"
                if isinstance(cid, int):
                    new_channels_list.append(f"{channel_name}, {cid}")
                    total_matches += 1
                else:
                    new_channels_list.append(f"{channel_name}, null")
                    unresolved_entries += 1

            # Update event with new format
            event['channels'] = new_channels_list
            # Remove legacy field if it exists (though we are building fresh here)
            if 'mapped_channels' in event:
                del event['mapped_channels']

    print(f"Mapping finished in {time.time() - t0:.2f}s")
    
    save_json(OUTPUT_FILE, schedule_data)
    save_json(MAP_FILE, saved_map)
    
    print(f"Done. Mapped {total_matches} channel entries to Stable Channel IDs.")
    print(f"Unresolved channel entries: {unresolved_entries}/{total_channel_entries}")
    print(f"Learned {unique_channels_mapped} new ID mappings.")

if __name__ == "__main__":
    map_channels()
