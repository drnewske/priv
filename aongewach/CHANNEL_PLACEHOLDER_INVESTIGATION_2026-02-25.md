# Channel Placeholder Investigation (2026-02-25)

## Scope
- Sources reviewed: `livesporttv`, `fanzo`, `wheresthematch (WITM)`.
- Goal: identify placeholder channel labels shown when a fixture has no final broadcaster assignment, then exclude them from all internal channel lists.

## Findings

### LiveSportTV
- API path used by scraper (`/data-today` + tournament expansion) can emit placeholder channel labels.
- Verified sample scrape for `2026-02-24` produced:
  - `1351` total channel entries
  - `68` placeholder entries
  - placeholder value observed: `TBA`
- Existing project data (`weekly_schedule_livesporttv.json`) also contains repeated `TBA` entries.

### FANZO
- Raw API probe across `2026-02-25` to `2026-03-03`:
  - `355` events scanned
  - `44` unique channel names
  - no placeholder labels observed (`TBA/TBC/TBD/...`)
- Current behavior appears to return real channel labels only.

### WITM
- Client-side listing probe across `2026-02-25` to `2026-03-03` found placeholder-style labels in channel logo titles/alts:
  - `TBC`
  - `Sky Sports TBC`
- Additional probe through `2026-03-06` also found `Premier Sports TBC`.
- This indicates WITM can expose unresolved broadcaster placeholders as channel labels.

## Implementation Outcome
- Added shared placeholder detection in `channel_name_placeholders.py`.
- Updated all channel-ingestion/cleaning stages to reject placeholder labels:
  - `scrape_schedule_livesporttv.py`
  - `scrape_schedule_fanzo.py`
  - `scrape_schedule_witm.py`
  - `merge_fanzo_witm.py`
  - `compose_weekly_schedule.py`
  - `map_channels.py`
  - `scan_sports_channels.py`

## Placeholder Rules Applied
- Exact placeholders filtered: `TBA`, `TBC`, `TBD`, `N/A`, `None`, `Null`, `Unknown`, `No Channel(s)`, `No Broadcaster(s)`, `To Be Announced`, `To Be Confirmed`, `Not Available`, and dash-only placeholders.
- Suffix placeholders filtered: labels ending in `TBA`, `TBC`, or `TBD` (example: `Sky Sports TBC`).
