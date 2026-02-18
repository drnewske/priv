# Automated Sports Data Workflows

This system runs automatically via GitHub Actions to keep schedules, team data, and channel mappings up to date.

## 1. Weekly Pipeline Update
Workflow: `update_schedule.yml`
- Runs: Weekly (Mondays at 05:00 UTC)
- What it does:
1. Scrapes weekly schedule from FANZO (`scrape_schedule_fanzo.py`).
2. Scans IPTV playlists for channels found in the schedule (`scan_sports_channels.py`).
3. Maps events to team IDs/logos (`map_schedule_to_teams.py`).
4. Maps schedule channel names to stream channel IDs (`map_channels.py`).
5. Outputs final JSON to `e104f869d64e3d41256d5398.json`.
- Manual trigger: Actions -> "Update Weekly Schedule" -> "Run workflow"

## 2. Manual Maintenance Workflows
- `scan_channels.yml`: manual channel scan + stream pruning (`stream_tester.py`).

## 3. Manual Maintenance Scripts
- `build_teams_db.py`: legacy team DB refresh script (now at repo root).
- `scan_specific_playlist.py`: targeted channel scans for missing channels/debugging.

## 4. Legacy Team Assets (Repo Root)
- `spdb_teams.json`, `teams.json`, `spdb_build_state.json`
- `fuzzy_match.py`, `build_teams_db.py`, `build_teams_mirror.py`, `fetch_missing_teams.py`

## Pipeline Order
1. `scrape_schedule_fanzo.py` -> `weekly_schedule.json`
2. `scan_sports_channels.py` -> merges/updates `channels.json`
3. `map_schedule_to_teams.py` -> `weekly_schedule_mapped.json`
4. `map_channels.py` -> `e104f869d64e3d41256d5398.json` (final output)
