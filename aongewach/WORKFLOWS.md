# Automated Sports Data Workflows

This system runs automatically via GitHub Actions to keep schedules, team data, and channel mappings up to date.

## 1. Weekly Pipeline Update
Workflow: `update_schedule.yml`
- Runs: Weekly (Mondays at 05:00 UTC)
- What it does:
1. Scrapes weekly schedule from FANZO (`scrape_schedule_fanzo.py`).
2. Scrapes weekly schedule from Where's The Match (`scrape_schedule.py`) to `weekly_schedule_witm.json`.
3. Merges exact FANZO/WITM events and unions channel lists (`merge_schedule_channels.py`).
4. Scans IPTV playlists for channels found in the merged schedule (`scan_sports_channels.py`), tests each matched stream URL inline with ffprobe/ffmpeg, and keeps only alive streams.
5. Maps events to team IDs/logos (`map_schedule_to_teams.py`).
6. Maps schedule channel names to stream channel IDs (`map_channels.py`).
7. Outputs final JSON to `e104f869d64e3d41256d5398.json`.
- Manual trigger: Actions -> "Update Weekly Schedule" -> "Run workflow"

## 2. Manual Maintenance Workflows
- `scan_channels.yml`: manual channel scan only.
  - Uses the same inline stream testing behavior as pipeline scan.
  - Batch testing runs per playlist with workers (`test-workers=12` by default).
- `stream_test.yml`: manual stream validation/pruning only (`stream_tester.py`).
  - Supports run-time inputs in Actions UI: `workers`, `timeout`, `retry_failed`,
    `use_ffmpeg_fallback`, `progress_every`, `verbose`, and `show_failures`.
  - Current accuracy defaults: `workers=12`, `timeout=8`, `retry_failed=1`,
    `use_ffmpeg_fallback=true`, `progress_every=50`.
- `refresh_streams_pipeline.yml`: dead-stream-first full pipeline.
  - Starts with `stream_tester.py` to prune dead URLs in `channels.json`.
  - Then runs FANZO + WITM scrape, channel merge, batched playlist refill scan,
    team mapping, and final channel mapping.

## 3. Manual Maintenance Scripts
- `build_teams_db.py`: legacy team DB refresh script (now at repo root).
- `scan_specific_playlist.py`: targeted channel scans for missing channels/debugging.

## 4. Legacy Team Assets (Repo Root)
- `spdb_teams.json`, `teams.json`, `spdb_build_state.json`
- `fuzzy_match.py`, `build_teams_db.py`, `build_teams_mirror.py`, `fetch_missing_teams.py`

## Pipeline Order
1. `scrape_schedule_fanzo.py` -> `weekly_schedule.json`
2. `scrape_schedule.py --output weekly_schedule_witm.json` -> `weekly_schedule_witm.json`
3. `merge_schedule_channels.py` -> merges into `weekly_schedule.json`
4. `scan_sports_channels.py` -> updates `channels.json` using tested-alive streams (cap: 5 working streams/channel)
5. `map_schedule_to_teams.py` -> `weekly_schedule_mapped.json`
6. `map_channels.py` -> `e104f869d64e3d41256d5398.json` (final output)
