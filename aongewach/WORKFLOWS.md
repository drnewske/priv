# Automated Sports Data Workflows

This system runs automatically via GitHub Actions to keep schedules, team data, and channel mappings up to date.

## 1. Weekly Pipeline Update
Workflow: `update_schedule.yml`
- Runs: Weekly (Mondays at 06:00 UTC)
- What it does:
1. Scrapes weekly schedule from LiveSportTV (`scrape_schedule_livesporttv.py`).
2. Scans IPTV playlists for channels found in the schedule (`scan_sports_channels.py`), tests each matched stream URL inline with ffprobe/ffmpeg, and keeps only alive streams.
3. Maps events to team IDs/logos (`map_schedule_to_teams.py`).
4. Maps schedule channel names to stream channel IDs (`map_channels.py`).
5. Outputs final JSON to `e104f869d64e3d41256d5398.json`.
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
  - Then runs LiveSportTV scrape, batched playlist refill scan,
    team mapping, and final channel mapping.

## 3. Manual Maintenance Scripts
- `build_teams_db.py`: legacy team DB refresh script (now at repo root).
- `scan_specific_playlist.py`: targeted channel scans for missing channels/debugging.
- `scrape_schedule_livesporttv.py`: LiveSportTV guide scraper (HTML + data-today + tournament API).

## 4. Legacy Team Assets (Repo Root)
- `spdb_teams.json`, `teams.json`, `spdb_build_state.json`
- `fuzzy_match.py`, `build_teams_db.py`, `build_teams_mirror.py`, `fetch_missing_teams.py`

## Pipeline Order
1. `scrape_schedule_livesporttv.py --days 7 --output weekly_schedule.json` -> `weekly_schedule.json`
2. `scan_sports_channels.py` -> updates `channels.json` using tested-alive streams (cap: 5 working streams/channel)
3. `map_schedule_to_teams.py` -> `weekly_schedule_mapped.json`
4. `map_channels.py` -> `e104f869d64e3d41256d5398.json` (final output)
