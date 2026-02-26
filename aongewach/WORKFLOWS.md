# Automated Sports Data Workflows

This system runs automatically via GitHub Actions to keep schedules, team data, and channel mappings up to date.

## 1. Weekly Pipeline Update
Workflow: `update_schedule.yml`
- Runs: Weekly (Mondays at 06:00 UTC)
- What it does:
1. Scrapes weekly schedules from all sources in parallel:
   - LiveSportTV (`scrape_schedule_livesporttv.py`) for soccer/football events,
     including per-match channel enrichment from the fixture page (`LIVE` + `INTERNATIONAL` rows)
     without geo-country filtering.
   - FANZO (`scrape_schedule_fanzo.py`) + WITM (`scrape_schedule_witm.py`) for non-soccer
     channel/logo reinforcement.
2. Merges FANZO + WITM (`merge_fanzo_witm.py`) by exact event matching.
3. Composes final `weekly_schedule.json` (`compose_weekly_schedule.py`):
   soccer from LiveSportTV, non-soccer from FANZO/WITM.
4. Scans IPTV playlists for channels found in the composed schedule (`scan_sports_channels.py`), uses boundary-aware target matching, prefers Xtream live API endpoints when credentials are present, tests each matched stream URL inline with ffprobe/ffmpeg, rejects non-live VOD/series URL paths (group title + URL path + name heuristics for direct M3U), and prunes non-target channels from `channels.json`.
5. Maps schedule channel names to stream channel IDs (`map_channels.py`) with no per-event channel cap.
6. Outputs final JSON to `e104f869d64e3d41256d5398.json`.
- Manual trigger: Actions -> "Update Weekly Schedule" -> "Run workflow"

## 2. Manual Maintenance Workflows
- `daily_schedule_worker.yml`: scheduled daily worker (09:00 UTC) + manual trigger.
  - Runs `run_pipeline_daily_worker.py` for one UTC day.
  - Builds that day's schedule, scans playlists/external URLs for those channels, validates links
    with stream testing workers (default: `20`), then maps to final output.
- `manual_scrape_map_only.yml`: manual-only scrape + map worker (no stream scan).
  - Runs `run_pipeline_scrape_map.py` directly.
  - Does not run `scan_sports_channels.py`, so no lovestory/external stream testing is performed.
- `scrape_and_map.yml`: manual scrape + mapper only (no channel scan).
  - Runs `run_pipeline_scrape_map.py` to run the same multi-source compose flow
    (LiveSportTV soccer + FANZO/WITM non-soccer) and then map channels
    against existing `channels.json`.
  - Useful when stream links are already populated and you only want fresh schedule + mapping.
- `scan_channels.yml`: manual channel scan only.
  - Uses the same inline stream testing behavior as pipeline scan.
  - Batch testing runs per playlist with workers (`test-workers=20` by default).
- `stream_test.yml`: manual stream validation/pruning only (`stream_tester.py`).
  - Supports run-time inputs in Actions UI: `workers`, `timeout`, `retry_failed`,
    `use_ffmpeg_fallback`, `progress_every`, `verbose`, and `show_failures`.
  - Current accuracy defaults: `workers=20`, `timeout=8`, `retry_failed=1`,
    `use_ffmpeg_fallback=true`, `progress_every=50`.
- `refresh_streams_pipeline.yml`: dead-stream-first full pipeline.
  - Starts with `stream_tester.py` to prune dead URLs in `channels.json`.
  - Then runs multi-source schedule compose (LiveSportTV + FANZO/WITM), batched playlist refill scan,
    and final channel mapping.

## 3. Manual Maintenance Scripts
- `scrape_schedule_livesporttv.py`: LiveSportTV guide scraper (HTML + data-today + tournament API)
  returning soccer events only and enriching channels from match pages without geo filtering.
- `scrape_schedule_fanzo.py`: FANZO guide scraper (default: non-soccer only).
- `scrape_schedule_witm.py`: Where's The Match guide scraper (default: non-soccer only).
- `merge_fanzo_witm.py`: exact-match FANZO/WITM merger for channel + logo reinforcement.
- `compose_weekly_schedule.py`: final composer (LSTV soccer + FANZO/WITM non-soccer).
- `run_pipeline_scrape_map.py`: scrape + channel map runner (no playlist scan).
- `run_pipeline_daily_worker.py`: one-day scrape + scan + map runner (daily worker core).
- `channel_geo_rules.json`: legacy geo configuration file retained for backwards compatibility.
- `scan_sports_channels.py`: scans sources in this order:
  1. `external_playlists.txt` (repo root, one URL per line, optional `Name|URL`)
  2. `lovestory.json` featured playlists

## 4. Archived Legacy Files (`aongewach/legacy/`)
- `legacy/scripts/`: archived historical scripts retained for reference.
- `legacy/data/`: archived one-off or transitional schedule outputs.
- `legacy/cache/`: old `__pycache__` artifacts retained for history.

## 5. Legacy Team Assets (Repo Root)
- `spdb_teams.json`, `teams.json`, `spdb_build_state.json`
- `fuzzy_match.py`, `build_teams_db.py`, `build_teams_mirror.py`, `fetch_missing_teams.py`

## Pipeline Order
1. `scrape_schedule_livesporttv.py --days 7 --output weekly_schedule_livesporttv.json` (parallel block)
2. `scrape_schedule_fanzo.py --days 7 --output weekly_schedule_fanzo.json` (parallel block)
3. `scrape_schedule_witm.py --days 7 --output weekly_schedule_witm.json` (parallel block)
4. `merge_fanzo_witm.py --fanzo weekly_schedule_fanzo.json --witm weekly_schedule_witm.json --output weekly_schedule_fanzo_enriched.json`
5. `compose_weekly_schedule.py --livesporttv weekly_schedule_livesporttv.json --fanzo-witm weekly_schedule_fanzo_enriched.json --output weekly_schedule.json`
6. `scan_sports_channels.py` -> updates `channels.json` using boundary-aware name matching + tested-alive streams (cap: 5 working streams/channel), skips non-live VOD/series URLs, and prunes non-target channels
7. `map_channels.py` -> `e104f869d64e3d41256d5398.json` (final mapped output, uncapped)
