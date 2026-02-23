# Automated Sports Data Workflows

This system runs automatically via GitHub Actions to keep schedules, team data, and channel mappings up to date.

## 1. Weekly Pipeline Update
Workflow: `update_schedule.yml`
- Runs: Weekly (Mondays at 06:00 UTC)
- What it does:
1. Scrapes weekly schedule from LiveSportTV (`scrape_schedule_livesporttv.py`) to source soccer/football events,
   including per-match country enrichment from the fixture page (`LIVE` + `INTERNATIONAL` rows)
   and per-event `channel_candidates` metadata with country evidence.
2. Scrapes FANZO non-soccer schedule (`scrape_schedule_fanzo.py`).
3. Scrapes WITM non-soccer schedule (`scrape_schedule_witm.py`) for channel/logo reinforcement.
4. Merges FANZO + WITM (`merge_fanzo_witm.py`) by exact event matching.
5. Composes final `weekly_schedule.json` (`compose_weekly_schedule.py`):
   soccer from LiveSportTV, non-soccer from FANZO/WITM.
6. Scans IPTV playlists for channels found in the composed schedule (`scan_sports_channels.py`), tests each matched stream URL inline with ffprobe/ffmpeg, and keeps only alive streams.
7. Maps schedule channel names to stream channel IDs (`map_channels.py`) and applies
   final per-event geo cap (max 5 mapped channels/event; UK<=2, US<=2, fill Others).
8. Outputs final JSON to `e104f869d64e3d41256d5398.json`.
- Manual trigger: Actions -> "Update Weekly Schedule" -> "Run workflow"

## 2. Manual Maintenance Workflows
- `scrape_and_map.yml`: manual scrape + mapper only (no channel scan).
  - Runs `run_pipeline_scrape_map.py` to run the same multi-source compose flow
    (LiveSportTV soccer + FANZO/WITM non-soccer) and then map/cap channels
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
  with fixture-country channel enrichment controlled by `channel_geo_rules.json`.
- `scrape_schedule_fanzo.py`: FANZO guide scraper (default: non-soccer only).
- `scrape_schedule_witm.py`: Where's The Match guide scraper (default: non-soccer only).
- `merge_fanzo_witm.py`: exact-match FANZO/WITM merger for channel + logo reinforcement.
- `compose_weekly_schedule.py`: final composer (LSTV soccer + FANZO/WITM non-soccer).
- `run_pipeline_scrape_map.py`: scrape + channel map runner (no playlist scan).
- `channel_geo_rules.json`: configurable geo profile and UK/US/Others selection rules.
  - `country_groups`: country-driven bucketing (UK/US/preferred Others).
  - `match_country_enrichment`: source-country extraction settings from match pages.
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
1. `scrape_schedule_livesporttv.py --days 7 --geo-rules-file channel_geo_rules.json --output weekly_schedule_livesporttv.json`
2. `scrape_schedule_fanzo.py --days 7 --output weekly_schedule_fanzo.json`
3. `scrape_schedule_witm.py --days 7 --output weekly_schedule_witm.json`
4. `merge_fanzo_witm.py --fanzo weekly_schedule_fanzo.json --witm weekly_schedule_witm.json --output weekly_schedule_fanzo_enriched.json`
5. `compose_weekly_schedule.py --livesporttv weekly_schedule_livesporttv.json --fanzo-witm weekly_schedule_fanzo_enriched.json --output weekly_schedule.json`
6. `scan_sports_channels.py` -> updates `channels.json` using tested-alive streams (cap: 5 working streams/channel)
7. `map_channels.py --geo-rules-file channel_geo_rules.json` -> `e104f869d64e3d41256d5398.json` (final mapped + capped output)
