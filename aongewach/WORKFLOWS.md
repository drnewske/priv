# Automated Sports Data Workflows

This system runs automatically via GitHub Actions to keep sports schedules, team data, and channel mappings up to date.

## 1. Weekly Pipeline Update
**Workflow:** `pipeline.yml`
- **Runs:** Weekly (Mondays at 05:00 UTC)
- **What it does:** Orchestrates the entire data update process in one go.
  1. **Scrapes** the latest schedule from Wheresthematch.com (`scrape_schedule.py`)
  2. **Scans** IPTV playlists for active channels (`scan_sports_channels.py`)
  3. **Tests streams** with `ffprobe/ffmpeg` and removes dead URLs (`stream_tester.py`)
  4. **Mapping:**
     - Teams: Maps event names to Team IDs/Logos (`map_schedule_to_teams.py`)
     - Channels: Maps schedule channels to stream URLs (`map_channels.py`)
  4. **Outputs** the final JSON to `e104f869d64e3d41256d5398.json`
- **Manual Trigger:** Go to Actions → "Update Sports Pipeline" → "Run workflow"

## 2. Maintenance Scripts (Manual)
- **`build_teams_db.py`**: Run occasionally to fully refresh the team database from TheSportsDB.
- **`scan_specific_playlist.py`**: Run manually to add specific channels or debug playlist issues.

## How it Works (The Pipeline)
The system is designed as a pipeline executed by `run_pipeline.py`:
1. `scrape_schedule.py` → produces `weekly_schedule.json`
2. `scan_sports_channels.py` → MERGES new channels into `channels.json`
3. `stream_tester.py` → validates stream URLs and prunes dead links in `channels.json`
4. `map_schedule_to_teams.py` → reads schedule + teams DB → produces `weekly_schedule_mapped.json`
   - *Self-learning:* If it finds a new team via API, it saves it to `spdb_teams.json`.
5. `map_channels.py` → reads mapped schedule + `channels.json` → produces `e104f869d64e3d41256d5398.json` (Final Output)
