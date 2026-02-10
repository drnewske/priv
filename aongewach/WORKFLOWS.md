# Automated Sports Data Workflows

This system runs automatically via GitHub Actions to keep sports schedules, team data, and channel mappings up to date.

## 1. Weekly Schedule Update
**Workflow:** `update_schedule.yml`
- **Runs:** Every Monday at 05:00 UTC
- **What it does:**
  1. **Scrapes** the latest schedule from Wheresthematch.com (`scrape_schedule.py`)
  2. **Builds/Updates** the teams database from TheSportsDB if needed (`build_teams_db.py`)
  3. **Maps** teams to IDs/Logos using fuzzy matching (`map_schedule_to_teams.py`)
  4. **Maps** channels to IPTV streams (`map_channels.py`)
  5. **Outputs** the final JSON to `aongewach/e104f869d64e3d41256d5398.json`
- **Manual Trigger:** Go to Actions → "Update Weekly Schedule" → "Run workflow"

## 2. Teams Database Update
**Workflow:** `fetch_teams.yml`
- **Runs:** Weekly on Monday at 03:00 UTC (before the schedule update)
- **What it does:**
  - Bulk-fetches all teams from TheSportsDB (~140 leagues).
  - Updates `spdb_teams.json` with new teams, aliases, and logos.
  - This ensures the database is fresh for the schedule mapping.
- **Manual Trigger:** Go to Actions → "Build Teams DB" → "Run workflow"

## 3. Channel Scanning (Separate System)
**Workflow:** `scan_channels.yml`
- **Runs:** Every 6 hours
- **What it does:** Scans IPTV servers for active sports channels.
- **Output:** Updates `channels.json`
- **Manual Trigger:** Go to Actions → "Scan Sports Channels" → "Run workflow"

## How it Works (The Pipeline)
The system is designed as a pipeline:
1. `scrape_schedule.py` → produces `weekly_schedule.json`
2. `build_teams_db.py` → updates `spdb_teams.json` (TheSportsDB data)
3. `map_schedule_to_teams.py` → reads schedule + teams DB → produces `weekly_schedule_mapped.json`
   - *Self-learning:* If it matches a team with high confidence, it saves the alias to `spdb_teams.json`.
4. `map_channels.py` → reads mapped schedule + `channels.json` → produces `e104f869d64e3d41256d5398.json` (Final Output)

## Maintenance
- **New Aliases:** The system learns automatically, but you can manually add aliases to `spdb_teams.json` if a team is consistently missed.
- **New Channels:** Update `channel_map.json` to hardcode specific channel mappings if automatic matching fails.
