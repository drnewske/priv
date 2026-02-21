# LiveSportTV Recon Notes

## Site Shape
- Base: `https://www.livesporttv.com/`
- Date schedule: `https://www.livesporttv.com/schedules/YYYY-MM-DD/`
- Sport pages: `/{sport-slug}/matches/`
- Match detail: `/{sport-slug}/matches/{match-slug}/{match-key}/`
- Competition detail: `/{sport-slug}/competitions/{country}/{competition}/{competition-key}/`
- Channel detail: `/{sport-slug}/channels/{channel-slug}/{channel-key}/`

## Frontend Endpoints Used by the Site
- `GET /data-today`
- `GET /api/collapsible/tournament/`
- `GET /api/collapsible/match/` (not needed for schedule ingestion)

## How Data Loads
1. `schedules/{date}` renders some tournaments + match rows directly (`li[data-match]`).
2. `data-today` returns additional tournament blocks (`li.is_expand` with `data-request_id` etc).
3. For each tournament block, frontend calls `/api/collapsible/tournament/` and receives:
   - `tournament`
   - `matches[]` where each entry includes a structured payload and `html`.

## Key Match Fields
- `match.key`
- `match.fx_id`
- `match.url`
- `match.status.value` / `match.status.progress`
- `home.name`, `home.image`, `home.url`
- `away.name`, `away.image`, `away.url`
- `tv_listings.value` (stringified list), `tv_listings.html` (anchor tags)

## Current Repo Integration
- New scraper: `aongewach/scrape_schedule_livesporttv.py`
- Output format is aligned to existing schedule consumers (`generated_at`, `source`, `schedule[]`, `events[]`).
- Dependency added: `cloudscraper`.

