# Ad Serving Model (Current `ads.json`)

## Active pacing controls
- `global_min_gap_seconds`: `300` (minimum 5 minutes between any two ads across the app).
- `interval_seconds` (global default): `1800`.
- Placement cooldowns:
  - `native_event_player`: `900`
  - `native_event_player_related`: `600`
  - `iptv_player_entry`: `1800`
  - `iptv_series_entry`: `1500`
  - `iptv_vod_detail_entry`: `1200`
  - `predictions_entry`: `1200`
  - `predictions_bet_link`: `900`
  - `league_tables_entry`: `1500`
  - `league_tables_open`: `900`
  - `match_replays`: `1200`
  - `free_playlists`: `900`

## One-hour example: 30 interactions/user
Assumptions:
- 30 interactions happen in 1 hour (about one tap every 2 minutes).
- Every interaction is ad-eligible (wiring point reached).
- Inventory exists and ad launch succeeds.

Resulting ad volume with the current timers:
- Hard upper bound for this cadence: **10 ads/hour**.
  - Reason: global 5-minute gap + 2-minute interaction spacing means the next eligible tap usually lands at ~6-minute steps.
- Mixed-behavior simulation (20,000 randomized runs across the wired surfaces):
  - Average: **9.51 ads/hour**
  - P10/P50/P90: **9 / 10 / 10**
  - Max observed: **10**

Single-surface examples (same 30 taps/hour):
- `native_event_player_related` only: **6 ads/hour**
- `predictions_entry` only: **3 ads/hour**
- `iptv_player_entry` only: **2 ads/hour**

## Practical takeaway
With this config, most users at ~30 interactions/hour should see around **9-10 ads/hour** in mixed navigation, while high-friction surfaces stay constrained by their own placement cooldowns.
