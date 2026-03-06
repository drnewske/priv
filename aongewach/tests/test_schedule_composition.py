import os
import sys
import unittest

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(TEST_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from compose_weekly_schedule import compose_payload
from merge_fanzo_witm import merge_payloads


class ScheduleCompositionTests(unittest.TestCase):
    def test_compose_merges_football_and_keeps_fanzo_team_data(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "Arsenal v Chelsea",
                            "sport": "Football",
                            "time": "16:30",
                            "start_time_iso": "2026-03-02T16:30:00Z",
                            "channels": ["Sky Sports Main Event"],
                            "home_team": "Arsenal",
                            "away_team": "Chelsea",
                            "home_team_id": 42,
                            "away_team_id": 38,
                            "home_team_logo": "fanzo-home.png",
                            "away_team_logo": "fanzo-away.png",
                        },
                        {
                            "name": "Snooker Final",
                            "sport": "Snooker",
                            "time": "19:00",
                            "channels": ["Eurosport 1"],
                        },
                    ],
                }
            ]
        }
        huhsports = {
            "matches": [
                {
                    "date": "2026-03-02",
                    "time": "16:30",
                    "start_time": 1772469000,
                    "league": "Premier League",
                    "home_team": "Arsenal",
                    "away_team": "Chelsea",
                    "home_team_id": 9999,
                    "away_team_id": 9998,
                    "home_team_logo": "huh-home.png",
                    "away_team_logo": "huh-away.png",
                    "tv_names": ["Peacock", "Sky Sports Main Event"],
                },
                {
                    "date": "2026-03-02",
                    "time": "20:00",
                    "start_time": 1772481600,
                    "league": "La Liga",
                    "home_team": "Real Madrid",
                    "away_team": "Getafe",
                    "tv_names": ["Premier Sports 1 HD"],
                },
            ]
        }

        composed = compose_payload(fanzo, huhsports)
        events = composed["schedule"][0]["events"]
        by_name = {event["name"]: event for event in events}

        self.assertIn("Arsenal v Chelsea", by_name)
        self.assertIn("Snooker Final", by_name)
        self.assertIn("Real Madrid v Getafe", by_name)
        self.assertEqual(3, len(events))

        merged_football = by_name["Arsenal v Chelsea"]
        self.assertEqual(
            ["Sky Sports Main Event"],
            merged_football["channels"],
        )
        self.assertEqual(42, merged_football["home_team_id"])
        self.assertEqual(38, merged_football["away_team_id"])
        self.assertEqual("fanzo-home.png", merged_football["home_team_logo"])
        self.assertEqual("fanzo-away.png", merged_football["away_team_logo"])

        # Output must stay normalized and source-agnostic.
        self.assertEqual(
            {
                "name",
                "start_time_iso",
                "time",
                "sport",
                "competition",
                "competition_logo",
                "sport_logo",
                "channels",
                "home_team",
                "away_team",
                "home_team_id",
                "away_team_id",
                "home_team_logo",
                "away_team_logo",
            },
            set(merged_football.keys()),
        )

    def test_compose_drops_events_with_placeholder_only_channels(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "A v B",
                            "sport": "Football",
                            "time": "11:00",
                            "channels": ["TBA", "Sky Sports TBC"],
                        }
                    ],
                }
            ]
        }
        huhsports = {"matches": []}

        composed = compose_payload(fanzo, huhsports)
        self.assertEqual([], composed["schedule"][0]["events"])

    def test_compose_recovers_tbc_football_event_from_huhsports(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "Real Madrid v Getafe",
                            "sport": "Football",
                            "time": "20:00",
                            "channels": ["Not Televised"],
                            "home_team": "TBC",
                            "away_team": "To Be Confirmed",
                            "competition": "La Liga",
                        }
                    ],
                }
            ]
        }
        huhsports = {
            "matches": [
                {
                    "date": "2026-03-02",
                    "time": "20:00",
                    "start_time": 1772481600,
                    "league": "La Liga",
                    "home_team": "Real Madrid",
                    "away_team": "Getafe",
                    "tv_names": ["Premier Sports 1 HD"],
                }
            ]
        }

        composed = compose_payload(fanzo, huhsports)
        events = composed["schedule"][0]["events"]
        self.assertEqual(1, len(events))
        event = events[0]
        self.assertEqual("Real Madrid", event["home_team"])
        self.assertEqual("Getafe", event["away_team"])
        self.assertEqual(["Premier Sports 1 HD"], event["channels"])

    def test_compose_drops_not_televised_football_without_huhsports_match(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "Team A v Team B",
                            "sport": "Football",
                            "time": "18:00",
                            "channels": ["Not Televised"],
                            "home_team": "Team A",
                            "away_team": "Team B",
                        }
                    ],
                }
            ]
        }
        huhsports = {"matches": []}

        composed = compose_payload(fanzo, huhsports)
        self.assertEqual([], composed["schedule"][0]["events"])

    def test_compose_fills_missing_football_competition_logo_from_witm_enriched_fanzo(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "A v B",
                            "sport": "Football",
                            "time": "16:00",
                            "channels": ["Sky Sports"],
                            "competition": "Premier League",
                            "competition_logo": "https://witm.example/premier.png",
                        }
                    ],
                }
            ]
        }
        huhsports = {
            "matches": [
                {
                    "date": "2026-03-02",
                    "time": "20:00",
                    "start_time": 1772481600,
                    "league": "Premier League",
                    "home_team": "Real Madrid",
                    "away_team": "Getafe",
                    "tv_names": ["Premier Sports 1 HD"],
                }
            ]
        }

        composed = compose_payload(fanzo, huhsports)
        events = composed["schedule"][0]["events"]
        huh_only = next(event for event in events if event["name"] == "Real Madrid v Getafe")
        self.assertEqual("https://witm.example/premier.png", huh_only["competition_logo"])

    def test_compose_reads_huhsports_channel_objects_as_channel_names(self):
        fanzo = {"schedule": [{"date": "2026-03-02", "events": []}]}
        huhsports = {
            "matches": [
                {
                    "date": "2026-03-02",
                    "time": "20:00",
                    "start_time": 1772481600,
                    "league": "La Liga",
                    "home_team": "Real Madrid",
                    "away_team": "Getafe",
                    "channels": [
                        {"id": 1, "name": "Sky Sports Main Event", "country_code": "GB"},
                        {"id": 2, "name": "Sky Sports Main Event", "country_code": "GB"},
                        {"id": 3, "name": "Not Televised", "country_code": "GB"},
                    ],
                    "tv_names": ["Sky Sports Main Event"],
                }
            ]
        }

        composed = compose_payload(fanzo, huhsports)
        events = composed["schedule"][0]["events"]
        self.assertEqual(1, len(events))
        self.assertEqual(["Sky Sports Main Event"], events[0]["channels"])

    def test_compose_reads_flashscore_events_payload(self):
        fanzo = {"schedule": [{"date": "2026-03-02", "events": []}]}
        flashscore = {
            "source": "flashscore.com",
            "events": [
                {
                    "home_team": "Arsenal",
                    "away_team": "Chelsea",
                    "home_team_logo": "https://static.flashscore.com/res/image/data/home.png",
                    "away_team_logo": "https://static.flashscore.com/res/image/data/away.png",
                    "start_date": "2026-03-02",
                    "start_time": "16:30",
                    "start_time_utc": "2026-03-02T16:30:00Z",
                    "channels": [{"name": "NBC (Usa)", "url": "https://example.com/nbc"}],
                }
            ],
        }

        composed = compose_payload(fanzo, flashscore, secondary_source="flashscore.com")
        events = composed["schedule"][0]["events"]
        self.assertEqual(1, len(events))
        self.assertEqual("Arsenal v Chelsea", events[0]["name"])
        self.assertEqual("2026-03-02T16:30:00Z", events[0]["start_time_iso"])
        self.assertEqual("16:30", events[0]["time"])
        self.assertEqual(["NBC"], events[0]["channels"])

    def test_compose_fanzo_overlap_keeps_fanzo_and_drops_flashscore_uk_channel(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "Arsenal v Chelsea",
                            "sport": "Football",
                            "time": "16:30",
                            "start_time_iso": "2026-03-02T16:30:00Z",
                            "channels": ["Sky Sports Main Event"],
                            "home_team": "Arsenal",
                            "away_team": "Chelsea",
                        }
                    ],
                }
            ]
        }
        flashscore = {
            "source": "flashscore.com",
            "events": [
                {
                    "home_team": "Arsenal",
                    "away_team": "Chelsea",
                    "start_date": "2026-03-02",
                    "start_time": "16:30",
                    "start_time_utc": "2026-03-02T16:30:00Z",
                    "channels": [
                        {"name": "NBC (Usa)", "url": "https://example.com/nbc"},
                        {"name": "SuperSport Premier League (Rsa)", "url": "https://example.com/ss"},
                        {"name": "beIN Sports MENA 1 (Ara)", "url": "https://example.com/bein"},
                        {"name": "TNT Sports 1 (Gbr)", "url": "https://example.com/tnt"},
                    ],
                }
            ],
        }

        composed = compose_payload(fanzo, flashscore, secondary_source="flashscore.com")
        events = composed["schedule"][0]["events"]
        self.assertEqual(1, len(events))
        channels = events[0]["channels"]
        self.assertIn("Sky Sports Main Event", channels)
        self.assertIn("NBC", channels)
        self.assertIn("SuperSport Premier League", channels)
        self.assertIn("beIN Sports MENA 1", channels)
        self.assertNotIn("TNT Sports 1", channels)

    def test_compose_matches_alias_style_overlap_without_team_database(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-07",
                    "events": [
                        {
                            "name": "Newcastle vs Man City",
                            "sport": "Football",
                            "time": "20:00",
                            "start_time_iso": "2026-03-07T20:00:00Z",
                            "competition": "English FA Cup",
                            "channels": ["Sky Sports Main Event"],
                            "home_team": "Newcastle",
                            "away_team": "Man City",
                            "home_team_logo": "fanzo-newcastle.png",
                            "away_team_logo": "fanzo-mancity.png",
                        }
                    ],
                }
            ]
        }
        flashscore = {
            "source": "flashscore.com",
            "events": [
                {
                    "home_team": "Newcastle",
                    "away_team": "Manchester City",
                    "away_team_slug": "manchester-city",
                    "competition": "FA Cup",
                    "competition_full": "England: FA Cup",
                    "start_date": "2026-03-07",
                    "start_time": "20:00",
                    "start_time_utc": "2026-03-07T20:00:00Z",
                    "channels": [{"name": "NBC (Usa)", "url": "https://example.com/nbc"}],
                }
            ],
        }

        composed = compose_payload(fanzo, flashscore, secondary_source="flashscore.com")
        events = composed["schedule"][0]["events"]
        self.assertEqual(1, len(events))
        event = events[0]
        self.assertEqual("fanzo-newcastle.png", event["home_team_logo"])
        self.assertEqual("fanzo-mancity.png", event["away_team_logo"])
        self.assertIn("NBC", event["channels"])

    def test_compose_persists_fanzo_team_logos_and_flashscore_aliases_in_registry(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-07",
                    "events": [
                        {
                            "name": "Newcastle vs Man City",
                            "sport": "Football",
                            "time": "20:00",
                            "start_time_iso": "2026-03-07T20:00:00Z",
                            "competition": "English FA Cup",
                            "channels": ["Sky Sports Main Event"],
                            "home_team": "Newcastle",
                            "away_team": "Man City",
                            "home_team_id": 9001,
                            "away_team_id": 9002,
                            "home_team_logo": "fanzo-newcastle.png",
                            "away_team_logo": "fanzo-mancity.png",
                        }
                    ],
                }
            ]
        }
        flashscore = {
            "source": "flashscore.com",
            "events": [
                {
                    "home_team": "Newcastle United",
                    "home_team_slug": "newcastle-united",
                    "away_team": "Manchester City",
                    "away_team_short": "Man City",
                    "away_team_slug": "manchester-city",
                    "competition": "FA Cup",
                    "competition_full": "England: FA Cup",
                    "start_date": "2026-03-07",
                    "start_time": "20:00",
                    "start_time_utc": "2026-03-07T20:00:00Z",
                    "channels": [{"name": "NBC (Usa)", "url": "https://example.com/nbc"}],
                }
            ],
        }
        registry = {}

        compose_payload(
            fanzo,
            flashscore,
            secondary_source="flashscore.com",
            team_logo_registry=registry,
        )

        registry_teams = {entry["team_key"]: entry for entry in registry["teams"]}
        self.assertEqual("fanzo-mancity.png", registry_teams["manchester city"]["logo"])
        self.assertEqual([9002], registry_teams["manchester city"]["fanzo_ids"])
        self.assertIn("Man City", registry_teams["manchester city"]["aliases"])
        self.assertIn("Manchester City", registry_teams["manchester city"]["aliases"])
        self.assertIn("manchester-city", registry_teams["manchester city"]["aliases"])
        self.assertIn("flashscore.com", registry_teams["manchester city"]["sources"])

    def test_compose_dedupes_existing_team_logo_registry_entries(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "Arsenal v Chelsea",
                            "sport": "Football",
                            "time": "16:30",
                            "channels": ["Sky Sports Main Event"],
                            "home_team": "Arsenal",
                            "away_team": "Chelsea",
                            "home_team_id": 42,
                            "home_team_logo": "fanzo-arsenal.png",
                            "away_team_logo": "fanzo-chelsea.png",
                        }
                    ],
                }
            ]
        }
        flashscore = {"source": "flashscore.com", "events": []}
        registry = {
            "teams": [
                {
                    "team_key": "arsenal",
                    "name": "Arsenal",
                    "logo": "fanzo-arsenal.png",
                    "fanzo_ids": [42],
                    "aliases": ["Arsenal"],
                    "sources": ["fanzo"],
                }
            ]
        }

        compose_payload(
            fanzo,
            flashscore,
            secondary_source="flashscore.com",
            team_logo_registry=registry,
        )

        arsenal_entries = [entry for entry in registry["teams"] if entry["team_key"] == "arsenal"]
        self.assertEqual(1, len(arsenal_entries))
        self.assertEqual([42], arsenal_entries[0]["fanzo_ids"])

    def test_compose_registry_avoids_short_alias_cross_contamination(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-02",
                    "events": [
                        {
                            "name": "AEK Larnaca v Omonia",
                            "sport": "Football",
                            "time": "16:30",
                            "channels": ["Sky Sports Main Event"],
                            "home_team": "AEK Larnaca",
                            "away_team": "Omonia",
                            "home_team_logo": "fanzo-aek-larnaca.png",
                            "away_team_logo": "fanzo-omonia.png",
                        }
                    ],
                }
            ]
        }
        flashscore = {
            "source": "flashscore.com",
            "events": [
                {
                    "home_team": "Al Ittihad",
                    "home_team_short": "AL",
                    "home_team_slug": "al-ittihad",
                    "away_team": "Al Shabab",
                    "away_team_slug": "al-shabab",
                    "competition": "Saudi Pro League",
                    "competition_full": "Saudi Arabia: Saudi Professional League",
                    "start_date": "2026-03-02",
                    "start_time": "20:00",
                    "start_time_utc": "2026-03-02T20:00:00Z",
                    "home_team_logo": "https://static.flashscore.com/res/image/data/al-ittihad-small.png",
                    "away_team_logo": "https://static.flashscore.com/res/image/data/al-shabab-small.png",
                    "channels": [{"name": "SSC 1 (Sau)", "url": "https://example.com/ssc1"}],
                }
            ],
        }
        registry = {}

        composed = compose_payload(
            fanzo,
            flashscore,
            secondary_source="flashscore.com",
            team_logo_registry=registry,
        )

        registry_teams = {entry["team_key"]: entry for entry in registry["teams"]}
        self.assertEqual(["AEK Larnaca"], registry_teams["aek larnaca"]["aliases"])
        flashscore_event = composed["schedule"][0]["events"][1]
        self.assertEqual(
            "https://static.flashscore.com/res/image/data/al-ittihad-small.png",
            flashscore_event["home_team_logo"],
        )

    def test_compose_upgrades_flashscore_only_logos_from_livesporttv(self):
        fanzo = {"schedule": [{"date": "2026-03-06", "events": []}]}
        flashscore = {
            "source": "flashscore.com",
            "events": [
                {
                    "home_team": "Wolves",
                    "home_team_slug": "wolverhampton",
                    "away_team": "Liverpool",
                    "away_team_slug": "liverpool",
                    "competition": "FA Cup",
                    "competition_full": "England: FA Cup",
                    "start_date": "2026-03-06",
                    "start_time": "20:00",
                    "start_time_utc": "2026-03-06T20:00:00Z",
                    "home_team_logo": "https://static.flashscore.com/res/image/data/wolves-small.png",
                    "away_team_logo": "https://static.flashscore.com/res/image/data/liverpool-small.png",
                    "channels": [{"name": "NBC (Usa)", "url": "https://example.com/nbc"}],
                }
            ],
        }
        livesporttv = {
            "source": "livesporttv.com",
            "schedule": [
                {
                    "date": "2026-03-06",
                    "events": [
                        {
                            "name": "Wolverhampton Wanderers v Liverpool",
                            "sport": "Soccer",
                            "competition": "English FA Cup",
                            "time": "20:00",
                            "start_time_iso": "2026-03-06T20:00:00Z",
                            "home_team": "Wolverhampton Wanderers",
                            "away_team": "Liverpool",
                            "home_team_logo": "https://www.livesporttv.com/uploads/teams/wolves.png",
                            "away_team_logo": "https://www.livesporttv.com/uploads/teams/liverpool.png",
                        }
                    ],
                }
            ],
        }

        composed = compose_payload(
            fanzo,
            flashscore,
            secondary_source="flashscore.com",
            livesporttv_payload=livesporttv,
        )
        event = composed["schedule"][0]["events"][0]
        self.assertEqual("https://www.livesporttv.com/uploads/teams/wolves.png", event["home_team_logo"])
        self.assertEqual("https://www.livesporttv.com/uploads/teams/liverpool.png", event["away_team_logo"])
        self.assertEqual(1, composed["composition"]["football_secondary_events_logo_enriched_from_livesporttv"])

    def test_compose_uses_registry_before_livesporttv_for_flashscore_only_logos(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-03-05",
                    "events": [
                        {
                            "name": "Wolverhampton Wanderers v Chelsea",
                            "sport": "Football",
                            "time": "18:00",
                            "start_time_iso": "2026-03-05T18:00:00Z",
                            "competition": "Premier League",
                            "channels": ["Sky Sports Main Event"],
                            "home_team": "Wolverhampton Wanderers",
                            "away_team": "Chelsea",
                            "home_team_logo": "fanzo-wolves.png",
                            "away_team_logo": "fanzo-chelsea.png",
                        }
                    ],
                },
                {
                    "date": "2026-03-06",
                    "events": [],
                },
            ]
        }
        flashscore = {
            "source": "flashscore.com",
            "events": [
                {
                    "home_team": "Wolves",
                    "home_team_slug": "wolverhampton",
                    "away_team": "Liverpool",
                    "away_team_slug": "liverpool",
                    "competition": "FA Cup",
                    "competition_full": "England: FA Cup",
                    "start_date": "2026-03-06",
                    "start_time": "20:00",
                    "start_time_utc": "2026-03-06T20:00:00Z",
                    "home_team_logo": "https://static.flashscore.com/res/image/data/wolves-small.png",
                    "away_team_logo": "https://static.flashscore.com/res/image/data/liverpool-small.png",
                    "channels": [{"name": "NBC (Usa)", "url": "https://example.com/nbc"}],
                }
            ],
        }
        livesporttv = {
            "source": "livesporttv.com",
            "schedule": [
                {
                    "date": "2026-03-06",
                    "events": [
                        {
                            "name": "Wolverhampton Wanderers v Liverpool",
                            "sport": "Soccer",
                            "competition": "English FA Cup",
                            "time": "20:00",
                            "start_time_iso": "2026-03-06T20:00:00Z",
                            "home_team": "Wolverhampton Wanderers",
                            "away_team": "Liverpool",
                            "home_team_logo": "https://www.livesporttv.com/uploads/teams/wolves.png",
                            "away_team_logo": "https://www.livesporttv.com/uploads/teams/liverpool.png",
                        }
                    ],
                }
            ],
        }
        registry = {}

        composed = compose_payload(
            fanzo,
            flashscore,
            secondary_source="flashscore.com",
            livesporttv_payload=livesporttv,
            team_logo_registry=registry,
        )

        march_6 = next(day for day in composed["schedule"] if day["date"] == "2026-03-06")
        event = march_6["events"][0]
        self.assertEqual("fanzo-wolves.png", event["home_team_logo"])
        self.assertEqual("https://www.livesporttv.com/uploads/teams/liverpool.png", event["away_team_logo"])
        self.assertEqual(1, composed["composition"]["football_secondary_events_logo_enriched_from_registry"])
        self.assertEqual(1, composed["composition"]["football_secondary_events_logo_enriched_from_livesporttv"])

        registry_teams = {entry["team_key"]: entry for entry in registry["teams"]}
        self.assertIn("Wolves", registry_teams["wolverhampton wanderers"]["aliases"])
        self.assertIn("wolverhampton", registry_teams["wolverhampton wanderers"]["aliases"])

    def test_compose_embeds_static_sport_assets_payload(self):
        fanzo = {"schedule": [{"date": "2026-03-02", "events": []}]}
        flashscore = {"source": "flashscore.com", "events": []}
        sport_assets = {
            "generated_at": "2026-03-06T12:00:00Z",
            "source": "flashscore.com",
            "sports": {
                "soccer": {
                    "label": "Football",
                    "svg_data_uri": "data:image/svg+xml;utf8,<svg></svg>",
                }
            },
        }

        composed = compose_payload(
            fanzo,
            flashscore,
            secondary_source="flashscore.com",
            sport_assets_payload=sport_assets,
        )
        self.assertEqual(sport_assets, composed["sport_assets"])

    def test_merge_enriches_channels_and_sport_logo_from_witm(self):
        fanzo = {
            "schedule": [
                {
                    "date": "2026-02-23",
                    "events": [
                        {
                            "name": "Player A v Player B",
                            "time": "14:00",
                            "sport": "Snooker",
                            "channels": ["Fanzo One", "TBA"],
                            "sport_logo": None,
                        }
                    ],
                }
            ]
        }
        witm = {
            "schedule": [
                {
                    "date": "2026-02-23",
                    "events": [
                        {
                            "name": "Player A v Player B",
                            "time": "14:00",
                            "sport": "Snooker",
                            "channels": ["WITM Sports", "WITM Radio", "TBC", "Sky Sports TBC"],
                            "sport_logo": "https://cdn.example/logo.png",
                        }
                    ],
                }
            ]
        }

        merged, stats = merge_payloads(fanzo, witm)
        event = merged["schedule"][0]["events"][0]

        self.assertEqual(["Fanzo One", "WITM Sports"], event["channels"])
        self.assertEqual("https://cdn.example/logo.png", event["sport_logo"])
        self.assertEqual(1, stats["matched_events"])
        self.assertEqual(1, stats["channels_added"])


if __name__ == "__main__":
    unittest.main()
