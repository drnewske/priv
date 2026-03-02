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
            ["Sky Sports Main Event", "Peacock"],
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
