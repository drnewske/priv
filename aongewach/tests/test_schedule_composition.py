import unittest
import os
import sys

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(TEST_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from compose_weekly_schedule import compose_payload
from merge_fanzo_witm import merge_payloads


class ScheduleCompositionTests(unittest.TestCase):
    def test_compose_uses_livesporttv_for_soccer_and_fanzo_for_non_soccer(self):
        livesporttv = {
            "schedule": [
                {
                    "date": "2026-02-23",
                    "events": [
                        {
                            "name": "Soccer A v Soccer B",
                            "sport": "Soccer",
                            "time": "10:00",
                            "channels": ["Sky Sports", "BBC Radio 5 Live"],
                        },
                        {
                            "name": "Tennis A v Tennis B",
                            "sport": "Tennis",
                            "time": "11:00",
                            "channels": ["Tennis TV"],
                        },
                    ],
                }
            ]
        }
        fanzo_witm = {
            "schedule": [
                {
                    "date": "2026-02-23",
                    "events": [
                        {
                            "name": "Cricket A v Cricket B",
                            "sport": "Cricket",
                            "time": "12:00",
                            "channels": ["Star Sports", "TalkSport Radio"],
                        },
                        {
                            "name": "Football A v Football B",
                            "sport": "Football",
                            "time": "13:00",
                            "channels": ["Should Not Appear"],
                        },
                    ],
                }
            ]
        }

        composed = compose_payload(livesporttv, fanzo_witm)
        events = composed["schedule"][0]["events"]
        names = [event["name"] for event in events]

        self.assertIn("Soccer A v Soccer B", names)
        self.assertIn("Cricket A v Cricket B", names)
        self.assertNotIn("Tennis A v Tennis B", names)
        self.assertNotIn("Football A v Football B", names)

        soccer = next(event for event in events if event["name"] == "Soccer A v Soccer B")
        cricket = next(event for event in events if event["name"] == "Cricket A v Cricket B")
        self.assertEqual(["Sky Sports"], soccer["channels"])
        self.assertEqual(["Star Sports"], cricket["channels"])

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
                            "channels": ["Fanzo One"],
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
                            "channels": ["WITM Sports", "WITM Radio"],
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
