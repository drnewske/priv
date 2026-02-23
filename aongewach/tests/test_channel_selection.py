import unittest
from pathlib import Path
import sys

TESTS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = TESTS_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from channel_selection import (
    build_channel_candidates,
    index_channel_candidates,
    load_geo_rules,
    merge_channel_candidates,
    select_mapped_event_channels,
)


class ChannelSelectionTests(unittest.TestCase):
    def setUp(self):
        self.rules = load_geo_rules(None)

    def test_merge_channel_candidates_dedup_and_profile_union(self):
        existing = [
            {
                "name": "Sky Sports Premier League",
                "profiles": ["default"],
                "bucket_hints": ["uk"],
                "preferred_other": False,
            }
        ]
        incoming = [
            {
                "name": "sky sports premier league",
                "profiles": ["uk"],
                "bucket_hints": ["uk"],
                "preferred_other": False,
            }
        ]
        merged = merge_channel_candidates(existing, incoming)
        self.assertEqual(1, len(merged))
        self.assertEqual("Sky Sports Premier League", merged[0]["name"])
        self.assertEqual(["default", "uk"], merged[0]["profiles"])
        self.assertEqual(["uk"], merged[0]["bucket_hints"])

    def test_metadata_first_classification_overrides_heuristics(self):
        mapped = [{"name": "Paramount+", "id": 100, "raw": "Paramount+, 100"}]
        # Force metadata to classify Paramount+ as UK for this event.
        candidate_index = index_channel_candidates(
            [
                {
                    "name": "Paramount+",
                    "profiles": ["uk"],
                    "bucket_hints": ["uk"],
                    "preferred_other": False,
                }
            ]
        )
        selected, stats = select_mapped_event_channels(mapped, self.rules, candidate_index)
        self.assertEqual(1, len(selected))
        self.assertEqual(1, stats["selected_uk"])
        self.assertEqual(0, stats["selected_us"])

    def test_quota_enforcement_two_uk_two_us_max(self):
        mapped = [
            {"name": "Sky Sports Premier League", "id": 1, "raw": "Sky Sports Premier League, 1"},
            {"name": "TNT Sports", "id": 2, "raw": "TNT Sports, 2"},
            {"name": "Premier Sports 1", "id": 3, "raw": "Premier Sports 1, 3"},
            {"name": "Fanatiz USA", "id": 4, "raw": "Fanatiz USA, 4"},
            {"name": "DAZN USA", "id": 5, "raw": "DAZN USA, 5"},
            {"name": "Peacock", "id": 6, "raw": "Peacock, 6"},
            {"name": "SuperSport Premier League", "id": 7, "raw": "SuperSport Premier League, 7"},
        ]
        selected, stats = select_mapped_event_channels(mapped, self.rules, candidate_index={})
        self.assertEqual(5, len(selected))
        self.assertEqual(2, stats["selected_uk"])
        self.assertEqual(2, stats["selected_us"])
        self.assertEqual(1, stats["selected_other"])

    def test_preferred_others_are_prioritized(self):
        mapped = [
            {"name": "Generic Other Channel", "id": 10, "raw": "Generic Other Channel, 10"},
            {"name": "SuperSport Laliga", "id": 11, "raw": "SuperSport Laliga, 11"},
            {"name": "MBC Shahid", "id": 12, "raw": "MBC Shahid, 12"},
        ]
        # Reduce to 2 total to verify preferred-other ordering.
        local_rules = dict(self.rules)
        local_rules["max_event_channels"] = 2
        selected, _stats = select_mapped_event_channels(mapped, local_rules, candidate_index={})
        selected_names = [row["name"] for row in selected]
        self.assertEqual(["SuperSport Laliga", "MBC Shahid"], selected_names)

    def test_mapped_only_selection_excludes_unmapped(self):
        mapped = [
            {"name": "Sky Sports Premier League", "id": 1, "raw": "Sky Sports Premier League, 1"},
            {"name": "Unknown", "id": None, "raw": "Unknown, null"},
        ]
        selected, _stats = select_mapped_event_channels(mapped, self.rules, candidate_index={})
        self.assertEqual(1, len(selected))
        self.assertEqual("Sky Sports Premier League", selected[0]["name"])

    def test_fallback_without_candidates_uses_heuristics(self):
        mapped = [
            {"name": "Sky Sports Football", "id": 21, "raw": "Sky Sports Football, 21"},
            {"name": "Fanatiz USA", "id": 22, "raw": "Fanatiz USA, 22"},
            {"name": "SuperSport Football", "id": 23, "raw": "SuperSport Football, 23"},
        ]
        selected, stats = select_mapped_event_channels(mapped, self.rules, candidate_index={})
        self.assertEqual(3, len(selected))
        self.assertEqual(1, stats["selected_uk"])
        self.assertEqual(1, stats["selected_us"])
        self.assertEqual(1, stats["selected_other"])

    def test_build_channel_candidates_shape(self):
        candidates = build_channel_candidates(
            ["Sky Sports Premier League", "Sky Sports Premier League", "Fanatiz USA"],
            profile_name="uk",
            bucket_hint="uk",
            preferred_other=False,
        )
        self.assertEqual(2, len(candidates))
        self.assertEqual("Sky Sports Premier League", candidates[0]["name"])
        self.assertEqual(["uk"], candidates[0]["profiles"])
        self.assertEqual(["uk"], candidates[0]["bucket_hints"])

    def test_country_metadata_classification(self):
        local_rules = dict(self.rules)
        local_rules["classification"] = {
            "uk": {"exact": [], "keywords": []},
            "us": {"exact": [], "keywords": []},
            "preferred_other": {"exact": [], "keywords": []},
        }
        local_rules["country_groups"] = {
            "uk": ["United Kingdom"],
            "us": ["United States"],
            "preferred_other": ["South Africa"],
        }
        mapped = [
            {"name": "Unknown UK Channel", "id": 100, "raw": "Unknown UK Channel, 100"},
            {"name": "Unknown US Channel", "id": 101, "raw": "Unknown US Channel, 101"},
            {"name": "Unknown ZA Channel", "id": 102, "raw": "Unknown ZA Channel, 102"},
        ]
        candidate_index = index_channel_candidates(
            [
                {"name": "Unknown UK Channel", "countries": ["United Kingdom"]},
                {"name": "Unknown US Channel", "countries": ["United States"]},
                {"name": "Unknown ZA Channel", "countries": ["South Africa"]},
            ]
        )
        selected, stats = select_mapped_event_channels(mapped, local_rules, candidate_index)
        self.assertEqual(3, len(selected))
        self.assertEqual(1, stats["selected_uk"])
        self.assertEqual(1, stats["selected_us"])
        self.assertEqual(1, stats["selected_other_preferred"])


if __name__ == "__main__":
    unittest.main()
