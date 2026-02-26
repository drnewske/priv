import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import sys

TESTS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = TESTS_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from scan_sports_channels import (
    ChannelNormalizer,
    SportsScanner,
    infer_server_type,
    is_non_live_m3u_entry,
    is_probable_live_stream_url,
    load_target_channels,
)


class ScanChannelGuardrailsTests(unittest.TestCase):
    def test_vod_group_filter(self):
        self.assertTrue(is_non_live_m3u_entry("Movies", "Sky Sports", ""))
        self.assertTrue(is_non_live_m3u_entry("| VOD |", "ESPN", ""))
        self.assertFalse(is_non_live_m3u_entry("Sports", "Sky Sports", ""))

    def test_vod_url_filter(self):
        self.assertTrue(is_non_live_m3u_entry("", "Sky Sports", "https://x.test/movie/u/p/1.mp4"))
        self.assertTrue(is_non_live_m3u_entry("", "Sky Sports", "https://x.test/series/u/p/1.mkv"))
        self.assertFalse(is_non_live_m3u_entry("", "Sky Sports", "https://x.test/live/u/p/1.ts"))

    def test_vod_name_filter_when_group_missing(self):
        self.assertTrue(is_non_live_m3u_entry("", "Sky Captain (2004)", ""))
        self.assertTrue(is_non_live_m3u_entry("", "Show Name S01E03", ""))
        self.assertFalse(is_non_live_m3u_entry("Sports", "Sky Captain (2004)", ""))

    @mock.patch("scan_sports_channels.shutil.which", return_value="ffprobe")
    def test_boundary_matching_blocks_false_positives(self, _which):
        scanner = SportsScanner(target_channels=["fox", "one"], allow_ffmpeg_fallback=False)
        self.assertIsNone(scanner._find_target_match("Foxtel Sports"))
        self.assertIsNone(scanner._find_target_match("Zone Premium HD"))
        self.assertEqual("fox", scanner._find_target_match("FOX HD"))
        self.assertEqual("one", scanner._find_target_match("One Sports"))

    def test_min_target_length_guard(self):
        payload = {
            "schedule": [
                {
                    "date": "2026-02-26",
                    "events": [
                        {"channels": ["A", "BT", "Sky Sports Main Event"]},
                    ],
                }
            ]
        }
        with tempfile.NamedTemporaryFile("w+", suffix=".json", delete=False, encoding="utf-8") as handle:
            json.dump(payload, handle)
            temp_path = handle.name
        try:
            targets = load_target_channels(temp_path)
        finally:
            Path(temp_path).unlink(missing_ok=True)

        self.assertIn("Sky Sports Main Event", targets)
        self.assertNotIn("A", targets)
        self.assertNotIn("BT", targets)

    def test_xtream_m3u_is_forced_to_api_mode(self):
        url = "https://example.com/get.php?username=u&password=p&type=m3u_plus"
        self.assertEqual("api", infer_server_type(url))

    def test_non_live_url_guard(self):
        self.assertFalse(is_probable_live_stream_url("https://x.example/movie/a/b/123.mp4"))
        self.assertFalse(is_probable_live_stream_url("https://x.example/series/u/p/999.mkv"))
        self.assertTrue(is_probable_live_stream_url("https://x.example/live/u/p/12345.ts"))

    def test_quality_tiers_are_detected(self):
        normalizer = ChannelNormalizer()
        self.assertEqual("4K", normalizer.extract_quality("Sky Sports 4K"))
        self.assertEqual("FHD", normalizer.extract_quality("BT Sport FHD"))
        self.assertEqual("HD", normalizer.extract_quality("ESPN HD"))
        self.assertEqual("SD", normalizer.extract_quality("DAZN SD"))

    def test_normalize_uses_valid_regexes(self):
        normalizer = ChannelNormalizer()
        value = normalizer.normalize("Sky   Sports   []  ()  {}  HD")
        self.assertEqual("Sky Sports", value)


if __name__ == "__main__":
    unittest.main()
