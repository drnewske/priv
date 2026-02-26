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
        self.assertEqual("one", scanner._find_target_match("One HD"))
        self.assertIsNone(scanner._find_target_match("One Sports"))

    @mock.patch("scan_sports_channels.shutil.which", return_value="ffprobe")
    def test_geo_prefix_is_ignored_but_numeric_suffix_is_not(self, _which):
        scanner = SportsScanner(target_channels=["Canal+ Sport", "RTL"], allow_ffmpeg_fallback=False)
        self.assertEqual("canal+ sport", scanner._find_target_match("AF - CANAL+ SPORT FHD"))
        self.assertEqual("rtl", scanner._find_target_match("NL - RTL 4K"))
        self.assertIsNone(scanner._find_target_match("AF - CANAL+ SPORT 2"))
        self.assertIsNone(scanner._find_target_match("NL - RTL 7 4K"))

    @mock.patch("scan_sports_channels.shutil.which", return_value="ffprobe")
    def test_sport24_and_sky_sport24_do_not_cross_match(self, _which):
        scanner = SportsScanner(
            target_channels=["Sport 24", "Sky Sport 24"],
            allow_ffmpeg_fallback=False,
        )
        self.assertEqual("sky sport 24", scanner._find_target_match("IT - SKY SPORT 24 UHD"))
        self.assertEqual("sport 24", scanner._find_target_match("UK - SPORT 24 HD"))
        scanner_only_short = SportsScanner(target_channels=["Sport 24"], allow_ffmpeg_fallback=False)
        self.assertIsNone(scanner_only_short._find_target_match("IT - SKY SPORT 24 UHD"))

    @mock.patch("scan_sports_channels.shutil.which", return_value="ffprobe")
    def test_event_only_feed_names_are_rejected(self, _which):
        scanner = SportsScanner(
            target_channels=["Vidio", "TNT Sports", "Sky Sports Main Event"],
            allow_ffmpeg_fallback=False,
        )
        self.assertIsNone(scanner._find_target_match("UK - VIDIO LIVE EVENTS | 15"))
        self.assertIsNone(
            scanner._find_target_match("D+ (UK) Events 47: TNT Sports Reload | Wed 31 Jul 20:45")
        )
        self.assertEqual("sky sports main event", scanner._find_target_match("UK: SKY SPORTS MAIN EVENT UHD"))

    @mock.patch("scan_sports_channels.shutil.which", return_value="ffprobe")
    def test_domain_cap_allows_quality_children_from_same_domain(self, _which):
        scanner = SportsScanner(
            target_channels=["Sky Sports Main Event"],
            max_streams_per_channel=2,  # cap is 2 domains
            allow_ffmpeg_fallback=False,
        )
        streams = [
            {"name": "Sky Sports Main Event HD", "url": "https://a.example/live/1.ts"},
            {"name": "Sky Sports Main Event FHD", "url": "https://a.example/live/2.ts"},
            {"name": "Sky Sports Main Event UHD", "url": "https://b.example/live/3.ts"},
            {"name": "Sky Sports Main Event 4K", "url": "https://c.example/live/4.ts"},
            {"name": "Sky Sports Main Event HD Backup 1", "url": "https://a.example/live/5.ts"},
        ]

        with mock.patch.object(scanner, "_validate_stream_url", return_value=True):
            added = scanner.process_streams(streams, api_instance=None, source_label="unit")

        self.assertEqual(3, added)  # a: HD + FHD, b: UHD/4K
        self.assertEqual(2, len(scanner.channel_domains["Sky Sports Main Event"]))
        self.assertTrue(scanner._is_channel_complete("Sky Sports Main Event"))

        kept_urls = set()
        for urls in scanner.channels["Sky Sports Main Event"]["qualities"].values():
            kept_urls.update(urls)
        self.assertIn("https://a.example/live/1.ts", kept_urls)
        self.assertIn("https://a.example/live/2.ts", kept_urls)
        self.assertIn("https://b.example/live/3.ts", kept_urls)
        self.assertNotIn("https://c.example/live/4.ts", kept_urls)
        self.assertNotIn("https://a.example/live/5.ts", kept_urls)

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
