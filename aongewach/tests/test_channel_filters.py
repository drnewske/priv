import os
import sys
import unittest

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(TEST_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from channel_filters import (  # noqa: E402
    REGION_ME,
    REGION_UK,
    REGION_US,
    REGION_ZA,
    detect_channel_region,
    is_usable_channel_name,
    normalize_channel_name,
    select_regional_channel_names,
)


class ChannelFilterTests(unittest.TestCase):
    def test_streaming_filters_block_platform_names(self):
        self.assertFalse(is_usable_channel_name("ESPN+ (Usa)"))
        self.assertFalse(is_usable_channel_name("DAZN (Rsa)"))
        self.assertFalse(is_usable_channel_name("Paramount+ (USA)"))
        self.assertTrue(is_usable_channel_name("ESPN 2 (Usa)"))
        self.assertTrue(is_usable_channel_name("DAZN 1 (Por)"))

    def test_detect_channel_region(self):
        self.assertEqual(REGION_US, detect_channel_region("NBC (Usa)"))
        self.assertEqual(REGION_ZA, detect_channel_region("SuperSport Premier League (Rsa)"))
        self.assertEqual(REGION_UK, detect_channel_region("TNT Sports 1 (Gbr)"))
        self.assertEqual(REGION_ME, detect_channel_region("beIN Sports MENA 1 (Ara)"))

    def test_normalize_channel_name_strips_country_suffix(self):
        self.assertEqual("CANAL+ Sport 1", normalize_channel_name("CANAL+ Sport 1 (Afr)"))
        self.assertEqual("BBC Sport", normalize_channel_name("BBC Sport (Gbr)"))
        self.assertEqual("DAZN 2", normalize_channel_name("DAZN 2 (Por)"))

    def test_regional_selection_prefers_target_mix(self):
        selected = select_regional_channel_names(
            [
                "NBC (Usa)",
                "SuperSport Premier League (Rsa)",
                "TNT Sports 1 (Gbr)",
                "beIN Sports MENA 1 (Ara)",
                "beIN Sports MENA 2 (Ara)",
            ],
            max_channels=4,
            include_uk=True,
        )
        self.assertEqual(4, len(selected))
        self.assertIn("NBC", selected)
        self.assertIn("SuperSport Premier League", selected)
        self.assertIn("TNT Sports 1", selected)
        self.assertIn("beIN Sports MENA 1", selected)

        selected_no_uk = select_regional_channel_names(
            [
                "NBC (Usa)",
                "SuperSport Premier League (Rsa)",
                "TNT Sports 1 (Gbr)",
                "beIN Sports MENA 1 (Ara)",
            ],
            max_channels=4,
            include_uk=False,
        )
        self.assertNotIn("TNT Sports 1", selected_no_uk)

    def test_africa_slot_prefers_supersport_then_canal(self):
        selected = select_regional_channel_names(
            [
                "Azam Sports 4 (Afr)",
                "CANAL+ Sport 1 (Afr)",
                "SuperSport Football (Afr)",
            ],
            max_channels=1,
            include_uk=False,
        )
        self.assertEqual(["SuperSport Football"], selected)


if __name__ == "__main__":
    unittest.main()
