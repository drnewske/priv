import unittest
from pathlib import Path
import sys

TESTS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = TESTS_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from channel_name_placeholders import is_placeholder_channel_name


class ChannelNamePlaceholderTests(unittest.TestCase):
    def test_exact_placeholder_names(self):
        self.assertTrue(is_placeholder_channel_name("TBA"))
        self.assertTrue(is_placeholder_channel_name("tbc"))
        self.assertTrue(is_placeholder_channel_name("No Channel Available"))

    def test_suffix_placeholders(self):
        self.assertTrue(is_placeholder_channel_name("Sky Sports TBC"))
        self.assertTrue(is_placeholder_channel_name("Premier Sports TBD"))

    def test_regular_channel_names_not_marked(self):
        self.assertFalse(is_placeholder_channel_name("Sky Sports Main Event"))
        self.assertFalse(is_placeholder_channel_name("TBC Sports Network"))


if __name__ == "__main__":
    unittest.main()
