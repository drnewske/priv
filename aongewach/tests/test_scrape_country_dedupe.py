import unittest

from bs4 import BeautifulSoup

from aongewach.scrape_schedule_livesporttv import _parse_match_international_channels


class ScrapeCountryDedupTests(unittest.TestCase):
    def test_duplicate_channel_is_owned_by_first_country_only(self) -> None:
        html = """
        <div class="inter_nation">
          <div class="list-tv-international">
            <div class="list-tv-country">Spain</div>
            <div class="list-tv-name">
              <a>Sky Sports</a>, <a>Canal+</a>
            </div>
          </div>
          <div class="list-tv-international">
            <div class="list-tv-country">Kenya</div>
            <div class="list-tv-name">
              <a>Sky Sports</a>, <a>SuperSport</a>
            </div>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        rows = _parse_match_international_channels(
            soup,
            keep_noisy_channels=False,
            target_countries=["Spain", "Kenya"],
            include_all=False,
        )

        self.assertEqual(rows.get("Spain"), ["Sky Sports", "Canal+"])
        self.assertEqual(rows.get("Kenya"), ["SuperSport"])


if __name__ == "__main__":
    unittest.main()
