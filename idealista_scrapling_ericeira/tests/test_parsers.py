from __future__ import annotations

import unittest

from idealista_ericeira_scraper.parsers import canonicalize_listing_url, extract_listing_id


class ParserTests(unittest.TestCase):
    def test_extract_listing_id_from_url(self) -> None:
        self.assertEqual(extract_listing_id("https://www.idealista.pt/imovel/33078708/"), "33078708")

    def test_canonicalize_relative_listing_url(self) -> None:
        self.assertEqual(
            canonicalize_listing_url("/imovel/34247104/?xtmc=1", "https://www.idealista.pt/comprar-casas/mafra/ericeira/"),
            "https://www.idealista.pt/imovel/34247104/",
        )


if __name__ == "__main__":
    unittest.main()
