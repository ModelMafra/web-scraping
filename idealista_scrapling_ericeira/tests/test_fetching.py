from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from idealista_ericeira_scraper.fetching import load_proxy_entries


class FetchingTests(unittest.TestCase):
    def test_load_proxy_entries_supports_urls_and_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            proxy_file = Path(temp_dir) / "proxies.txt"
            proxy_file.write_text(
                "\n".join(
                    [
                        "# comentario",
                        "http://user:pass@proxy1.example.com:8000",
                        '{"server":"http://proxy2.example.com:8000","username":"user","password":"pass"}',
                    ]
                ),
                encoding="utf-8",
            )

            proxies = load_proxy_entries(proxy_file)

            self.assertEqual(proxies[0], "http://user:pass@proxy1.example.com:8000")
            self.assertEqual(proxies[1]["server"], "http://proxy2.example.com:8000")


if __name__ == "__main__":
    unittest.main()
