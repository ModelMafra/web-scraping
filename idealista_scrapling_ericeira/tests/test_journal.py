from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from idealista_ericeira_scraper.core import append_jsonl, build_resume_state


class JournalTests(unittest.TestCase):
    def test_resume_state_rebuilds_progress(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            index_path = root / "index.jsonl"
            details_path = root / "details.jsonl"
            journal_path = root / "journal.jsonl"

            append_jsonl(index_path, {"listing_id": "33078708", "url": "https://www.idealista.pt/imovel/33078708/"})
            append_jsonl(index_path, {"listing_id": "34247104", "url": "https://www.idealista.pt/imovel/34247104/"})
            append_jsonl(details_path, {"listing_id": "33078708", "title": "Moradia"})
            append_jsonl(journal_path, {"event": "page_done", "page_url": "https://www.idealista.pt/comprar-casas/mafra/ericeira/"})
            append_jsonl(journal_path, {"event": "detail_failed", "listing_id": "34247104"})

            state = build_resume_state(index_path, details_path, journal_path)

            self.assertEqual(state.indexed_listing_ids, {"33078708", "34247104"})
            self.assertEqual(state.completed_listing_ids, {"33078708"})
            self.assertEqual(state.discovered_pages, {"https://www.idealista.pt/comprar-casas/mafra/ericeira/"})
            self.assertEqual(state.failure_counts["34247104"], 1)


if __name__ == "__main__":
    unittest.main()
