from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from idealista_ericeira_scraper.io_utils import append_jsonl, read_jsonl, utc_now_iso


@dataclass
class ResumeState:
    discovered_pages: set[str] = field(default_factory=set)
    indexed_listing_ids: set[str] = field(default_factory=set)
    completed_listing_ids: set[str] = field(default_factory=set)
    failure_counts: Counter[str] = field(default_factory=Counter)


class Journal:
    def __init__(self, path: Path, run_id: str) -> None:
        self.path = path
        self.run_id = run_id

    def record(self, event: str, **payload) -> None:
        append_jsonl(
            self.path,
            {
                "event": event,
                "run_id": self.run_id,
                "ts": utc_now_iso(),
                **payload,
            },
        )


def build_resume_state(index_path: Path, details_path: Path, journal_path: Path) -> ResumeState:
    state = ResumeState()

    for record in read_jsonl(index_path) or []:
        listing_id = record.get("listing_id")
        if listing_id:
            state.indexed_listing_ids.add(str(listing_id))

    for record in read_jsonl(details_path) or []:
        listing_id = record.get("listing_id")
        if listing_id:
            state.completed_listing_ids.add(str(listing_id))

    for record in read_jsonl(journal_path) or []:
        event = record.get("event")
        page_url = record.get("page_url")
        listing_id = record.get("listing_id")

        if event == "page_done" and page_url:
            state.discovered_pages.add(page_url)
        if event == "detail_failed" and listing_id:
            state.failure_counts[str(listing_id)] += 1
        if event == "detail_done" and listing_id:
            state.failure_counts.pop(str(listing_id), None)

    return state
