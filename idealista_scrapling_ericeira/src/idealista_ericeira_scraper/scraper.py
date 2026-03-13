from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
import re
from time import sleep as time_sleep
from time import sleep

from idealista_ericeira_scraper.core import (
    AppConfig,
    Journal,
    ProjectPaths,
    ScraplingClient,
    TargetConfig,
    append_jsonl,
    build_resume_state,
    filter_output_record,
    load_config,
    load_output_selection,
    read_jsonl,
    write_text_file,
)
from idealista_ericeira_scraper.parsers import (
    extract_listing_details,
    extract_listing_links,
    extract_next_page_url,
    is_blocked_html,
    page_number_from_url,
    response_status,
    response_text,
)


class BlockedResponseError(RuntimeError):
    pass


COOKIE_BUTTON_PATTERNS = (
    re.compile(r"aceitar", re.I),
    re.compile(r"accept", re.I),
    re.compile(r"aceptar", re.I),
    re.compile(r"agree", re.I),
)

COOKIE_SELECTOR_CANDIDATES = (
    "#didomi-notice-agree-button",
    "[data-testid='didomi-notice-agree-button']",
    "[id*='didomi'] button",
    "[class*='didomi'] button",
    "button[id*='accept']",
    "button[class*='accept']",
    "[id*='cookie'] button",
    "[class*='cookie'] button",
    "[id*='consent'] button",
    "[class*='consent'] button",
)


def dismiss_cookie_banner(page) -> bool:
    roots = [page, *list(getattr(page, "frames", []))]

    try:
        page.evaluate(
            """
            () => {
              const selectors = [
                '#didomi-notice-agree-button',
                '[data-testid="didomi-notice-agree-button"]',
                '[id*="didomi"] button',
                '[class*="didomi"] button'
              ];
              for (const selector of selectors) {
                const element = document.querySelector(selector);
                if (element) {
                  element.click();
                  return true;
                }
              }
              return false;
            }
            """
        )
        page.wait_for_timeout(500)
    except Exception:
        pass

    for root in roots:
        for pattern in COOKIE_BUTTON_PATTERNS:
            try:
                locator = root.get_by_role("button", name=pattern)
                if locator.count() > 0:
                    locator.first.click(timeout=1_500)
                    page.wait_for_timeout(700)
                    return True
            except Exception:
                continue

        for selector in COOKIE_SELECTOR_CANDIDATES:
            try:
                locator = root.locator(selector)
                if locator.count() > 0:
                    locator.first.click(timeout=1_500)
                    page.wait_for_timeout(700)
                    return True
            except Exception:
                continue

    return False


class IdealistaCrawler:
    def __init__(
        self,
        config_path: str | None = None,
        mode_override: str | None = None,
        headless_override: bool | None = None,
        logger=None,
    ) -> None:
        config, paths = load_config(config_path)
        if mode_override:
            config = replace(config, fetch=replace(config.fetch, mode=mode_override))
        if headless_override is not None:
            config = replace(config, fetch=replace(config.fetch, headless=headless_override))

        self.config: AppConfig = config
        self.paths: ProjectPaths = paths
        self.paths.ensure_dirs()
        if self.config.fetch.user_data_dir:
            Path(self.config.fetch.user_data_dir).mkdir(parents=True, exist_ok=True)
        self.run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.journal = Journal(self.paths.journal_file, self.run_id)
        self.state = build_resume_state(
            self.paths.discovery_index,
            self.paths.details_output,
            self.paths.journal_file,
        )
        self.output_selection = load_output_selection(self.paths.selection_file)
        self.logger = logger

    def status(self) -> dict:
        pending = len(self.state.indexed_listing_ids - self.state.completed_listing_ids)
        return {
            "config_file": str(self.paths.config_file),
            "discovered_pages": len(self.state.discovered_pages),
            "indexed_listings": len(self.state.indexed_listing_ids),
            "completed_listings": len(self.state.completed_listing_ids),
            "pending_listings": pending,
            "failure_counts": dict(self.state.failure_counts),
            "fetch_mode": self.config.fetch.mode,
            "proxy_configured": bool(self.config.fetch.proxy or self.config.fetch.proxies_file),
            "proxy_rotation": bool(self.config.fetch.proxies_file),
            "selection_file": str(self.paths.selection_file),
            "selected_output_fields": self.output_selection["selected_fields"],
        }

    def page_extract(self, target_names: list[str] | None = None, max_pages: int = 1) -> dict:
        saved = 0
        indexed_now = 0
        pages_done = 0
        page_results = []

        with ScraplingClient(self.config.fetch) as client:
            for target in self._selected_targets(target_names):
                current_url = target.search_url
                seen_in_run: set[str] = set()
                target_pages_done = 0

                while current_url:
                    if current_url in seen_in_run:
                        break
                    if max_pages and pages_done >= max_pages:
                        return {
                            "indexed_now": indexed_now,
                            "page_results": page_results,
                            "pages_done": pages_done,
                            "saved": saved,
                        }
                    if self.config.run.max_pages_per_target and target_pages_done >= self.config.run.max_pages_per_target:
                        break

                    self._log(f"[page] A abrir {current_url}")
                    response = client.fetch(current_url, page_action=self._build_page_action(current_url))
                    html = response_text(response)
                    status = response_status(response)
                    if (status is not None and status >= 400) or is_blocked_html(html):
                        self._store_blocked_html("page_extract", current_url, html)
                        self.journal.record(
                            "page_blocked",
                            http_status=status,
                            stage="page_extract",
                            page_url=current_url,
                            target_name=target.name,
                        )
                        raise BlockedResponseError(f"Resposta bloqueada em {current_url}")

                    listing_links = extract_listing_links(response, current_url)
                    page_number = page_number_from_url(current_url)

                    if not listing_links:
                        self._log(f"[page {page_number}] Nenhum anuncio encontrado em {current_url}")
                        self.state.discovered_pages.add(current_url)
                        self.journal.record(
                            "page_empty",
                            stage="page_extract",
                            page_url=current_url,
                            page_number=page_number,
                            target_name=target.name,
                        )
                        break

                    self._log(f"[page {page_number}] {len(listing_links)} anuncios encontrados em {target.name}")
                    page_indexed = 0
                    page_saved = 0
                    items = []

                    for item in listing_links:
                        seed = self._build_seed(item, target, current_url, page_number)
                        if self._index_seed(seed, stage="page_extract", log_prefix=f"[page {page_number}] "):
                            indexed_now += 1
                            page_indexed += 1

                        outcome = self._extract_seed(
                            client,
                            seed,
                            stage="page_extract",
                            log_prefix=f"[page {page_number}] ",
                        )
                        items.append(outcome)
                        if outcome["status"] == "saved":
                            saved += 1
                            page_saved += 1

                    self.state.discovered_pages.add(current_url)
                    seen_in_run.add(current_url)
                    pages_done += 1
                    target_pages_done += 1
                    self.journal.record(
                        "page_done",
                        stage="page_extract",
                        discovered_count=len(listing_links),
                        indexed_now=page_indexed,
                        page_number=page_number,
                        page_url=current_url,
                        saved_now=page_saved,
                        target_name=target.name,
                    )
                    page_results.append(
                        {
                            "items": items,
                            "indexed_now": page_indexed,
                            "page_number": page_number,
                            "page_url": current_url,
                            "saved": page_saved,
                            "target_name": target.name,
                        }
                    )
                    self._log(
                        f"[page {page_number}] Concluida: {page_saved} guardados agora, "
                        f"{page_indexed} indexados agora, ficheiro={self.paths.details_output}"
                    )

                    next_url = extract_next_page_url(response, current_url)
                    if not next_url:
                        break
                    current_url = next_url
                    self._sleep()

        return {
            "indexed_now": indexed_now,
            "page_results": page_results,
            "pages_done": pages_done,
            "saved": saved,
        }

    def warmup(
        self,
        target_names: list[str] | None = None,
        limit: int = 1,
        manual_seconds: int = 0,
        manual: bool = False,
    ) -> dict:
        results = []
        with ScraplingClient(self.config.fetch) as client:
            for target in self._selected_targets(target_names)[:limit]:
                page_action = self._build_page_action(
                    target.search_url,
                    manual=manual,
                    manual_seconds=manual_seconds,
                )
                response = client.fetch(
                    target.search_url,
                    wait_ms=max(self.config.fetch.wait_ms, 2500),
                    page_action=page_action,
                )
                html = response_text(response)
                status = response_status(response)
                blocked = (status is not None and status >= 400) or is_blocked_html(html)
                results.append(
                    {
                        "blocked": blocked,
                        "body_length": len(html),
                        "http_status": status,
                        "manual": manual,
                        "manual_seconds": manual_seconds,
                        "target_name": target.name,
                        "url": target.search_url,
                    }
                )
        return {"results": results}

    def discover(self, target_names: list[str] | None = None, max_pages: int | None = None) -> dict:
        indexed_now = 0
        pages_done = 0
        with ScraplingClient(self.config.fetch) as client:
            for target in self._selected_targets(target_names):
                current_url = target.search_url
                seen_in_run: set[str] = set()
                target_pages_done = 0
                while current_url:
                    if current_url in seen_in_run:
                        break
                    if max_pages and pages_done >= max_pages:
                        return {"pages_done": pages_done, "indexed_now": indexed_now}
                    if self.config.run.max_pages_per_target and target_pages_done >= self.config.run.max_pages_per_target:
                        break

                    self._log(f"[discover] A abrir {current_url}")
                    response = client.fetch(current_url, page_action=self._build_page_action(current_url))
                    html = response_text(response)
                    status = response_status(response)
                    if (status is not None and status >= 400) or is_blocked_html(html):
                        self._store_blocked_html("discover", current_url, html)
                        self.journal.record(
                            "page_blocked",
                            http_status=status,
                            stage="discover",
                            page_url=current_url,
                            target_name=target.name,
                        )
                        raise BlockedResponseError(f"Resposta bloqueada em {current_url}")

                    listing_links = extract_listing_links(response, current_url)
                    page_number = page_number_from_url(current_url)

                    if not listing_links:
                        self.state.discovered_pages.add(current_url)
                        self.journal.record(
                            "page_empty",
                            stage="discover",
                            page_url=current_url,
                            page_number=page_number,
                            target_name=target.name,
                        )
                        break

                    self._log(f"[discover] Pagina {page_number}: {len(listing_links)} anuncios encontrados")
                    page_new = 0
                    for item in listing_links:
                        seed = self._build_seed(item, target, current_url, page_number)
                        if self._index_seed(seed, stage="discover", log_prefix=f"[discover p{page_number}] "):
                            indexed_now += 1
                            page_new += 1

                    self.state.discovered_pages.add(current_url)
                    seen_in_run.add(current_url)
                    pages_done += 1
                    target_pages_done += 1
                    self.journal.record(
                        "page_done",
                        stage="discover",
                        discovered_count=len(listing_links),
                        indexed_now=page_new,
                        page_number=page_number,
                        page_url=current_url,
                        target_name=target.name,
                    )

                    next_url = extract_next_page_url(response, current_url)
                    if not next_url:
                        break
                    current_url = next_url
                    self._sleep()

        return {"pages_done": pages_done, "indexed_now": indexed_now}

    def extract(self, target_names: list[str] | None = None, limit: int | None = None) -> dict:
        saved = 0
        with ScraplingClient(self.config.fetch) as client:
            for seed in self._iter_index_records(target_names):
                outcome = self._extract_seed(client, seed, stage="extract", log_prefix="[extract] ")
                if outcome["status"] == "saved":
                    saved += 1
                    if limit and saved >= limit:
                        break

        return {"saved": saved}

    def crawl(
        self,
        target_names: list[str] | None = None,
        max_pages: int | None = None,
        limit: int | None = None,
    ) -> dict:
        discover_stats = self.discover(target_names=target_names, max_pages=max_pages)
        extract_stats = self.extract(target_names=target_names, limit=limit)
        return {"discover": discover_stats, "extract": extract_stats}

    def _iter_index_records(self, target_names: list[str] | None = None):
        selected = set(target_names or [])
        for record in read_jsonl(self.paths.discovery_index) or []:
            if selected and record.get("target_name") not in selected:
                continue
            yield record

    def _selected_targets(self, target_names: list[str] | None = None) -> list[TargetConfig]:
        if not target_names:
            return self.config.targets
        selected = set(target_names)
        return [target for target in self.config.targets if target.name in selected]

    def _build_seed(self, item: dict, target: TargetConfig, page_url: str, page_number: int) -> dict:
        return {
            "listing_id": item["listing_id"],
            "listing_type": target.listing_type,
            "page_number": page_number,
            "page_url": page_url,
            "position": item["position"],
            "property_scope": target.property_scope,
            "target_name": target.name,
            "url": item["url"],
        }

    def _index_seed(self, seed: dict, *, stage: str, log_prefix: str = "") -> bool:
        if seed["listing_id"] in self.state.indexed_listing_ids:
            return False

        append_jsonl(self.paths.discovery_index, seed)
        self.state.indexed_listing_ids.add(seed["listing_id"])
        self.journal.record("listing_indexed", stage=stage, **seed)
        self._log(f"{log_prefix}[index] {seed['listing_id']} -> {seed['url']}")
        return True

    def _extract_seed(self, client: ScraplingClient, seed: dict, *, stage: str, log_prefix: str = "") -> dict:
        listing_id = seed["listing_id"]
        if listing_id in self.state.completed_listing_ids:
            self._log(f"{log_prefix}[skip] {listing_id} ja estava no output")
            return {"listing_id": listing_id, "status": "skipped_completed", "url": seed["url"]}

        if self.state.failure_counts.get(listing_id, 0) >= self.config.run.max_retries:
            self._log(f"{log_prefix}[skip] {listing_id} atingiu max_retries")
            return {"listing_id": listing_id, "status": "skipped_max_retries", "url": seed["url"]}

        self._log(f"{log_prefix}[fetch] {listing_id} -> {seed['url']}")
        try:
            response = client.fetch(seed["url"], page_action=self._build_page_action(seed["url"]))
            html = response_text(response)
            status = response_status(response)
            if (status is not None and status >= 400) or is_blocked_html(html):
                self._store_blocked_html(stage, seed["url"], html, listing_id=listing_id)
                self.journal.record(
                    "detail_blocked",
                    http_status=status,
                    stage=stage,
                    listing_id=listing_id,
                    target_name=seed["target_name"],
                    url=seed["url"],
                )
                self._log(f"{log_prefix}[blocked] {listing_id} -> http_status={status}")
                raise BlockedResponseError(f"Resposta bloqueada em {seed['url']}")

            record, html = extract_listing_details(response, seed)
            if self.config.run.save_html_snapshots:
                snapshot_path = self.paths.html_snapshots / f"{listing_id}.html"
                write_text_file(snapshot_path, html, overwrite=self.config.run.snapshot_overwrite)
                record["html_snapshot_path"] = str(snapshot_path.relative_to(self.paths.root))

            output_record = filter_output_record(record, self.output_selection["selected_fields"])
            append_jsonl(self.paths.details_output, output_record)
            self.state.completed_listing_ids.add(listing_id)
            self.state.failure_counts.pop(listing_id, None)
            self.journal.record(
                "detail_done",
                stage=stage,
                listing_id=listing_id,
                target_name=seed["target_name"],
                url=seed["url"],
            )
            self._log(
                f"{log_prefix}[save] {listing_id} | {output_record.get('title') or '-'} | "
                f"{output_record.get('price_text') or '-'} | {self.paths.details_output}"
            )
            self._sleep()
            return {
                "listing_id": listing_id,
                "price_text": output_record.get("price_text"),
                "status": "saved",
                "title": output_record.get("title"),
                "url": seed["url"],
            }
        except BlockedResponseError:
            if self.config.run.stop_on_blocked_response:
                raise
            return {"listing_id": listing_id, "status": "blocked", "url": seed["url"]}
        except Exception as exc:
            self.state.failure_counts[listing_id] += 1
            self.journal.record(
                "detail_failed",
                stage=stage,
                attempt=self.state.failure_counts[listing_id],
                error=str(exc),
                listing_id=listing_id,
                target_name=seed["target_name"],
                url=seed["url"],
            )
            self._log(f"{log_prefix}[error] {listing_id} -> {exc}")
            self._sleep()
            return {"error": str(exc), "listing_id": listing_id, "status": "failed", "url": seed["url"]}

    def _build_page_action(
        self,
        target_url: str,
        *,
        manual: bool = False,
        manual_seconds: int = 0,
    ):
        if self.config.fetch.mode == "http":
            return None

        def page_action(page):
            cookies_clicked = dismiss_cookie_banner(page)
            if cookies_clicked:
                print(f"[cookies] Banner aceite automaticamente em {target_url}.", flush=True)

            if manual:
                print(
                    "[manual-warmup] Browser aberto em "
                    f"{target_url}. Resolve o desafio manualmente e carrega Enter aqui para continuar.",
                    flush=True,
                )
                try:
                    input()
                except EOFError as exc:
                    raise RuntimeError(
                        "O modo manual requer um terminal interativo. Corre o comando diretamente no teu terminal."
                    ) from exc
                return None

            if manual_seconds > 0:
                print(
                    f"[manual-warmup] Browser aberto para {manual_seconds}s em {target_url}. "
                    "Interage manualmente com a pagina se aparecer desafio."
                )
                time_sleep(manual_seconds)

            return None

        return page_action

    def _log(self, message: str) -> None:
        if self.logger is not None:
            self.logger(message)
            return
        print(message, flush=True)

    def _sleep(self) -> None:
        if self.config.run.request_delay_seconds > 0:
            sleep(self.config.run.request_delay_seconds)

    def _store_blocked_html(
        self,
        stage: str,
        url: str,
        html: str,
        listing_id: str | None = None,
    ) -> None:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        identifier = listing_id or "search"
        filename = f"blocked_{stage}_{identifier}_{timestamp}.html"
        path = self.paths.logs_dir / filename
        write_text_file(path, f"URL: {url}\n\n{html}", overwrite=True)
