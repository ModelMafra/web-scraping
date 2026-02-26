from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

URL = "https://extranet.infarmed.pt/pmro/Publico/ListagemPublica.aspx"
TABLE_SELECTOR = "#ctl00_ContentPlaceHolder1_gvDoacoesPublic"
PAGER_LINKS_SELECTOR = (
    "a[id^='ctl00_ContentPlaceHolder1_pageCounterDoacoes_'][id$='_Pager']"
)
SELECTED_PAGE_SELECTOR = (
    "a.pager-class-selected[id^='ctl00_ContentPlaceHolder1_pageCounterDoacoes_'][id$='_Pager']"
)
ELLIPSIS_REGEX = re.compile(r"^\s*\.\.\.\s*$")


def get_selected_page(page: Page) -> int:
    text = page.locator(SELECTED_PAGE_SELECTOR).first.inner_text().strip()
    match = re.search(r"\d+", text)
    if not match:
        raise RuntimeError(f"Nao foi possivel ler a pagina selecionada: {text!r}")
    return int(match.group(0))


def get_headers(page: Page) -> list[str]:
    headers = page.eval_on_selector(
        TABLE_SELECTOR,
        """(table) =>
            Array.from(table.querySelectorAll('tr th'))
                .map((th) => (th.textContent || '').replace(/\\s+/g, ' ').trim())
        """,
    )
    return [h for h in headers if h]


def extract_rows(page: Page, headers: list[str], page_number: int) -> list[dict[str, Any]]:
    raw_rows = page.eval_on_selector(
        TABLE_SELECTOR,
        """(table) => {
            const rows = [];
            const trs = Array.from(table.querySelectorAll('tr'));
            for (const tr of trs) {
                const tds = Array.from(tr.querySelectorAll('td'));
                if (!tds.length) continue;
                rows.push(
                    tds.map((td) => (td.textContent || '').replace(/\\s+/g, ' ').trim())
                );
            }
            return rows;
        }""",
    )

    items: list[dict[str, Any]] = []
    for row in raw_rows:
        item: dict[str, Any] = {"pagina": page_number}
        for idx, header in enumerate(headers):
            item[header] = row[idx] if idx < len(row) else ""
        if len(row) > len(headers):
            for extra_idx in range(len(headers), len(row)):
                item[f"coluna_{extra_idx + 1}"] = row[extra_idx]
        items.append(item)
    return items


def wait_for_selected_page_change(page: Page, previous_page: int, timeout: int = 30000) -> bool:
    try:
        page.wait_for_function(
            """({selector, previousPage}) => {
                const el = document.querySelector(selector);
                if (!el) return false;
                const current = parseInt((el.textContent || '').trim(), 10);
                return !Number.isNaN(current) && current !== previousPage;
            }""",
            arg={"selector": SELECTED_PAGE_SELECTOR, "previousPage": previous_page},
            timeout=timeout,
        )
        return True
    except PlaywrightTimeoutError:
        return False


def wait_for_selected_page(page: Page, target_page: int, timeout: int = 30000) -> bool:
    try:
        page.wait_for_function(
            """({selector, targetPage}) => {
                const el = document.querySelector(selector);
                if (!el) return false;
                const current = parseInt((el.textContent || '').trim(), 10);
                return !Number.isNaN(current) && current === targetPage;
            }""",
            arg={"selector": SELECTED_PAGE_SELECTOR, "targetPage": target_page},
            timeout=timeout,
        )
        return True
    except PlaywrightTimeoutError:
        return False


def click_page_number(page: Page, page_number: int) -> bool:
    pattern = re.compile(rf"^\s*{page_number}\s*$")
    locator = page.locator(PAGER_LINKS_SELECTOR).filter(has_text=pattern)
    if locator.count() == 0:
        return False
    locator.first.click()
    return wait_for_selected_page(page, page_number)


def click_forward_ellipsis(page: Page, current_page: int) -> bool:
    for _ in range(2):
        ellipsis = page.locator(PAGER_LINKS_SELECTOR).filter(has_text=ELLIPSIS_REGEX)
        if ellipsis.count() == 0:
            return False
        ellipsis.last.click()
        if wait_for_selected_page_change(page, current_page, timeout=20000):
            return True
    return False


def save_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def scrape_all(max_pages: int | None, headless: bool) -> list[dict[str, Any]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_selector(TABLE_SELECTOR, timeout=120000)
        page.wait_for_selector(SELECTED_PAGE_SELECTOR, timeout=120000)

        headers = get_headers(page)
        if not headers:
            browser.close()
            raise RuntimeError("Nao foram encontrados cabecalhos na tabela.")

        all_rows: list[dict[str, Any]] = []
        visited_pages: set[int] = set()

        safety_limit = (max_pages or 100000) + 200
        steps = 0

        while steps < safety_limit:
            steps += 1
            current_page = get_selected_page(page)

            if current_page not in visited_pages:
                page_rows = extract_rows(page, headers, current_page)
                all_rows.extend(page_rows)
                visited_pages.add(current_page)
                print(f"Pagina {current_page}: {len(page_rows)} linhas")

            if max_pages is not None and len(visited_pages) >= max_pages:
                break

            next_page = current_page + 1
            moved = click_page_number(page, next_page)
            if not moved:
                moved = click_forward_ellipsis(page, current_page)
            if not moved:
                break

            new_page = get_selected_page(page)
            if new_page in visited_pages and new_page <= current_page:
                break

        browser.close()
        return all_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper Playwright da tabela publica do Infarmed (PMRO)."
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limite de paginas a extrair (para teste). Por defeito extrai tudo.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Executa com browser visivel.",
    )
    parser.add_argument(
        "--json-output",
        default="infarmed_pmro_doacoes.json",
        help="Ficheiro de saida JSON.",
    )
    parser.add_argument(
        "--csv-output",
        default="infarmed_pmro_doacoes.csv",
        help="Ficheiro de saida CSV.",
    )
    args = parser.parse_args()

    rows = scrape_all(max_pages=args.max_pages, headless=not args.headed)

    json_path = Path(args.json_output)
    csv_path = Path(args.csv_output)
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    save_csv(csv_path, rows)

    print(f"Total de registos extraidos: {len(rows)}")
    print(f"JSON: {json_path.resolve()}")
    print(f"CSV: {csv_path.resolve()}")


if __name__ == "__main__":
    main()
