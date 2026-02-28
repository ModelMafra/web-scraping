"""
Frontend de scraping OLX Carros com Streamlit + Playwright.

Como correr:
1) pip install streamlit playwright pandas
2) playwright install chromium
3) streamlit run olx_scraper_frontend_streamlit.py
"""

from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import streamlit as st
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

BASE_URL = "https://www.olx.pt/carros-motos-e-barcos/carros/"
OUTPUT_DIR = Path("output")
BRANDS_CACHE_FILE = OUTPUT_DIR / "brands_discovered.json"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint_olx_carros_frontend.json"
INCREMENTAL_JSONL_FILE = OUTPUT_DIR / "olx_carros_incremental_frontend.jsonl"
INCREMENTAL_CSV_FILE = OUTPUT_DIR / "olx_carros_incremental_frontend.csv"
FINAL_JSON_FILE = OUTPUT_DIR / "olx_carros_final_frontend.json"
FINAL_CSV_FILE = OUTPUT_DIR / "olx_carros_final_frontend.csv"

FIELDNAMES = [
    "ad_id",
    "title",
    "price",
    "location",
    "posted_date",
    "specs",
    "image_url",
    "ad_url",
    "brand_selected",
    "model_selected",
    "source_page_url",
    "scraped_at_utc",
]

PT_MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}
DATE_PT_PATTERN = re.compile(
    r"(?P<day>\d{1,2})\s+de\s+(?P<month>[a-zA-Zçãõáàâéêíóôúü]+)\s+de\s+(?P<year>\d{4})",
    flags=re.IGNORECASE,
)


@dataclass
class ScrapeConfig:
    run_mode: str  # crash_recovery | daily_refresh
    headless: bool
    delay_seconds: float


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def clean_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_price_value(value: str) -> Optional[int]:
    text = clean_text(value)
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    return int(digits)


def split_location_and_date(value: str) -> Tuple[str, str]:
    text = clean_text(value)
    if not text:
        return "", ""

    lower_text = text.lower()
    if "hoje" in lower_text:
        location = re.sub(r"\s*-\s*(para o topo a\s*)?hoje\s*$", "", text, flags=re.IGNORECASE)
        return clean_text(location), datetime.now().date().isoformat()

    if "ontem" in lower_text:
        location = re.sub(r"\s*-\s*(para o topo a\s*)?ontem\s*$", "", text, flags=re.IGNORECASE)
        return clean_text(location), (datetime.now().date() - timedelta(days=1)).isoformat()

    match = DATE_PT_PATTERN.search(text)
    if not match:
        return text, ""

    day = int(match.group("day"))
    month_name = match.group("month").strip().lower()
    month = PT_MONTHS.get(month_name)
    year = int(match.group("year"))

    posted_date = ""
    if month is not None:
        posted_date = f"{year:04d}-{month:02d}-{day:02d}"

    location = clean_text(text[: match.start()])
    location = re.sub(r"\s*-\s*para o topo a\s*$", "", location, flags=re.IGNORECASE)
    location = re.sub(r"\s*-\s*$", "", location).strip()

    return location, posted_date


def cookie_overlay_visible(page: Page) -> bool:
    try:
        return page.evaluate(
            """() => {
                const selectors = [
                    '#onetrust-consent-sdk .onetrust-pc-dark-filter',
                    '#onetrust-consent-sdk #onetrust-pc-sdk',
                    '#onetrust-consent-sdk #onetrust-banner-sdk',
                    '.onetrust-pc-dark-filter',
                ];
                return selectors.some((selector) => {
                    return [...document.querySelectorAll(selector)].some((el) => {
                        const st = window.getComputedStyle(el);
                        if (!st) return false;
                        const hidden = st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0';
                        const rect = el.getBoundingClientRect();
                        return !hidden && rect.width > 0 && rect.height > 0;
                    });
                });
            }"""
        )
    except Exception:
        return False


def hide_cookie_overlay(page: Page) -> None:
    try:
        page.evaluate(
            """() => {
                const selectors = [
                    '#onetrust-consent-sdk .onetrust-pc-dark-filter',
                    '#onetrust-consent-sdk #onetrust-pc-sdk',
                    '#onetrust-consent-sdk #onetrust-banner-sdk',
                    '.onetrust-pc-dark-filter',
                    '#onetrust-consent-sdk',
                ];
                for (const selector of selectors) {
                    for (const el of document.querySelectorAll(selector)) {
                        el.style.setProperty('display', 'none', 'important');
                        el.style.setProperty('visibility', 'hidden', 'important');
                        el.style.setProperty('pointer-events', 'none', 'important');
                    }
                }
            }"""
        )
    except Exception:
        pass


def accept_cookies(page: Page) -> None:
    selectors = [
        "#onetrust-accept-btn-handler",
        "#accept-recommended-btn-handler",
        "button:has-text('Aceitar')",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Accept')",
        "button:has-text('Accept all')",
        "button[aria-label='Fechar']",
        ".onetrust-close-btn-handler",
    ]

    for _ in range(3):
        clicked_any = False
        for selector in selectors:
            locator = page.locator(selector)
            if locator.count() > 0:
                try:
                    locator.first.click(timeout=1800, force=True)
                    page.wait_for_timeout(250)
                    clicked_any = True
                except Exception:
                    pass

        if not cookie_overlay_visible(page):
            return

        if not clicked_any:
            page.wait_for_timeout(300)

    hide_cookie_overlay(page)


def click_with_retry(page: Page, locator, timeout: int = 15000) -> None:
    try:
        locator.click(timeout=timeout)
        return
    except Exception:
        accept_cookies(page)
        hide_cookie_overlay(page)
        page.wait_for_timeout(250)
        locator.click(timeout=timeout, force=True)


def append_jsonl(rows: List[dict], path: Path) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_csv(rows: List[dict], path: Path, fieldnames: List[str]) -> None:
    if not rows:
        return
    file_exists = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def load_checkpoint(path: Path) -> dict:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "completed_models": [],
        "in_progress": None,
        "total_rows_written": 0,
        "updated_at_utc": None,
    }


def save_checkpoint(path: Path, payload: dict) -> None:
    payload["updated_at_utc"] = utc_now_iso()
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def model_key(brand_name: str, model_label: str) -> str:
    return f"{brand_name}|||{model_label}"


def list_brands_from_home(page: Page) -> List[dict]:
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_selector("[data-testid='category-dropdown']", timeout=45000)
    page.wait_for_timeout(1200)
    accept_cookies(page)

    dropdowns = page.locator("[data-testid='category-dropdown']")
    if dropdowns.count() < 2:
        raise RuntimeError("Não foi possível encontrar o dropdown de marcas.")

    click_with_retry(page, dropdowns.nth(1), timeout=20000)
    page.wait_for_timeout(900)
    page.wait_for_selector("#category-dropdown-list", timeout=10000)

    raw_values = page.locator("#category-dropdown-list button").all_inner_texts()

    brands = []
    for raw in raw_values:
        text = clean_text(raw)
        if not text:
            continue
        if text.lower().startswith("todos os anúncios"):
            continue

        match = re.match(r"^(.*?)(\d+)?$", text)
        if match:
            name = clean_text(match.group(1))
            count = int(match.group(2)) if match.group(2) else None
        else:
            name = text
            count = None

        if name:
            brands.append({"name": name, "count": count})

    page.keyboard.press("Escape")
    return brands


def open_multi_select_filter(page: Page, filter_label: str) -> bool:
    return page.evaluate(
        """(targetLabel) => {
            const blocks = [...document.querySelectorAll('[data-testid=\\"multi-select-filter\\"]')];
            const target = blocks.find((block) => {
                const label = block.querySelector('.css-95hdyi');
                const text = (label?.textContent || '').trim();
                return text === targetLabel;
            });
            if (!target) return false;
            const button = target.querySelector('[data-testid=\\"dropdown-head\\"]');
            if (!button) return false;
            button.click();
            return true;
        }""",
        filter_label,
    )


def get_brand_url_and_models(page: Page, brand_name: str) -> Tuple[str, List[str]]:
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_selector("[data-testid='category-dropdown']", timeout=45000)
    page.wait_for_timeout(1200)
    accept_cookies(page)

    dropdowns = page.locator("[data-testid='category-dropdown']")
    if dropdowns.count() < 2:
        raise RuntimeError("Dropdown de marcas não encontrado.")

    click_with_retry(page, dropdowns.nth(1), timeout=20000)
    page.wait_for_timeout(900)

    selected = page.evaluate(
        """(targetBrand) => {
            const cleanName = (text) => {
                const raw = (text || '').replace(/\\s+/g, ' ').trim();
                return raw.replace(/\\d+$/, '').trim();
            };
            const options = [...document.querySelectorAll('#category-dropdown-list button')];
            const target = options.find((btn) => cleanName(btn.textContent) === targetBrand);
            if (!target) return false;
            target.click();
            return true;
        }""",
        brand_name,
    )

    if not selected:
        return "", []

    page.wait_for_timeout(1200)
    accept_cookies(page)
    brand_url = page.url

    model_filter_found = open_multi_select_filter(page, "Modelo")
    if not model_filter_found:
        return brand_url, []

    page.wait_for_timeout(700)
    models = page.evaluate(
        """() => {
            const values = [];
            const options = [...document.querySelectorAll('label[role=\\"option\\"]')];
            for (const option of options) {
                const text = (option.textContent || '').replace(/\\s+/g, ' ').trim();
                if (!text) continue;
                if (text.toLowerCase() === 'mostrar tudo') continue;
                values.push(text);
            }
            return values;
        }"""
    )

    page.keyboard.press("Escape")
    return brand_url, models


def click_model_and_get_url(page: Page, brand_url: str, model_label: str) -> Optional[str]:
    page.goto(brand_url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1200)
    accept_cookies(page)

    model_filter_found = open_multi_select_filter(page, "Modelo")
    if not model_filter_found:
        return None

    page.wait_for_timeout(600)
    old_url = page.url

    clicked = page.evaluate(
        """(targetModel) => {
            const options = [...document.querySelectorAll('label[role=\\"option\\"]')];
            const target = options.find((option) => {
                const text = (option.textContent || '').replace(/\\s+/g, ' ').trim();
                return text === targetModel;
            });
            if (!target) return false;
            target.click();
            return true;
        }""",
        model_label,
    )

    if not clicked:
        return None

    for _ in range(6):
        page.wait_for_timeout(400)
        if page.url != old_url and "filter_enum_modelo" in page.url:
            return page.url

    try:
        page.keyboard.press("Escape")
    except Exception:
        pass

    for _ in range(10):
        page.wait_for_timeout(400)
        if page.url != old_url and "filter_enum_modelo" in page.url:
            return page.url

    try:
        page.mouse.click(20, 20)
    except Exception:
        pass

    for _ in range(6):
        page.wait_for_timeout(400)
        if page.url != old_url and "filter_enum_modelo" in page.url:
            return page.url

    return None


def extract_cards(page: Page, brand_name: str, model_label: str) -> List[dict]:
    payload = {
        "brand": brand_name,
        "model": model_label,
        "source_page_url": page.url,
        "scraped_at_utc": utc_now_iso(),
    }

    rows = page.evaluate(
        """(data) => {
            const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            const cards = [...document.querySelectorAll('[data-cy=\\"l-card\\"]')];

            return cards.map((card) => {
                const title = clean(card.querySelector('[data-cy=\\"ad-card-title\\"] h4')?.textContent);
                const price = clean(card.querySelector('[data-testid=\\"ad-price\\"]')?.textContent);
                const locationAndDate = clean(card.querySelector('[data-testid=\\"location-date\\"]')?.textContent);

                const specs = [...card.querySelectorAll('.css-1kfqt7f .css-h59g4b')]
                    .map((el) => clean(el.textContent))
                    .filter(Boolean)
                    .join(' | ');

                const imageEl = card.querySelector('img');
                const imageSrc = imageEl?.getAttribute('src') || imageEl?.getAttribute('data-src') || '';
                const imageUrl = imageSrc ? new URL(imageSrc, location.origin).href : '';

                const adAnchor = card.querySelector('[data-cy=\\"ad-card-title\\"] a') || card.querySelector('a[href]');
                const href = adAnchor?.getAttribute('href') || '';
                const absoluteUrl = href ? new URL(href, location.origin).href : '';
                const normalizedUrl = absoluteUrl ? absoluteUrl.split('?')[0] : '';

                return {
                    ad_id: clean(card.id || ''),
                    title,
                    price,
                    location_and_date: locationAndDate,
                    specs,
                    image_url: imageUrl,
                    ad_url: normalizedUrl,
                    brand_selected: data.brand,
                    model_selected: data.model,
                    source_page_url: data.source_page_url,
                    scraped_at_utc: data.scraped_at_utc,
                };
            });
        }""",
        payload,
    )

    normalized_rows = []
    for row in rows:
        row["price"] = parse_price_value(row.get("price", ""))
        location, posted_date = split_location_and_date(row.get("location_and_date", ""))
        row["location"] = location
        row["posted_date"] = posted_date
        row.pop("location_and_date", None)
        clean_row = {field: row.get(field) for field in FIELDNAMES}
        normalized_rows.append(clean_row)

    return normalized_rows


def get_next_page_url(page: Page) -> Optional[str]:
    forward = page.locator("[data-cy='pagination-forward']")
    if forward.count() == 0:
        return None
    href = forward.first.get_attribute("href")
    if not href:
        return None
    return urljoin("https://www.olx.pt", href)


def consolidate_outputs() -> Tuple[int, int]:
    if not INCREMENTAL_JSONL_FILE.exists():
        return 0, 0

    rows = []
    with INCREMENTAL_JSONL_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    if not rows:
        return 0, 0

    df = pd.DataFrame(rows)
    before = len(df)
    dedupe_cols = ["ad_id", "ad_url", "brand_selected", "model_selected"]
    existing_cols = [col for col in dedupe_cols if col in df.columns]
    if existing_cols:
        df = df.drop_duplicates(subset=existing_cols, keep="last")

    after = len(df)
    df.to_csv(FINAL_CSV_FILE, index=False, encoding="utf-8-sig")
    df.to_json(FINAL_JSON_FILE, orient="records", force_ascii=False, indent=2)
    return before, after


def discover_brands(force_refresh: bool = False) -> List[dict]:
    ensure_output_dir()
    if BRANDS_CACHE_FILE.exists() and not force_refresh:
        with BRANDS_CACHE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 2200})
        brands = list_brands_from_home(page)
        browser.close()

    with BRANDS_CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(brands, f, ensure_ascii=False, indent=2)
    return brands


def load_models_for_selected_brands(selected_brands: Iterable[str], headless: bool) -> Dict[str, List[str]]:
    results: Dict[str, List[str]] = {}
    if not selected_brands:
        return results

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page(viewport={"width": 1600, "height": 2200})

        for brand in selected_brands:
            try:
                _, models = get_brand_url_and_models(page, brand)
                results[brand] = models
            except Exception:
                results[brand] = []

        browser.close()

    return results


def run_scrape_job(
    selected_map: Dict[str, List[str]],
    config: ScrapeConfig,
    log: Callable[[str], None],
    progress: Callable[[float], None],
) -> None:
    ensure_output_dir()

    checkpoint_enabled = config.run_mode == "crash_recovery"
    checkpoint = load_checkpoint(CHECKPOINT_FILE) if checkpoint_enabled else {
        "completed_models": [],
        "in_progress": None,
        "total_rows_written": 0,
        "updated_at_utc": None,
    }
    completed_models = set(checkpoint.get("completed_models", []))

    total_targets = sum(len(models) for models in selected_map.values())
    done_targets = 0

    log(f"Modo: {config.run_mode}")
    log(f"Marcas: {len(selected_map)} | pares marca-modelo: {total_targets}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config.headless)
        context = browser.new_context(locale="pt-PT", viewport={"width": 1600, "height": 2200})
        page = context.new_page()

        for brand_name, models in selected_map.items():
            log(f"\\n========== Marca: {brand_name} ==========")

            try:
                brand_url, available_models = get_brand_url_and_models(page, brand_name)
            except Exception as error:
                log(f"[ERRO] Falha ao carregar marca '{brand_name}': {error}")
                done_targets += len(models)
                progress(min(done_targets / max(total_targets, 1), 1.0))
                continue

            if not brand_url:
                log(f"[AVISO] Marca '{brand_name}' sem URL válida.")
                done_targets += len(models)
                progress(min(done_targets / max(total_targets, 1), 1.0))
                continue

            target_models = models if models else available_models
            for model_label in target_models:
                key = model_key(brand_name, model_label)
                if checkpoint_enabled and key in completed_models:
                    done_targets += 1
                    progress(min(done_targets / max(total_targets, 1), 1.0))
                    continue

                in_progress = checkpoint.get("in_progress") or {}
                if (
                    checkpoint_enabled
                    and in_progress.get("brand_name") == brand_name
                    and in_progress.get("model_label") == model_label
                    and in_progress.get("next_page_url")
                ):
                    current_url = in_progress["next_page_url"]
                    log(f"[RETOMA] {brand_name} -> {model_label}")
                else:
                    model_url = click_model_and_get_url(page, brand_url, model_label)
                    if not model_url:
                        log(f"[AVISO] Modelo não clicável: {brand_name} -> {model_label}")
                        done_targets += 1
                        progress(min(done_targets / max(total_targets, 1), 1.0))
                        continue
                    current_url = model_url

                checkpoint["in_progress"] = {
                    "brand_name": brand_name,
                    "model_label": model_label,
                    "next_page_url": current_url,
                }
                checkpoint["completed_models"] = sorted(completed_models)
                if checkpoint_enabled:
                    save_checkpoint(CHECKPOINT_FILE, checkpoint)

                visited_pages = set()
                while current_url and current_url not in visited_pages:
                    visited_pages.add(current_url)
                    log(f"  [PÁGINA] {current_url}")

                    page.goto(current_url, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(1200)
                    accept_cookies(page)

                    rows = extract_cards(page, brand_name, model_label)
                    append_jsonl(rows, INCREMENTAL_JSONL_FILE)
                    append_csv(rows, INCREMENTAL_CSV_FILE, FIELDNAMES)

                    checkpoint["total_rows_written"] = checkpoint.get("total_rows_written", 0) + len(rows)
                    next_url = get_next_page_url(page)
                    checkpoint["in_progress"] = {
                        "brand_name": brand_name,
                        "model_label": model_label,
                        "next_page_url": next_url,
                    }
                    checkpoint["completed_models"] = sorted(completed_models)
                    if checkpoint_enabled:
                        save_checkpoint(CHECKPOINT_FILE, checkpoint)

                    if next_url == current_url:
                        break

                    current_url = next_url
                    time.sleep(config.delay_seconds)

                completed_models.add(key)
                checkpoint["completed_models"] = sorted(completed_models)
                checkpoint["in_progress"] = None
                if checkpoint_enabled:
                    save_checkpoint(CHECKPOINT_FILE, checkpoint)

                done_targets += 1
                progress(min(done_targets / max(total_targets, 1), 1.0))

        context.close()
        browser.close()

    before, after = consolidate_outputs()
    log("\\nScraping concluído.")
    log(f"JSONL incremental: {INCREMENTAL_JSONL_FILE}")
    log(f"CSV incremental:   {INCREMENTAL_CSV_FILE}")
    if before or after:
        log(f"Final (dedupe): {after}/{before} -> {FINAL_CSV_FILE} | {FINAL_JSON_FILE}")


def ui() -> None:
    st.set_page_config(page_title="OLX Scraper Frontend", layout="wide")
    st.title("OLX Carros Scraper - Frontend")
    st.caption("Escolhe marcas/modelos e executa scraping incremental para CSV e JSONL.")

    ensure_output_dir()

    with st.sidebar:
        st.header("Configuração")
        headless = st.checkbox("Headless", value=True)
        delay_seconds = st.slider("Delay entre páginas (segundos)", 0.0, 5.0, 1.0, 0.1)
        run_mode = st.selectbox(
            "Modo de execução",
            options=["daily_refresh", "crash_recovery"],
            index=0,
            help="daily_refresh revarre tudo; crash_recovery retoma do checkpoint.",
        )

        st.markdown("---")
        if st.button("Reset checkpoint", use_container_width=True):
            if CHECKPOINT_FILE.exists():
                CHECKPOINT_FILE.unlink()
                st.success("Checkpoint removido.")
            else:
                st.info("Não existe checkpoint para remover.")

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button("Atualizar marcas do site", use_container_width=True):
            try:
                brands = discover_brands(force_refresh=True)
                st.session_state["brands"] = brands
                st.success(f"Marcas atualizadas: {len(brands)}")
            except Exception as e:
                st.error(f"Erro ao atualizar marcas: {e}")

    if "brands" not in st.session_state:
        try:
            st.session_state["brands"] = discover_brands(force_refresh=False)
        except Exception as e:
            st.error(f"Não consegui carregar marcas: {e}")
            st.stop()

    brands = st.session_state.get("brands", [])
    brand_names = [b["name"] for b in brands]

    with col_b:
        st.metric("Marcas disponíveis", len(brand_names))

    mode = st.radio(
        "Como queres selecionar o scraping?",
        ["Marcas inteiras", "Modelos específicos"],
        horizontal=True,
    )

    selected_brands = st.multiselect("Escolhe marca(s)", brand_names)

    if mode == "Modelos específicos":
        if st.button("Carregar modelos das marcas selecionadas"):
            if not selected_brands:
                st.warning("Seleciona pelo menos uma marca primeiro.")
            else:
                model_map = load_models_for_selected_brands(selected_brands, headless=headless)
                st.session_state["model_map"] = model_map
                st.success("Modelos carregados.")

        model_map = st.session_state.get("model_map", {})
        selected_map: Dict[str, List[str]] = {}
        for brand in selected_brands:
            models = model_map.get(brand, [])
            if not models:
                st.info(f"Sem modelos carregados para {brand}.")
                continue
            picked = st.multiselect(f"Modelos - {brand}", models, key=f"models_{brand}")
            if picked:
                selected_map[brand] = picked
    else:
        # Marcas inteiras: modelos vazios sinalizam "todos os modelos da marca".
        selected_map = {brand: [] for brand in selected_brands}

    st.markdown("---")
    run = st.button("Iniciar scraping", type="primary", use_container_width=True)

    if run:
        if not selected_map:
            st.warning("Seleciona marcas/modelos antes de iniciar.")
            return

        log_box = st.empty()
        progress_bar = st.progress(0)
        logs: List[str] = []

        def log(msg: str) -> None:
            logs.append(msg)
            log_box.code("\n".join(logs[-200:]), language="text")

        def set_progress(value: float) -> None:
            progress_bar.progress(max(0.0, min(1.0, value)))

        config = ScrapeConfig(
            run_mode=run_mode,
            headless=headless,
            delay_seconds=delay_seconds,
        )

        try:
            run_scrape_job(
                selected_map=selected_map,
                config=config,
                log=log,
                progress=set_progress,
            )
            set_progress(1.0)
            st.success("Scraping terminado com sucesso.")
        except PlaywrightTimeoutError as e:
            st.error(f"Timeout no Playwright: {e}")
        except Exception as e:
            st.error(f"Erro durante scraping: {e}")

    st.markdown("---")
    st.subheader("Ficheiros de output")
    for p in [
        BRANDS_CACHE_FILE,
        CHECKPOINT_FILE,
        INCREMENTAL_JSONL_FILE,
        INCREMENTAL_CSV_FILE,
        FINAL_JSON_FILE,
        FINAL_CSV_FILE,
    ]:
        exists = "Sim" if p.exists() else "Não"
        st.write(f"- `{p}`: {exists}")


if __name__ == "__main__":
    ui()
