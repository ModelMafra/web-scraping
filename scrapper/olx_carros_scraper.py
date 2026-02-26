from __future__ import annotations

import json
import os
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

OLX_CARROS_URL = "https://www.olx.pt/carros-motos-e-barcos/carros/"
OUTPUT_FILE = Path("olx_carros_ads.json")


def dismiss_cookie_banner(page: Page) -> None:
    selectors = [
        "button:has-text('Aceitar')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "#onetrust-accept-btn-handler",
    ]

    for selector in selectors:
        try:
            page.locator(selector).first.click(timeout=2000)
            page.wait_for_timeout(500)
            return
        except Exception:
            continue


def scrape_olx_carros(max_items: int = 20, headless: bool = True) -> list[dict[str, str]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(OLX_CARROS_URL, wait_until="domcontentloaded", timeout=60000)

        dismiss_cookie_banner(page)

        for _ in range(4):
            page.mouse.wheel(0, 6000)
            page.wait_for_timeout(1200)

        ads = page.evaluate(
            """
            (maxItems) => {
                const seen = new Set();
                const cards = Array.from(document.querySelectorAll('a[href*="/d/anuncio/"]'));
                const results = [];

                for (const cardLink of cards) {
                    const href = cardLink.getAttribute('href');
                    if (!href) continue;

                    const fullUrl = new URL(href, window.location.origin).href;
                    if (seen.has(fullUrl)) continue;
                    seen.add(fullUrl);

                    const card = cardLink.closest('li, article, div');

                    const title =
                        cardLink.querySelector('[data-cy="ad-card-title"]')?.textContent?.trim() ||
                        cardLink.querySelector('h4, h5, h6, p')?.textContent?.trim() ||
                        cardLink.getAttribute('aria-label') ||
                        cardLink.textContent?.trim() ||
                        '';

                    const price =
                        card?.querySelector('[data-testid*="ad-price"]')?.textContent?.trim() ||
                        card?.querySelector('[data-testid*="price"]')?.textContent?.trim() ||
                        '';

                    const locationDate =
                        card?.querySelector('[data-testid*="location-date"]')?.textContent?.trim() ||
                        card?.querySelector('[data-testid*="location"]')?.textContent?.trim() ||
                        '';

                    results.push({
                        title,
                        price,
                        location_date: locationDate,
                        url: fullUrl,
                    });

                    if (results.length >= maxItems) break;
                }

                return results;
            }
            """,
            max_items,
        )

        browser.close()
        return ads


def main() -> None:
    max_items = int(os.getenv("MAX_ITEMS", "20"))
    headless = os.getenv("HEADLESS", "true").lower() == "true"

    ads = scrape_olx_carros(max_items=max_items, headless=headless)

    OUTPUT_FILE.write_text(json.dumps(ads, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Anuncios encontrados: {len(ads)}")
    print(f"Ficheiro gerado: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()
