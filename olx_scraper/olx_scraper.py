#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup

DEFAULT_BASE_URL = "https://www.olx.pt/carros-motos-e-barcos/carros/"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

PRICE_RE = re.compile(r"(\d[\d\s\.]*)(?:\s?€)")
AD_ID_RE = re.compile(r"\bID:\s*(\d{6,})\b")
POSTED_RE = re.compile(r"\bPublicado\s+(.+)")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def fetch(url: str, timeout: int = 20, retries: int = 3, backoff: float = 1.6):
    last_err = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.text
        except Exception as err:
            last_err = err
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
                continue
            raise last_err


def make_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url
    parts = urlparse(base_url)
    qs = parse_qs(parts.query)
    qs["page"] = [str(page)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parts._replace(query=new_query))


def parse_brand_links(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")
    links = {}
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("/carros-motos-e-barcos/carros/"):
            continue
        # skip the base category link
        if href.rstrip("/") == "/carros-motos-e-barcos/carros":
            continue
        # only one segment after /carros/... (brand)
        tail = href[len("/carros-motos-e-barcos/carros/"):].strip("/")
        if "/" in tail:
            continue
        full = urljoin(base_url, href)
        name = normalize_whitespace(a.get_text(" ")) or tail
        links[full] = name
    return links


def parse_listing_links(html: str, base_url: str, include_external: bool = False):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/"):
            full = urljoin(base_url, href)
        else:
            full = href
        if "/d/anuncio/" in full and "olx.pt" in full:
            links.add(full.split("?")[0])
            continue
        if include_external:
            if any(domain in full for domain in ("standvirtual.com", "imovirtual.com")):
                links.add(full.split("?")[0])
    return sorted(links)


def extract_attributes_from_dl(soup: BeautifulSoup):
    attrs = {}
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = normalize_whitespace(dt.get_text(" "))
            val = normalize_whitespace(dd.get_text(" "))
            if key and val:
                attrs[key] = val
    return attrs


def extract_attributes_from_text(soup: BeautifulSoup):
    attrs = {}
    text = soup.get_text("\n")
    for raw in text.splitlines():
        line = normalize_whitespace(raw)
        if not line or ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = normalize_whitespace(key)
        val = normalize_whitespace(val)
        if not key or not val:
            continue
        if len(key) > 45:
            continue
        # skip noisy keys
        if key.lower() in ("descrição", "descrição do vendedor"):
            continue
        # filter out timestamps and UI labels
        if key.lower().startswith("utilizador"):
            continue
        attrs.setdefault(key, val)
    return attrs


def find_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return normalize_whitespace(h1.get_text(" "))
    title = soup.find("title")
    if title:
        return normalize_whitespace(title.get_text(" "))
    return ""


def find_price(soup: BeautifulSoup) -> str:
    # common meta tags
    meta = soup.find("meta", attrs={"property": "product:price:amount"})
    if meta and meta.get("content"):
        return normalize_whitespace(meta["content"] + " €")
    meta = soup.find("meta", attrs={"itemprop": "price"})
    if meta and meta.get("content"):
        return normalize_whitespace(meta["content"] + " €")
    # explicit price elements
    for attr in ("data-testid", "itemprop"):
        tag = soup.find(attrs={attr: re.compile("price", re.I)})
        if tag:
            txt = normalize_whitespace(tag.get_text(" "))
            if "€" in txt:
                return txt
    # fallback: first short line with euro
    text = soup.get_text("\n")
    for raw in text.splitlines():
        line = normalize_whitespace(raw)
        if "€" in line and 1 < len(line) < 30:
            return line
    # last resort: regex
    m = PRICE_RE.search(text)
    if m:
        return normalize_whitespace(m.group(1) + " €")
    return ""


def find_posted(text: str) -> str:
    for raw in text.splitlines():
        line = normalize_whitespace(raw)
        if line.lower().startswith("publicado"):
            return line
    m = POSTED_RE.search(text)
    if m:
        return normalize_whitespace(m.group(1))
    return ""


def find_location(text: str) -> str:
    lines = [normalize_whitespace(l) for l in text.splitlines() if normalize_whitespace(l)]
    for i, line in enumerate(lines):
        if line.lower() == "localização" and i + 1 < len(lines):
            return lines[i + 1]
    # fallback: a line with " - " near posted date
    for line in lines:
        if " - " in line and any(month in line.lower() for month in [
            "janeiro", "fevereiro", "março", "abril", "maio", "junho",
            "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
        ]):
            return line.split(" - ")[0]
    return ""


def find_seller_type(text: str) -> str:
    if re.search(r"\bParticular\b", text):
        return "Particular"
    if re.search(r"\bProfissional\b", text):
        return "Profissional"
    return ""


def find_seller_name(soup: BeautifulSoup) -> str:
    # attempt to find seller name near common headings
    for tag in soup.find_all(["h3", "h4", "span", "div"]):
        txt = normalize_whitespace(tag.get_text(" "))
        if not txt:
            continue
        if txt.lower().startswith("publicado"):
            continue
        if txt.lower() in ("particular", "profissional"):
            continue
        if "No OLX" in txt or "Esteve online" in txt:
            continue
        # heuristic: seller name is usually short
        if 2 <= len(txt) <= 40:
            # skip common UI labels
            if txt.lower() in ("contactar", "mensagem", "telefone", "ver telefone"):
                continue
            return txt
    return ""


def extract_images(soup: BeautifulSoup):
    urls = []
    for img in soup.find_all("img", src=True):
        src = img.get("src")
        if not src:
            continue
        if "olx" in src and src not in urls:
            urls.append(src)
    return urls


def parse_listing_detail(url: str):
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")

    attrs = extract_attributes_from_dl(soup)
    if not attrs:
        attrs = extract_attributes_from_text(soup)

    title = find_title(soup)
    price_text = find_price(soup)
    location = find_location(text)
    posted = find_posted(text)
    seller_type = find_seller_type(text)
    seller_name = find_seller_name(soup)

    ad_id = ""
    m = AD_ID_RE.search(text)
    if m:
        ad_id = m.group(1)

    description = ""
    # try to locate description section
    lines = [normalize_whitespace(l) for l in text.splitlines() if normalize_whitespace(l)]
    for i, line in enumerate(lines):
        if line.lower() in ("descrição", "descricao", "descrição do vendedor") and i + 1 < len(lines):
            description = lines[i + 1]
            break

    images = extract_images(soup)

    return {
        "listing_url": url,
        "source_domain": urlparse(url).netloc,
        "title": title,
        "price": price_text,
        "location": location,
        "posted": posted,
        "ad_id": ad_id,
        "seller_type": seller_type,
        "seller_name": seller_name,
        "description": description,
        "images": images,
        "attributes": attrs,
    }


def build_rows(
    base_url: str,
    output_jsonl: str,
    include_external: bool,
    max_pages: int | None,
    max_listings: int | None,
    delay: float,
    brand_filter: set | None,
):
    base_html = fetch(base_url)
    brand_links = parse_brand_links(base_html, base_url)
    if brand_filter:
        brand_links = {
            url: name for url, name in brand_links.items()
            if name.lower() in brand_filter or url.rstrip("/").split("/")[-1] in brand_filter
        }
    if not brand_links:
        raise RuntimeError("No brand links found. The page structure may have changed.")

    attr_keys = set()
    total = 0

    with open(output_jsonl, "w", encoding="utf-8") as jf:
        for brand_url, brand_name in sorted(brand_links.items(), key=lambda x: x[1].lower()):
            page = 1
            seen_links = set()
            while True:
                if max_pages and page > max_pages:
                    break
                page_url = make_page_url(brand_url, page)
                html = fetch(page_url)
                listing_links = parse_listing_links(html, base_url, include_external)
                new_links = [l for l in listing_links if l not in seen_links]
                if not new_links:
                    break
                seen_links.update(new_links)

                for link in new_links:
                    if max_listings and total >= max_listings:
                        return attr_keys
                    try:
                        row = parse_listing_detail(link)
                    except Exception as err:
                        print(f"WARN: failed to parse {link}: {err}", file=sys.stderr)
                        continue
                    row["brand"] = brand_name
                    row["scraped_at"] = datetime.utcnow().isoformat() + "Z"

                    # also pull model from attributes if present
                    model = row.get("attributes", {}).get("Modelo") or row.get("attributes", {}).get("Modelo do veículo")
                    if model:
                        row["model"] = model

                    # collect attribute keys
                    for k in row.get("attributes", {}).keys():
                        attr_keys.add(k)

                    jf.write(json.dumps(row, ensure_ascii=False) + "\n")
                    total += 1

                    if delay:
                        time.sleep(delay)

                page += 1
    return attr_keys


def write_csv_from_jsonl(jsonl_path: str, csv_path: str, attr_keys: list[str], explode_attributes: bool):
    base_fields = [
        "brand",
        "model",
        "listing_url",
        "source_domain",
        "title",
        "price",
        "location",
        "posted",
        "ad_id",
        "seller_type",
        "seller_name",
        "description",
        "images",
        "scraped_at",
    ]

    if explode_attributes:
        fieldnames = base_fields + attr_keys
    else:
        fieldnames = base_fields + ["attributes"]

    with open(csv_path, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()

        with open(jsonl_path, "r", encoding="utf-8") as jf:
            for line in jf:
                row = json.loads(line)
                out = {k: row.get(k, "") for k in base_fields}
                out["images"] = json.dumps(row.get("images", []), ensure_ascii=False)

                if explode_attributes:
                    attrs = row.get("attributes", {}) or {}
                    for key in attr_keys:
                        out[key] = attrs.get(key, "")
                else:
                    out["attributes"] = json.dumps(row.get("attributes", {}), ensure_ascii=False)

                writer.writerow(out)


def main():
    parser = argparse.ArgumentParser(description="Scrape OLX car listings by brand and output CSV.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base category URL.")
    parser.add_argument("--output", default="olx_carros.csv", help="CSV output path.")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay in seconds between listing requests.")
    parser.add_argument("--max-pages", type=int, default=None, help="Max pages per brand (default: all).")
    parser.add_argument("--max-listings", type=int, default=None, help="Global max listings to scrape.")
    parser.add_argument("--include-external", action="store_true", help="Include external listings (e.g., standvirtual).")
    parser.add_argument("--brands", nargs="*", help="Limit to brand names or slugs (case-insensitive).")
    parser.add_argument("--no-explode-attributes", action="store_true", help="Keep all attributes in a JSON column instead of CSV columns.")

    args = parser.parse_args()

    brand_filter = None
    if args.brands:
        brand_filter = {b.strip().lower() for b in args.brands if b.strip()}

    tmp_jsonl = os.path.splitext(args.output)[0] + ".jsonl"

    attr_keys = build_rows(
        base_url=args.base_url,
        output_jsonl=tmp_jsonl,
        include_external=args.include_external,
        max_pages=args.max_pages,
        max_listings=args.max_listings,
        delay=args.delay,
        brand_filter=brand_filter,
    )

    attr_keys = sorted(attr_keys)
    explode = not args.no_explode_attributes
    write_csv_from_jsonl(tmp_jsonl, args.output, attr_keys, explode_attributes=explode)

    print(f"Done. CSV written to {args.output}")


if __name__ == "__main__":
    main()
