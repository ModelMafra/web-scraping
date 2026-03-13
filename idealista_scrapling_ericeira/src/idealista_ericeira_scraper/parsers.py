from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urljoin, urlsplit, urlunsplit
import json
import re

from idealista_ericeira_scraper.io_utils import text_sha256, utc_now_iso

IDEALISTA_BASE = "https://www.idealista.pt"
LISTING_ID_RE = re.compile(r"/imovel/(?P<listing_id>\d+)(?:/|$)")
PAGE_NUMBER_RE = re.compile(r"/pagina-(?P<page>\d+)(?:[/.]|$)")
BLOCKED_PATTERNS = (
    "please enable js",
    "disable any ad blocker",
    "captcha-delivery.com",
    "attention required",
    "access denied",
)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def css_getall(response, selector: str) -> list[str]:
    try:
        values = response.css(selector).getall()
    except Exception:
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def css_get(response, selector: str) -> str | None:
    try:
        value = response.css(selector).get()
    except Exception:
        return None
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def response_text(response) -> str:
    for attribute in ("body", "text", "html"):
        value = getattr(response, attribute, None)
        if value is None:
            continue
        if callable(value):
            try:
                value = value()
            except TypeError:
                continue
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)
    text_nodes = [clean_text(text) for text in css_getall(response, "body ::text")]
    return " ".join(text_nodes).strip()


def response_status(response) -> int | None:
    for attribute in ("status", "status_code"):
        value = getattr(response, attribute, None)
        if isinstance(value, int):
            return value
    return None


def is_blocked_html(html: str) -> bool:
    lowered = html.lower()
    return any(pattern in lowered for pattern in BLOCKED_PATTERNS)


def strip_query_and_fragment(url: str) -> str:
    split = urlsplit(url)
    return urlunsplit((split.scheme, split.netloc, split.path, "", ""))


def extract_listing_id(url: str) -> str | None:
    match = LISTING_ID_RE.search(url)
    return match.group("listing_id") if match else None


def canonicalize_listing_url(url: str, base_url: str = IDEALISTA_BASE) -> str | None:
    absolute = strip_query_and_fragment(urljoin(base_url, url))
    listing_id = extract_listing_id(absolute)
    if not listing_id:
        return None
    return f"{IDEALISTA_BASE}/imovel/{listing_id}/"


def page_number_from_url(url: str) -> int:
    match = PAGE_NUMBER_RE.search(url)
    return int(match.group("page")) if match else 1


def extract_listing_links(response, page_url: str) -> list[dict]:
    links = css_getall(response, 'a[href*="/imovel/"]::attr(href)')
    seen_ids: set[str] = set()
    results: list[dict] = []

    for position, href in enumerate(links, start=1):
        canonical_url = canonicalize_listing_url(href, base_url=page_url)
        if not canonical_url:
            continue
        listing_id = extract_listing_id(canonical_url)
        if not listing_id or listing_id in seen_ids:
            continue
        seen_ids.add(listing_id)
        results.append(
            {
                "listing_id": listing_id,
                "url": canonical_url,
                "position": position,
            }
        )
    return results


def extract_next_page_url(response, current_url: str) -> str | None:
    for selector in (
        'link[rel="next"]::attr(href)',
        'a[rel="next"]::attr(href)',
        'a[href*="pagina-"]::attr(href)',
    ):
        candidates = css_getall(response, selector)
        if not candidates:
            continue
        current_page = page_number_from_url(current_url)
        best_url: str | None = None
        best_page: int | None = None
        for href in candidates:
            absolute = strip_query_and_fragment(urljoin(current_url, href))
            next_page = page_number_from_url(absolute)
            if next_page <= current_page:
                continue
            if best_page is None or next_page < best_page:
                best_page = next_page
                best_url = absolute
        if best_url:
            return best_url
    return None


def _flatten_json_like(value) -> Iterable[dict]:
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _flatten_json_like(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _flatten_json_like(nested)


def find_key_values(payload, key: str) -> list:
    matches = []
    for item in _flatten_json_like(payload):
        if key in item:
            matches.append(item[key])
    return matches


def parse_json_ld(response) -> list:
    objects = []
    for raw in css_getall(response, 'script[type="application/ld+json"]::text'):
        try:
            objects.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return objects


def extract_images_from_json_ld(json_ld_objects: list) -> list[str]:
    images: list[str] = []
    for payload in json_ld_objects:
        for value in find_key_values(payload, "image"):
            if isinstance(value, str):
                images.append(value)
            elif isinstance(value, list):
                images.extend(str(item) for item in value if isinstance(item, str))
    return dedupe_preserving_order(images)


def dedupe_preserving_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def first_non_empty(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value:
            cleaned = clean_text(value)
            if cleaned:
                return cleaned
    return None


def extract_meta(response) -> dict:
    data = {}
    for selector, key_prefix in (
        ("meta[property]", "property"),
        ("meta[name]", "name"),
    ):
        try:
            elements = response.css(selector)
        except Exception:
            continue
        for element in elements:
            attributes = getattr(element, "attrib", {})
            if not attributes:
                continue
            key = attributes.get(key_prefix)
            content = attributes.get("content")
            if key and content:
                data[key] = content
    return data


def extract_feature_list(response) -> list[str]:
    selectors = (
        ".details-property_features li::text",
        ".details-property-feature-one li::text",
        ".details-property-feature-two li::text",
        '[class*="feature"] li::text',
        '[class*="details"] li::text',
        ".info-features span::text",
    )
    values: list[str] = []
    for selector in selectors:
        values.extend(clean_text(item) for item in css_getall(response, selector))
    return dedupe_preserving_order(item for item in values if item)


def extract_definition_pairs(response) -> dict[str, str]:
    pairs: dict[str, str] = {}
    labels = [clean_text(item) for item in css_getall(response, "dt::text")]
    values = [clean_text(item) for item in css_getall(response, "dd::text")]
    if len(labels) == len(values):
        for label, value in zip(labels, values):
            if label and value:
                pairs[label] = value
    return pairs


def extract_colon_pairs(feature_list: Iterable[str]) -> dict[str, str]:
    pairs = {}
    for item in feature_list:
        if ":" not in item:
            continue
        key, value = item.split(":", 1)
        key = clean_text(key)
        value = clean_text(value)
        if key and value:
            pairs[key] = value
    return pairs


def parse_price_amount(price_text: str | None) -> int | None:
    if not price_text:
        return None
    digits = re.sub(r"\D", "", price_text)
    return int(digits) if digits else None


def extract_listing_details(response, seed: dict) -> tuple[dict, str]:
    html = response_text(response)
    json_ld = parse_json_ld(response)
    meta = extract_meta(response)
    feature_list = extract_feature_list(response)
    definition_pairs = extract_definition_pairs(response)
    feature_map = {**definition_pairs, **extract_colon_pairs(feature_list)}
    visible_text = " ".join(clean_text(text) for text in css_getall(response, "body ::text"))

    title = first_non_empty(
        [
            css_get(response, "h1::text"),
            meta.get("og:title"),
            next((value for value in find_key_values(json_ld, "name") if isinstance(value, str)), None),
        ]
    )
    description = first_non_empty(
        [
            css_get(response, '[class*="comment"] *::text'),
            meta.get("description"),
            next((value for value in find_key_values(json_ld, "description") if isinstance(value, str)), None),
        ]
    )
    address_value = next((value for value in find_key_values(json_ld, "address") if value), None)
    address = None
    if isinstance(address_value, dict):
        address = first_non_empty(
            [
                address_value.get("streetAddress"),
                address_value.get("addressLocality"),
                address_value.get("addressRegion"),
            ]
        )

    price_text = first_non_empty(
        [
            css_get(response, '[class*="price"]::text'),
            next((value for value in find_key_values(json_ld, "price") if isinstance(value, str)), None),
        ]
    )

    images = dedupe_preserving_order(
        [
            *css_getall(response, 'meta[property="og:image"]::attr(content)'),
            *css_getall(response, "img::attr(src)"),
            *extract_images_from_json_ld(json_ld),
        ]
    )

    final_url = strip_query_and_fragment(getattr(response, "url", seed["url"]))
    record = {
        **seed,
        "challenge_detected": is_blocked_html(html),
        "description": description,
        "feature_list": feature_list,
        "features": feature_map,
        "fetched_at": utc_now_iso(),
        "final_url": final_url,
        "html_sha256": text_sha256(html),
        "images": images,
        "json_ld": json_ld,
        "meta": meta,
        "page_text_excerpt": clean_text(visible_text)[:2000],
        "price_amount_eur": parse_price_amount(price_text),
        "price_text": price_text,
        "title": title,
        "address": address,
    }
    return record, html
