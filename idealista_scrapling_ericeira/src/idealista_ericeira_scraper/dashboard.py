from __future__ import annotations

import json
from math import ceil, floor
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse
import webbrowser

from dash import ALL, Dash, Input, Output, State, ctx, dcc, html
from dash.exceptions import PreventUpdate
import numpy as np
import pandas as pd
import plotly.graph_objects as go

from idealista_ericeira_scraper.core import build_paths, default_project_root, read_jsonl

DASHBOARD_CSS = """
body {
  margin: 0;
  min-height: 100vh;
  background:
    radial-gradient(circle at 12% 18%, rgba(255, 255, 255, 0.72), transparent 20%),
    radial-gradient(circle at 85% 15%, rgba(255, 255, 255, 0.38), transparent 18%),
    linear-gradient(140deg, #d8e7e6 0%, #efe7d7 55%, #f4efe2 100%);
  color: #1d262b;
  font-family: "Avenir Next", "Segoe UI", sans-serif;
}

* { box-sizing: border-box; }

.dash-shell {
  width: min(1720px, calc(100vw - 28px));
  margin: 14px auto;
  display: grid;
  gap: 18px;
}

.panel {
  border-radius: 28px;
  background: rgba(255, 255, 255, 0.84);
  border: 1px solid rgba(22, 39, 46, 0.12);
  box-shadow: 0 28px 70px rgba(20, 34, 40, 0.16);
  backdrop-filter: blur(12px);
  padding: 20px 22px;
  min-width: 0;
}

.hero {
  display: grid;
  gap: 10px;
  background:
    linear-gradient(135deg, rgba(255,255,255,0.9), rgba(213,239,240,0.82)),
    linear-gradient(180deg, rgba(255,255,255,0.75), rgba(255,255,255,0.35));
}

.eyebrow {
  margin: 0;
  font-size: 12px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: #0f6b72;
}

.hero h1,
.section-title,
.detail-title {
  margin: 0;
  font-family: "Iowan Old Style", "Palatino Linotype", serif;
  letter-spacing: -0.03em;
}

.hero h1 {
  font-size: clamp(36px, 5vw, 58px);
  line-height: 0.96;
}

.hero p,
.muted,
.helper,
.detail-description,
.empty-state {
  margin: 0;
  color: #66747a;
  line-height: 1.6;
}

.hero-actions {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
}

.nav-link {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  padding: 10px 14px;
  border: 1px solid rgba(15, 107, 114, 0.18);
  background: rgba(255,255,255,0.88);
  color: #0d3038;
  font-weight: 800;
  text-decoration: none;
}

.dashboard-grid {
  display: grid;
  grid-template-columns: 360px minmax(0, 1fr);
  gap: 18px;
  align-items: start;
}

.sidebar {
  display: grid;
  gap: 16px;
  align-content: start;
  position: sticky;
  top: 14px;
}

.sidebar-section {
  display: grid;
  gap: 12px;
}

.sidebar-section h2,
.panel-head h2,
.detail-section h2 {
  margin: 0;
  font-family: "Iowan Old Style", "Palatino Linotype", serif;
  font-size: 28px;
  letter-spacing: -0.03em;
}

.control-grid {
  display: grid;
  gap: 12px;
}

.control-stack {
  display: grid;
  gap: 8px;
}

.control-label {
  font-size: 13px;
  font-weight: 800;
  color: #45555c;
}

.mini-help {
  font-size: 12px;
  color: #66747a;
}

.button-row {
  display: grid;
  gap: 10px;
}

.kpi-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 14px;
}

.kpi-card,
.context-card,
.detail-meta-card {
  border-radius: 20px;
  border: 1px solid rgba(22, 39, 46, 0.12);
  background: rgba(255,255,255,0.88);
  padding: 14px 16px;
  transition: transform 180ms ease, box-shadow 180ms ease;
}

.kpi-card:hover,
.context-card:hover,
.listing-card:hover,
.detail-meta-card:hover {
  transform: translateY(-1px);
  box-shadow: 0 14px 32px rgba(20, 34, 40, 0.08);
}

.kpi-label,
.context-label,
.detail-meta-label {
  font-size: 12px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #66747a;
}

.kpi-value,
.context-value,
.detail-meta-value {
  margin-top: 6px;
  font-size: 28px;
  font-weight: 800;
  line-height: 1.05;
}

.kpi-sub,
.context-sub {
  margin-top: 6px;
  font-size: 12px;
  color: #66747a;
  line-height: 1.4;
}

.graph-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 18px;
}

.graph-panel {
  display: grid;
  gap: 12px;
}

.graph-box {
  min-height: 360px;
}

.lower-grid {
  display: grid;
  grid-template-columns: 420px minmax(0, 1fr);
  gap: 18px;
}

.list-panel {
  display: grid;
  gap: 14px;
  align-content: start;
}

.list-count {
  font-size: 13px;
  font-weight: 800;
  color: #66747a;
}

.listing-list {
  display: grid;
  gap: 10px;
  max-height: 980px;
  overflow: auto;
  padding-right: 4px;
}

.listing-card {
  width: 100%;
  border: 1px solid rgba(22, 39, 46, 0.12);
  border-radius: 22px;
  background: rgba(255,255,255,0.88);
  padding: 10px;
  display: grid;
  grid-template-columns: 84px minmax(0, 1fr);
  gap: 12px;
  text-align: left;
  cursor: pointer;
}

.listing-card.is-selected {
  background: rgba(213,239,240,0.82);
  border-color: rgba(15, 107, 114, 0.34);
}

.listing-thumb {
  width: 84px;
  height: 84px;
  border-radius: 16px;
  overflow: hidden;
  background: linear-gradient(180deg, #0d2430, #102a35);
  display: grid;
  place-items: center;
}

.listing-thumb img {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.listing-empty-thumb {
  padding: 8px;
  color: rgba(236, 244, 245, 0.82);
  font-size: 12px;
  text-align: center;
}

.listing-main {
  display: grid;
  gap: 4px;
  align-content: start;
}

.listing-title {
  margin: 0;
  font-size: 15px;
  font-weight: 800;
  line-height: 1.35;
  color: #1d262b;
}

.listing-meta {
  margin: 0;
  color: #66747a;
  font-size: 13px;
  line-height: 1.45;
}

.detail-shell {
  display: grid;
  gap: 16px;
}

.detail-head {
  display: grid;
  gap: 8px;
}

.detail-title {
  font-size: clamp(28px, 3vw, 44px);
  line-height: 0.98;
}

.pill-row {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.pill {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 7px 11px;
  background: rgba(15, 107, 114, 0.1);
  color: #0f6b72;
  font-size: 12px;
  font-weight: 800;
}

.detail-main-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.15fr) minmax(320px, 0.85fr);
  gap: 18px;
}

.gallery {
  display: grid;
  gap: 12px;
}

.hero-image {
  height: 420px;
  border-radius: 22px;
  overflow: hidden;
  background: linear-gradient(180deg, #0d2430, #102a35);
}

.hero-image img {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.thumb-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(72px, 1fr));
  gap: 8px;
}

.thumb-grid img {
  width: 100%;
  height: 72px;
  object-fit: cover;
  display: block;
  border-radius: 14px;
  border: 1px solid rgba(22, 39, 46, 0.12);
}

.detail-side {
  display: grid;
  gap: 14px;
  align-content: start;
}

.detail-meta-grid,
.context-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.detail-section {
  display: grid;
  gap: 10px;
}

.feature-list {
  margin: 0;
  padding-left: 18px;
  display: grid;
  gap: 7px;
  color: #1d262b;
  line-height: 1.5;
}

.section-actions {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
}

.status-note {
  min-height: 20px;
  font-size: 12px;
  font-weight: 800;
  color: #0f6b72;
}

.empty-state {
  display: grid;
  place-items: center;
  min-height: 220px;
  text-align: center;
  border: 1px dashed rgba(22, 39, 46, 0.18);
  border-radius: 22px;
  padding: 20px;
}

button {
  border: 0;
  border-radius: 999px;
  padding: 11px 15px;
  font: inherit;
  font-weight: 800;
  cursor: pointer;
}

button.primary {
  background: #0f6b72;
  color: white;
}

button.secondary {
  background: rgba(15, 107, 114, 0.12);
  color: #0f6b72;
}

button.ghost {
  background: rgba(21, 37, 44, 0.08);
  color: #1d262b;
}

.dash-dropdown .Select-control,
.dash-dropdown .Select-menu-outer {
  border-radius: 16px !important;
}

@media (max-width: 1480px) {
  .kpi-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .graph-grid {
    grid-template-columns: 1fr;
  }

  .lower-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 1120px) {
  .dashboard-grid,
  .detail-main-grid,
  .detail-meta-grid,
  .context-grid {
    grid-template-columns: 1fr;
  }

  .sidebar {
    position: static;
  }

  .kpi-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
"""

DASHBOARD_INDEX = f"""<!DOCTYPE html>
<html>
  <head>
    {{%metas%}}
    <title>Dashboard Analítico Ericeira</title>
    {{%favicon%}}
    {{%css%}}
    <style>{DASHBOARD_CSS}</style>
  </head>
  <body>
    {{%app_entry%}}
    <footer>
      {{%config%}}
      {{%scripts%}}
      {{%renderer%}}
    </footer>
  </body>
</html>
"""

AMENITY_FILTER_OPTIONS = (
    ("garage_included", "Garagem"),
    ("has_pool", "Piscina"),
    ("has_elevator", "Elevador"),
    ("has_terrace_or_varanda", "Terraço / varanda"),
)

DISPLAY_COLUMNS = [
    "listing_id",
    "title",
    "address",
    "property_type",
    "bedrooms",
    "bathrooms",
    "price_amount_eur",
    "area_m2",
    "price_per_m2_eur",
    "garage_included",
    "has_pool",
    "has_elevator",
    "has_terrace_or_varanda",
    "condition_bucket",
    "images_count",
    "url",
    "fetched_at",
]


def _short_text(value: str | None, limit: int = 280) -> str:
    text = str(value or "").strip()
    if not text:
        return "Sem descrição guardada."
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _safe_number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip().replace(" ", "")
    if not cleaned:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif cleaned.count(".") == 1 and len(cleaned.split(".")[1]) == 3:
        cleaned = cleaned.replace(".", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _project_root_from_config(config_path: str | None = None) -> Path:
    if not config_path:
        return default_project_root()
    config_file = Path(config_path).expanduser()
    if not config_file.is_absolute():
        config_file = (default_project_root() / config_file).resolve()
    if config_file.parent.name == "config":
        return config_file.parent.parent
    return default_project_root()


def _latest_detail_records(config_path: str | None = None) -> list[dict[str, Any]]:
    details_path = build_paths(_project_root_from_config(config_path)).details_output
    latest_by_listing: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(details_path) or []:
        listing_id = str(record.get("listing_id") or "").strip()
        if not listing_id:
            continue
        normalized = dict(record, listing_id=listing_id)
        previous = latest_by_listing.get(listing_id)
        if previous is None or str(normalized.get("fetched_at") or "") >= str(previous.get("fetched_at") or ""):
            latest_by_listing[listing_id] = normalized
    records = list(latest_by_listing.values())
    records.sort(key=lambda item: str(item.get("fetched_at") or ""), reverse=True)
    return records


def _clean_images(raw_images: list[Any] | None, limit: int = 48) -> list[str]:
    images: list[str] = []
    seen_assets: set[str] = set()
    for item in raw_images or []:
        if not isinstance(item, str):
            continue
        url = item.strip()
        if not url:
            continue
        lowered = url.lower()
        if any(token in lowered for token in (".svg", "logo", "icon", "flag", "avatar", "social", "maps.googleapis", "googleapis")):
            continue
        if not re.search(r"\.(?:jpe?g|png|webp)(?:\?|$)", lowered):
            continue
        asset_name = Path(urlparse(url).path).name
        asset_key = asset_name.rsplit(".", 1)[0] if asset_name else lowered
        if asset_key in seen_assets:
            continue
        seen_assets.add(asset_key)
        images.append(url)
        if len(images) >= limit:
            break
    return images


def _extract_area_m2(feature_list: list[str], fallback_text: str = "") -> float | None:
    preferred: list[float] = []
    fallback: list[float] = []
    for item in feature_list:
        matches = re.findall(r"(\d[\d\s\.,]*)\s*m²", item, flags=re.I)
        for match in matches:
            parsed = _safe_number(match)
            if parsed is None:
                continue
            if "área bruta" in item.lower() or "area bruta" in item.lower():
                preferred.append(parsed)
            fallback.append(parsed)
    if preferred:
        return max(preferred)
    if fallback:
        return max(fallback)
    for match in re.findall(r"(\d[\d\s\.,]*)\s*m²", fallback_text, flags=re.I):
        parsed = _safe_number(match)
        if parsed is not None:
            return parsed
    return None


def _extract_bedrooms(feature_list: list[str], title: str) -> str | None:
    for text in [*feature_list, title]:
        match = re.search(r"\bt\s*(\d{1,2})\b", text, flags=re.I)
        if match:
            return f"T{int(match.group(1))}"
    return None


def _extract_property_type(feature_list: list[str], title: str) -> str | None:
    if title:
        match = re.match(
            r"([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s]+?)(?:\s+t\d+\b|\s+à\s+(?:venda|renda|arrendar|alugar)\b)",
            title,
            flags=re.I,
        )
        if match:
            return match.group(1).strip()
    for item in feature_list:
        lowered = item.lower()
        if any(
            token in lowered
            for token in (
                "m²",
                "casa de banho",
                "casas de banho",
                "garagem",
                "elevador",
                "terraço",
                "terraco",
                "varanda",
                "estado",
                "piso",
                "andar",
                "ar condicionado",
                "lote de",
            )
        ):
            continue
        cleaned = item.strip()
        if cleaned:
            return cleaned
    return None


def _extract_bathrooms(feature_list: list[str]) -> int | None:
    for item in feature_list:
        match = re.search(r"(\d+)\s+casas?\s+de\s+banho", item, flags=re.I)
        if match:
            return int(match.group(1))
    return None


def _has_any(feature_list: list[str], *tokens: str) -> bool:
    joined = " | ".join(item.lower() for item in feature_list)
    return any(token in joined for token in tokens)


def _condition_bucket(feature_list: list[str]) -> str:
    joined = " | ".join(item.lower() for item in feature_list)
    if any(token in joined for token in ("novo empreendimento", "construído em 2025", "construido em 2025", "novo")):
        return "novo"
    if any(token in joined for token in ("segunda mão/bom estado", "segunda mao/bom estado", "bom estado")):
        return "bom_estado"
    return "desconhecido"


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    feature_list = [str(item).strip() for item in (record.get("feature_list") or []) if str(item).strip()]
    images = _clean_images(record.get("images") or [])
    title = str(record.get("title") or "").strip()
    address = str(record.get("address") or "").strip() or "Sem localizacao"
    price_amount = _safe_number(record.get("price_amount_eur"))
    if price_amount is None:
        price_text = str(record.get("price_text") or "")
        match = re.search(r"(\d[\d\s\.,]*)\s*€", price_text)
        price_amount = _safe_number(match.group(1)) if match else None
    area_m2 = _extract_area_m2(feature_list, f"{title} {record.get('description') or ''}")
    price_per_m2 = (price_amount / area_m2) if price_amount is not None and area_m2 not in (None, 0) else None
    property_type = _extract_property_type(feature_list, title) or "Nao identificado"
    bedrooms = _extract_bedrooms(feature_list, title) or "Nao indicado"
    bathrooms = _extract_bathrooms(feature_list)
    return {
        "listing_id": str(record.get("listing_id") or "").strip(),
        "title": title or "Sem titulo",
        "address": address,
        "url": str(record.get("url") or record.get("final_url") or "").strip(),
        "fetched_at": str(record.get("fetched_at") or "").strip(),
        "price_amount_eur": price_amount,
        "area_m2": area_m2,
        "price_per_m2_eur": price_per_m2,
        "property_type": property_type,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "images_count": len(images),
        "preview_image_url": images[0] if images else None,
        "images": images,
        "description": _short_text(record.get("description")),
        "feature_list": feature_list,
        "garage_included": _has_any(feature_list, "garagem incluída", "garagem incluida", "lugar de garagem incluído", "lugar de garagem incluido"),
        "has_pool": _has_any(feature_list, "piscina"),
        "has_elevator": _has_any(feature_list, "elevador"),
        "has_terrace_or_varanda": _has_any(feature_list, "terraço", "terraco", "varanda"),
        "condition_bucket": _condition_bucket(feature_list),
    }


def _records_to_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows = [_normalize_record(record) for record in records]
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "listing_id",
                "title",
                "address",
                "url",
                "fetched_at",
                "price_amount_eur",
                "area_m2",
                "price_per_m2_eur",
                "property_type",
                "bedrooms",
                "bathrooms",
                "images_count",
                "preview_image_url",
                "images",
                "description",
                "feature_list",
                "garage_included",
                "has_pool",
                "has_elevator",
                "has_terrace_or_varanda",
                "condition_bucket",
            ]
        )
    frame["price_amount_eur"] = pd.to_numeric(frame["price_amount_eur"], errors="coerce")
    frame["area_m2"] = pd.to_numeric(frame["area_m2"], errors="coerce")
    frame["price_per_m2_eur"] = pd.to_numeric(frame["price_per_m2_eur"], errors="coerce")
    frame["bathrooms"] = pd.to_numeric(frame["bathrooms"], errors="coerce")
    frame["images_count"] = pd.to_numeric(frame["images_count"], errors="coerce").fillna(0).astype(int)
    frame["fetched_at"] = frame["fetched_at"].fillna("")
    frame = frame.sort_values("fetched_at", ascending=False, na_position="last").reset_index(drop=True)
    return frame


def load_dashboard_frame(config_path: str | None = None) -> pd.DataFrame:
    return _records_to_frame(_latest_detail_records(config_path))


def _serialize_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return json.loads(frame.to_json(orient="records"))


def _deserialize_frame(records: list[dict[str, Any]] | None) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame.from_records(records)
    for column in ("price_amount_eur", "area_m2", "price_per_m2_eur", "bathrooms", "images_count"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _numeric_bounds(series: pd.Series) -> tuple[int, int]:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return (0, 1)
    return (int(floor(valid.min())), int(ceil(valid.max())))


def _slider_step(bounds: tuple[int, int]) -> int:
    low, high = bounds
    spread = max(high - low, 1)
    return max(1, int(spread / 160))


def _slider_marks(bounds: tuple[int, int], suffix: str = "") -> dict[int, str]:
    low, high = bounds
    if low == high:
        return {low: f"{low:,}{suffix}".replace(",", ".")}
    mid = int((low + high) / 2)
    return {
        low: f"{low:,}{suffix}".replace(",", "."),
        mid: f"{mid:,}{suffix}".replace(",", "."),
        high: f"{high:,}{suffix}".replace(",", "."),
    }


def _sort_bedrooms(values: list[str]) -> list[str]:
    def sort_key(item: str) -> tuple[int, int | str]:
        match = re.match(r"^T(\d+)$", item)
        if match:
            return (0, int(match.group(1)))
        return (1, item)

    return sorted(values, key=sort_key)


def _filter_options(frame: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    return {
        "locations": [{"label": value, "value": value} for value in sorted(frame["address"].dropna().astype(str).unique())],
        "property_types": [{"label": value, "value": value} for value in sorted(frame["property_type"].dropna().astype(str).unique())],
        "bedrooms": [{"label": value, "value": value} for value in _sort_bedrooms(frame["bedrooms"].dropna().astype(str).unique().tolist())],
        "bathrooms": [
            {"label": str(int(value)), "value": int(value)}
            for value in sorted(v for v in frame["bathrooms"].dropna().unique().tolist())
        ],
    }


def _filter_defaults(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "price_range": list(_numeric_bounds(frame["price_amount_eur"])),
        "area_range": list(_numeric_bounds(frame["area_m2"])),
        "price_per_m2_range": list(_numeric_bounds(frame["price_per_m2_eur"])),
        "locations": [],
        "property_types": [],
        "bedrooms": [],
        "bathrooms": [],
        "amenities": [],
        "search_text": "",
        "sort_order": "latest",
    }


def _apply_range_filter(frame: pd.DataFrame, column: str, selected: list[int] | None, full_range: list[int]) -> pd.DataFrame:
    if not selected or len(selected) != 2 or selected == full_range:
        return frame
    series = pd.to_numeric(frame[column], errors="coerce")
    mask = series.between(selected[0], selected[1], inclusive="both")
    return frame.loc[mask]


def apply_dashboard_filters(frame: pd.DataFrame, filters: dict[str, Any], defaults: dict[str, Any]) -> pd.DataFrame:
    filtered = frame.copy()
    search_text = str(filters.get("search_text") or "").strip().lower()
    if search_text:
        search_space = (
            filtered["title"].fillna("")
            + " "
            + filtered["address"].fillna("")
            + " "
            + filtered["listing_id"].fillna("")
            + " "
            + filtered["property_type"].fillna("")
        ).str.lower()
        filtered = filtered.loc[search_space.str.contains(re.escape(search_text), na=False)]

    filtered = _apply_range_filter(filtered, "price_amount_eur", filters.get("price_range"), defaults["price_range"])
    filtered = _apply_range_filter(filtered, "area_m2", filters.get("area_range"), defaults["area_range"])
    filtered = _apply_range_filter(filtered, "price_per_m2_eur", filters.get("price_per_m2_range"), defaults["price_per_m2_range"])

    for column in ("locations", "property_types", "bedrooms", "bathrooms"):
        values = filters.get(column) or []
        if values:
            target_column = {
                "locations": "address",
                "property_types": "property_type",
                "bedrooms": "bedrooms",
                "bathrooms": "bathrooms",
            }[column]
            filtered = filtered.loc[filtered[target_column].isin(values)]

    amenity_values = set(filters.get("amenities") or [])
    for key, _label in AMENITY_FILTER_OPTIONS:
        if key in amenity_values:
            filtered = filtered.loc[filtered[key] == True]  # noqa: E712

    sort_order = filters.get("sort_order") or "latest"
    if sort_order == "price_desc":
        filtered = filtered.sort_values("price_amount_eur", ascending=False, na_position="last")
    elif sort_order == "price_asc":
        filtered = filtered.sort_values("price_amount_eur", ascending=True, na_position="last")
    else:
        filtered = filtered.sort_values("fetched_at", ascending=False, na_position="last")
    return filtered.reset_index(drop=True)


def _format_number(value: Any, digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "—"
    if digits == 0:
        return f"{int(round(float(value))):,}".replace(",", ".")
    return f"{float(value):,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _format_euro(value: Any, digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "—"
    return f"{_format_number(value, digits)} €"


def _format_percentile(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{round(value)}º percentil"


def _format_delta(value: float | None) -> str:
    if value is None:
        return "—"
    signal = "+" if value > 0 else ""
    return f"{signal}{_format_euro(value)}"


def _format_zscore(value: float | None) -> str:
    if value is None:
        return "—"
    signal = "+" if value > 0 else ""
    return f"{signal}{value:.2f}"


def _percentile_of_value(reference: pd.Series, value: float | None) -> float | None:
    cleaned = pd.to_numeric(reference, errors="coerce").dropna()
    if value is None or cleaned.empty:
        return None
    return float((cleaned <= value).mean() * 100)


def _zscore_of_value(reference: pd.Series, value: float | None) -> float | None:
    cleaned = pd.to_numeric(reference, errors="coerce").dropna()
    if value is None or len(cleaned) < 2:
        return None
    std = float(cleaned.std(ddof=0))
    if std == 0:
        return None
    return (float(value) - float(cleaned.mean())) / std


def compute_listing_context(filtered_df: pd.DataFrame, selected_listing_id: str | None) -> dict[str, Any]:
    if not selected_listing_id or filtered_df.empty:
        return {
            "comparison_cards": [],
            "export_row": None,
            "reference_count": 0,
            "selected": None,
        }

    selected_rows = filtered_df.loc[filtered_df["listing_id"] == selected_listing_id]
    if selected_rows.empty:
        return {
            "comparison_cards": [],
            "export_row": None,
            "reference_count": 0,
            "selected": None,
        }

    selected = selected_rows.iloc[0].to_dict()
    reference = filtered_df.loc[filtered_df["listing_id"] != selected_listing_id]
    reference_count = len(reference)

    def metric_context(label: str, metric_key: str, formatter, helper: str) -> dict[str, Any]:
        value = selected.get(metric_key)
        series = reference[metric_key] if metric_key in reference.columns else pd.Series(dtype=float)
        return {
            "label": label,
            "value": formatter(value),
            "helper": helper if pd.notna(value) else "Sem dado suficiente neste anúncio.",
        }

    selected_price = selected.get("price_amount_eur")
    selected_ppm2 = selected.get("price_per_m2_eur")
    selected_area = selected.get("area_m2")
    price_reference = reference["price_amount_eur"] if "price_amount_eur" in reference else pd.Series(dtype=float)
    ppm2_reference = reference["price_per_m2_eur"] if "price_per_m2_eur" in reference else pd.Series(dtype=float)
    area_reference = reference["area_m2"] if "area_m2" in reference else pd.Series(dtype=float)

    price_mean = float(price_reference.dropna().mean()) if not price_reference.dropna().empty else None
    price_median = float(price_reference.dropna().median()) if not price_reference.dropna().empty else None

    comparison_cards = [
        {
            "label": "Percentil do preço",
            "value": _format_percentile(_percentile_of_value(price_reference, selected_price)),
            "helper": "Posição do anúncio face aos restantes filtrados.",
        },
        {
            "label": "Percentil do preço / m²",
            "value": _format_percentile(_percentile_of_value(ppm2_reference, selected_ppm2)),
            "helper": "Compara valorização relativa por metro quadrado.",
        },
        {
            "label": "Percentil da área",
            "value": _format_percentile(_percentile_of_value(area_reference, selected_area)),
            "helper": "Tamanho do anúncio face ao resto do conjunto filtrado.",
        },
        {
            "label": "Preço vs média",
            "value": _format_delta(None if selected_price is None or price_mean is None else selected_price - price_mean),
            "helper": "Diferença para o preço médio dos outros anúncios filtrados.",
        },
        {
            "label": "Preço vs mediana",
            "value": _format_delta(None if selected_price is None or price_median is None else selected_price - price_median),
            "helper": "Diferença para a mediana dos outros anúncios filtrados.",
        },
        {
            "label": "Z-score do preço",
            "value": _format_zscore(_zscore_of_value(price_reference, selected_price)),
            "helper": "Desvio padrão relativo ao preço dos restantes anúncios.",
        },
        {
            "label": "Z-score do preço / m²",
            "value": _format_zscore(_zscore_of_value(ppm2_reference, selected_ppm2)),
            "helper": "Posição relativa do preço por metro quadrado.",
        },
    ]

    export_row = {
        "listing_id": selected.get("listing_id"),
        "title": selected.get("title"),
        "address": selected.get("address"),
        "price_amount_eur": selected_price,
        "price_per_m2_eur": selected_ppm2,
        "area_m2": selected_area,
        "price_percentile": _percentile_of_value(price_reference, selected_price),
        "price_per_m2_percentile": _percentile_of_value(ppm2_reference, selected_ppm2),
        "area_percentile": _percentile_of_value(area_reference, selected_area),
        "price_delta_vs_mean_eur": None if selected_price is None or price_mean is None else selected_price - price_mean,
        "price_delta_vs_median_eur": None if selected_price is None or price_median is None else selected_price - price_median,
        "price_zscore": _zscore_of_value(price_reference, selected_price),
        "price_per_m2_zscore": _zscore_of_value(ppm2_reference, selected_ppm2),
        "reference_count": reference_count,
    }

    return {
        "comparison_cards": comparison_cards,
        "export_row": export_row,
        "reference_count": reference_count,
        "selected": selected,
    }


def _graph_layout(title: str) -> dict[str, Any]:
    return {
        "title": {"text": title, "x": 0.02, "xanchor": "left"},
        "paper_bgcolor": "rgba(255,255,255,0)",
        "plot_bgcolor": "rgba(255,255,255,0)",
        "margin": {"l": 44, "r": 18, "t": 54, "b": 44},
        "font": {"family": '"Avenir Next", "Segoe UI", sans-serif', "color": "#1d262b"},
        "transition": {"duration": 320, "easing": "cubic-in-out"},
    }


def build_price_histogram(filtered_df: pd.DataFrame, selected_listing_id: str | None) -> go.Figure:
    selected_price = None
    reference = filtered_df
    if selected_listing_id:
        selected_rows = filtered_df.loc[filtered_df["listing_id"] == selected_listing_id]
        if not selected_rows.empty:
            selected_price = selected_rows.iloc[0]["price_amount_eur"]
            reference = filtered_df.loc[filtered_df["listing_id"] != selected_listing_id]
    values = pd.to_numeric(reference.get("price_amount_eur"), errors="coerce").dropna()

    fig = go.Figure()
    fig.add_trace(
        go.Histogram(
            x=values,
            nbinsx=min(max(len(values), 10), 40),
            marker_color="#6aa6ae",
            hovertemplate="Preço: %{x:,.0f} €<br>Anúncios: %{y}<extra></extra>",
            name="Distribuição",
        )
    )
    if selected_price is not None and not pd.isna(selected_price):
        fig.add_vline(
            x=float(selected_price),
            line_width=3,
            line_color="#cb5b2b",
            annotation_text="Anúncio selecionado",
            annotation_position="top right",
        )
    fig.update_layout(**_graph_layout("Distribuição de preço"))
    fig.update_xaxes(title="Preço (€)")
    fig.update_yaxes(title="N.º de anúncios")
    return fig


def build_price_per_m2_boxplot(filtered_df: pd.DataFrame, selected_listing_id: str | None) -> go.Figure:
    selected_value = None
    reference = filtered_df
    if selected_listing_id:
        selected_rows = filtered_df.loc[filtered_df["listing_id"] == selected_listing_id]
        if not selected_rows.empty:
            selected_value = selected_rows.iloc[0]["price_per_m2_eur"]
            reference = filtered_df.loc[filtered_df["listing_id"] != selected_listing_id]
    values = pd.to_numeric(reference.get("price_per_m2_eur"), errors="coerce").dropna()

    fig = go.Figure()
    fig.add_trace(
        go.Box(
            x=values,
            name="Restantes anúncios",
            marker_color="#0f6b72",
            boxmean="sd",
            orientation="h",
            hovertemplate="Preço / m²: %{x:,.0f} €<extra></extra>",
        )
    )
    if selected_value is not None and not pd.isna(selected_value):
        fig.add_trace(
            go.Scatter(
                x=[selected_value],
                y=["Restantes anúncios"],
                mode="markers",
                marker={"size": 13, "color": "#cb5b2b", "symbol": "diamond"},
                name="Anúncio selecionado",
                hovertemplate="Selecionado: %{x:,.0f} € / m²<extra></extra>",
            )
        )
    fig.update_layout(**_graph_layout("Contexto do preço por m²"))
    fig.update_xaxes(title="Preço por m² (€)")
    fig.update_yaxes(title="")
    return fig


def build_area_price_scatter(filtered_df: pd.DataFrame, selected_listing_id: str | None) -> go.Figure:
    working = filtered_df.copy()
    working["is_selected"] = working["listing_id"] == selected_listing_id
    reference = working.loc[~working["is_selected"]].copy()
    selected = working.loc[working["is_selected"]].copy()
    reference = reference.dropna(subset=["area_m2", "price_amount_eur"])
    selected = selected.dropna(subset=["area_m2", "price_amount_eur"])

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=reference["area_m2"],
            y=reference["price_amount_eur"],
            mode="markers",
            marker={"size": 10, "color": "#6aa6ae", "opacity": 0.72},
            customdata=np.stack([reference["listing_id"]], axis=-1) if not reference.empty else [],
            hovertemplate="Área: %{x:.0f} m²<br>Preço: %{y:,.0f} €<br>ID: %{customdata[0]}<extra></extra>",
            name="Restantes anúncios",
        )
    )

    if len(reference) >= 2:
        coeffs = np.polyfit(reference["area_m2"], reference["price_amount_eur"], deg=1)
        x_line = np.linspace(reference["area_m2"].min(), reference["area_m2"].max(), 80)
        y_line = coeffs[0] * x_line + coeffs[1]
        fig.add_trace(
            go.Scatter(
                x=x_line,
                y=y_line,
                mode="lines",
                line={"color": "#0f6b72", "width": 3},
                hoverinfo="skip",
                name="Tendência",
            )
        )

    if not selected.empty:
        fig.add_trace(
            go.Scatter(
                x=selected["area_m2"],
                y=selected["price_amount_eur"],
                mode="markers",
                marker={"size": 16, "color": "#cb5b2b", "symbol": "diamond"},
                customdata=np.stack([selected["listing_id"]], axis=-1),
                hovertemplate="Selecionado<br>Área: %{x:.0f} m²<br>Preço: %{y:,.0f} €<br>ID: %{customdata[0]}<extra></extra>",
                name="Selecionado",
            )
        )

    fig.update_layout(**_graph_layout("Área vs preço"))
    fig.update_xaxes(title="Área (m²)")
    fig.update_yaxes(title="Preço (€)")
    return fig


def _kpi_card(label: str, value: str, helper: str) -> html.Div:
    return html.Div(
        className="kpi-card",
        children=[
            html.Div(label, className="kpi-label"),
            html.Div(value, className="kpi-value"),
            html.Div(helper, className="kpi-sub"),
        ],
    )


def _context_card(label: str, value: str, helper: str) -> html.Div:
    return html.Div(
        className="context-card",
        children=[
            html.Div(label, className="context-label"),
            html.Div(value, className="context-value"),
            html.Div(helper, className="context-sub"),
        ],
    )


def _detail_meta_card(label: str, value: str) -> html.Div:
    return html.Div(
        className="detail-meta-card",
        children=[
            html.Div(label, className="detail-meta-label"),
            html.Div(value, className="detail-meta-value"),
        ],
    )


def _listing_card(record: dict[str, Any], is_selected: bool) -> html.Button:
    image = record.get("preview_image_url")
    thumb = (
        html.Img(src=image, alt=record.get("title") or record.get("listing_id"))
        if image
        else html.Div("Sem imagem", className="listing-empty-thumb")
    )
    extra_meta = " · ".join(
        item
        for item in (
            record.get("bedrooms"),
            f"{_format_number(record.get('area_m2'))} m²" if record.get("area_m2") is not None else None,
            f"{int(record.get('images_count') or 0)} imagens",
        )
        if item
    )
    return html.Button(
        id={"type": "listing-button", "index": record["listing_id"]},
        className="listing-card is-selected" if is_selected else "listing-card",
        n_clicks=0,
        children=[
            html.Div(thumb, className="listing-thumb"),
            html.Div(
                className="listing-main",
                children=[
                    html.P(record.get("title") or "Sem título", className="listing-title"),
                    html.P(record.get("address") or "Sem localização", className="listing-meta"),
                    html.P(_format_euro(record.get("price_amount_eur")), className="listing-meta"),
                    html.P(extra_meta or "Sem detalhe adicional", className="listing-meta"),
                ],
            ),
        ],
    )


def _build_detail_panel(context_payload: dict[str, Any]) -> html.Div:
    selected = context_payload.get("selected")
    if not selected:
        return html.Div(
            className="empty-state",
            children="Seleciona um anúncio na lista ou no gráfico para ver detalhe, comparação e imagens.",
        )

    images = selected.get("images") or []
    hero_image = images[0] if images else None
    thumbnails = [html.Img(src=url, alt=f"Imagem {index + 1}") for index, url in enumerate(images[:12])]
    feature_items = [html.Li(item) for item in (selected.get("feature_list") or [])[:16]] or [html.Li("Sem características guardadas.")]
    comparison_cards = context_payload.get("comparison_cards") or []
    reference_count = context_payload.get("reference_count") or 0

    return html.Div(
        className="detail-shell",
        children=[
            html.Div(
                className="detail-head",
                children=[
                    html.H2(selected.get("title") or "Sem título", className="detail-title"),
                    html.P(selected.get("address") or "Sem localização", className="helper"),
                    html.Div(
                        className="pill-row",
                        children=[
                            html.Span(_format_euro(selected.get("price_amount_eur")), className="pill"),
                            html.Span(selected.get("property_type") or "Tipo n/d", className="pill"),
                            html.Span(selected.get("bedrooms") or "Quartos n/d", className="pill"),
                            html.Span(f"{_format_number(selected.get('area_m2'))} m²" if selected.get("area_m2") is not None else "Área n/d", className="pill"),
                        ],
                    ),
                ],
            ),
            html.Div(
                className="detail-main-grid",
                children=[
                    html.Div(
                        className="gallery panel",
                        children=[
                            html.Div(
                                className="hero-image",
                                children=html.Img(src=hero_image, alt=selected.get("title")) if hero_image else html.Div("Sem imagem disponível", className="empty-state"),
                            ),
                            html.Div(className="thumb-grid", children=thumbnails or [html.Div("Sem imagens", className="empty-state")]),
                        ],
                    ),
                    html.Div(
                        className="detail-side",
                        children=[
                            html.Div(
                                className="detail-meta-grid",
                                children=[
                                    _detail_meta_card("ID", str(selected.get("listing_id") or "—")),
                                    _detail_meta_card("Data", str(selected.get("fetched_at") or "—")),
                                    _detail_meta_card("Preço / m²", _format_euro(selected.get("price_per_m2_eur"))),
                                    _detail_meta_card("Imagens", str(int(selected.get("images_count") or 0))),
                                    _detail_meta_card("Casas de banho", _format_number(selected.get("bathrooms"))),
                                    _detail_meta_card("Conjunto de referência", str(reference_count)),
                                ],
                            ),
                            html.Div(
                                className="panel detail-section",
                                children=[
                                    html.H2("Contexto estatístico"),
                                    html.P("Comparação do anúncio com os restantes anúncios filtrados.", className="helper"),
                                    html.Div(
                                        className="context-grid",
                                        children=[_context_card(card["label"], card["value"], card["helper"]) for card in comparison_cards],
                                    ),
                                ],
                            ),
                            html.Div(
                                className="panel detail-section",
                                children=[
                                    html.H2("Descrição"),
                                    html.P(selected.get("description") or "Sem descrição guardada.", className="detail-description"),
                                ],
                            ),
                            html.Div(
                                className="panel detail-section",
                                children=[
                                    html.H2("Características"),
                                    html.Ul(feature_items, className="feature-list"),
                                    html.Div(
                                        className="section-actions",
                                        children=[
                                            html.A("Abrir anúncio original", href=selected.get("url") or "#", target="_blank", rel="noreferrer", className="nav-link"),
                                        ],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )


def _empty_figure(title: str, message: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **_graph_layout(title),
        annotations=[
            {
                "text": message,
                "showarrow": False,
                "font": {"size": 15, "color": "#66747a"},
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
            }
        ],
    )
    return fig


def _dashboard_kpis(frame: pd.DataFrame) -> list[html.Div]:
    prices = frame["price_amount_eur"].dropna() if "price_amount_eur" in frame else pd.Series(dtype=float)
    areas = frame["area_m2"].dropna() if "area_m2" in frame else pd.Series(dtype=float)
    ppm2 = frame["price_per_m2_eur"].dropna() if "price_per_m2_eur" in frame else pd.Series(dtype=float)
    metrics = [
        ("Anúncios filtrados", _format_number(len(frame)), "Total do subconjunto visível agora."),
        ("Preço médio", _format_euro(prices.mean() if not prices.empty else None), "Média do preço no conjunto filtrado."),
        ("Preço mediano", _format_euro(prices.median() if not prices.empty else None), "Menos sensível a extremos do que a média."),
        ("Área média", f"{_format_number(areas.mean(), 1)} m²" if not areas.empty else "—", "Área bruta média dos anúncios filtrados."),
        ("Preço médio / m²", _format_euro(ppm2.mean() if not ppm2.empty else None), "Valorização média por metro quadrado."),
        ("Zonas únicas", _format_number(frame["address"].nunique() if "address" in frame else 0), "Cobertura geográfica no filtro atual."),
    ]
    return [_kpi_card(label, value, helper) for label, value, helper in metrics]


def _numeric_input_value(value: Any, fallback: list[int]) -> list[int]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return list(fallback)
    return [int(value[0]), int(value[1])]


def _current_filters_from_args(
    raw_records: list[dict[str, Any]] | None,
    price_range: list[int] | None,
    area_range: list[int] | None,
    ppm2_range: list[int] | None,
    locations: list[str] | None,
    property_types: list[str] | None,
    bedrooms: list[str] | None,
    bathrooms: list[int] | None,
    amenities: list[str] | None,
    search_text: str | None,
    sort_order: str | None,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    frame = _deserialize_frame(raw_records)
    defaults = _filter_defaults(frame) if not frame.empty else {
        "price_range": [0, 1],
        "area_range": [0, 1],
        "price_per_m2_range": [0, 1],
        "locations": [],
        "property_types": [],
        "bedrooms": [],
        "bathrooms": [],
        "amenities": [],
        "search_text": "",
        "sort_order": "latest",
    }
    filters = {
        "price_range": _numeric_input_value(price_range, defaults["price_range"]),
        "area_range": _numeric_input_value(area_range, defaults["area_range"]),
        "price_per_m2_range": _numeric_input_value(ppm2_range, defaults["price_per_m2_range"]),
        "locations": locations or [],
        "property_types": property_types or [],
        "bedrooms": bedrooms or [],
        "bathrooms": bathrooms or [],
        "amenities": amenities or [],
        "search_text": search_text or "",
        "sort_order": sort_order or "latest",
    }
    return frame, filters, defaults


def build_dashboard_app(config_path: str | None = None) -> Dash:
    base_frame = load_dashboard_frame(config_path)
    defaults = _filter_defaults(base_frame)
    options = _filter_options(base_frame)
    price_bounds = tuple(defaults["price_range"])
    area_bounds = tuple(defaults["area_range"])
    ppm2_bounds = tuple(defaults["price_per_m2_range"])

    app = Dash(__name__, title="Dashboard Analítico Ericeira")
    app.index_string = DASHBOARD_INDEX
    app.layout = html.Div(
        className="dash-shell",
        children=[
            dcc.Store(id="raw-data-store", storage_type="memory", data=_serialize_frame(base_frame)),
            dcc.Store(id="dataset-store", storage_type="memory"),
            dcc.Store(id="filter-store", storage_type="local"),
            dcc.Store(id="selected-listing-store", storage_type="memory"),
            dcc.Download(id="download-filtered-csv"),
            dcc.Download(id="download-comparison-csv"),
            html.Section(
                className="panel hero",
                children=[
                    html.P("Idealista / Ericeira", className="eyebrow"),
                    html.H1("Dashboard analítico de imóveis"),
                    html.P(
                        "Exploração reativa do dataset filtrado, comparação automática do anúncio selecionado e exportação rápida.",
                        className="muted",
                    ),
                    html.Div(
                        className="hero-actions",
                        children=[
                            html.A("Painel de scraping", href="http://127.0.0.1:8765/", target="_blank", rel="noreferrer", className="nav-link"),
                        ],
                    ),
                ],
            ),
            html.Div(
                className="dashboard-grid",
                children=[
                    html.Aside(
                        className="sidebar",
                        children=[
                            html.Div(
                                className="panel sidebar-section",
                                children=[
                                    html.H2("Filtros"),
                                    html.P("Todos os visuais e comparações reagem aos filtros abaixo.", className="helper"),
                                    html.Div(
                                        className="control-grid",
                                        children=[
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Pesquisar anúncio", className="control-label"),
                                                    dcc.Input(id="search-text", type="search", placeholder="Título, ID ou localização", value="", debounce=True),
                                                    html.Div("Procura por texto livre no conjunto já guardado.", className="mini-help"),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Ordenar lista", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="sort-order",
                                                        options=[
                                                            {"label": "Mais recentes", "value": "latest"},
                                                            {"label": "Preço mais alto", "value": "price_desc"},
                                                            {"label": "Preço mais baixo", "value": "price_asc"},
                                                        ],
                                                        value="latest",
                                                        clearable=False,
                                                        searchable=False,
                                                    ),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Preço (€)", className="control-label"),
                                                    dcc.RangeSlider(
                                                        id="price-range",
                                                        min=price_bounds[0],
                                                        max=price_bounds[1],
                                                        value=defaults["price_range"],
                                                        step=_slider_step(price_bounds),
                                                        marks=_slider_marks(price_bounds),
                                                        allowCross=False,
                                                        tooltip={"placement": "bottom", "always_visible": False},
                                                    ),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Área (m²)", className="control-label"),
                                                    dcc.RangeSlider(
                                                        id="area-range",
                                                        min=area_bounds[0],
                                                        max=area_bounds[1],
                                                        value=defaults["area_range"],
                                                        step=_slider_step(area_bounds),
                                                        marks=_slider_marks(area_bounds),
                                                        allowCross=False,
                                                        tooltip={"placement": "bottom", "always_visible": False},
                                                    ),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Preço por m² (€)", className="control-label"),
                                                    dcc.RangeSlider(
                                                        id="price-per-m2-range",
                                                        min=ppm2_bounds[0],
                                                        max=ppm2_bounds[1],
                                                        value=defaults["price_per_m2_range"],
                                                        step=_slider_step(ppm2_bounds),
                                                        marks=_slider_marks(ppm2_bounds),
                                                        allowCross=False,
                                                        tooltip={"placement": "bottom", "always_visible": False},
                                                    ),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Localização", className="control-label"),
                                                    dcc.Dropdown(id="locations-filter", options=options["locations"], value=[], multi=True, placeholder="Escolher zonas"),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Tipo de imóvel", className="control-label"),
                                                    dcc.Dropdown(id="property-type-filter", options=options["property_types"], value=[], multi=True, placeholder="Escolher tipos"),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Quartos", className="control-label"),
                                                    dcc.Dropdown(id="bedrooms-filter", options=options["bedrooms"], value=[], multi=True, placeholder="Escolher tipologias"),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Casas de banho", className="control-label"),
                                                    dcc.Dropdown(id="bathrooms-filter", options=options["bathrooms"], value=[], multi=True, placeholder="Escolher valores"),
                                                ],
                                            ),
                                            html.Div(
                                                className="control-stack",
                                                children=[
                                                    html.Label("Amenidades", className="control-label"),
                                                    dcc.Checklist(
                                                        id="amenities-filter",
                                                        options=[{"label": label, "value": key} for key, label in AMENITY_FILTER_OPTIONS],
                                                        value=[],
                                                        inputStyle={"marginRight": "8px"},
                                                        labelStyle={"display": "block", "marginBottom": "8px"},
                                                    ),
                                                ],
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                            html.Div(
                                className="panel sidebar-section",
                                children=[
                                    html.H2("Ações"),
                                    html.Div(
                                        className="button-row",
                                        children=[
                                            html.Button("Reset filtros", id="reset-filters-btn", className="ghost"),
                                            html.Button("Guardar filtros", id="save-filters-btn", className="secondary"),
                                            html.Button("Atualizar dados", id="refresh-data-btn", className="secondary"),
                                            html.Button("Exportar CSV", id="export-csv-btn", className="primary"),
                                            html.Button("Exportar comparação", id="export-comparison-btn", className="secondary"),
                                        ],
                                    ),
                                    html.Div(id="filter-status", className="status-note"),
                                ],
                            ),
                        ],
                    ),
                    html.Main(
                        className="main",
                        children=[
                            html.Div(id="kpi-grid", className="kpi-grid"),
                            html.Div(
                                className="graph-grid",
                                children=[
                                    html.Div(
                                        className="panel graph-panel",
                                        children=[
                                            html.Div(className="panel-head", children=[html.H2("Distribuição de preço"), html.P("Histograma do conjunto filtrado com destaque do anúncio selecionado.", className="helper")]),
                                            dcc.Graph(id="price-histogram", className="graph-box", config={"displayModeBar": False}),
                                        ],
                                    ),
                                    html.Div(
                                        className="panel graph-panel",
                                        children=[
                                            html.Div(className="panel-head", children=[html.H2("Preço por m²"), html.P("Boxplot de contexto relativo aos restantes anúncios filtrados.", className="helper")]),
                                            dcc.Graph(id="price-per-m2-boxplot", className="graph-box", config={"displayModeBar": False}),
                                        ],
                                    ),
                                    html.Div(
                                        className="panel graph-panel",
                                        children=[
                                            html.Div(className="panel-head", children=[html.H2("Área vs preço"), html.P("Scatter com tendência linear; clicar num ponto seleciona o anúncio.", className="helper")]),
                                            dcc.Graph(id="area-price-scatter", className="graph-box", config={"displayModeBar": False}),
                                        ],
                                    ),
                                ],
                            ),
                            html.Div(
                                className="lower-grid",
                                children=[
                                    html.Div(
                                        className="panel list-panel",
                                        children=[
                                            html.Div(className="panel-head", children=[html.H2("Lista de anúncios"), html.P("Clica num cartão para abrir detalhe e comparação.", className="helper")]),
                                            html.Div(id="list-count", className="list-count"),
                                            html.Div(id="listing-list", className="listing-list"),
                                        ],
                                    ),
                                    html.Div(
                                        className="panel",
                                        children=[
                                            html.Div(className="panel-head", children=[html.H2("Detalhe e comparação"), html.P("O anúncio selecionado é sempre comparado com o resto do conjunto filtrado.", className="helper")]),
                                            html.Div(id="detail-panel"),
                                        ],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    @app.callback(
        Output("filter-store", "data"),
        Output("filter-status", "children"),
        Input("save-filters-btn", "n_clicks"),
        State("price-range", "value"),
        State("area-range", "value"),
        State("price-per-m2-range", "value"),
        State("locations-filter", "value"),
        State("property-type-filter", "value"),
        State("bedrooms-filter", "value"),
        State("bathrooms-filter", "value"),
        State("amenities-filter", "value"),
        State("search-text", "value"),
        State("sort-order", "value"),
        prevent_initial_call=True,
    )
    def save_filters(
        n_clicks: int,
        price_range: list[int],
        area_range: list[int],
        price_per_m2_range: list[int],
        locations: list[str],
        property_types: list[str],
        bedrooms: list[str],
        bathrooms: list[int],
        amenities: list[str],
        search_text: str,
        sort_order: str,
    ) -> tuple[dict[str, Any], str]:
        if not n_clicks:
            raise PreventUpdate
        return (
            {
                "price_range": price_range,
                "area_range": area_range,
                "price_per_m2_range": price_per_m2_range,
                "locations": locations or [],
                "property_types": property_types or [],
                "bedrooms": bedrooms or [],
                "bathrooms": bathrooms or [],
                "amenities": amenities or [],
                "search_text": search_text or "",
                "sort_order": sort_order or "latest",
            },
            "Filtros guardados localmente no browser.",
        )

    @app.callback(
        Output("price-range", "value"),
        Output("area-range", "value"),
        Output("price-per-m2-range", "value"),
        Output("locations-filter", "value"),
        Output("property-type-filter", "value"),
        Output("bedrooms-filter", "value"),
        Output("bathrooms-filter", "value"),
        Output("amenities-filter", "value"),
        Output("search-text", "value"),
        Output("sort-order", "value"),
        Input("filter-store", "data"),
        prevent_initial_call=False,
    )
    def load_saved_filters(saved_filters: dict[str, Any] | None):
        if not saved_filters:
            raise PreventUpdate
        return (
            saved_filters.get("price_range", defaults["price_range"]),
            saved_filters.get("area_range", defaults["area_range"]),
            saved_filters.get("price_per_m2_range", defaults["price_per_m2_range"]),
            saved_filters.get("locations", []),
            saved_filters.get("property_types", []),
            saved_filters.get("bedrooms", []),
            saved_filters.get("bathrooms", []),
            saved_filters.get("amenities", []),
            saved_filters.get("search_text", ""),
            saved_filters.get("sort_order", "latest"),
        )

    @app.callback(
        Output("price-range", "value", allow_duplicate=True),
        Output("area-range", "value", allow_duplicate=True),
        Output("price-per-m2-range", "value", allow_duplicate=True),
        Output("locations-filter", "value", allow_duplicate=True),
        Output("property-type-filter", "value", allow_duplicate=True),
        Output("bedrooms-filter", "value", allow_duplicate=True),
        Output("bathrooms-filter", "value", allow_duplicate=True),
        Output("amenities-filter", "value", allow_duplicate=True),
        Output("search-text", "value", allow_duplicate=True),
        Output("sort-order", "value", allow_duplicate=True),
        Output("filter-store", "data", allow_duplicate=True),
        Output("selected-listing-store", "data", allow_duplicate=True),
        Output("filter-status", "children", allow_duplicate=True),
        Input("reset-filters-btn", "n_clicks"),
        State("raw-data-store", "data"),
        prevent_initial_call=True,
    )
    def reset_filters(n_clicks: int, raw_records: list[dict[str, Any]] | None):
        if not n_clicks:
            raise PreventUpdate
        frame = _deserialize_frame(raw_records)
        default_filters = _filter_defaults(frame)
        return (
            default_filters["price_range"],
            default_filters["area_range"],
            default_filters["price_per_m2_range"],
            [],
            [],
            [],
            [],
            [],
            "",
            "latest",
            default_filters,
            None,
            "Filtros repostos para o default do dataset.",
        )

    @app.callback(
        Output("raw-data-store", "data"),
        Output("filter-status", "children", allow_duplicate=True),
        Input("refresh-data-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def refresh_dataset(n_clicks: int):
        if not n_clicks:
            raise PreventUpdate
        refreshed = load_dashboard_frame(config_path)
        return _serialize_frame(refreshed), "Dataset recarregado a partir do JSONL."

    @app.callback(
        Output("dataset-store", "data"),
        Input("raw-data-store", "data"),
        Input("price-range", "value"),
        Input("area-range", "value"),
        Input("price-per-m2-range", "value"),
        Input("locations-filter", "value"),
        Input("property-type-filter", "value"),
        Input("bedrooms-filter", "value"),
        Input("bathrooms-filter", "value"),
        Input("amenities-filter", "value"),
        Input("search-text", "value"),
        Input("sort-order", "value"),
    )
    def update_filtered_dataset(
        raw_records: list[dict[str, Any]] | None,
        price_range: list[int],
        area_range: list[int],
        price_per_m2_range: list[int],
        locations: list[str],
        property_types: list[str],
        bedrooms: list[str],
        bathrooms: list[int],
        amenities: list[str],
        search_text: str,
        sort_order: str,
    ):
        frame, filters, current_defaults = _current_filters_from_args(
            raw_records,
            price_range,
            area_range,
            price_per_m2_range,
            locations,
            property_types,
            bedrooms,
            bathrooms,
            amenities,
            search_text,
            sort_order,
        )
        filtered = apply_dashboard_filters(frame, filters, current_defaults)
        return _serialize_frame(filtered)

    @app.callback(
        Output("selected-listing-store", "data"),
        Input({"type": "listing-button", "index": ALL}, "n_clicks"),
        Input("area-price-scatter", "clickData"),
        Input("dataset-store", "data"),
        State("selected-listing-store", "data"),
        prevent_initial_call=False,
    )
    def update_selected_listing(_button_clicks, scatter_click_data, dataset_records, current_selected):
        filtered = _deserialize_frame(dataset_records)
        if filtered.empty:
            return None
        valid_ids = set(filtered["listing_id"].astype(str))
        triggered = ctx.triggered_id
        if not triggered or triggered == "dataset-store":
            return current_selected if current_selected in valid_ids else None
        if triggered == "area-price-scatter":
            if not scatter_click_data or not scatter_click_data.get("points"):
                raise PreventUpdate
            customdata = scatter_click_data["points"][0].get("customdata") or []
            if not customdata:
                raise PreventUpdate
            return str(customdata[0])
        if isinstance(triggered, dict) and triggered.get("type") == "listing-button":
            return str(triggered.get("index"))
        raise PreventUpdate

    @app.callback(
        Output("kpi-grid", "children"),
        Output("price-histogram", "figure"),
        Output("price-per-m2-boxplot", "figure"),
        Output("area-price-scatter", "figure"),
        Output("list-count", "children"),
        Output("listing-list", "children"),
        Output("detail-panel", "children"),
        Input("dataset-store", "data"),
        Input("selected-listing-store", "data"),
    )
    def render_dashboard(filtered_records: list[dict[str, Any]] | None, selected_listing_id: str | None):
        filtered = _deserialize_frame(filtered_records)
        if filtered.empty:
            empty_chart = _empty_figure("Sem dados", "Nenhum anúncio corresponde ao filtro atual.")
            return (
                [_kpi_card("Anúncios filtrados", "0", "Ajusta os filtros para voltar a ver resultados.")],
                empty_chart,
                empty_chart,
                empty_chart,
                "0 anúncios",
                [html.Div("Nenhum anúncio corresponde ao filtro atual.", className="empty-state")],
                html.Div("Sem detalhe para mostrar com o filtro atual.", className="empty-state"),
            )

        if selected_listing_id not in set(filtered["listing_id"].astype(str)):
            selected_listing_id = None

        context_payload = compute_listing_context(filtered, selected_listing_id)
        cards = [_listing_card(record, record["listing_id"] == selected_listing_id) for record in filtered.to_dict("records")]
        count_label = f"{len(filtered)} anúncio" if len(filtered) == 1 else f"{len(filtered)} anúncios"

        return (
            _dashboard_kpis(filtered),
            build_price_histogram(filtered, selected_listing_id),
            build_price_per_m2_boxplot(filtered, selected_listing_id),
            build_area_price_scatter(filtered, selected_listing_id),
            count_label,
            cards,
            _build_detail_panel(context_payload),
        )

    @app.callback(
        Output("download-filtered-csv", "data"),
        Input("export-csv-btn", "n_clicks"),
        State("dataset-store", "data"),
        prevent_initial_call=True,
    )
    def export_filtered_csv(n_clicks: int, filtered_records: list[dict[str, Any]] | None):
        if not n_clicks:
            raise PreventUpdate
        frame = _deserialize_frame(filtered_records)
        if frame.empty:
            raise PreventUpdate
        export_frame = frame.copy()
        export_frame["feature_list"] = export_frame["feature_list"].apply(lambda items: " | ".join(items or []))
        export_frame["images"] = export_frame["images"].apply(lambda items: " | ".join(items or []))
        return dcc.send_data_frame(export_frame[DISPLAY_COLUMNS + ["description", "feature_list", "images"]].to_csv, "dashboard_filtrado.csv", index=False)

    @app.callback(
        Output("download-comparison-csv", "data"),
        Input("export-comparison-btn", "n_clicks"),
        State("dataset-store", "data"),
        State("selected-listing-store", "data"),
        prevent_initial_call=True,
    )
    def export_selected_context(n_clicks: int, filtered_records: list[dict[str, Any]] | None, selected_listing_id: str | None):
        if not n_clicks or not selected_listing_id:
            raise PreventUpdate
        filtered = _deserialize_frame(filtered_records)
        context_payload = compute_listing_context(filtered, selected_listing_id)
        export_row = context_payload.get("export_row")
        if not export_row:
            raise PreventUpdate
        export_frame = pd.DataFrame([export_row])
        return dcc.send_data_frame(export_frame.to_csv, f"comparacao_{selected_listing_id}.csv", index=False)

    return app


def serve_dashboard(
    host: str = "127.0.0.1",
    port: int = 8766,
    config_path: str | None = None,
    open_browser: bool = True,
) -> None:
    app = build_dashboard_app(config_path)
    url = f"http://{host}:{port}/"
    print(f"[dashboard] Frontend disponível em {url}", flush=True)
    print("[dashboard] Usa Ctrl+C para fechar.", flush=True)
    if open_browser:
        webbrowser.open(url)
    app.run(host=host, port=port, debug=False)
