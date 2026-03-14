from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd

from idealista_ericeira_scraper.cli import build_parser
from idealista_ericeira_scraper.core import append_jsonl
from idealista_ericeira_scraper.dashboard import _records_to_frame, compute_listing_context, load_dashboard_frame


class DashboardTests(unittest.TestCase):
    def test_records_to_frame_derives_metrics_and_flags(self) -> None:
        frame = _records_to_frame(
            [
                {
                    "listing_id": "1001",
                    "title": "Moradia independente à venda na Ericeira Sul",
                    "address": "Ericeira Sul, Ericeira",
                    "url": "https://www.idealista.pt/imovel/1001/",
                    "fetched_at": "2026-03-14T09:00:00Z",
                    "price_amount_eur": 500000,
                    "images": [
                        "https://img4.idealista.pt/blur/WEB_DETAIL/0/id.pro.pt.image.master/aa/bb/cc/123456.jpg",
                        "https://img4.idealista.pt/blur/WEB_DETAIL_TOP-L-L/0/id.pro.pt.image.master/aa/bb/cc/123456.jpg",
                        "https://img4.idealista.pt/blur/WEB_DETAIL/0/id.pro.pt.image.master/aa/bb/cc/123457.jpg",
                    ],
                    "description": "Moradia muito boa.",
                    "feature_list": [
                        "Moradia independente",
                        "200 m² área bruta, 140 m² úteis",
                        "T4",
                        "3 casas de banho",
                        "Lugar de garagem incluído no preço",
                        "Piscina",
                        "Elevador",
                        "Terraço e varanda",
                        "Segunda mão/bom estado",
                    ],
                }
            ]
        )

        record = frame.iloc[0]
        self.assertEqual(record["property_type"], "Moradia independente")
        self.assertEqual(record["bedrooms"], "T4")
        self.assertEqual(record["bathrooms"], 3)
        self.assertEqual(record["area_m2"], 200)
        self.assertEqual(record["price_per_m2_eur"], 2500)
        self.assertEqual(record["images_count"], 2)
        self.assertTrue(record["garage_included"])
        self.assertTrue(record["has_pool"])
        self.assertTrue(record["has_elevator"])
        self.assertTrue(record["has_terrace_or_varanda"])
        self.assertEqual(record["condition_bucket"], "bom_estado")

    def test_compute_listing_context_excludes_selected_listing_from_reference(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "listing_id": "a1",
                    "title": "Apartamento 1",
                    "address": "Ericeira",
                    "url": "https://www.idealista.pt/imovel/a1/",
                    "fetched_at": "2026-03-14T10:00:00Z",
                    "price_amount_eur": 400000,
                    "area_m2": 100,
                    "price_per_m2_eur": 4000,
                    "property_type": "Apartamento",
                    "bedrooms": "T2",
                    "bathrooms": 2,
                    "images_count": 5,
                    "preview_image_url": None,
                    "images": [],
                    "description": "desc",
                    "feature_list": ["Apartamento", "T2"],
                    "garage_included": False,
                    "has_pool": False,
                    "has_elevator": False,
                    "has_terrace_or_varanda": False,
                    "condition_bucket": "desconhecido",
                },
                {
                    "listing_id": "a2",
                    "title": "Apartamento 2",
                    "address": "Ericeira",
                    "url": "https://www.idealista.pt/imovel/a2/",
                    "fetched_at": "2026-03-14T10:10:00Z",
                    "price_amount_eur": 600000,
                    "area_m2": 120,
                    "price_per_m2_eur": 5000,
                    "property_type": "Apartamento",
                    "bedrooms": "T3",
                    "bathrooms": 2,
                    "images_count": 5,
                    "preview_image_url": None,
                    "images": [],
                    "description": "desc",
                    "feature_list": ["Apartamento", "T3"],
                    "garage_included": True,
                    "has_pool": False,
                    "has_elevator": True,
                    "has_terrace_or_varanda": True,
                    "condition_bucket": "bom_estado",
                },
                {
                    "listing_id": "a3",
                    "title": "Apartamento 3",
                    "address": "Ericeira Sul, Ericeira",
                    "url": "https://www.idealista.pt/imovel/a3/",
                    "fetched_at": "2026-03-14T10:20:00Z",
                    "price_amount_eur": 800000,
                    "area_m2": 160,
                    "price_per_m2_eur": 5000,
                    "property_type": "Apartamento",
                    "bedrooms": "T4",
                    "bathrooms": 3,
                    "images_count": 8,
                    "preview_image_url": None,
                    "images": [],
                    "description": "desc",
                    "feature_list": ["Apartamento", "T4"],
                    "garage_included": True,
                    "has_pool": True,
                    "has_elevator": True,
                    "has_terrace_or_varanda": True,
                    "condition_bucket": "novo",
                },
            ]
        )

        context = compute_listing_context(frame, "a2")

        self.assertEqual(context["reference_count"], 2)
        self.assertEqual(context["selected"]["listing_id"], "a2")
        self.assertEqual(context["export_row"]["price_delta_vs_mean_eur"], 0)
        self.assertEqual(context["export_row"]["price_delta_vs_median_eur"], 0)
        self.assertEqual(context["export_row"]["price_percentile"], 50.0)
        self.assertTrue(any(card["label"] == "Percentil do preço" for card in context["comparison_cards"]))

    def test_load_dashboard_frame_deduplicates_by_listing_id(self) -> None:
        with TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            details_path = project_root / "data" / "details" / "ericeira_ads.jsonl"
            config_path = project_root / "config" / "targets.toml"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                """
[run]

[fetch]

[[targets]]
name = "ericeira_buy_homes"
search_url = "https://example.com"
listing_type = "sale"
property_scope = "homes"
""".strip()
                + "\n",
                encoding="utf-8",
            )

            append_jsonl(
                details_path,
                {
                    "listing_id": "1001",
                    "title": "Antigo",
                    "address": "Ericeira",
                    "fetched_at": "2026-03-14T09:00:00Z",
                    "price_amount_eur": 500000,
                    "feature_list": ["Apartamento", "100 m² área bruta", "T2"],
                    "images": [],
                    "url": "https://www.idealista.pt/imovel/1001/",
                },
            )
            append_jsonl(
                details_path,
                {
                    "listing_id": "1001",
                    "title": "Novo",
                    "address": "Ericeira",
                    "fetched_at": "2026-03-14T10:00:00Z",
                    "price_amount_eur": 550000,
                    "feature_list": ["Apartamento", "110 m² área bruta", "T2"],
                    "images": [],
                    "url": "https://www.idealista.pt/imovel/1001/",
                },
            )

            frame = load_dashboard_frame(config_path)

            self.assertEqual(len(frame), 1)
            self.assertEqual(frame.iloc[0]["title"], "Novo")
            self.assertEqual(frame.iloc[0]["price_amount_eur"], 550000)

    def test_cli_parser_supports_dashboard_command(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["dashboard", "--port", "9000", "--no-browser"])

        self.assertEqual(args.command, "dashboard")
        self.assertEqual(args.port, 9000)
        self.assertTrue(args.no_browser)


if __name__ == "__main__":
    unittest.main()
