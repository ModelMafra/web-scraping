# OLX Carros Scraper

Scrapes OLX car listings by brand from `https://www.olx.pt/carros-motos-e-barcos/carros/` and writes a CSV.

Notes:
- This script requests each listing page to extract all available attributes (e.g., `Ano`, `Modelo`, `Combustível`, etc.).
- Attribute keys vary per ad. By default, the script **explodes** all attributes into CSV columns. If you prefer a single JSON column, use `--no-explode-attributes`.
- Be mindful of OLX terms of service and rate limits. The default delay is 1 second per listing.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python3 olx_scraper.py --output olx_carros.csv
```

Common options:

```bash
# Limit to a few brands and pages
python3 olx_scraper.py --brands audi bmw --max-pages 3 --output olx_carros.csv

# Keep attributes in a JSON column (smaller CSV header)
python3 olx_scraper.py --no-explode-attributes --output olx_carros.csv

# Include external listings (e.g., Standvirtual)
python3 olx_scraper.py --include-external
```

## Output columns

Base columns:
- `brand`, `model`, `listing_url`, `source_domain`, `title`, `price`, `location`, `posted`, `ad_id`,
  `seller_type`, `seller_name`, `description`, `images`, `scraped_at`

When exploding attributes, each attribute key becomes its own column.
When not exploding, all attributes are stored in `attributes` as JSON.
