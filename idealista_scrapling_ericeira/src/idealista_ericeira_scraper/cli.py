from __future__ import annotations

import argparse
import json
import sys

from idealista_ericeira_scraper.scraper import IdealistaCrawler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawler Idealista Ericeira com Scrapling e retoma por JSONL.")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="Caminho para o ficheiro TOML de configuracao.")
    common.add_argument("--mode", choices=["http", "dynamic", "stealth"], help="Override do modo de fetch.")
    common.add_argument(
        "--headful",
        action="store_true",
        help="Abre o browser em modo visivel quando o modo escolhido usa browser.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="Mostra o estado atual do projeto.", parents=[common])
    status_parser.add_argument("--target", action="append", help="Filtra por target especifico.")

    discover_parser = subparsers.add_parser("discover", help="Descobre links de anuncios.", parents=[common])
    discover_parser.add_argument("--target", action="append", help="Filtra por target especifico.")
    discover_parser.add_argument("--max-pages", type=int, help="Limita o numero de paginas descobertas nesta corrida.")

    extract_parser = subparsers.add_parser("extract", help="Extrai detalhe dos anuncios ja descobertos.", parents=[common])
    extract_parser.add_argument("--target", action="append", help="Filtra por target especifico.")
    extract_parser.add_argument("--limit", type=int, help="Limita o numero de anuncios extraidos nesta corrida.")

    crawl_parser = subparsers.add_parser("crawl", help="Faz discover e extract.", parents=[common])
    crawl_parser.add_argument("--target", action="append", help="Filtra por target especifico.")
    crawl_parser.add_argument("--max-pages", type=int, help="Limita paginas novas na fase discover.")
    crawl_parser.add_argument("--limit", type=int, help="Limita anuncios processados na fase extract.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    crawler = IdealistaCrawler(
        config_path=args.config,
        mode_override=args.mode,
        headless_override=not args.headful if args.mode in {"dynamic", "stealth"} or args.headful else None,
    )

    try:
        if args.command == "status":
            payload = crawler.status()
        elif args.command == "discover":
            payload = crawler.discover(target_names=args.target, max_pages=args.max_pages)
        elif args.command == "extract":
            payload = crawler.extract(target_names=args.target, limit=args.limit)
        else:
            payload = crawler.crawl(target_names=args.target, max_pages=args.max_pages, limit=args.limit)
    except Exception as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0
