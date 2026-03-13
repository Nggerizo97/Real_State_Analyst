"""
scripts/run_mercadolibre.py
============================
Runner individual para MercadoLibre.

Uso:
    python scripts/run_mercadolibre.py              # 999 páginas (default)
    python scripts/run_mercadolibre.py --pages 20   # N páginas
    python scripts/run_mercadolibre.py --headed      # Abre navegador visible
    python scripts/run_mercadolibre.py --show        # Imprime registros en consola
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scrapers.mercadolibre import MercadoLibreScraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper — MercadoLibre Inmuebles Colombia",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--pages", type=int, default=999,
        help="Máximo de páginas a recorrer.",
    )
    parser.add_argument(
        "--headed", action="store_true",
        help="Abre el navegador visible (debug).",
    )
    parser.add_argument(
        "--show", action="store_true",
        help="Imprime registros en consola al finalizar.",
    )
    args = parser.parse_args()

    scraper = MercadoLibreScraper()
    scraper.run(max_pages=args.pages, headless=not args.headed)

    if args.show and scraper.scraped_data:
        print(f"\n{'=' * 80}")
        print(f"  Registros extraídos ({len(scraper.scraped_data)} total)")
        print(f"{'=' * 80}")
        for i, r in enumerate(scraper.scraped_data, 1):
            print(
                f"[{i:>4}] {r.get('id_inmueble', '?'):15s} | "
                f"${r.get('precio_num', 0):>15,} | "
                f"{r.get('property_type', '?'):25s} | "
                f"{r.get('location', '?')[:40]}"
            )


if __name__ == "__main__":
    main()
