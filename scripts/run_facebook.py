"""
scripts/run_facebook.py
=========================
Runner individual para Facebook Marketplace.

Uso:
    python scripts/run_facebook.py              # 999 scrolls (default)
    python scripts/run_facebook.py --pages 10   # N scrolls de carga
    python scripts/run_facebook.py --show       # Imprime registros en consola
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scrapers.facebook import FacebookScraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper — Facebook Marketplace",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--pages", type=int, default=999, help="Máximo de scrolls de carga infinita.")
    parser.add_argument("--show", action="store_true", help="Imprime registros en consola al finalizar.")
    args = parser.parse_args()

    scraper = FacebookScraper()
    scraper.run(max_pages=args.pages)

    if args.show and scraper.scraped_data:
        print(f"\n{'=' * 60}")
        print(f"  Registros extraídos ({len(scraper.scraped_data)} total)")
        print(f"{'=' * 60}")
        for i, r in enumerate(scraper.scraped_data, 1):
            print(f"[{i:>3}] {r.get('id_inmueble', '?'):20s} | {r.get('price', 'N/A'):>15s} | {r.get('title', '?')[:50]}")


if __name__ == "__main__":
    main()
