"""
scripts/run_properati.py
==========================
Runner individual para Properati.

Uso:
    python scripts/run_properati.py              # 999 páginas (default)
    python scripts/run_properati.py --pages 20   # N páginas
    python scripts/run_properati.py --show       # Imprime registros en consola
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scrapers.properati import ProperatiScraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper — Properati",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--pages", type=int, default=999, help="Máximo de páginas a recorrer.")
    parser.add_argument("--show", action="store_true", help="Imprime registros en consola al finalizar.")
    args = parser.parse_args()

    scraper = ProperatiScraper()
    scraper.run(max_pages=args.pages)

    if args.show and scraper.scraped_data:
        print(f"\n{'=' * 60}")
        print(f"  Registros extraídos ({len(scraper.scraped_data)} total)")
        print(f"{'=' * 60}")
        for i, r in enumerate(scraper.scraped_data, 1):
            print(f"[{i:>3}] {r.get('id_inmueble', '?'):20s} | {r.get('price', 'N/A'):>15s} | {r.get('title', '?')[:50]}")


if __name__ == "__main__":
    main()
