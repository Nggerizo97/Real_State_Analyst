"""
scripts/run_fincaraiz_proyectos.py
==================================
Runner individual: FincaRaíz — proyectos de vivienda nueva.

Uso:
    python scripts/run_fincaraiz_proyectos.py              # 999 páginas (default)
    python scripts/run_fincaraiz_proyectos.py --pages 20   # N páginas
    python scripts/run_fincaraiz_proyectos.py --headed     # Abre navegador visible
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scrapers.fincaraiz_proyectos import FincaRaizProyectosScraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper — FincaRaíz — proyectos de vivienda nueva",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--pages", type=int, default=999, help="Máximo de páginas a recorrer.")
    parser.add_argument("--headed", action="store_true", help="Abre el navegador visible (debug).")
    args = parser.parse_args()

    FincaRaizProyectosScraper().run(max_pages=args.pages, headless=not args.headed)


if __name__ == "__main__":
    main()
