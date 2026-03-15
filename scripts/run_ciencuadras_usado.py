"""
scripts/run_ciencuadras_usado.py
=================================
Runner individual para CienCuadras — Inmuebles USADOS.

Uso:
    python scripts/run_ciencuadras_usado.py              # 10 páginas (default)
    python scripts/run_ciencuadras_usado.py --pages 50   # N páginas
    python scripts/run_ciencuadras_usado.py --headed      # Ventana visible
    python scripts/run_ciencuadras_usado.py --show        # Imprime registros en consola
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scrapers.ciencuadras_usado import CiencuadrasUsadoScraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper — CienCuadras Usado",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--pages", type=int, default=10,
        help="Máximo de páginas a recorrer.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Abre el navegador visible (headless=False).",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Imprime registros en consola al finalizar.",
    )
    args = parser.parse_args()

    scraper = CiencuadrasUsadoScraper()
    scraper.run(max_pages=args.pages, headless=not args.headed)

    if args.show and scraper.scraped_data:
        print(f"\\n{'=' * 80}")
        print(f"  Registros extraídos ({len(scraper.scraped_data)} total)")
        print(f"{'=' * 80}")
        for i, r in enumerate(scraper.scraped_data, 1):
            print(
                f"[{i:>4}] {r.get('id_inmueble', '?'):15s} "
                f"| {r.get('precio', 'N/A'):>20s} "
                f"| {r.get('tipo_inmueble', '?'):15s} "
                f"| {r.get('ubicacion', '?')[:40]}"
            )


if __name__ == "__main__":
    main()
