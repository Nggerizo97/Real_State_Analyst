"""
scripts/run_fincaraiz.py
========================
Runner individual para Finca Raíz.

Uso:
    python scripts/run_fincaraiz.py                                # 999 páginas (default)
    python scripts/run_fincaraiz.py --url-path /venta/bogota       # Path específico
    python scripts/run_fincaraiz.py --pages 20                     # N páginas
    python scripts/run_fincaraiz.py --headed                       # Ventana visible
    python scripts/run_fincaraiz.py --show                         # Imprime registros en consola
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scrapers.fincaraiz import FincaRaizScraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper — Finca Raíz",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--pages", type=int, default=999, help="Máximo de páginas a recorrer."
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
    parser.add_argument(
        "--url-path",
        type=str,
        default="/venta/casas-y-apartamentos",
        help="Ruta dentro de fincaraiz.com.co (ej. /venta/apartamentos/bogota).",
    )
    args = parser.parse_args()

    scraper = FincaRaizScraper(listing_path=args.url_path)
    scraper.run(max_pages=args.pages, headless=not args.headed)

    if args.show and scraper.scraped_data:
        print(f"\n{'=' * 70}")
        print(f"  Registros extraídos ({len(scraper.scraped_data)} total)")
        print(f"{'=' * 70}")
        for i, r in enumerate(scraper.scraped_data, 1):
            print(
                f"[{i:>3}] {r.get('id_inmueble', '?'):15s} "
                f"| {r.get('precio', 'N/A'):>25s} "
                f"| {r.get('tipo_inmueble', '?'):15s} "
                f"| {r.get('ubicacion', '?')[:40]}"
            )


if __name__ == "__main__":
    main()
