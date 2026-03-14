"""
scripts/run_ciencuadras.py
============================
Runner individual para Ciencuadras.

Uso:
    python scripts/run_ciencuadras.py              # 999 páginas (default)
    python scripts/run_ciencuadras.py --pages 20   # N páginas
    python scripts/run_ciencuadras.py --show       # Imprime registros en consola
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scrapers.ciencuadras import CiencuadrasScraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scraper — Ciencuadras",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--pages", type=int, default=999, help="Máximo de páginas a recorrer por filtro.")
    parser.add_argument("--headed", action="store_true", help="Abre el navegador visualmente.")
    parser.add_argument("--show", action="store_true", help="Imprime registros en consola al finalizar.")
    args = parser.parse_args()

    scraper = CiencuadrasScraper()
    scraper.run(max_pages=args.pages, headless=not args.headed)

    if args.show and scraper.scraped_data:
        print(f"\n{'=' * 60}")
        print(f"  Registros extraídos ({len(scraper.scraped_data)} total)")
        print(f"{'=' * 60}")
        for i, r in enumerate(scraper.scraped_data, 1):
            title_display = r.get('titulo', '?')
            if title_display == 'N/A' or title_display == '?':
                title_display = r.get('tipo_inmueble', '') + " " + r.get('ubicacion', '')
            state_tag = f"({r.get('estado_inmueble', '')})"
            print(f"[{i:>3}] {r.get('id_inmueble', '?'):20s} | {r.get('precio', 'N/A'):>15s} | {state_tag:9s} | {title_display[:40]}")


if __name__ == "__main__":
    main()
