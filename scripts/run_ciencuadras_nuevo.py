import argparse
import sys
import os

# Añadir el directorio raíz al sys.path para importaciones
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.scrapers.ciencuadras_nuevo import CiencuadrasNuevoScraper

def main():
    parser = argparse.ArgumentParser(description="Runner específico para Ciencuadras NUEVO (Proyectos)")
    parser.add_argument("--pages", type=int, default=999, help="Número máximo de páginas (default 999)")
    parser.add_argument("--headed", action="store_true", help="Ejecutar en modo visual")
    parser.add_argument("--show", action="store_true", help="Mostrar resultados en consola")
    
    args = parser.parse_args()
    
    scraper = CiencuadrasNuevoScraper()
    scraper.run(max_pages=args.pages, headless=not args.headed)

    if args.show and scraper.scraped_data:
        print(f"\n{'=' * 60}")
        print(f"  Ciencuadras NUEVO: {len(scraper.scraped_data)} registros")
        print(f"{'=' * 60}")
        for i, r in enumerate(scraper.scraped_data, 1):
            print(f"[{i:>3}] {r.get('id_inmueble', '?'):20s} | {r.get('precio', 'N/A'):>15s} | {r.get('titulo', 'N/A')[:50]}")

if __name__ == "__main__":
    main()
