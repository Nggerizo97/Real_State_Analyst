"""
main.py
=======
⚠️  DEPRECADO para uso en pipeline CI/CD.

Para ejecuciones en el pipeline, usar los runners individuales en scripts/:
    python scripts/run_bancolombia.py --pages 999
    python scripts/run_fincaraiz.py --pages 10
    python scripts/run_mercadolibre.py --pages 10
    ...

Este archivo se conserva como utilidad local para ejecutar todos los scrapers
habilitados en secuencia (útil para pruebas rápidas en desarrollo).
"""

from src.scrapers.fincaraiz import FincaRaizScraper
from src.scrapers.mercadolibre import MercadoLibreScraper
from src.scrapers.metrocuadrado import MetrocuadradoScraper
from src.scrapers.ciencuadras import CiencuadrasScraper
from src.scrapers.properati import ProperatiScraper
from src.scrapers.mitula import MitulaScraper
from src.scrapers.bancolombia_tu360 import BancolombiaTu360Scraper
from src.scrapers.davivienda import DaviviendaScraper
from src.scrapers.facebook import FacebookScraper

from src.utils.logger import get_logger
from config.settings import PORTALS_CONFIG

logger = get_logger("RealStateMain")

scraper_registry = {
    "fincaraiz": FincaRaizScraper,
    "mercadolibre": MercadoLibreScraper,
    "metrocuadrado": MetrocuadradoScraper,
    "ciencuadras": CiencuadrasScraper,
    "properati": ProperatiScraper,
    "mitula": MitulaScraper,
    "bancolombia_tu360": BancolombiaTu360Scraper,
    "davivienda": DaviviendaScraper,
    "facebook": FacebookScraper
}

def run_pipeline():
    """
    Entry point local (desarrollo).
    Para CI/CD usar los runners individuales en scripts/.
    """
    logger.info("=========================================================")
    logger.info(" ⚠️  Modo Local — Para CI/CD usar scripts/run_*.py")
    logger.info("=========================================================")

    for portal, config in PORTALS_CONFIG.items():
        if config.get("enabled"):
            logger.info(f"\n>>> Desplegando Módulo: {portal.upper()} <<<")
            scraper_class = scraper_registry.get(portal)

            if scraper_class:
                try:
                    scraper_instance = scraper_class()
                    scraper_instance.run(max_pages=2)  # Limitado para pruebas locales
                except Exception as e:
                    logger.error(f"Fallo crítico en el módulo {portal}: {e}")
            else:
                logger.warning(f"El scraper para '{portal}' está habilitado en config pero no está implementado.")

    logger.info("\n=========================================================")
    logger.info(" Todos los pipelines finalizaron. S3 Bronze Actualizado.")
    logger.info("=========================================================")

if __name__ == "__main__":
    run_pipeline()
