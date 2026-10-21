"""
src/scrapers/metrocuadrado_proyectos.py
=======================================
Proyectos nuevos en Metrocuadrado.

URL base: https://www.metrocuadrado.com/proyectos/
Paginación: rc-pagination (click en "siguiente"), igual que /inmuebles/venta.

Mismas tarjetas (div.property-card__container) que el listado normal, así que
reutiliza la extracción de MetrocuadradoScraper. Va a su propia fuente en S3
(raw/metrocuadrado_proyectos) y marca estado_inmueble="nuevo".
"""

from src.scrapers.metrocuadrado import MetrocuadradoScraper


class MetrocuadradoProyectosScraper(MetrocuadradoScraper):
    START_PATH = "/proyectos/"
    ID_PREFIX = "MCP"
    ESTADO_INMUEBLE = "nuevo"

    def __init__(self):
        super().__init__(portal_name="metrocuadrado_proyectos")
