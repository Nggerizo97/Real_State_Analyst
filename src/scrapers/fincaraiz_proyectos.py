"""
src/scrapers/fincaraiz_proyectos.py
===================================
Proyectos de vivienda nueva / sobre planos en FincaRaíz.

URL base: https://www.fincaraiz.com.co/proyectos-vivienda
Paginación: /proyectos-vivienda/pagina2, /pagina3, …

Las tarjetas son las mismas del listado normal (div.listingBoxCard), así que
reutiliza la extracción de FincaRaizScraper. Va a su propia fuente en S3
(raw/fincaraiz_proyectos) y marca estado_inmueble="nuevo"; Silver refina a
"sobre_planos" si el título lo dice.
"""

from src.scrapers.fincaraiz import FincaRaizScraper


class FincaRaizProyectosScraper(FincaRaizScraper):
    ID_PREFIX = "FRP"
    ESTADO_INMUEBLE = "nuevo"

    def __init__(self):
        super().__init__(listing_path="/proyectos-vivienda", portal_name="fincaraiz_proyectos")
