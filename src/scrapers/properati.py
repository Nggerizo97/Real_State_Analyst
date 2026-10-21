"""
src/scrapers/properati.py
=========================
Spider para Properati Colombia — multi-ciudad.
Fixes: retry en goto, detección de duplicados robusta, cobertura multi-ciudad.
"""

import math
from datetime import datetime
from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper

# Ciudades a raspar: slug de Properati tal como aparece en sus URLs
# (/s/<slug>/apartamento/venta). Properati cambió a slugs con departamento;
# los viejos ("bogota", "medellin"...) devuelven 404 y el scraper los tomaba
# como "sin resultados". Agregar más según los enlaces de la home.
TARGET_CITIES = [
    "bogota-d-c-colombia",
    "medellin-antioquia",
    "cali-valle-del-cauca",
    "barranquilla-atlantico",
    "bucaramanga",
]


class ProperatiScraper(BaseScraper):

    def __init__(self):
        super().__init__("properati")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]

    # ------------------------------------------------------------------
    # Orquestador principal — itera ciudades
    # ------------------------------------------------------------------

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        # max_pages es el presupuesto del run completo (como en los demás
        # scrapers), no por ciudad: Bogotá sola tiene ~380 páginas y 5 ciudades
        # x 200 páginas no caben en el timeout de 120 min del job.
        pages_per_city = max(1, math.ceil(max_pages / len(TARGET_CITIES)))
        for city in TARGET_CITIES:
            self.logger.info(f"Properati — iniciando ciudad: {city} (hasta {pages_per_city} páginas)")
            self._scrape_city(page, city, pages_per_city)

    # ------------------------------------------------------------------
    # Scraping por ciudad
    # ------------------------------------------------------------------

    def _scrape_city(self, page: Page, city: str, max_pages: int) -> None:
        previous_page_ids: set = set()
        current_page = 1

        while current_page <= max_pages:
            self.logger.info(f"Properati [{city}] — Página {current_page}/{max_pages}")

            # La paginación es por ruta (/venta/2). Con ?page=2 Properati
            # devuelve siempre la página 1.
            url = f"{self.base_url}/s/{city}/apartamento/venta"
            if current_page > 1:
                url += f"/{current_page}"

            # Retry en goto — igual que ciencuadras
            loaded = False
            for attempt in range(3):
                try:
                    page.goto(url, timeout=45_000, wait_until="domcontentloaded")
                    loaded = True
                    break
                except Exception as e:
                    self.logger.warning(f"[{city}] Intento {attempt + 1} falló (goto): {e}")
                    page.wait_for_timeout(2000)

            if not loaded:
                self.logger.error(f"[{city}] No se pudo cargar p{current_page} tras 3 intentos.")
                break

            if "no encontrada" in page.title().lower():
                self.logger.error(
                    f"[{city}] 404 en {url}: el slug de ciudad ya no existe en Properati."
                )
                break

            self.human_delay(page, 2000, 4000)

            items = page.query_selector_all("article.snippet")
            if not items:
                self.logger.info(f"[{city}] Sin resultados en p{current_page} — fin de ciudad.")
                break

            self.logger.info(f"[{city}] {len(items)} inmuebles en página {current_page}")

            # Detección de fin de resultados por duplicados
            # Fix: activar solo después de 3 páginas para evitar falsos positivos
            current_ids = {
                item.get_attribute("data-idanuncio")
                for item in items
                if item.get_attribute("data-idanuncio")
            }

            if current_page > 3 and current_ids and current_ids.issubset(previous_page_ids):
                self.logger.info(f"[{city}] Fin de resultados detectado por duplicados.")
                break

            previous_page_ids = previous_page_ids | current_ids

            nuevos = sum(1 for item in items if self._extract_property(item, city))
            self.logger.info(f"[{city}] Página {current_page}: {nuevos} nuevos.")

            self.on_page_done(current_page)
            current_page += 1

    # ------------------------------------------------------------------
    # Extracción de datos
    # ------------------------------------------------------------------

    def _extract_property(self, element, city: str = "") -> bool:
        try:
            id_attr = element.get_attribute("data-idanuncio")
            if not id_attr:
                return False

            # El enlace va en data-url (/detalle/... o /proyecto/...); el
            # selector anterior a[href*='/p/'] ya no existe.
            href = element.get_attribute("data-url") or ""
            if not href:
                link_el = element.query_selector("a.title[href]")
                href = link_el.get_attribute("href") if link_el else ""
            full_url = f"{self.base_url}{href}" if href.startswith("/") else href

            title_el = element.query_selector(".title")
            price_el = element.query_selector(".price")
            location_el = element.query_selector(".location")

            price_raw = price_el.inner_text().strip() if price_el else "0"
            precio_num = self.parse_price(price_raw)

            habitaciones = self._text(element, ".properties__bedrooms")
            banos = self._text(element, ".properties__bathrooms")
            area = self._text(element, ".properties__area")

            prop_data = {
                "id_inmueble": f"PR-{id_attr}",
                "titulo": title_el.inner_text().strip() if title_el else "N/A",
                "tipo_inmueble": "apartamento",
                "precio": price_raw,
                "precio_num": precio_num,
                "ubicacion": location_el.inner_text().strip() if location_el else city,
                "habitaciones": habitaciones,
                "banos": banos,
                "area": area,
                "garajes": "",
                # Properati mezcla proyectos (/proyecto/..., precio "Desde $")
                # con avisos de reventa; el resto lo infiere Silver.
                "estado_inmueble": "nuevo" if "/proyecto/" in full_url else "",
                "url": full_url,
                "portal": self.portal_name,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }

            return self.process_and_upload(prop_data, f"PR-{id_attr}")

        except Exception as e:
            self.logger.error(f"Error parseando item Properati: {e}")
            return False

    @staticmethod
    def _text(element, selector: str) -> str:
        el = element.query_selector(selector)
        return el.inner_text().strip() if el else ""
