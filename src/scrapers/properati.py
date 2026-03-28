"""
src/scrapers/properati.py
=========================
Spider para Properati Colombia — multi-ciudad.
Fixes: retry en goto, detección de duplicados robusta, cobertura multi-ciudad.
"""

import re
from datetime import datetime
from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper

# Ciudades a raspar — agregar más según cobertura de Properati Colombia
TARGET_CITIES = [
    "bogota",
    "medellin",
    "cali",
    "barranquilla",
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
        for city in TARGET_CITIES:
            self.logger.info(f"Properati — iniciando ciudad: {city}")
            self._scrape_city(page, city, max_pages)

    # ------------------------------------------------------------------
    # Scraping por ciudad
    # ------------------------------------------------------------------

    def _scrape_city(self, page: Page, city: str, max_pages: int) -> None:
        previous_page_ids: set = set()
        current_page = 1

        while current_page <= max_pages:
            self.logger.info(f"Properati [{city}] — Página {current_page}/{max_pages}")

            url = (
                f"{self.base_url}/s/{city}/apartamento/venta"
                if current_page == 1
                else f"{self.base_url}/s/{city}/apartamento/venta?page={current_page}"
            )

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

            link_el = element.query_selector("a[href*='/p/']")
            href = link_el.get_attribute("href") if link_el else ""
            full_url = f"{self.base_url}{href}" if href.startswith("/") else href

            title_el = element.query_selector(".title")
            price_el = element.query_selector(".price")
            location_el = element.query_selector(".location")

            price_raw = price_el.inner_text().strip() if price_el else "0"
            precio_num = self.parse_price(price_raw)

            area = habitaciones = banos = ""
            for spec in element.query_selector_all(".properties__item"):
                text = spec.inner_text().lower()
                if "m²" in text or "m2" in text:
                    area = text
                elif "hab" in text:
                    habitaciones = text
                elif "baño" in text:
                    banos = text

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
                "url": full_url,
                "portal": self.portal_name,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }

            return self.process_and_upload(prop_data, f"PR-{id_attr}")

        except Exception as e:
            self.logger.error(f"Error parseando item Properati: {e}")
            return False
