"""
src/scrapers/mercadolibre.py
============================
Scraper de inmuebles en venta en MercadoLibre Colombia.

URL base: https://listado.mercadolibre.com.co/inmuebles/venta/_NoIndex_True
Pagina con click en "Siguiente" hasta agotar páginas o alcanzar el límite.
"""

import re
import time
from datetime import datetime

from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper


class MercadoLibreScraper(BaseScraper):
    def __init__(self):
        super().__init__("mercadolibre")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        base_url = f"{self.base_url}/inmuebles/venta/_NoIndex_True"

        for current_page in range(1, max_pages + 1):
            if current_page == 1:
                url = base_url
            else:
                offset = (current_page - 1) * 48 + 1
                # The format is domain/inmuebles/venta/_Desde_49_NoIndex_True
                url = f"{self.base_url}/inmuebles/venta/_Desde_{offset}_NoIndex_True"

            self.logger.info(
                f"Página {current_page}/{max_pages}: {url[:100]}"
            )

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            except Exception as e:
                self.logger.error(f"Error al navegar a la página {current_page}: {e}")
                self.human_delay(page=None, min_ms=5000, max_ms=10000)
                continue

            self.human_delay(page)

            # Buscar tarjetas con selectores estables + fallback
            items = self._wait_for_items(page)
            if not items:
                self.logger.info("Sin resultados o redirigido a CAPTCHA. Deteniendo paginación.")
                break

            self.logger.info(
                f"Encontrados {len(items)} inmuebles en página {current_page}"
            )

            for item in items:
                self._extract_property(item, page.url)

            self.human_delay(page, 1500, 3000)

    # ------------------------------------------------------------------
    # Espera de items con fallback
    # ------------------------------------------------------------------

    def _wait_for_items(self, page: Page):
        """Espera y retorna los items con selector principal + fallback."""
        try:
            page.wait_for_selector(
                "li.ui-search-layout__item", timeout=15_000
            )
            return page.query_selector_all("li.ui-search-layout__item")
        except Exception:
            self.logger.warning(
                "Timeout con .ui-search-layout__item, probando .andes-card"
            )
        try:
            page.wait_for_selector(".andes-card.poly-card", timeout=10_000)
            return page.query_selector_all(".andes-card.poly-card")
        except Exception as e:
            self.logger.error(
                f"No se encontraron items. Posible CAPTCHA o cambio de DOM: {e}"
            )
            return []

    # ------------------------------------------------------------------
    # Extracción de una propiedad
    # ------------------------------------------------------------------

    def _extract_property(self, element, source_url: str) -> None:
        try:
            # Título + enlace
            title_el = element.query_selector("a.poly-component__title")
            title = title_el.inner_text().strip() if title_el else "N/A"
            href = title_el.get_attribute("href") if title_el else ""

            # ID del inmueble (MCO-123456)
            id_attr = self._extract_id(href)

            # Tipo de inmueble (Casa, Apartamento, Lote...)
            headline_el = element.query_selector(
                "span.poly-component__headline"
            )
            property_type = (
                headline_el.inner_text().strip() if headline_el else "N/A"
            )

            # Precio
            price_el = element.query_selector(
                "span.andes-money-amount__fraction"
            )
            price_raw = price_el.inner_text().strip() if price_el else "0"
            precio_num = self.parse_price(price_raw)

            # Ubicación
            location_el = element.query_selector(
                "span.poly-component__location"
            )
            location = (
                location_el.inner_text().strip() if location_el else "N/A"
            )

            # Atributos (habitaciones, baños, m²)
            specs = element.query_selector_all("li.poly-attributes_list__item")
            habitaciones = ""
            banos = ""
            area = ""
            for spec in specs:
                txt = spec.inner_text().strip().lower()
                if "habitaci" in txt:
                    habitaciones = spec.inner_text().strip()
                elif "baño" in txt:
                    banos = spec.inner_text().strip()
                elif "m²" in txt or "ha " in txt:
                    area = spec.inner_text().strip()

            prop_data = {
                "id_inmueble": id_attr,
                "title": title,
                "property_type": property_type,
                "price": price_raw,
                "precio_num": precio_num,
                "location": location,
                "habitaciones": habitaciones,
                "banos": banos,
                "area": area,
                "source": self.portal_name,
                "url": href if href else source_url,
                "extracted_at": datetime.utcnow().isoformat(),
            }

            self.process_and_upload(prop_data, id_attr)

        except Exception as e:
            self.logger.error(f"Error extrayendo propiedad: {e}")

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_id(href: str) -> str:
        """Extrae MCO-123456 de la URL de MercadoLibre."""
        if not href:
            return str(int(time.time() * 1000))
        match = re.search(r"(MCO-?\d+)", href)
        return match.group(1) if match else str(int(time.time() * 1000))
