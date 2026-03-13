"""
src/scrapers/fincaraiz.py
=========================
Scraper de inmuebles en venta en FincaRaíz Colombia.

URL base: https://www.fincaraiz.com.co/venta/casas-y-apartamentos
Paginación por URL: /pagina2, /pagina3, …
~21 resultados por página.
"""

import re
import time
from datetime import datetime

from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper


class FincaRaizScraper(BaseScraper):
    def __init__(self, listing_path: str = "/venta/casas-y-apartamentos"):
        super().__init__("fincaraiz")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        self.listing_path = listing_path

    # ------------------------------------------------------------------
    # Navegación y paginación
    # ------------------------------------------------------------------

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        for current_page in range(1, max_pages + 1):
            if current_page == 1:
                url = f"{self.base_url}{self.listing_path}"
            else:
                url = f"{self.base_url}{self.listing_path}/pagina{current_page}"

            self.logger.info(
                f"FR — Página {current_page}/{max_pages}: {url}"
            )

            # Hasta 3 reintentos por fallas efímeras de red
            for attempt in range(3):
                try:
                    page.goto(url, timeout=45_000, wait_until="domcontentloaded")
                    break
                except Exception as e:
                    self.logger.warning(
                        f"Intento {attempt + 1} falló al cargar {url}: {e}"
                    )
                    time.sleep(2)
            else:
                self.logger.error(f"No se pudo cargar {url} tras 3 intentos.")
                break

            self.human_delay(page)

            # Esperar a que el listado dinámico cargue
            cards = self._wait_for_cards(page)
            if not cards:
                self.logger.info(
                    "No se encontraron inmuebles. Posible fin de paginación."
                )
                break

            self.logger.info(
                f"Encontrados {len(cards)} inmuebles en página {current_page}"
            )

            for card in cards:
                self._extract_property(card)

            # Verificar si hay página siguiente
            next_link = page.query_selector(
                "a[href*=pagina]:has-text('>')"
            )
            if not next_link:
                self.logger.info(
                    "No hay botón 'Siguiente'. Fin de la paginación."
                )
                break

    # ------------------------------------------------------------------
    # Esperar tarjetas
    # ------------------------------------------------------------------

    def _wait_for_cards(self, page: Page, timeout: int = 10_000):
        """Espera a que aparezcan las tarjetas de resultados."""
        try:
            page.wait_for_selector(
                "div.listingBoxCard", timeout=timeout, state="attached"
            )
        except Exception:
            return []
        return page.query_selector_all("div.listingBoxCard")

    # ------------------------------------------------------------------
    # Extracción de datos por tarjeta
    # ------------------------------------------------------------------

    def _extract_property(self, card) -> None:
        try:
            link = card.query_selector("a.lc-data")
            if not link:
                return

            href = link.get_attribute("href") or ""
            property_id = self._extract_id(href)
            if not property_id:
                return

            # Precio
            price_el = card.query_selector("p.main-price")
            price_raw = price_el.inner_text().strip() if price_el else "N/A"
            precio_num = self.parse_price(price_raw)

            # Título
            title_el = card.query_selector(".lc-title")
            title = title_el.inner_text().strip() if title_el else "N/A"

            # Ubicación (incluye tipo: "Apartamento en Pance, Cali, …")
            loc_el = card.query_selector("strong.lc-location")
            location_raw = loc_el.inner_text().strip() if loc_el else "N/A"

            # Separar tipo de inmueble de la ubicación
            property_type, location = self._parse_location(location_raw)

            # Especificaciones: habitaciones, baños, área
            specs = card.query_selector_all("span.lc-typologyTag__item")
            habitaciones = ""
            banos = ""
            area = ""
            for spec in specs:
                text = spec.inner_text().strip()
                if "Hab" in text:
                    habitaciones = text
                elif "Baño" in text:
                    banos = text
                elif "m²" in text:
                    area = text

            full_url = (
                f"{self.base_url}{href}" if href.startswith("/") else href
            )

            prop_data = {
                "id_inmueble": f"FR-{property_id}",
                "titulo": title,
                "tipo_inmueble": property_type,
                "precio": price_raw,
                "precio_num": precio_num,
                "ubicacion": location,
                "habitaciones": habitaciones,
                "banos": banos,
                "area": area,
                "url": full_url,
                "portal": self.portal_name,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }

            self.process_and_upload(prop_data, f"FR-{property_id}")

        except Exception as e:
            self.logger.error(f"Error parseando inmueble: {e}")

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_id(href: str) -> str:
        """Extrae el ID numérico del final de la URL de FincaRaíz."""
        if not href:
            return ""
        match = re.search(r"/(\d{4,})$", href)
        return match.group(1) if match else ""

    @staticmethod
    def _parse_location(location_raw: str) -> tuple:
        """
        Separa 'Apartamento en Pance, Cali, Valle del cauca'
        en  ('Apartamento', 'Pance, Cali, Valle del cauca')
        """
        match = re.match(r"^(\w+)\s+en\s+(.+)$", location_raw, re.IGNORECASE)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return "N/A", location_raw
