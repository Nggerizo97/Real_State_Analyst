"""
Scraper de inmuebles en venta en Metrocuadrado Colombia.

URL base: https://www.metrocuadrado.com/inmuebles/venta
Paginación: Vía parámetros URL (currentPage=N).
~68 tarjetas por página. Requiere scroll profundo para lazy-loading.
"""

import re
import time
import random
from datetime import datetime

from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper


class MetrocuadradoScraper(BaseScraper):
    def __init__(self):
        super().__init__("metrocuadrado")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]

    # ------------------------------------------------------------------
    # Navegación y paginación
    # ------------------------------------------------------------------

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        base_search_url = f"{self.base_url}/inmuebles/venta"
        previous_page_ids = set()

        for current_page in range(1, max_pages + 1):
            if current_page == 1:
                url = base_search_url
            else:
                url = f"{base_search_url}/?currentPage={current_page}"

            self.logger.info(f"MC — Página {current_page}/{max_pages}: {url}")

            # Intento de carga con reintentos
            for attempt in range(3):
                try:
                    page.goto(url, timeout=45_000, wait_until="domcontentloaded")
                    break
                except Exception as e:
                    self.logger.warning(f"Intento {attempt + 1} falló navegando a p {current_page}: {e}")
                    time.sleep(2)
            else:
                self.logger.error("No se pudo cargar la página. Deteniendo.")
                break

            self.human_delay(page, 2000, 4000)

            # Scroll para cargar todas las tarjetas (lazy-loading)
            self._scroll_to_load(page)

            cards = self._wait_for_cards(page)
            if not cards:
                self.logger.info("No se encontraron tarjetas o redirigido a CAPTCHA. Fin de paginación.")
                break

            # --- DETECCIÓN DE FIN DE RESULTADOS ---
            current_ids = []
            for card in cards:
                href_el = card.query_selector("a[href]")
                if href_el:
                    href = href_el.get_attribute("href") or ""
                    p_id = self._extract_id(href)
                    if p_id:
                        current_ids.append(p_id)
            
            current_ids_set = set(current_ids)
            if current_page > 1 and current_ids_set and current_ids_set.issubset(previous_page_ids):
                self.logger.warning("Detectada repetición de datos (Página Duplicada). Deteniendo.")
                break
            
            previous_page_ids = current_ids_set
            # --------------------------------------

            self.logger.info(f"Encontradas {len(cards)} tarjetas en página {current_page}")

            for card in cards:
                self._extract_property(card, page)

    # ------------------------------------------------------------------
    # Scroll para disparar lazy-loading
    # ------------------------------------------------------------------

    def _scroll_to_load(self, page: Page, max_scrolls: int = 15) -> None:
        """Scrollea incrementalmente con pausas para cargar tarjetas lazy y Shadow DOM."""
        for i in range(max_scrolls):
            page.evaluate("window.scrollBy(0, 800)")
            page.wait_for_timeout(random.randint(400, 800))

    # ------------------------------------------------------------------
    # Esperar tarjetas
    # ------------------------------------------------------------------

    def _wait_for_cards(self, page: Page, timeout: int = 12_000):
        """Espera y retorna las tarjetas de resultados."""
        try:
            page.wait_for_selector(
                ".property-list__results div.property-card__container", timeout=timeout, state="attached"
            )
        except Exception:
            return []
        return page.query_selector_all(".property-list__results div.property-card__container")

    # ------------------------------------------------------------------
    # Extracción de datos por tarjeta
    # ------------------------------------------------------------------

    def _extract_property(self, card, page: Page) -> None:
        try:
            # Link
            link = card.query_selector("a[href]")
            if not link:
                return
            href = link.get_attribute("href") or ""

            # ID del inmueble desde el href
            property_id = self._extract_id(href)
            if not property_id:
                return

            # Precio
            price_el = card.query_selector(".property-card__detail-price")
            price_raw = price_el.inner_text().strip() if price_el else "N/A"
            precio_num = self.parse_price(price_raw)

            # Título (contiene tipo + ubicación)
            title_el = card.query_selector(".property-card__detail-title h2")
            title = title_el.inner_text().strip() if title_el else "N/A"

            # Tipo de inmueble y ubicación desde el título
            property_type, location = self._parse_title(title)

            # Specs: shadow DOM
            habitaciones, banos, area, garajes = self._extract_specs(card)

            full_url = (
                f"{self.base_url}{href}" if href.startswith("/") else href
            )

            prop_data = {
                "id_inmueble": f"MC-{property_id}",
                "titulo": title,
                "tipo_inmueble": property_type,
                "precio": price_raw,
                "precio_num": precio_num,
                "ubicacion": location,
                "habitaciones": habitaciones,
                "banos": banos,
                "area": area,
                "garajes": garajes,
                "url": full_url,
                "portal": self.portal_name,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }

            self.process_and_upload(prop_data, f"MC-{property_id}")

        except Exception as e:
            self.logger.error(f"Error parseando tarjeta: {e}")

    # ------------------------------------------------------------------
    # Specs: Shadow DOM
    # ------------------------------------------------------------------

    def _extract_specs(self, card) -> tuple:
        """Extrae habitaciones, baños, área, garajes via Shadow DOM."""
        habitaciones = ""
        banos = ""
        area = ""
        garajes = ""

        specs_el = card.query_selector("pt-main-specs")
        if specs_el:
            try:
                features = specs_el.evaluate("""
                    el => {
                        if (!el.shadowRoot) return [];
                        const items = el.shadowRoot.querySelectorAll('.pt-main-specs--feature');
                        return Array.from(items).map(f => {
                            const t = f.querySelector('pt-text');
                            return t ? (t.textContent || '') : '';
                        });
                    }
                """)
                for feat in features:
                    feat = feat.strip()
                    if "m²" in feat or "m2" in feat:
                        area = feat
                    elif "hab" in feat.lower():
                        habitaciones = feat
                    elif "bañ" in feat.lower():
                        banos = feat
                    elif "par" in feat.lower() or "gar" in feat.lower():
                        garajes = feat
            except Exception:
                pass

        return habitaciones, banos, area, garajes

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_id(href: str) -> str:
        if not href:
            return ""
        match = re.search(r"(\d{4,}-[A-Z0-9-]+|MC\d+)", href, re.IGNORECASE)
        return match.group(1) if match else ""

    @staticmethod
    def _parse_title(title: str) -> tuple:
        match = re.match(
            r"^(\w+)\s+en\s+(?:Venta|Arriendo)[,\s]+(.+)$",
            title,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return "N/A", title
