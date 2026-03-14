"""
src/scrapers/metrocuadrado.py
==============================
Scraper de inmuebles en venta en Metrocuadrado Colombia.

URL base: https://www.metrocuadrado.com/inmuebles/venta/?search=form
Paginación: Vía parámetros URL (currentPage=N) para evadir bloqueos de click.
~68 tarjetas por página (3 destacadas + 65 normales). Se requiere scroll
para disparar lazy-loading.
"""

import re
import time
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
        base_search_url = f"{self.base_url}/inmuebles/venta/?search=form"
        previous_page_ids = set()
        for current_page in range(1, max_pages + 1):
            if current_page == 1:
                url = base_search_url
            else:
                url = f"{base_search_url}&currentPage={current_page}"

            self.logger.info(
                f"MC — Página {current_page}/{max_pages}: {url[:100]}"
            )

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
            # Si todas las tarjetas de esta página ya las vimos en la anterior,
            # Metrocuadrado nos está mostrando el fallback de "Destacados" o repitiendo.
            current_ids = []
            for card in cards:
                href = card.query_selector("a[href]")
                if href:
                    p_id = self._extract_id(href.get_attribute("href") or "")
                    if p_id:
                        current_ids.append(p_id)
            
            current_ids_set = set(current_ids)
            if current_page > 1 and current_ids_set and current_ids_set.issubset(previous_page_ids):
                self.logger.warning("Detectada repetición de datos (Página Duplicada). Continuando por solicitud de 'camino completo'.")
            
            previous_page_ids = current_ids_set
            # --------------------------------------

            self.logger.info(
                f"Encontradas {len(cards)} tarjetas en página {current_page}"
            )

            for card in cards:
                self._extract_property(card, page)

    # ------------------------------------------------------------------
    # Scroll para disparar lazy-loading
    # ------------------------------------------------------------------

    def _scroll_to_load(self, page: Page, max_scrolls: int = 12) -> None:
        """Scrollea incrementalmente hasta el fondo para cargar tarjetas lazy."""
        for _ in range(max_scrolls):
            page.evaluate("window.scrollBy(0, 800)")
            page.wait_for_timeout(600)

    # ------------------------------------------------------------------
    # Esperar tarjetas
    # ------------------------------------------------------------------

    def _wait_for_cards(self, page: Page, timeout: int = 12_000):
        """Espera y retorna las tarjetas de resultados (excluyendo los destacados top)."""
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

            # Specs: intentar shadow DOM, luego fallback a img alt
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
    # Specs: Shadow DOM + fallback img alt
    # ------------------------------------------------------------------

    def _extract_specs(self, card) -> tuple:
        """
        Extrae habitaciones, baños, área, garajes.
        1) Intenta leer pt-main-specs (Shadow DOM).
        2) Fallback: parsear img alt.
        """
        habitaciones = ""
        banos = ""
        area = ""
        garajes = ""

        # Intento 1: Shadow DOM
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

                if area or habitaciones:
                    return habitaciones, banos, area, garajes
            except Exception:
                pass

        # Intento 2: Fallback → img alt
        img = card.query_selector("img[alt]")
        if img:
            alt = img.get_attribute("alt") or ""
            habitaciones, banos, area, garajes = self._parse_img_alt(alt)

        return habitaciones, banos, area, garajes

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_id(href: str) -> str:
        """
        Extrae el ID del inmueble de la URL.
        Patterns: /14392-M3445787, /17156-C0035-10, /MC6286185
        """
        if not href:
            return ""
        match = re.search(r"(\d{4,}-[A-Z0-9-]+|MC\d+)", href, re.IGNORECASE)
        return match.group(1) if match else ""

    @staticmethod
    def _parse_title(title: str) -> tuple:
        """
        Separa 'Apartamento en Venta, La Concepcion, Barranquilla'
        en  ('Apartamento', 'La Concepcion, Barranquilla')
        """
        match = re.match(
            r"^(\w+)\s+en\s+(?:Venta|Arriendo)[,\s]+(.+)$",
            title,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return "N/A", title

    @staticmethod
    def _parse_img_alt(alt: str) -> tuple:
        """
        Parsea el atributo alt de la imagen:
        'Foto de Apartamento … con 3 habitaciones, 4 baños, área 108.01 m2, 1 garaje - ID'
        """
        hab = ""
        ban = ""
        area = ""
        gar = ""

        m = re.search(r"(\d+)\s*habitaci", alt, re.IGNORECASE)
        if m:
            hab = f"{m.group(1)} hab."

        m = re.search(r"(\d+)\s*baño", alt, re.IGNORECASE)
        if m:
            ban = f"{m.group(1)} bañ."

        m = re.search(r"[aá]rea\s*([\d.,]+)\s*m2?", alt, re.IGNORECASE)
        if m:
            area = f"{m.group(1)} m²"

        m = re.search(r"(\d+)\s*garaje", alt, re.IGNORECASE)
        if m:
            gar = f"{m.group(1)} par."

        return hab, ban, area, gar
