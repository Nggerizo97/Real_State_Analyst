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
        url = f"{self.base_url}/inmuebles/venta/?search=form"

        self.logger.info(f"MC — Cargando URL inicial: {url}")

        for attempt in range(3):
            try:
                # networkidle es más seguro para SPAs pesadas como M2
                page.goto(url, timeout=60_000, wait_until="networkidle")
                break
            except Exception as e:
                self.logger.warning(f"Intento {attempt + 1} falló navegando a M2: {e}")
                time.sleep(2)
        else:
            self.logger.error("No se pudo cargar la página inicial de Metrocuadrado.")
            return

        end_page = self.start_page + max_pages
        for current_page in range(self.start_page, end_page):
            self.logger.info(
                f"MC — Página {current_page}/{end_page - 1}: {page.url[:100]}..."
            )

            self.human_delay(page, 2000, 4000)

            # Scroll profundo para lazy-loading
            self._scroll_to_load(page)

            cards = self._wait_for_cards(page)
            if not cards:
                self.logger.warning(f"Página {current_page} sin tarjetas visibles. Intentando avanzar de todas formas.")
            else:
                self.logger.info(f"Encontradas {len(cards)} tarjetas en página {current_page}")
                new_count = 0
                for card in cards:
                    if self._extract_property(card, page):
                        new_count += 1
                self.logger.info(f"Resultados de página {current_page}: {new_count} nuevos/actualizados.")

            self.on_page_done(current_page)

            # Navegar a la siguiente página (Mandato de Recorrido Total)
            if current_page < end_page - 1:
                self.logger.info(f"Intentando avanzar a la página {current_page + 1}...")
                if not self._click_next(page, current_page):
                    self.logger.info("Fin de resultados naturales (Paginador no encontrado o inactivo).")
                    self.checkpoint.clear()
                    break
            else:
                self.logger.info(f"Límite {max_pages} alcanzado.")
                break

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
        """Espera y retorna las tarjetas de resultados."""
        try:
            # Selector específico de M2 para tarjetas
            page.wait_for_selector(
                "div.property-card__container", timeout=timeout, state="attached"
            )
        except Exception:
            return []
        return page.query_selector_all("div.property-card__container")

    # ------------------------------------------------------------------
    # Paginación — click en "next" (rc-pagination)
    # ------------------------------------------------------------------

    def _click_next(self, page: Page, current_page: int) -> bool:
        """
        Hace click en el botón 'next' de rc-pagination.
        Usa lógica de reintento por número si el click genérico falla.
        """
        page_before = self._get_active_page(page)

        # Buscar el contenedor de paginación
        next_li = page.query_selector("li.rc-pagination-next")
        if not next_li:
            self.logger.warning("No se encontró el botón 'Siguiente' (li.rc-pagination-next)")
            return False

        disabled = next_li.get_attribute("aria-disabled")
        if disabled == "true":
            return False

        next_btn = next_li.query_selector("button")
        if not next_btn:
            return False

        try:
            next_btn.scroll_into_view_if_needed()
            page.wait_for_timeout(500)
            next_btn.click()
            # Esperar a que la SPA recargue las tarjetas (networkidle)
            page.wait_for_timeout(4000)

            # Verificar que la página realmente cambió
            page_after = self._get_active_page(page)
            if page_before and page_after and page_after <= page_before:
                self.logger.warning(
                    f"Click en 'next' no cambió la página (antes={page_before}, después={page_after}). Reintentando click numérico..."
                )
                target = page_before + 1
                target_li = page.query_selector(f"li.rc-pagination-item-{target}")
                if target_li:
                    target_li.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    target_li.click()
                    page.wait_for_timeout(4000)
                    page_after = self._get_active_page(page)

                if page_after and page_after <= page_before:
                    self.logger.error("Paginación bloqueada — no se pudo avanzar al número esperado.")
                    return False

            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(1000)
            return True
        except Exception as e:
            self.logger.error(f"Error al hacer click en 'next': {e}")
            return False

    @staticmethod
    def _get_active_page(page: Page) -> int:
        """Lee el número de página activa del paginador rc-pagination."""
        active = page.query_selector("li.rc-pagination-item-active")
        if active:
            try:
                return int(active.inner_text().strip())
            except ValueError:
                pass
        return 0

    # ------------------------------------------------------------------
    # Extracción de datos por tarjeta
    # ------------------------------------------------------------------

    def _extract_property(self, card, page: Page) -> bool:
        try:
            # Link
            link = card.query_selector("a[href]")
            if not link:
                return False
            href = link.get_attribute("href") or ""

            # ID del inmueble desde el href
            property_id = self._extract_id(href)
            if not property_id:
                return False

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

            return self.process_and_upload(prop_data, f"MC-{property_id}")

        except Exception as e:
            self.logger.error(f"Error parseando tarjeta: {e}")
            return False

    # ------------------------------------------------------------------
    # Specs: Shadow DOM + fallback img alt
    # ------------------------------------------------------------------

    def _extract_specs(self, card) -> tuple:
        """
        Extrae habitaciones, baños, área, garajes.
        1) Intenta leer pt-main-specs (Shadow DOM).
        2) Fallback: parsear img alt (como hacía la versión local funcional).
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

        # Intento 2: Fallback → img alt (parseo manual del texto descriptivo)
        img = card.query_selector("img[alt]")
        if img:
            alt = img.get_attribute("alt") or ""
            # Parseo manual estilo usuario
            m = re.search(r"(\d+)\s*habitaci", alt, re.IGNORECASE)
            if m: habitaciones = f"{m.group(1)} hab."
            m = re.search(r"(\d+)\s*baño", alt, re.IGNORECASE)
            if m: banos = f"{m.group(1)} bañ."
            m = re.search(r"[aá]rea\s*([\d.,]+)\s*m2?", alt, re.IGNORECASE)
            if m: area = f"{m.group(1)} m²"
            m = re.search(r"(\d+)\s*garaje", alt, re.IGNORECASE)
            if m: garajes = f"{m.group(1)} par."

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
