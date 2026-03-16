"""
Scraper de inmuebles USADOS en venta — CienCuadras Colombia.

URL: https://www.ciencuadras.com/venta
Paginación: click en flecha "next" (li.following). ~28 tarjetas por página.
Verificación de página activa para evitar loops infinitos.
"""

import re
import time
from datetime import datetime

from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper


class CiencuadrasUsadoScraper(BaseScraper):

    _URL_PATH = "/venta"
    _CARD_SELECTOR = "a.style-none[href^='/inmueble/']"

    def __init__(self):
        super().__init__("ciencuadras_usado")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]

    # ------------------------------------------------------------------
    # Orquestador principal
    # ------------------------------------------------------------------

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        url = f"{self.base_url}{self._URL_PATH}"

        self.logger.info("=" * 50)
        self.logger.info("  FASE — Compra usado")
        self.logger.info("=" * 50)
        self.logger.info(f"CC-Usado — Navegando a: {url}")

        for attempt in range(3):
            try:
                page.goto(url, timeout=60_000, wait_until="domcontentloaded")
                break
            except Exception as e:
                self.logger.warning(f"Intento {attempt + 1} falló (goto): {e}")
                page.wait_for_timeout(2000)
        else:
            self.logger.error(f"No se pudo cargar {url} tras 3 intentos.")
            return

        page.wait_for_timeout(5000)
        self._scrape_pages(page, max_pages)

    # ------------------------------------------------------------------
    # Barrido de páginas
    # ------------------------------------------------------------------

    def _scrape_pages(self, page: Page, max_pages: int) -> None:
        for current_page in range(1, max_pages + 1):
            self.logger.info(f"CC-Usado — Página {current_page}/{max_pages}")
            # B. Espera y detección robusta
            cards = []
            self.human_delay(page, 2000, 4000)
            
            # 1. Esperar a que los esqueletos desaparezcan (visto en debug screenshot)
            try:
                self.logger.info("Esperando a que desaparezcan los skeleton loaders...")
                page.wait_for_selector(".p-skeleton", state="detached", timeout=20000)
            except:
                self.logger.warning("Skeleton loaders persistieron o no se detectaron, procediendo...")

            self._scroll_to_load(page)

            cards = self._wait_for_cards(page)
            if not cards:
                self.logger.warning(f"Página {current_page} no muestra tarjetas aún, reintentando...")
                page.wait_for_timeout(7000)
                cards = self._wait_for_cards(page)
                if not cards:
                    self.logger.error("No se encontraron tarjetas. Fin.")
                    break

            self.logger.info(f"Encontradas {len(cards)} tarjetas en página {current_page}")

            for card_link in cards:
                self._extract_property(card_link)

            if current_page < max_pages:
                self.logger.info(f"Saltando a la página {current_page + 1}...")
                if not self._click_next(page, current_page):
                    self.logger.info("Fin de paginación natural.")
                    break
            else:
                self.logger.info(f"Límite {max_pages} alcanzado.")
                break

    # ------------------------------------------------------------------
    # Scroll para lazy-loading
    # ------------------------------------------------------------------

    def _scroll_to_load(self, page: Page, max_scrolls: int = 10) -> None:
        for _ in range(max_scrolls):
            page.evaluate("window.scrollBy(0, 800)")
            page.wait_for_timeout(500)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(500)

    # ------------------------------------------------------------------
    # Esperar tarjetas
    # ------------------------------------------------------------------

    def _wait_for_cards(self, page: Page, timeout: int = 15_000):
        try:
            page.wait_for_selector(
                self._CARD_SELECTOR, timeout=timeout, state="attached"
            )
            return page.query_selector_all(self._CARD_SELECTOR)
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Paginación — click en flecha "next"
    # ------------------------------------------------------------------

    def _click_next(self, page: Page, current_page: int) -> bool:
        page_before = self._get_active_page(page)

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        following = page.query_selector("li.following")
        if not following:
            page.evaluate("window.scrollBy(0, -400)")
            page.wait_for_timeout(1000)
            following = page.query_selector("li.following")
            if not following:
                return False

        cls = following.get_attribute("class") or ""
        if "hide" in cls:
            return False

        try:
            following.scroll_into_view_if_needed()
            page.wait_for_timeout(500)
            following.click()
            page.wait_for_timeout(4000)

            page_after = self._get_active_page(page)
            if page_before and page_after and page_after <= page_before:
                self.logger.warning(
                    f"Click en 'next' no cambió la página (antes={page_before}, después={page_after}). Reintentando click numérico..."
                )
                target = page_before + 1
                target_li = page.query_selector(
                    f"ul.pagination.desktop li:has(a:text-is('{target}'))"
                )
                if target_li:
                    target_li.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    target_li.click()
                    page.wait_for_timeout(5000)
                    page_after = self._get_active_page(page)

                if page_after and page_after <= page_before:
                    return False

            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(500)
            return True
        except Exception as e:
            self.logger.error(f"Error al hacer click en 'next': {e}")
            return False

    @staticmethod
    def _get_active_page(page: Page) -> int:
        active = page.query_selector("ul.pagination.desktop li.active")
        if active:
            try:
                return int(active.inner_text().strip())
            except ValueError:
                pass
        return 0

    # ------------------------------------------------------------------
    # Extracción de datos por tarjeta
    # ------------------------------------------------------------------

    def _extract_property(self, card_link) -> None:
        try:
            href = card_link.get_attribute("href") or ""
            if not href:
                return

            article = card_link.query_selector("article.card.result")
            if not article:
                article = card_link.query_selector("article.card")
            if not article:
                return

            property_id = self._extract_id(article)
            if not property_id:
                return

            price_el = article.query_selector("span.card__price-big")
            price_raw = price_el.inner_text().strip() if price_el else "N/A"
            precio_num = self.parse_price(price_raw)

            h3_el = article.query_selector("div.card__location h3")
            type_raw = h3_el.inner_text().strip() if h3_el else "N/A"
            property_type = self._parse_type(type_raw)

            loc_el = article.query_selector("h4.card__location-label")
            location = loc_el.inner_text().strip() if loc_el else "N/A"

            area, habitaciones, banos, garajes = self._extract_specs(article)

            full_url = f"{self.base_url}{href}"

            prop_data = {
                "id_inmueble": f"CC-{property_id}",
                "titulo": type_raw,
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

            self.process_and_upload(prop_data, f"CC-{property_id}")

        except Exception as e:
            self.logger.error(f"Error parseando tarjeta: {e}")

    # ------------------------------------------------------------------
    # Specs: área, habitaciones, baños, garajes
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_specs(article) -> tuple:
        area = ""
        habitaciones = ""
        banos = ""
        garajes = ""

        spans = article.query_selector_all(
            "ciencuadras-specs-results .specs p span"
        )
        for span in spans:
            text = span.inner_text().strip()
            if "m2" in text.lower() or "m²" in text.lower():
                area = text
            elif "habit" in text.lower():
                habitaciones = text
            elif "baño" in text.lower():
                banos = text
            elif "garaje" in text.lower() or "gar" in text.lower():
                garajes = text

        return area, habitaciones, banos, garajes

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_id(article) -> str:
        qa = article.get_attribute("data-qa-id") or ""
        m = re.search(r"(\d+)$", qa)
        return m.group(1) if m else ""

    @staticmethod
    def _parse_type(type_raw: str) -> str:
        m = re.match(r"^(\S+)\s+[Ee]n\s+", type_raw)
        return m.group(1) if m else type_raw
