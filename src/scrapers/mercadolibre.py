"""
src/scrapers/mercadolibre.py
============================
Scraper de inmuebles en venta en MercadoLibre Colombia.

URL base: https://listado.mercadolibre.com.co/inmuebles/venta/_NoIndex_True
Pagina por offset (_Desde_N) hasta agotar resultados o alcanzar el límite.

MercadoLibre redirige a /gz/account-verification las IPs de datacenter
(GitHub Actions, Cloudflare WARP). En ese caso el run se marca como
bloqueado y el runner sale con código != 0. En CI corre en un self-hosted
runner con IP residencial (job scrape-mercadolibre en scraper_cron.yml).
"""

import re
from datetime import datetime

from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper

_BLOCK_URL_MARKERS = ("account-verification", "loginType=", "/login")


class MercadoLibreScraper(BaseScraper):
    def __init__(self):
        super().__init__("mercadolibre")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        base_url = f"{self.base_url}/inmuebles/venta/_NoIndex_True"
        previous_page_ids = set()
        end_page = self.start_page + max_pages

        # Warm-up: visit homepage before listings to appear as a natural user
        self._warmup(page)
        if self.blocked:
            return

        for current_page in range(self.start_page, end_page):
            if current_page == 1:
                url = base_url
            else:
                offset = (current_page - 1) * 48 + 1
                # The format is domain/inmuebles/venta/_Desde_49_NoIndex_True
                url = f"{self.base_url}/inmuebles/venta/_Desde_{offset}_NoIndex_True"

            self.logger.info(
                f"ML — Página {current_page}/{end_page - 1}: {url[:100]}"
            )

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            except Exception as e:
                self.logger.error(f"Error al navegar a la página {current_page}: {e}")
                self.human_delay(page=None, min_ms=5000, max_ms=10000)
                continue

            # Detect login wall / bot-detection redirect
            if self._is_blocked_url(page.url):
                self.mark_blocked(
                    f"redirigido a verificación en página {current_page}: {page.url[:80]}"
                )
                break

            self.human_delay(page)

            # Buscar tarjetas con selectores estables + fallback
            items = self._wait_for_items(page)
            if not items:
                # En la página 1 una lista vacía no es fin de resultados sino
                # bloqueo/cambio de DOM. Si se retomó desde un checkpoint, puede
                # ser que ML ya no tenga más páginas: se reinicia el ciclo.
                if self.pages_scraped == 0 and self.start_page == 1:
                    self.mark_blocked(
                        f"sin resultados en la primera página ({page.url[:80]}) "
                        "— posible CAPTCHA o cambio de DOM"
                    )
                else:
                    self.logger.info("Sin resultados. Fin de la paginación.")
                    self.checkpoint.clear()
                break

            # --- DETECCIÓN DE FIN DE RESULTADOS ---
            current_ids = []
            for item in items:
                link_el = item.query_selector("a.poly-component__title")
                if link_el:
                    href = link_el.get_attribute("href") or ""
                    p_id = self._extract_id(href)
                    if p_id:
                        current_ids.append(p_id)
            
            current_ids_set = set(current_ids)
            if current_page > 1 and current_ids_set and current_ids_set.issubset(previous_page_ids):
                self.logger.info("Detección de fin de resultados (Duplicate): Finalizando.")
                self.checkpoint.clear()
                break
            previous_page_ids = current_ids_set
            # --------------------------------------

            self.logger.info(
                f"Encontrados {len(items)} inmuebles en página {current_page}"
            )

            for item in items:
                self._extract_property(item, page.url)

            # Flush local cada 10 páginas + checkpoint en S3 para retomar
            self.on_page_done(current_page)

            self.human_delay(page, 1500, 3000)

    # ------------------------------------------------------------------
    # Warm-up navigation
    # ------------------------------------------------------------------

    def _warmup(self, page: Page) -> None:
        """Visit ML homepage before scraping to simulate natural navigation."""
        try:
            self.logger.info("Warm-up: visitando homepage de MercadoLibre...")
            page.goto("https://www.mercadolibre.com.co", wait_until="domcontentloaded", timeout=30_000)
            if self._is_blocked_url(page.url):
                self.mark_blocked(f"redirigido a verificación en el warm-up: {page.url[:80]}")
                return
            self.human_delay(page, 3000, 5000)
        except Exception as e:
            self.logger.warning(f"Warm-up falló (ignorando): {e}")

    @staticmethod
    def _is_blocked_url(url: str) -> bool:
        return any(t in url for t in _BLOCK_URL_MARKERS)

    # ------------------------------------------------------------------
    # Espera de items con fallback
    # ------------------------------------------------------------------

    def _wait_for_items(self, page: Page):
        """Espera y retorna los items con selector principal + fallback."""
        # Abort immediately if we're on a login/verification wall
        if self._is_blocked_url(page.url):
            self.logger.warning("Login wall detectado (negative_traffic). Deteniendo.")
            return []
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

            # ID del inmueble (MCO-123456). Sin ID estable no se puede
            # deduplicar (SCD2) ni cruzar con Silver: se descarta.
            id_attr = self._extract_id(href)
            if not id_attr:
                self.logger.debug(f"Tarjeta sin ID MCO, se omite: {href[:80]}")
                return

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
        """Extrae MCO-123456 de la URL de MercadoLibre ('' si no hay)."""
        match = re.search(r"(MCO-?\d+)", href or "")
        return match.group(1) if match else ""
