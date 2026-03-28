"""
src/scrapers/bancolombia_tu360.py
=================================
Spider para Inmobiliaria Tu360 de Bancolombia.
URL principal: https://inmobiliariotu360.bancolombia.com/venta

Flujo:
  1. Navegar a /venta?page=N (paginación por URL, ~36 cards/página).
  2. Extraer tarjetas visibles via JS bulk extraction.
  3. Fallback: click "Mostrar más" si la paginación URL se agota.

Selectores basados en el DOM real VTEX / mkpinmobiliarioco:
  - Tarjetas : article[class*='vtex-product-summary-2-x-element']
  - Campos   : clases productCardDetailXxx
  - Botón    : div[class*='vtex-button__label'] con texto "Mostrar"
"""

import re
import time
from datetime import datetime
from urllib.parse import urljoin

from playwright.sync_api import Page

from config.settings import PORTALS_CONFIG
from src.scrapers.base_scraper import BaseScraper

# ---------------------------------------------------------------------------
# Selectores CSS — basados en el HTML real del sitio VTEX
# ---------------------------------------------------------------------------
_SEL = {
    "card":      "article[class*='vtex-product-summary-2-x-element']",
    "load_more": "div[class*='vtex-button__label']",
}

# ---------------------------------------------------------------------------
# JavaScript para extracción masiva — se ejecuta dentro del navegador.
# Una sola llamada en vez de N×7 locators por tarjeta.
# ---------------------------------------------------------------------------
_JS_EXTRACT = """
(startIdx) => {
    const cards = document.querySelectorAll(
        "article[class*='vtex-product-summary-2-x-element']"
    );
    const results = [];
    for (let i = startIdx; i < cards.length; i++) {
        const c = cards[i];
        const txt = (s) => (c.querySelector(s)?.innerText?.trim()) || '';
        const a = c.closest("a[href]") || c.querySelector("a[href]");
        results.push({
            brand:   txt("p[class*='productCardDetailBrand']"),
            status:  txt("p[class*='productCardDetailStatus']"),
            title:   txt("p[class*='productCardDetailTitle']"),
            price:   txt("p[class*='productCardDetailPrice']"),
            details: Array.from(
                c.querySelectorAll("p[class*='productCardDetailText']")
            ).map(e => e.innerText?.trim() || ''),
            href: a ? a.getAttribute("href") : ''
        });
    }
    return {total: cards.length, items: results};
}
"""


class BancolombiaTu360Scraper(BaseScraper):
    """Spider para inmobiliariotu360.bancolombia.com — hereda BaseScraper (S3)."""

    def __init__(self):
        super().__init__("bancolombia_tu360")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"].rstrip("/")

    # ------------------------------------------------------------------
    # Método abstracto obligatorio
    # ------------------------------------------------------------------

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        """
        Orquesta el scraping completo:
          1. Bloquea recursos pesados (imágenes, fuentes).
          2. Paginación por URL  →  /venta?page=N
          3. Fallback click "Mostrar más" si las páginas URL se agotan.
        """
        listing_url = f"{self.base_url}/venta"
        self.logger.info(f"Iniciando scraping en: {listing_url}")

        # Bloquear imágenes y fuentes → carga mucho más rápida
        page.route(
            "**/*.{png,jpg,jpeg,gif,webp,ico,woff,woff2,ttf,eot}",
            lambda route: route.abort(),
        )

        self._scrape_listing(page, listing_url, max_pages=max_pages)

        self.logger.info(
            f"{'=' * 55}\n"
            f"  Scraping completo.\n"
            f"  Inmuebles nuevos: {len(self.scraped_data)}\n"
            f"{'=' * 55}"
        )

    # ------------------------------------------------------------------
    # Scraping del listado (URL-based + click extension)
    # ------------------------------------------------------------------

    def _scrape_listing(self, page: Page, url: str, max_pages: int = 999) -> None:
        """
        Fase 1 — Navega a /venta?page=1, ?page=2, … (~36 cards/página).
        Fase 2 — Click "Mostrar más" desde la última página buena.
        """
        # --- Fase 1: paginación por URL ---
        page_num = self.start_page
        end_page = self.start_page + max_pages
        consecutive_empty = 0
        previous_total_dom = 0

        while page_num < end_page:
            page_url = f"{url}?page={page_num}"

            try:
                page.goto(page_url, timeout=45_000, wait_until="domcontentloaded")
                self._wait_for_vtex(page)
                self._scroll_to_load(page)
            except Exception as e:
                self.logger.warning(f"Error cargando {page_url}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page_num += 1
                continue

            # Detectar "Sin resultados para tu búsqueda"
            if self._no_results_message(page):
                self.logger.info("  'Sin resultados para tu búsqueda' — fin del listado.")
                break

            accepted, total_dom = self._extract_cards_js(page, page_url, start_index=0)

            # --- DETECCIÓN DE FIN DE RESULTADOS ---
            # Si el total de tarjetas en el DOM no aumenta o son duplicadas de la página anterior
            current_ids = set()
            # En Bancolombia, los IDs se procesan dentro de _extract_cards_js, 
            # pero podemos pre-validar o confiar en el total_dom y accepted.
            # Sin embargo, para mayor robustez, usaremos el total_dom como señal.
            if total_dom > 0 and total_dom == previous_total_dom:
                consecutive_empty += 1
            else:
                consecutive_empty = 0
            
            previous_total_dom = total_dom
            # --------------------------------------

            self.logger.info(
                f"  Página {page_num}: +{accepted} nuevos | "
                f"Cards DOM: {total_dom} | Sesión: {len(self.scraped_data)}"
            )

            if total_dom == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    self.logger.info("  3 páginas vacías consecutivas — fin URL.")
                    break
            elif consecutive_empty >= 3:
                self.logger.info("  Contenido repetido detectado — fin URL.")
                break
            else:
                consecutive_empty = 0

            page_num += 1
            self.human_delay(page, min_ms=800, max_ms=1_800)

        # --- Fase 2: click "Mostrar más" desde última página buena ---
        if consecutive_empty >= 3:
            last_good = page_num - consecutive_empty
            self.logger.info(
                f"  URL agotada en página {last_good}. "
                f"Intentando click 'Mostrar más'…"
            )
            try:
                page.goto(
                    f"{url}?page={last_good}",
                    timeout=45_000, wait_until="domcontentloaded",
                )
                self._wait_for_vtex(page)
            except Exception:
                return

            cards_in_dom = page.evaluate(
                f"document.querySelectorAll(\"{_SEL['card']}\").length"
            )
            click_fails = 0

            while click_fails < 3:
                if not self._click_load_more(page, cards_in_dom):
                    click_fails += 1
                    continue

                click_fails = 0
                accepted, new_total = self._extract_cards_js(
                    page, url, start_index=cards_in_dom
                )
                if new_total <= cards_in_dom:
                    self.logger.info("  Click no produjo tarjetas nuevas — fin.")
                    break
                cards_in_dom = new_total
                self.logger.info(
                    f"  Click: +{accepted} nuevos | "
                    f"DOM: {new_total} | Sesión: {len(self.scraped_data)}"
                )
                self.human_delay(page, min_ms=800, max_ms=1_800)

    # ------------------------------------------------------------------
    # Click en "Mostrar más" (con prevención de navegación)
    # ------------------------------------------------------------------

    def _click_load_more(self, page: Page, current_count: int) -> bool:
        """
        Click "Mostrar más" inyectando un preventDefault en el <a>
        para evitar navegación. VTEX React sigue procesando el click.
        """
        try:
            found = page.evaluate("""
                () => {
                    const labels = document.querySelectorAll(
                        "div[class*='vtex-button__label']"
                    );
                    for (const label of labels) {
                        if (label.textContent.trim().toLowerCase()
                                .includes('mostrar')) {
                            const anchor = label.closest('a');
                            if (anchor) {
                                anchor.addEventListener(
                                    'click',
                                    e => e.preventDefault(),
                                    {once: true, capture: true}
                                );
                            }
                            return true;
                        }
                    }
                    return false;
                }
            """)

            if not found:
                return False

            btn = page.locator(
                f"{_SEL['load_more']}:has-text('Mostrar')"
            ).first
            btn.scroll_into_view_if_needed()
            btn.click()

            # Esperar a que aparezcan más tarjetas en el DOM
            sel_card = _SEL["card"]
            page.wait_for_function(
                f"document.querySelectorAll(\"{sel_card}\").length"
                f" > {current_count}",
                timeout=20_000,
            )
            return True

        except Exception as e:
            self.logger.warning(f"Click 'Mostrar más' falló: {e}")
            return False

    # ------------------------------------------------------------------
    # Extracción rápida de tarjetas (JavaScript en el navegador)
    # ------------------------------------------------------------------

    def _extract_cards_js(
        self, page: Page, source_url: str, start_index: int = 0
    ) -> tuple:
        """
        Extrae tarjetas usando una sola llamada JS — ordenes de magnitud
        más rápido que locators individuales de Playwright.

        Returns:
            (accepted, total_dom)
        """
        try:
            result = page.evaluate(_JS_EXTRACT, start_index)
        except Exception as e:
            self.logger.warning(f"Error en extracción JS: {e}")
            return (0, start_index)

        total_dom = result["total"]
        items = result["items"]
        accepted = 0

        for raw in items:
            try:
                title = raw.get("title", "")
                price_raw = raw.get("price", "")
                precio_num = self.parse_price(price_raw)

                if not title and not precio_num:
                    continue

                details = raw.get("details", [])
                href = raw.get("href", "")
                listing_url = (
                    urljoin(self.base_url + "/", href)
                    if href and not href.startswith("http")
                    else href or source_url
                )

                # Extraer slug como ID del inmueble
                slug = listing_url.rstrip("/").split("/")[-1].split("?")[0]
                if slug == "p":
                    parts = listing_url.rstrip("/").split("/")
                    slug = parts[-2] if len(parts) >= 2 else slug
                property_id = f"BC-{slug or int(time.time() * 1_000)}"

                prop_data = {
                    "id_inmueble":  property_id,
                    "brand":        raw.get("brand") or "N/A",
                    "status":       raw.get("status") or "N/A",
                    "title":        title or "N/A",
                    "city":         details[0] if len(details) > 0 else "N/A",
                    "area":         details[1] if len(details) > 1 else "N/A",
                    "rooms":        details[2] if len(details) > 2 else "N/A",
                    "price":        price_raw or "N/A",
                    "precio_num":   precio_num,
                    "listing_url":  listing_url,
                    "source":       self.portal_name,
                    "extracted_at": datetime.utcnow().isoformat(),
                }

                if self.process_and_upload(prop_data, property_id):
                    accepted += 1

            except Exception as e:
                self.logger.warning(f"Error procesando tarjeta: {e}")
                continue

        return (accepted, total_dom)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _wait_for_vtex(self, page: Page) -> None:
        """Espera a que VTEX renderice las tarjetas y el DOM se estabilice."""
        try:
            page.wait_for_selector(
                _SEL["card"], state="attached", timeout=15_000
            )
            sel = _SEL["card"]
            page.wait_for_function(
                f"""
                () => new Promise(resolve => {{
                    const sel = "{sel}";
                    const n1 = document.querySelectorAll(sel).length;
                    setTimeout(() => {{
                        const n2 = document.querySelectorAll(sel).length;
                        resolve(n1 === n2);
                    }}, 2000);
                }})
                """,
                timeout=20_000,
            )
        except Exception:
            page.wait_for_timeout(4_000)

    def _scroll_to_load(self, page: Page) -> None:
        """Scroll progresivo para activar lazy-render de VTEX."""
        height = page.evaluate("document.body.scrollHeight")
        steps = 6
        for i in range(1, steps + 1):
            page.evaluate(f"window.scrollTo(0, {int(height * i / steps)})")
            page.wait_for_timeout(400)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(500)

    def _no_results_message(self, page: Page) -> bool:
        """Detecta el mensaje 'Sin resultados para tu búsqueda' de VTEX."""
        try:
            return page.evaluate(
                "document.body.innerText.includes('Sin resultados para tu búsqueda')"
            )
        except Exception:
            return False

    @staticmethod
    def parse_price(raw: str) -> int:
        """Extrae el valor numérico de un string de precio (COP)."""
        if not raw:
            return 0
        digits = re.sub(r"[^\d]", "", str(raw))
        return int(digits) if digits else 0
