"""
src/scrapers/ciencuadras_usado.py
=================================
Spider para inmuebles USADOS en venta — CienCuadras Colombia.

Híbrido Resiliente:
  - Llama a self.on_page_done() cada página → flush local cada 10.
  - Sincroniza con S3 al finalizar (BaseScraper lifecycle).
"""

import re
from datetime import datetime
from playwright.sync_api import Page
from config.settings import PORTALS_CONFIG
from src.utils.checkpoint import CheckpointManager

class CiencuadrasUsadoScraper(BaseScraper):

    _URL_PATH = "/venta"
    _CARD_SELECTOR = "a.style-none[href^='/inmueble/']"

    def __init__(self):
        super().__init__("ciencuadras_usado")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        self.checkpoint = CheckpointManager(self.portal_name)

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

        page.wait_for_timeout(3000)
        self._scrape_pages(page, max_pages)

    # ------------------------------------------------------------------
    # Barrido de páginas con checkpoint
    # ------------------------------------------------------------------

    def _scrape_pages(self, page: Page, max_pages: int) -> None:
        # Retomar desde checkpoint si existe
        last_completed = self.checkpoint.load()
        start_page = (last_completed or 0) + 1
        total_scraped = 0

        if last_completed:
            self.logger.info(
                f"Retomando desde página {start_page} "
                f"(checkpoint: última completada = {last_completed})"
            )
            if not self._navigate_to_page(page, last_completed + 1):
                self.logger.error(
                    f"No se pudo navegar directamente a la página {start_page}. "
                    "Se intentará avanzar secuencialmente o empezar desde 1."
                )
                # No borramos el checkpoint aquí, dejamos que _navigate_to_page maneje el fallback
        
        end_page = start_page + max_pages - 1
        finished_cleanly = False

        for current_page in range(start_page, end_page + 1):
            self.logger.info(f"CC-Usado — Página {current_page} (batch hasta {end_page})")

            self.human_delay(page, 2000, 4000)

            # Esperar a que los skeleton loaders desaparezcan (Angular hydration)
            try:
                page.wait_for_selector(".p-skeleton", state="detached", timeout=20_000)
            except Exception:
                self.logger.warning("Skeleton loaders persistieron — procediendo...")

            self._scroll_to_load(page)

            # Detección de tarjetas con reintentos
            cards = self._wait_for_cards_with_retries(page, current_page)
            if not cards:
                self.logger.error(f"Página {current_page} persistió vacía — fin del scraping.")
                finished_cleanly = True # No hay más páginas
                break

            self.logger.info(f"Encontradas {len(cards)} tarjetas en página {current_page}")

            nuevos_en_pagina = 0
            for card_link in cards:
                if self._extract_property(card_link):
                    nuevos_en_pagina += 1
            
            total_scraped += nuevos_en_pagina
            self.logger.info(f"Resultados p{current_page}: {nuevos_en_pagina} nuevos/actualizados.")

            # Flush periódico y guardar checkpoint después de cada página
            self.on_page_done()
            self.checkpoint.save(last_page=current_page, total_scraped=total_scraped)

            if current_page < end_page:
                self.logger.info(f"Intentando avanzar a la página {current_page + 1}...")
                if not self._click_next(page, current_page):
                    self.logger.info("Fin de paginación natural.")
                    finished_cleanly = True
                    break
            else:
                self.logger.info(
                    f"Batch de {max_pages} páginas completado. "
                    f"Checkpoint en página {current_page} para el próximo run."
                )

        # Solo borrar el checkpoint si terminamos porque no hay más páginas
        if finished_cleanly:
            self.checkpoint.clear()
            self.logger.info(
                f"Ciclo completo — checkpoint borrado. "
                f"Próximo run empezará desde página 1."
            )

    # ------------------------------------------------------------------
    # Navegar directamente a una página (para retomar checkpoint)
    # ------------------------------------------------------------------

    def _navigate_to_page(self, page: Page, target_page: int) -> bool:
        """
        Intenta navegar a la página N. Como CienCuadras es AJAX,
        intentamos click en el número de página si es visible.
        """
        if target_page <= 1:
            return True

        self.logger.info(f"Navegando directo a página {target_page}...")

        # Intentar click en número de página en la paginación (si está en el rango visible)
        try:
            target_li = page.query_selector(
                f"ul.pagination.desktop li:has(a:text-is('{target_page}'))"
            )
            if target_li:
                target_li.scroll_into_view_if_needed()
                page.wait_for_timeout(500)
                self._dismiss_overlays(page)
                target_li.click()
                page.wait_for_timeout(5000)
                active = self._get_active_page(page)
                if active == target_page:
                    return True
        except Exception as e:
            self.logger.debug(f"Error en click directo a p{target_page}: {e}")

        self.logger.warning(
            f"No se pudo navegar directo a página {target_page}. "
            "Se avanzará secuencialmente desde donde esté."
        )
        return False

    def _wait_for_cards_with_retries(self, page: Page, current_page: int, max_retries: int = 3):
        for attempt in range(1, max_retries + 1):
            cards = self._wait_for_cards(page)
            if cards:
                return cards
            if attempt < max_retries:
                self.logger.info(f"Detección vacía p{current_page}, reintentando {attempt}/{max_retries - 1}...")
                page.wait_for_timeout(7000)
        return []

    # ------------------------------------------------------------------
    # Scroll de carga (idéntico al del usuario)
    # ------------------------------------------------------------------

    def _scroll_to_load(self, page: Page, max_scrolls: int = 10) -> None:
        for _ in range(max_scrolls):
            page.evaluate("window.scrollBy(0, 800)")
            page.wait_for_timeout(400)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(400)

    # ------------------------------------------------------------------
    # Esperar tarjetas
    # ------------------------------------------------------------------

    def _wait_for_cards(self, page: Page, timeout: int = 15_000):
        try:
            page.wait_for_selector(self._CARD_SELECTOR, timeout=timeout, state="attached")
            return page.query_selector_all(self._CARD_SELECTOR)
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Paginación — click en flecha "next"
    # ------------------------------------------------------------------

    def _dismiss_overlays(self, page: Page) -> None:
        """
        Cierra popups/overlays que puedan interceptar clicks de paginación.
        Actualmente maneja: Survicate survey widget.
        """
        # 1. Intentar cerrar el widget de Survicate via botón de cierre
        close_selectors = [
            "[class*='survicate'] [class*='close']",
            "[class*='survicate'] button[aria-label*='close' i]",
            "[class*='survicate'] button[aria-label*='cerrar' i]",
            "#survicate-box button",
        ]
        for sel in close_selectors:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click(timeout=3000)
                    self.logger.info(f"Overlay cerrado via botón: {sel}")
                    page.wait_for_timeout(500)
                    return
            except Exception:
                continue

        # 2. Si no hay botón de cierre, remover el overlay del DOM directamente
        try:
            removed = page.evaluate("""
                () => {
                    const targets = [
                        document.getElementById('survicate-box'),
                        ...document.querySelectorAll('[class*="survicate"]'),
                        ...document.querySelectorAll('[id*="survicate"]'),
                    ];
                    let count = 0;
                    for (const el of targets) {
                        if (el) { el.remove(); count++; }
                    }
                    return count;
                }
            """)
            if removed:
                self.logger.info(f"Overlay Survicate removido del DOM ({removed} elementos).")
                page.wait_for_timeout(300)
        except Exception as e:
            self.logger.warning(f"No se pudo remover overlay: {e}")

    def _click_next(self, page: Page, current_page: int) -> bool:
        page_before = self._get_active_page(page)

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Cerrar cualquier overlay antes de intentar el click
        self._dismiss_overlays(page)

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

            # Segundo dismiss justo antes del click (el overlay puede reaparecer)
            self._dismiss_overlays(page)

            following.click(timeout=10_000)
            page.wait_for_timeout(4000)

            page_after = self._get_active_page(page)
            if page_before and page_after and page_after <= page_before:
                self.logger.warning(f"Click en 'next' no avanzó (p{page_before}). Reintentando click numérico...")
                target = page_before + 1
                target_li = page.query_selector(f"ul.pagination.desktop li:has(a:text-is('{target}'))")
                if target_li:
                    target_li.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    self._dismiss_overlays(page)
                    target_li.click(timeout=10_000)
                    page.wait_for_timeout(5000)
                    page_after = self._get_active_page(page)

                if page_after and page_after <= page_before:
                    return False

            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(500)
            return True
        except Exception as e:
            self.logger.error(f"Error en click 'next': {e}")
            return False

    @staticmethod
    def _get_active_page(page: Page) -> int:
        active = page.query_selector("ul.pagination.desktop li.active")
        if active:
            try:
                return int(active.inner_text().strip())
            except:
                pass
        return 0

    # ------------------------------------------------------------------
    # Extracción de datos por tarjeta
    # ------------------------------------------------------------------

    def _extract_property(self, card_link) -> bool:
        try:
            href = card_link.get_attribute("href") or ""
            if not href: return False

            article = card_link.query_selector("article.card")
            if not article: return False

            property_id = self._extract_id(article)
            if not property_id: return False

            price_el = article.query_selector("span.card__price-big")
            price_raw = price_el.inner_text().strip() if price_el else "N/A"
            precio_num = self.parse_price(price_raw)

            h3_el = article.query_selector("div.card__location h3")
            type_raw = h3_el.inner_text().strip() if h3_el else "N/A"
            property_type = self._parse_type(type_raw)

            loc_el = article.query_selector("h4.card__location-label")
            location = loc_el.inner_text().strip() if loc_el else "N/A"

            area, habitaciones, banos, garajes = self._extract_specs(article)

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
                "url": f"{self.base_url}{href}",
                "portal": self.portal_name,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }

            return self.process_and_upload(prop_data, f"CC-{property_id}")
        except Exception as e:
            self.logger.error(f"Error parseando tarjeta: {e}")
            return False

    @staticmethod
    def _extract_specs(article) -> tuple:
        area = habitaciones = banos = garajes = ""
        spans = article.query_selector_all("ciencuadras-specs-results .specs p span")
        for span in spans:
            text = span.inner_text().strip()
            lower = text.lower()
            if "m2" in lower or "m²" in lower: area = text
            elif "habit" in lower: habitaciones = text
            elif "baño" in lower: banos = text
            elif "garaje" in lower or "gar" in lower: garajes = text
        return area, habitaciones, banos, garajes

    @staticmethod
    def _extract_id(article) -> str:
        qa = article.get_attribute("data-qa-id") or ""
        m = re.search(r"(\d+)$", qa)
        return m.group(1) if m else ""

    @staticmethod
    def _parse_type(type_raw: str) -> str:
        m = re.match(r"^(\S+)\s+[Ee]n\s+", type_raw)
        return m.group(1) if m else type_raw
