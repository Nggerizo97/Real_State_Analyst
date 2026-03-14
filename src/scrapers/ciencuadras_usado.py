import re
import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class CiencuadrasUsadoScraper(BaseScraper):
    def __init__(self):
        super().__init__("ciencuadras_usado")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int) -> None:
        base_search_url = f"{self.base_url}/venta"
        self.logger.info("--- INICIANDO CIENCUADRAS USADO (VENTA) ---")
        
        previous_page_ids = set()
        
        for current_page in range(1, max_pages + 1):
            url = f"{base_search_url}?page={current_page}"
            self.logger.info(f"CC (Usado) — Cargando Página {current_page}/{max_pages}: {url}")
            
            # Navegación directa por URL (más robusta en headless que clicks SPA)
            if not self._safe_goto(page, url):
                break

            self.human_delay(page, 2000, 4000)
            self._scroll_for_lazy_loading(page)

            cards = self._get_cards(page, current_page)
            if cards is None:
                # Reintento si no hay tarjetas pero no hay mensaje de "No resultados"
                if not page.query_selector("div.no-results"):
                    self.logger.info("Reintentando carga por falta de tarjetas...")
                    page.reload(wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    cards = self._get_cards(page, current_page)
                
                if cards is None:
                    break

            # Procesar y detectar duplicados (ignorando destacados)
            is_loop = self._process_cards(cards, previous_page_ids)
            if is_loop:
                self.logger.warning("Detectada repetición de datos (Página Duplicada). Continuando por solicitud de 'camino completo'.")
            
            # Verificar si es la última página por UI
            if self._is_last_page(page):
                self.logger.info("Fin de portal: Botón 'Siguiente' oculto.")
                break

            self.human_delay(page, 1000, 2000)

    def _safe_goto(self, page: Page, url: str) -> bool:
        for attempt in range(3):
            try:
                self.logger.info(f"Navegando a {url} (Intento {attempt+1})...")
                # wait_until='load' es más conservador que domcontentloaded
                page.goto(url, timeout=45_000, wait_until="load")
                
                # Espera inicial para que disparen APIs de resultados
                page.wait_for_timeout(5000)
                
                # Cerrar filtros si están abiertos
                try:
                    page.wait_for_selector(".btn-close-clean", timeout=3000)
                    page.evaluate('() => { const b = document.querySelector(".btn-close-clean"); if(b) b.click(); }')
                    self.logger.info("Filtros laterales cerrados exitosamente.")
                    page.wait_for_timeout(2000)
                except:
                    pass
                
                return True
            except Exception as e:
                self.logger.warning(f"Intento {attempt + 1} falló: {e}")
                time.sleep(2)
        return False

    def _scroll_for_lazy_loading(self, page: Page):
        # Descenso escalonado para disparar hidratación (muy importante para Ciencuadras)
        for y in [400, 800, 1200]:
            page.evaluate(f"window.scrollTo(0, {y})")
            page.wait_for_timeout(1500)
        
        # Scroll extra para el resto del contenido
        for _ in range(5):
            page.evaluate("window.scrollBy(0, 800)")
            page.wait_for_timeout(800)
            
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(2000)

    def _get_cards(self, page: Page, current_page: int):
        # Asegurar scroll ANTES de buscar tarjetas para disparar lazy-loading
        self._scroll_for_lazy_loading(page)
        
        try:
            # Esperar a que el TÍTULO de una tarjeta esté presente (esto confirma que no es un skeleton)
            page.wait_for_selector("article.card h3, article.card .card__title", timeout=20_000, state="attached")
        except:
            if page.query_selector("div.no-results, :has-text('Pronto tendremos un inmueble así')"):
                self.logger.info("Mensaje de 'Sin resultados' detectado.")
            else:
                self.logger.info(f"No se detectó contenido real en las tarjetas de la pág {current_page} (posible timeout de skeletons).")
                # Guardar captura de pantalla para debug si no hay tarjetas
                page.screenshot(path=f"debug_no_cards_usado_p{current_page}.png")
            return None
        
        cards = page.query_selector_all("article.card")
        self.logger.info(f"Encontradas {len(cards)} tarjetas totales (hidratadas).")
        
        # Verificar si hay alguna orgánica (no destacada)
        organic_cards = [c for c in cards if not c.evaluate("el => el.classList.contains('detach')")]
        if not organic_cards:
            self.logger.info(f"Solo se encontraron tarjetas destacadas en pág {current_page}. Reintentando...")
            return None
            
        return cards

    def _is_last_page(self, page: Page) -> bool:
        next_btn = page.query_selector('[data-qa-id="cc-rs-rs_paginator_results_next"]')
        if next_btn:
            classes = next_btn.get_attribute("class") or ""
            return "hide" in classes
        return True

    def _process_cards(self, cards, previous_page_ids: set) -> bool:
        current_pure_ids = []
        for card in cards:
            is_featured = card.evaluate("el => el.classList.contains('detach')")
            qa_id = card.get_attribute("data-qa-id")
            
            if qa_id and not is_featured:
                current_pure_ids.append(qa_id)
        
        current_ids_set = set(current_pure_ids)
        
        if current_ids_set and previous_page_ids and current_ids_set.issubset(previous_page_ids):
            return True # Loop real
            
        previous_page_ids.update(current_ids_set)
        
        for card in cards:
            self._extract_property(card)
        return False

    def _extract_property(self, card) -> None:
        try:
            link_el = card.query_selector("a.card, a")
            href = link_el.get_attribute("href") if link_el else ""
            full_url = f"{self.base_url}{href}" if href.startswith("/") else href
            
            qa_id = card.get_attribute("data-qa-id") or ""
            qa_id = qa_id.replace("cc-rs-rs-card_property_", "").replace("cc-rs-rs-card_project_", "")
                
            property_id = f"CC-USADO-{qa_id}"

            is_featured = card.evaluate("el => el.classList.contains('detach')")

            price_el = card.query_selector(".card__price-big")
            price_raw = price_el.inner_text().strip() if price_el else "N/A"
            precio_num = self.parse_price(price_raw)

            title = card.query_selector("h3, .card__title").inner_text().strip() if card.query_selector("h3, .card__title") else "N/A"
            location = card.query_selector(".card__location-label, h4").inner_text().strip() if card.query_selector(".card__location-label, h4") else "N/A"

            specs_els = card.query_selector_all("ciencuadras-specs-results .specs p span")
            specs_texts = [s.inner_text().strip().lower() for s in specs_els]
            
            habitaciones, banos, area, garajes = "", "", "", ""
            for spec in specs_texts:
                if "m2" in spec or "m²" in spec: area = spec
                elif "hab" in spec: habitaciones = spec
                elif "bañ" in spec: banos = spec
                elif "parq" in spec or "gar" in spec: garajes = spec

            prop_data = {
                "id_inmueble": property_id,
                "titulo": title,
                "estado_inmueble": "Usado",
                "precio": price_raw,
                "precio_num": precio_num,
                "ubicacion": location,
                "habitaciones": habitaciones,
                "banos": banos,
                "area": area,
                "garajes": garajes,
                "url": full_url,
                "portal": "ciencuadras_usado",
                "is_featured": is_featured,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }
            self.process_and_upload(prop_data, property_id)
        except Exception as e:
            self.logger.error(f"Error parseando tarjeta Ciencuadras Usado: {e}")
