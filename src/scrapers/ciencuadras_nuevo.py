import re
import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class CiencuadrasNuevoScraper(BaseScraper):
    def __init__(self):
        super().__init__("ciencuadras_nuevo")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int) -> None:
        base_search_url = f"{self.base_url}/proyectos-vivienda-nueva/bogota/apartamento"
        self.logger.info("--- INICIANDO CIENCUADRAS NUEVO (PROYECTOS) ---")
        
        previous_page_ids = set()
        
        for current_page in range(1, max_pages + 1):
            url = f"{base_search_url}?page={current_page}"
            self.logger.info(f"CC (Nuevo) — Cargando Página {current_page}/{max_pages}: {url}")
            
            # Navegación directa por URL
            if not self._safe_goto(page, url):
                break

            self.human_delay(page, 2000, 4000)
            self._scroll_for_lazy_loading(page)

            cards = self._get_cards(page, current_page)
            if cards is None:
                # Reintento
                if not page.query_selector("div.no-results"):
                    self.logger.info("Reintentando carga por falta de tarjetas...")
                    page.reload(wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    cards = self._get_cards(page, current_page)
                
                if cards is None:
                    break

            # Procesar y detectar duplicados (ignorando destacados)
            should_stop_duplicates = self._process_cards(cards, previous_page_ids)
            
            # Verificar si es la última página por UI
            if self._is_last_page(page):
                self.logger.info("Fin por UI: Botón 'Siguiente' oculto.")
                break

            if should_stop_duplicates:
                self.logger.info("Fin por Duplicados (Puros): Loop detectado tras filtrar destacados.")
                break

            self.human_delay(page, 1000, 2000)

    def _safe_goto(self, page: Page, url: str) -> bool:
        for attempt in range(3):
            try:
                page.goto(url, timeout=45_000, wait_until="domcontentloaded")
                page.wait_for_timeout(2000) # Tiempo extra para hidratación
                return True
            except Exception as e:
                self.logger.warning(f"Intento {attempt + 1} falló: {e}")
                time.sleep(2)
        return False

    def _scroll_for_lazy_loading(self, page: Page):
        for _ in range(8):
            page.evaluate("window.scrollBy(0, 800)")
            page.wait_for_timeout(500)
        page.evaluate("window.scrollTo(0, 0)")

    def _get_cards(self, page: Page, current_page: int):
        try:
            # Esperar a que las tarjetas orgánicas (no solo las destacadas) estén presentes
            page.wait_for_selector("article.card:not(.detach)", timeout=15_000, state="attached")
        except:
            if page.query_selector("div.no-results, :has-text('Pronto tendremos un inmueble así')"):
                self.logger.info("Mensaje de 'Sin resultados' detectado.")
            else:
                self.logger.info(f"No se encontraron tarjetas orgánicas en pág {current_page}.")
            return None
        
        cards = page.query_selector_all("article.card")
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
            return True 
            
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
                
            property_id = f"CC-NUEVO-{qa_id}"

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
                "estado_inmueble": "Nuevo",
                "precio": price_raw,
                "precio_num": precio_num,
                "ubicacion": location,
                "habitaciones": habitaciones,
                "banos": banos,
                "area": area,
                "garajes": garajes,
                "url": full_url,
                "portal": "ciencuadras_nuevo",
                "is_featured": is_featured,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }
            self.process_and_upload(prop_data, property_id)
        except Exception as e:
            self.logger.error(f"Error parseando tarjeta Ciencuadras Nuevo: {e}")
