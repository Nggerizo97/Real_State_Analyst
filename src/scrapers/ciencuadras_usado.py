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
        base_search_url = f"{self.base_url}/venta/bogota/apartamento"
        self.logger.info("--- INICIANDO CIENCUADRAS USADO (VENTA) ---")
        previous_page_ids = set()
        
        for current_page in range(1, max_pages + 1):
            url = f"{base_search_url}?page={current_page}"
            self.logger.info(f"CC (Usado) — Página {current_page}/{max_pages}: {url}")
            
            if not self._safe_goto(page, url):
                break

            self.human_delay(page, 2000, 4000)
            self._scroll_for_lazy_loading(page)

            cards = self._get_cards(page, current_page)
            if cards is None:
                break

            # Terminar si el botón 'Siguiente' está oculto
            if self._is_last_page(page):
                self.logger.info("Fin por UI: Botón 'Siguiente' oculto.")
                self._process_cards(cards, previous_page_ids)
                break

            # Terminar si hay loop de duplicados
            if self._process_cards(cards, previous_page_ids):
                self.logger.info("Fin por Duplicados: Loop detectado.")
                break

            self.human_delay(page, 1000, 2000)

    def _safe_goto(self, page: Page, url: str) -> bool:
        for attempt in range(3):
            try:
                page.goto(url, timeout=45_000, wait_until="domcontentloaded")
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
            page.wait_for_selector("ciencuadras-card, article.card.result", timeout=12_000, state="attached")
        except:
            if page.query_selector("div.no-results, :has-text('Pronto tendremos un inmueble así')"):
                self.logger.info("Mensaje de 'Sin resultados' detectado.")
            else:
                self.logger.info(f"No se encontraron tarjetas en pág {current_page}.")
            return None
        cards = page.query_selector_all("ciencuadras-card")
        if not cards:
            cards = page.query_selector_all("article.card")
        return cards

    def _is_last_page(self, page: Page) -> bool:
        next_btn = page.query_selector('li[data-qa-id="cc-rs-rs_paginator_results_next"]')
        if next_btn:
            classes = next_btn.get_attribute("class") or ""
            return "hide" in classes
        return False

    def _process_cards(self, cards, previous_page_ids: set) -> bool:
        current_ids = []
        for card in cards:
            article_el = card.query_selector("article.card") or card
            qa_id = article_el.get_attribute("data-qa-id")
            if qa_id:
                current_ids.append(qa_id)
        
        current_ids_set = set(current_ids)
        if current_ids_set and previous_page_ids and current_ids_set.issubset(previous_page_ids):
            return True
        previous_page_ids.update(current_ids_set)
        for card in cards:
            self._extract_property(card)
        return False

    def _extract_property(self, card) -> None:
        try:
            link_el = card.query_selector("a.style-none, a.card")
            if not link_el:
                link_el = card.evaluate_handle("el => el.closest('a')") 
            
            href = ""
            if link_el:
                href = link_el.get_attribute("href") or ""
            full_url = f"{self.base_url}{href}" if href.startswith("/") else href
            
            article_el = card.query_selector("article.card") or card
            qa_id = article_el.get_attribute("data-qa-id")
            if qa_id:
                qa_id = qa_id.replace("cc-rs-rs-card_property_", "").replace("cc-rs-rs-card_project_", "")
            else:
                match = re.search(r"/(\d+)$", href)
                qa_id = match.group(1) if match else str(int(time.time() * 1000))
                
            property_id = f"CC-USADO-{qa_id}"

            price_el = card.query_selector(".card__price-big")
            price_raw = price_el.inner_text().strip() if price_el else "N/A"
            if price_raw == "N/A" or price_raw == "":
                desde_el = card.query_selector(".card__price--from, .card__desde, span:has-text('Desde')")
                if desde_el:
                    price_raw = f"Desde {desde_el.inner_text().strip()}"
            precio_num = self.parse_price(price_raw)

            h3_el = card.query_selector("h3, div.card__location h3")
            title = h3_el.inner_text().strip() if h3_el else "N/A"
            
            loc_el = card.query_selector("h4.card__location-label, .card__location-label")
            location = loc_el.inner_text().strip() if loc_el else "N/A"

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
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }
            self.process_and_upload(prop_data, property_id)
        except Exception as e:
            self.logger.error(f"Error parseando tarjeta Ciencuadras Usado: {e}")
