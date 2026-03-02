import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class CiencuadrasScraper(BaseScraper):
    def __init__(self):
        super().__init__("ciencuadras")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        current_page = 1
        url = f"{self.base_url}/venta/apartamentos/bogota"
        
        while current_page <= max_pages:
            self.logger.info(f"Ciencuadras - Navegando a página {current_page}: {url}")
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                page.wait_for_timeout(4000) 
            except Exception as e:
                self.logger.error(f"Error: {e}")
                break
            
            items = page.query_selector_all("article.card.result")
            self.logger.info(f"Encontrados {len(items)} inmuebles (Ciencuadras).")
            
            if not items:
                self.logger.info("No se hallaron inmuebles. Finalizando paginación.")
                break
                
            for item in items:
                # Find the wrapper link first
                link = item.evaluate_handle("el => el.closest('a')")
                source_url = self.base_url
                if link:
                    try:
                        href = link.get_property("href")
                        if href:
                            source_url = str(href)
                    except Exception as e:
                        self.logger.warning(f"No href property on closest 'a' tag: {e}")
                self._extract_property(item, source_url)
                
            # Lógica de Paginación.
            # Se busca el botón Next
            next_button = page.query_selector("button.next-btn.carousel-arrow, li.pagination-next a")
            
            if next_button and (next_button.is_visible() or next_button.is_enabled()):
                current_page += 1
                url = f"{self.base_url}/venta/apartamentos/bogota?page={current_page}"
            else:
                self.logger.info("Botón Siguiente no encontrado. Deteniendo.")
                break
            
    def _extract_property(self, element, source_url):
        try:
            id_attr = element.get_attribute("data-qa-id")
            if id_attr:
                id_attr = id_attr.replace("cc-rs-rs-card_property_", "")
            else:
                id_attr = str(int(time.time() * 1000))
                
            title_el = element.query_selector("h3.ng-star-inserted")
            price_el = element.query_selector(".card__price-big")
            location_el = element.query_selector(".card__location-label")
            
            prop_data = {
                "id_inmueble": f"CC-{id_attr}",
                "title": title_el.inner_text().strip() if title_el else "N/A",
                "price": price_el.inner_text().strip() if price_el else "N/A",
                "location": location_el.inner_text().strip() if location_el else "N/A",
                "source": self.portal_name,
                "url": source_url,
                "extracted_at": datetime.utcnow().isoformat()
            }
            self.process_and_upload(prop_data, f"CC-{id_attr}")
        except Exception as e:
            self.logger.error(f"Error procesando item Ciencuadras: {e}")
