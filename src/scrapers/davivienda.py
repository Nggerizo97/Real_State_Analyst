import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class DaviviendaScraper(BaseScraper):
    def __init__(self):
        super().__init__("davivienda")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        current_page = 1
        url = f"{self.base_url}/bienes" # Ajustar a ruta real
        
        while current_page <= max_pages:
            self.logger.info(f"Davivienda - Navegando a página {current_page}: {url}")
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                page.wait_for_timeout(4000) 
            except Exception as e:
                self.logger.error(f"Error: {e}")
                break
            
            # TODO: Ajustar selector CSS 
            items = page.query_selector_all(".bienes-item")
            self.logger.info(f"Encontrados {len(items)} inmuebles (Davivienda).")
            
            for item in items:
                self._extract_property(item, page.url)
                
            current_page += 1
            break
            
    def _extract_property(self, element, source_url):
        try:
            id_attr = element.get_attribute("data-id") or str(int(time.time() * 1000))
            prop_data = {
                "id_inmueble": f"DV-{id_attr}",
                "title": "N/A",
                "price": "N/A",
                "source": self.portal_name,
                "url": source_url,
                "extracted_at": datetime.utcnow().isoformat()
            }
            self.process_and_upload(prop_data, f"DV-{id_attr}")
        except Exception as e:
            self.logger.error(f"Error procesando item: {e}")
