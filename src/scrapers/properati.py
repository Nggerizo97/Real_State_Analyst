import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class ProperatiScraper(BaseScraper):
    def __init__(self):
        super().__init__("properati")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        current_page = 1
        # Properati URL Estructura
        url = f"{self.base_url}/s/bogota/apartamento/venta"
        
        while current_page <= max_pages:
            self.logger.info(f"Properati - Navegando a página {current_page}: {url}")
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                page.wait_for_timeout(4000) 
            except Exception as e:
                self.logger.error(f"Error: {e}")
                break
            
            items = page.query_selector_all("article.snippet")
            self.logger.info(f"Encontrados {len(items)} inmuebles (Properati).")
            
            if not items:
                self.logger.info("No se encontraron más inmuebles, finalizando.")
                break
                
            for item in items:
                # get url from element
                item_url_attr = item.get_attribute("data-url")
                item_url = item_url_attr if item_url_attr else page.url
                self._extract_property(item, item_url)
                
            current_page += 1
            url = f"{self.base_url}/s/bogota/apartamento/venta?page={current_page}"
            
    def _extract_property(self, element, source_url):
        try:
            id_attr = element.get_attribute("data-idanuncio") or str(int(time.time() * 1000))
            
            title_el = element.query_selector(".title")
            price_el = element.query_selector(".price")
            location_el = element.query_selector(".location")
            area_el = element.query_selector(".properties__area")
            
            prop_data = {
                "id_inmueble": f"PR-{id_attr}",
                "title": title_el.inner_text().strip() if title_el else "N/A",
                "price": price_el.inner_text().strip() if price_el else "N/A",
                "location": location_el.inner_text().strip() if location_el else "N/A",
                "area": area_el.inner_text().strip() if area_el else "N/A",
                "source": self.portal_name,
                "url": source_url,
                "extracted_at": datetime.utcnow().isoformat()
            }
            self.process_and_upload(prop_data, f"PR-{id_attr}")
        except Exception as e:
            self.logger.error(f"Error procesando item Properati: {e}")
