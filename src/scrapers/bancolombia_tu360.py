import time
import json
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class BancolombiaTu360Scraper(BaseScraper):
    def __init__(self):
        super().__init__("bancolombia_tu360")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        current_page = 1
        url = self.base_url
        
        while current_page <= max_pages:
            self.logger.info(f"Bancolombia - Navegando a página {current_page}: {url}")
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                # Bancolombia / VTEX loads data dynamically
                page.wait_for_timeout(5000) 
            except Exception as e:
                self.logger.error(f"Error cargando Bancolombia: {e}")
                break
            
            # Estrategia 1: Extraer JSON-LD de Schema.org que VTEX inyecta
            script_tags = page.locator("script[type='application/ld+json']").all_inner_texts()
            extracted_items = 0
            
            for content in script_tags:
                try:
                    data = json.loads(content)
                    if data.get("@type") == "ItemList":
                        items = data.get("itemListElement", [])
                        for el in items:
                            product = el.get("item", {})
                            if product:
                                self._extract_from_json(product)
                                extracted_items += 1
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    self.logger.error(f"Error extrayendo desde JSON-LD en Bancolombia: {e}")
                    
            self.logger.info(f"Encontrados {extracted_items} inmuebles vía JSON-LD (Bancolombia).")
            
            # TODO: Localizar el botón de Siguiente y avanzar hasta max_pages si no usa Scroll
            # next_btn = page.query_selector("a[rel='next']")
            # Si se encuentra, url = next_btn.get_attribute('href')
            
            current_page += 1
            # Rompe por ahora ya que la URL de paginación o el click requiere un manejo específico
            break
            
    def _extract_from_json(self, product: dict):
        try:
            # VTEX Structure: "sku" or "mpn" as ID
            id_attr = product.get("sku") or product.get("mpn") or str(int(time.time() * 1000))
            name = product.get("name", "N/A")
            url = product.get("@id", "N/A")
            
            # Extraer precio de las Offers
            price = "N/A"
            offers = product.get("offers", {})
            if "lowPrice" in offers:
                price = offers.get("lowPrice")
            elif "offers" in offers and len(offers["offers"]) > 0:
                price = offers["offers"][0].get("price", "N/A")
                
            prop_data = {
                "id_inmueble": f"BC-{id_attr}",
                "title": name,
                "price": str(price),
                "source": self.portal_name,
                "url": url,
                "extracted_at": datetime.utcnow().isoformat()
            }
            self.process_and_upload(prop_data, f"BC-{id_attr}")
        except Exception as e:
            self.logger.error(f"Error procesando item Bancolombia: {e}")

    def _extract_property(self, element, source_url):
        # Fallback manual en caso de que JSON-LD falle
        pass
