import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class MercadoLibreScraper(BaseScraper):
    def __init__(self):
        super().__init__("mercadolibre")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        # Usa la URL base configurada o con filtros iniciales
        url = f"{self.base_url}/inmuebles/casas/_NoIndex_True"
        
        for current_page in range(1, max_pages + 1):
            self.logger.info(f"{self.portal_name.capitalize()} - Navegando a página {current_page}: {url}")
            try:
                # Wait until network is mostly idle to ensure dynamic React content loads
                page.goto(url, wait_until="networkidle", timeout=30000)
            except Exception as e:
                self.logger.error(f"Error navegando a la página {current_page}: {e}")
                break
                
            self.human_delay(page) # Simula humano explorando el DOM
            
            # Mercadolibre cambió su DOM a .andes-card o .ui-search-layout__item dependiendo del A/B testing
            # Usar un selector mixto y darle más tiempo de vida
            try:
                page.wait_for_selector(".ui-search-layout__item", timeout=15000)
                items = page.query_selector_all(".ui-search-layout__item")
            except Exception:
                self.logger.warning("Timeout esperando .ui-search-layout__item. Intentando dom alternativo.")
                try:
                    page.wait_for_selector(".andes-card", timeout=10000)
                    items = page.query_selector_all(".andes-card")
                except Exception as e:
                    self.logger.error(f"Error crítico en DOM de ML. Página posiblemente bloqueada por Captcha. {e}")
                    break
                    
            self.logger.info(f"Encontrados {len(items)} inmuebles en la página {current_page}")
            
            if not items:
                self.logger.info("Lista vacía. Deteniendo scraper para MercadoLibre.")
                break
                
            for item in items:
                self._extract_property(item, page.url)
                
            self.human_delay(page, 1500, 3000) # Pausa entre páginas
            
            # Buscar el botón "Siguiente" usando el selector de MercadoLibre
            next_button = page.query_selector("li.andes-pagination__button--next a")
            if next_button:
                url = next_button.get_attribute("href")
            else:
                self.logger.info("No hay más páginas.")
                break

    def _extract_property(self, element, source_url):
        try:
            # En base a la estructura: poly-component__title tiene el enlace y titulo
            link_el = element.query_selector(".poly-component__title")
            title = link_el.inner_text().strip() if link_el else "N/A"
            href = link_el.get_attribute("href") if link_el else ""
            
            if href:
                # Extraer MCO-123456... de la URL
                import re
                match = re.search(r'(MCO-\d+)', href)
                id_attr = match.group(1) if match else str(int(time.time() * 1000))
            else:
                id_attr = str(int(time.time() * 1000))

            price_el = element.query_selector(".andes-money-amount__fraction")
            location_el = element.query_selector(".poly-component__location")
            
            # Extraer specs de la lista (ej. 4 habs | 3 baños)
            specs = element.query_selector_all(".poly-attributes_list__item")
            area = specs[-1].inner_text().strip() if specs and len(specs) > 0 else "N/A"

            prop_data = {
                "id_inmueble": id_attr,
                "title": title,
                "price": price_el.inner_text().strip() if price_el else "N/A",
                "location": location_el.inner_text().strip() if location_el else "N/A",
                "area": area,
                "source": self.portal_name,
                "url": href if href else source_url,
                "extracted_at": datetime.utcnow().isoformat()
            }
            
            self.process_and_upload(prop_data, id_attr)
        except Exception as e:
            self.logger.error(f"Error extrayendo property de MercadoLibre: {e}")
