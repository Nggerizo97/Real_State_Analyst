import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class MetrocuadradoScraper(BaseScraper):
    def __init__(self):
        super().__init__("metrocuadrado")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        current_page = 1
        url = f"{self.base_url}/venta/apartamentos/bogota"
        
        while current_page <= max_pages:
            self.logger.info(f"Metrocuadrado - Navegando a página {current_page}: {url}")
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                page.wait_for_timeout(4000) # Wait for JS to render
            except Exception as e:
                self.logger.error(f"Error en Metrocuadrado: {e}")
                break
            
            items = page.query_selector_all("div.property-card__container") 
            self.logger.info(f"Encontrados {len(items)} inmuebles (Metrocuadrado).")
            
            if not items:
                self.logger.info("No se hallaron inmuebles. Finalizando paginación.")
                break
                
            for item in items:
                self._extract_property(item, self.base_url)
                
            # Buscar botón de siguiente en la paginación
            # El HTML es dinámico, suele usar enlaces o botones de next.
            next_button = page.query_selector("li.next a, button[aria-label='Siguiente'], a.page-link-next")
            if next_button and (next_button.is_visible() or next_button.is_enabled()):
                url = next_button.get_attribute("href")
                if not url.startswith("http"):
                    url = f"{self.base_url}{url}"
                current_page += 1
            else:
                self.logger.info("Botón Siguiente no encontrado. Deteniendo.")
                break
            
    def _extract_property(self, element, base_url):
        try:
            link = element.query_selector("a")
            if not link:
                return

            href = link.get_attribute("href")
            # Extraer el ID de la URL que tiene formato: /inmueble/.../[id]?src_url=...
            # Ejemplo: /inmueble/venta-casa-ibague-el-vergel-3-habitaciones-3-banos-2-garajes/21223-M6382795?src_url...
            id_attr = None
            if href:
                path_part = href.split("?")[0]
                id_attr = path_part.split("/")[-1]
                
            if not id_attr:
                id_attr = str(int(time.time() * 1000))
                
            title_el = element.query_selector("div.property-card__detail-title h2")
            price_el = element.query_selector("div.property-card__detail-price")
            location_el = element.query_selector("div.property-card__detail-top__left div")
            
            full_url = f"{base_url}{href}" if href and href.startswith("/") else href
            
            prop_data = {
                "id_inmueble": f"M2-{id_attr}",
                "title": title_el.inner_text().strip() if title_el else "N/A",
                "price": price_el.inner_text().split("\n")[0].strip() if price_el else "N/A",
                "location": location_el.inner_text().strip() if location_el else "N/A",
                "source": self.portal_name,
                "url": full_url,
                "extracted_at": datetime.utcnow().isoformat()
            }
            self.process_and_upload(prop_data, f"M2-{id_attr}")
        except Exception as e:
            self.logger.error(f"Error procesando item M2: {e}")
