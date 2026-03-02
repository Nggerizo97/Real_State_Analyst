import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class FincaRaizScraper(BaseScraper):
    def __init__(self):
        super().__init__("fincaraiz")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        current_page = 1
        # La URL inicial (Ajustar según zona geográfica)
        url = f"{self.base_url}/venta/apartamentos/bogota"
        
        while current_page <= max_pages:
            self.logger.info(f"FR - Navegando a página {current_page}: {url}")
            
            # Lógica de reintento para evitar fallas efímeras de red
            for attempt in range(3):
                try:
                    page.goto(url, timeout=45000, wait_until="domcontentloaded")
                    break
                except Exception as e:
                    self.logger.warning(f"Intento {attempt+1} falló al cargar {url}: {e}")
                    time.sleep(2)
            
            # Esperamos que los elementos dinámicos carguen
            page.wait_for_timeout(3000) 
            
            # Buscamos los contenedores de los inmuebles
            articles = page.query_selector_all("div.listingBoxCard")
            self.logger.info(f"Encontrados {len(articles)} inmuebles en página {current_page}")
            
            if not articles:
                self.logger.info("No se hallaron inmuebles. Posible fin de paginación o bloqueo por anti-bot.")
                break
                
            for article in articles:
                self._extract_property(article, self.base_url)
                
            # Lógica dinámica de Paginación. Se busca botón de 'siguiente'
            # (El selector debe ajustarse si el DOM de la página cambia)
            next_button = page.query_selector("button:has-text('Siguiente'), a[aria-label='Next']")

            
            if next_button and next_button.is_visible() and next_button.is_enabled():
                current_page += 1
                # Simula agregar un parámetro de paginación o dar click (en este caso param)
                url = f"{self.base_url}/venta/apartamentos/bogota?pagina={current_page}"
            else:
                self.logger.info("Botón de 'Siguiente' no encontrado o deshabilitado. Deteniendo iteración.")
                break

    def _extract_property(self, element, base_url):
        try:
            id_attr = None
            href = None
            
            # Estrategia de fallback sacando el ID desde la URL
            link = element.query_selector("a.lc-data")
            if link:
                href = link.get_attribute("href")
                if href:
                    id_attr = href.split("/")[-1]
            
            if not id_attr:
                return
                
            # Extracción del resto de metadata
            price_el = element.query_selector("p.main-price")
            title_el = element.query_selector("h2.lc-title")
            location_el = element.query_selector("strong.lc-location")
            
            full_url = f"{base_url}{href}" if href and href.startswith("/") else href
            
            prop_data = {
                "id_inmueble": f"FR-{id_attr}",
                "title": title_el.inner_text().strip() if title_el else "N/A",
                "price": price_el.inner_text().strip() if price_el else "N/A",
                "location": location_el.inner_text().strip() if location_el else "N/A",
                "source": self.portal_name,
                "url": full_url,
                "extracted_at": datetime.utcnow().isoformat()
            }
            
            # Llama al método Base que se encarga de subir al S3 (deduplicando)
            self.process_and_upload(prop_data, f"FR-{id_attr}")
        except Exception as e:
            self.logger.error(f"Error parseando el inmueble: {e}")
