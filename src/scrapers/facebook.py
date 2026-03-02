from typing import Dict, Any
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG
import time
from datetime import datetime
import re

class FacebookScraper(BaseScraper):
    def __init__(self):
        super().__init__("facebook")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    def scrape_pages(self, page: Page, max_pages: int):
        # Parametrización de ciudad y coordenadas (se puede mover a YAML o BD)
        # La URL base proporcionada fue: https://web.facebook.com/marketplace/bogota/propertyrentals
        city = "bogota"
        lat = "4.625"
        lon = "-74.167"
        radius = "500"
        
        # Construye la URL variable
        url = f"{self.base_url}/{city}/propertyrentals?exact=false&latitude={lat}&longitude={lon}&radius={radius}&radius_in_km={radius}"
        
        self.logger.info(f"{self.portal_name.capitalize()} - Navegando a URL: {url}")
        
        try:
            page.goto(url, wait_until="networkidle", timeout=60000)
            # En Facebook suele salir un banner de cookies o login
            self.human_delay(page, 3000, 5000)
            
            try:
                # Intentar cerrar el popup de inicio de sesión si aparece (la X suele tener aria-label="Close" o "Cerrar")
                close_btn = page.query_selector("[aria-label='Cerrar']") or page.query_selector("[aria-label='Close']")
                if close_btn:
                    close_btn.click()
                    self.logger.info("Cerrado popup de login/cookies.")
            except Exception:
                pass
                
        except Exception as e:
            self.logger.error(f"Error navegando a {url}: {e}")
            return
            
        # Paginación en Facebook es por scroll infinito con virtualización (descarga items no visibles)
        # Extraemos incrementalmente antes de hacer scroll profundo
        processed_ids = set()
        
        for i in range(max_pages):
            self.logger.info(f"{self.portal_name.capitalize()} - Extrayendo items visibles, scroll {i+1}/{max_pages}...")
            
            # Esperar a que haya al menos un elemento
            try:
                page.wait_for_selector("a[href*='/item/']", timeout=5000)
            except:
                pass
                
            links = page.query_selector_all("a[href*='/item/']")
            for link in links:
                try:
                    href = link.get_attribute("href")
                    if not href:
                        continue
                        
                    match = re.search(r'/item/(\d+)', href)
                    if not match:
                        continue
                    
                    item_id = match.group(1)
                    if item_id in processed_ids:
                        continue
                        
                    processed_ids.add(item_id)
                    
                    # Llamar extract inmediatamente para capturar texto antes de que React desmonte el elemento
                    self._extract_property(page, link, item_id, href)
                    
                except Exception as eval_err:
                    pass
            
            page.keyboard.press("PageDown")
            page.keyboard.press("PageDown")
            self.human_delay(page, 2000, 4000)
            
        self.logger.info(f"Encontrados {len(processed_ids)} propiedades totales iteradas.")
        
        if len(processed_ids) == 0:
            with open("fb_debug.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            self.logger.info("Guardado fb_debug.html para inspeccionar DOM.")
                
    def _extract_property(self, page: Page, link_element, item_id: str, href: str):
        # Como las clases en Facebook ("x1lliihq x6s0dn4" etc) cambian diariamente,
        # La forma más robusta de extraer información es tomando TODO el texto
        # dentro del tag <a> (que envuelve la tarjeta entera) y usando heurísticas.
        
        text_content = link_element.inner_text()
        
        if not text_content:
            return
            
        # Separa los textos por line break
        lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        
        price = "N/A"
        title = "N/A"
        location = "N/A"
        
        # Heurísticas:
        # - La línea con "$" es el precio
        # - La línea con "Bogotá", "Cundinamarca", "Colombia" suele ser la ubicación
        # - El resto tiende a ser el título (e.g. "Apartamento 3 habs")
        for line in lines:
            # Detecta precios
            if "$" in line or "COP" in line:
                # Nos quedamos con la primera coincidencia de precio
                if price == "N/A":
                    price = line
            # Detecta ubicación (podemos relajar esto si se buscan otras ciudades)
            elif "Bogot" in line or "Cundinamarca" in line or "Colombia" in line:
                location = line
            else:
                # Toma el primer texto que no es precio ni ubicación como título
                if title == "N/A" and len(line) > 5:
                    title = line
                    
        # Formatear el URL absoluto
        full_url = href if href.startswith("http") else f"https://web.facebook.com{href}"
        
        prop_data = {
            "id_inmueble": f"FB-{item_id}",
            "title": title,
            "price": price,
            "location": location,
            "source": self.portal_name,
            "url": full_url,
            "raw_facebook_text": " | ".join(lines), # Útil para debugging si cambian la estructura
            "extracted_at": datetime.utcnow().isoformat()
        }
        
        self.process_and_upload(prop_data, prop_data["id_inmueble"])
