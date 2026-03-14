import re
import time
from datetime import datetime
from playwright.sync_api import Page
from src.scrapers.base_scraper import BaseScraper
from config.settings import PORTALS_CONFIG

class CiencuadrasScraper(BaseScraper):
    def __init__(self):
        super().__init__("ciencuadras")
        self.base_url = PORTALS_CONFIG[self.portal_name]["base_url"]
        
    # ------------------------------------------------------------------
    # Navegación y paginación
    # ------------------------------------------------------------------

    def scrape_pages(self, page: Page, max_pages: int) -> None:
        # Rutas descubiertas correspondientes a "Nuevo" y "Usado"
        search_urls = [
            ("Nuevo", f"{self.base_url}/proyectos-vivienda-nueva/bogota/apartamento"),
            ("Usado", f"{self.base_url}/venta/bogota/apartamento")
        ]

        for estado, base_search_url in search_urls:
            self.logger.info(f"--- Iniciando extracción para etiqueta CIENCUADRAS: {estado} ---")
            
            previous_page_ids = set()
            for current_page in range(1, max_pages + 1):
                url = f"{base_search_url}?page={current_page}"
                
                self.logger.info(f"CC ({estado}) — Página {current_page}/{max_pages}: {url[:100]}")
                
                # Intento de carga con reintentos
                for attempt in range(3):
                    try:
                        page.goto(url, timeout=45_000, wait_until="domcontentloaded")
                        break
                    except Exception as e:
                        self.logger.warning(f"Intento {attempt + 1} falló navegando a p {current_page}: {e}")
                        time.sleep(2)
                else:
                    self.logger.error(f"No se pudo cargar la página {url}. Saltando a siguiente estado.")
                    break

                self.human_delay(page, 2000, 4000)

                # Scroll para cargar lazy images y asegurar renderizado
                for _ in range(8):
                    page.evaluate("window.scrollBy(0, 800)")
                    page.wait_for_timeout(500)
                page.evaluate("window.scrollTo(0, 0)")
                
                # Esperamos las tarjetas de ciencuadras (proyecto o result normal)
                try:
                    page.wait_for_selector("ciencuadras-card, article.card.result", timeout=12_000, state="attached")
                except:
                    # Si no hay tarjetas, revisamos si es por mensaje de "Sin resultados"
                    if page.query_selector("div.no-results, :has-text('Pronto tendremos un inmueble así')"):
                        self.logger.info(f"Mensaje de 'Sin resultados' detectado en {estado}. Fin.")
                    else:
                        self.logger.info(f"No se encontraron tarjetas en {estado} pág {current_page}. Fin de paginación.")
                    break

                # Capturamos todos los contenedores relevantes
                cards = page.query_selector_all("ciencuadras-card")
                if not cards:
                    cards = page.query_selector_all("article.card")
                    
                if not cards:
                    self.logger.info(f"Lista de tarjetas vacía. Fin de paginación para {estado}.")
                    break

                # --- DETECCIÓN DE FIN DE RESULTADOS (LOOP A PÁGINA 1) ---
                # Ciencuadras a veces vuelve a la página 1 silenciosamente si el índice es muy alto.
                current_ids = []
                for card in cards:
                    # Obtenemos el ID de forma similar a _extract_property para pre-validar
                    article_el = card.query_selector("article.card") or card
                    qa_id = article_el.get_attribute("data-qa-id")
                    if qa_id:
                        current_ids.append(qa_id)
                
                current_ids_set = set(current_ids)
                if current_page > 1 and current_ids_set and current_ids_set.issubset(previous_page_ids):
                    self.logger.info("Detección de fin de resultados (Duplicate/Loop): Finalizando.")
                    break
                
                previous_page_ids = current_ids_set
                # -------------------------------------------------------

                self.logger.info(f"Encontradas {len(cards)} tarjetas en {estado} página {current_page}")

                for card in cards:
                    self._extract_property(card, estado)

    # ------------------------------------------------------------------
    # Extracción de datos por tarjeta
    # ------------------------------------------------------------------

    def _extract_property(self, card, estado: str) -> None:
        try:
            # 1. Extraer Link y URL
            link_el = card.query_selector("a.style-none, a.card")
            if not link_el:
                link_el = card.evaluate_handle("el => el.closest('a')") 
            
            href = ""
            if link_el:
                href = link_el.get_attribute("href") or ""
                
            full_url = f"{self.base_url}{href}" if href.startswith("/") else href
            
            # 2. Extraer ID
            article_el = card.query_selector("article.card") or card
            qa_id = article_el.get_attribute("data-qa-id")
            if qa_id:
                qa_id = qa_id.replace("cc-rs-rs-card_property_", "").replace("cc-rs-rs-card_project_", "")
            else:
                match = re.search(r"/(\d+)$", href)
                qa_id = match.group(1) if match else str(int(time.time() * 1000))
                
            property_id = f"CC-{qa_id}"

            # 3. Extraer Precios
            price_el = card.query_selector(".card__price-big")
            price_raw = price_el.inner_text().strip() if price_el else "N/A"
            if price_raw == "N/A" or price_raw == "":
                # Fallback al desde
                desde_el = card.query_selector(".card__price--from, .card__desde, span:has-text('Desde')")
                if desde_el:
                    price_raw = f"Desde {desde_el.inner_text().strip()}"
                    
            precio_num = self.parse_price(price_raw)

            # 4. Extraer Ubicación y Título
            h3_el = card.query_selector("h3, div.card__location h3")
            title = h3_el.inner_text().strip() if h3_el else "N/A"
            
            loc_el = card.query_selector("h4.card__location-label, .card__location-label")
            location = loc_el.inner_text().strip() if loc_el else "N/A"

            # 5. Extraer Specs
            specs_els = card.query_selector_all("ciencuadras-specs-results .specs p span")
            specs_texts = [s.inner_text().strip().lower() for s in specs_els]
            
            habitaciones, banos, area, garajes = "", "", "", ""
            for spec in specs_texts:
                if "m2" in spec or "m²" in spec:
                    area = spec
                elif "hab" in spec:
                    habitaciones = spec
                elif "bañ" in spec:
                    banos = spec
                elif "parq" in spec or "gar" in spec:
                    garajes = spec

            prop_data = {
                "id_inmueble": property_id,
                "titulo": title,
                "estado_inmueble": estado,
                "precio": price_raw,
                "precio_num": precio_num,
                "ubicacion": location,
                "habitaciones": habitaciones,
                "banos": banos,
                "area": area,
                "garajes": garajes,
                "url": full_url,
                "portal": self.portal_name,
                "fecha_extraccion": datetime.now().isoformat(timespec="seconds"),
            }

            self.process_and_upload(prop_data, property_id)

        except Exception as e:
            self.logger.error(f"Error parseando tarjeta Ciencuadras: {e}")
