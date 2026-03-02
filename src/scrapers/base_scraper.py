from abc import ABC, abstractmethod
from typing import Dict, Any
from playwright.sync_api import sync_playwright, Page, BrowserContext
from fake_useragent import UserAgent
import random
import time
import hashlib
import pandas as pd
from datetime import datetime
from src.utils.s3_connector import S3Connector
from src.utils.logger import get_logger
from config.settings import S3_BRONZE_PREFIX

class BaseScraper(ABC):
    """
    Clase Base Abstracta para todos los scrapers de Real Estate.
    Define el patrón Factory/Strategy para estandarizar la extracción.
    """
    
    def __init__(self, portal_name: str):
        self.portal_name = portal_name
        self.s3 = S3Connector()
        self.logger = get_logger(self.__class__.__name__)
        # Construye la ruta ej. raw/fincaraiz/
        self.prefix = f"{S3_BRONZE_PREFIX}/{self.portal_name}"
        
        # --- NUEVO: Infraestructura DataOps "Parquet Batching" ---
        self.scraped_data = []  # Memoria temporal para acumular inmuebles
        self.hash_index_key = f"{self.prefix}/_hash_index.txt"
        self.historical_hashes = set()

    def run(self, max_pages: int = 5):
        """Ciclo de vida principal del scraper (Playwright y Manejador de Errores)."""
        self.logger.info(f"Iniciando ingesta Serverless para {self.portal_name}")
        
        # 1. Cargar la "memoria" del bot (Zero Cost Optimization real)
        self.logger.info(f"Descargando memoria de hashes previa de S3...")
        self.historical_hashes = self.s3.download_hash_index(self.hash_index_key)
        self.logger.info(f"Cargados {len(self.historical_hashes)} hashes históricos.")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            
            # 2. Generar User-Agent dinámico aleatorio (Solo Desktop Chrome/Edge para estandarizar DOM)
            modern_uas = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36'
            ]
            user_agent = random.choice(modern_uas)
            
            # 3. Viewport dinámico (distintas resoluciones)
            width = random.choice([1280, 1366, 1440, 1920])
            height = random.choice([800, 768, 900, 1080])
            
            context = browser.new_context(
                viewport={'width': width, 'height': height},
                user_agent=user_agent
            )
            
            # 1. Aplicar variables Stealth inyectando JS manual
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            
            page = context.new_page()
            
            try:
                # Se delega la lógica específica de navegación y extracción a las clases hijas
                self.scrape_pages(page, max_pages)
            except Exception as e:
                self.logger.error(f"Error crítico durante el Scraping de {self.portal_name}: {e}")
            finally:
                context.close()
                browser.close()
                
                # --- NUEVO: Fase de Batch Upload (Data Lake Optimization) ---
                if self.scraped_data:
                    self.logger.info(f"Preparando conversión a Parquet de {len(self.scraped_data)} nuevos inmuebles/actualizaciones...")
                    df = pd.DataFrame(self.scraped_data)
                    
                    # Generar nombre del archivo Parquet con timestamp
                    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    parquet_key = f"{self.prefix}/batches/{self.portal_name}_{timestamp}.parquet"
                    
                    if self.s3.upload_parquet(parquet_key, df):
                        self.logger.info(f"Subida masiva exitosa a S3: {parquet_key}")
                        # Solo actualizamos el índice maestro si el Parquet subió correctamente
                        self.logger.info("Actualizando índice de Hashes Maestro...")
                        self.s3.upload_hash_index(self.hash_index_key, self.historical_hashes)
                    else:
                        self.logger.error(f"Fallo al subir el batch Parquet a S3. Los hashes no se actualizarán.")
                else:
                    self.logger.info(f"No hay inmuebles nuevos o cambios de precio para subir.")
                    
                self.logger.info(f"Ingesta finalizada para {self.portal_name}")

    @abstractmethod
    def scrape_pages(self, page: Page, max_pages: int):
        """
        Método que las clases hijas deben implementar obligatoriamente.
        Debe encapsular la iteración sobre páginas (ej. if next_button click() o if page_num < max_pages).
        """
        pass

    def process_and_upload(self, property_data: Dict[str, Any], property_id: str) -> bool:
        """
        Maneja la lógica de validación S3 en Memoria (Deduplicación) y acumulación "Zero Cost".
        Implementa SCD Type 2: Si el precio cambia, generará un Hash diferente.
        """
        precio = property_data.get("precio_num", 0)
        unique_string = f"{property_id}_{precio}"
        price_hash = hashlib.md5(unique_string.encode('utf-8')).hexdigest()[:10]
        id_hash = f"{property_id}_{price_hash}"
        
        # --- ZERO COST MECHANISM: Validar contra memoria RAM en vez de llamadas web S3 ---
        if id_hash in self.historical_hashes:
            self.logger.info(f"Inmueble {property_id} (Precio sin cambios) ignorado (SCD Type 2).")
            return False
            
        # Si es nuevo, lo agregamos a nuestra memoria local temporal y al log global de hashes
        self.historical_hashes.add(id_hash)
        self.scraped_data.append(property_data)
        self.logger.info(f"Nuevo/Actualizado: {property_id} agregado a la cola de procesamiento (Batch).")
        
        return True
        
    def human_delay(self, page: Page = None, min_ms: int = 2000, max_ms: int = 5000):
        """
        Simula comportamiento humano con tiempos de espera aleatorios (Jitter) y scroll.
        """
        delay = random.randint(min_ms, max_ms)
        self.logger.debug(f"Esperando {delay}ms para simular comportamiento humano...")
        
        if page:
            # Hacer un pequeño scroll aleatorio mientras espera
            try:
                page.mouse.wheel(0, random.randint(100, 500))
                page.wait_for_timeout(delay)
            except Exception:
                time.sleep(delay / 1000.0)
        else:
            time.sleep(delay / 1000.0)
