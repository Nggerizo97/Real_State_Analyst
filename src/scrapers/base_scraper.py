"""
src/scrapers/base_scraper.py
============================
Clase Base Abstracta para todos los scrapers de Real Estate.
Define el patrón Factory/Strategy para estandarizar la extracción.

Híbrido Resiliente:
  - Guarda en CSV local (data/batches/) cada 10 páginas.
  - Sube batch Parquet final a S3 al terminar.
  - Sincroniza hashes SCD Type 2 con S3.
"""

import csv
import hashlib
import os
import random
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set

import pandas as pd
from playwright.sync_api import Page, sync_playwright

from config.settings import S3_BRONZE_PREFIX
from src.utils.logger import get_logger
from src.utils.s3_connector import S3Connector

# ---------------------------------------------------------------------------
# Configuración de Resiliencia Local
# ---------------------------------------------------------------------------
ROOT_DATA_DIR = Path("data")
BATCHES_DIR = ROOT_DATA_DIR / "batches"
LOCAL_HASHES_DIR = ROOT_DATA_DIR / "hashes"

FLUSH_EVERY = 10  # Páginas entre flushes a disco local

# Campos del CSV — orden fijo para compatibilidad
CSV_FIELDS = [
    "id_inmueble", "titulo", "tipo_inmueble",
    "precio", "precio_num", "ubicacion",
    "habitaciones", "banos", "area", "garajes",
    "url", "portal", "fecha_extraccion",
]

# ---------------------------------------------------------------------------
# User-Agents (randomizados para mayor sigilo)
# ---------------------------------------------------------------------------
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Stealth JS (inyectado para evadir DataDome/PerimeterX)
# ---------------------------------------------------------------------------
_STEALTH_SCRIPT = """
(() => {
    // 1. Eliminar rastro de Headless
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    
    // 2. Mockear Plugins y MimeTypes
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'mimeTypes', { get: () => [1, 2, 3, 4] });
    
    // 3. Mockear window.chrome
    window.chrome = {
        runtime: {},
        app: { InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' }, isInstalled: false, getDetails: function() {}, getIsInstalled: function() { return false; }, runningState: function() { return 'cannot_run'; } },
        csi: function() {},
        loadTimes: function() {}
    };
    
    // 4. Parchear permissions API
    const origQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            origQuery(parameters)
    );
    
    // 5. Parchear getParameter para WebGL (muy usado por DataDome)
    const getParameterProxyHandler = {
        apply: function(target, ctx, args) {
            const param = (args || [])[0];
            if (param === 37445) return 'Google Inc. (NVIDIA API)';
            if (param === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)';
            return Reflect.apply(target, ctx, args);
        }
    };
    const proxy = new Proxy(WebGLRenderingContext.prototype.getParameter, getParameterProxyHandler);
    Object.defineProperty(WebGLRenderingContext.prototype, 'getParameter', {
        configurable: true, enumerable: false, writable: false, value: proxy
    });
})();
"""


class BaseScraper(ABC):
    """
    Clase Base Abstracta para todos los scrapers de Real Estate.
    Maneja: Persistencia Híbrida (CSV Local Buffer -> S3 Parquet).
    """

    def __init__(self, portal_name: str):
        self.portal_name = portal_name
        self.s3 = S3Connector()
        self.logger = get_logger(self.__class__.__name__)
        self.prefix = f"{S3_BRONZE_PREFIX}/{self.portal_name}"

        # Estado en RAM
        self.scraped_data: List[Dict[str, Any]] = []
        self.total_processed_in_run: List[Dict[str, Any]] = [] # Para el Parquet final
        
        # Hashes (SCD Type 2)
        self.hash_index_key = f"{self.prefix}/_hash_index.txt"
        self.historical_hashes: Set[str] = set()

        # Directorios locales
        self.batches_dir = BATCHES_DIR / portal_name
        self.batches_dir.mkdir(parents=True, exist_ok=True)
        LOCAL_HASHES_DIR.mkdir(parents=True, exist_ok=True)

        # Manejo de archivos
        self._csv_path: Path | None = None
        self._csv_file = None
        self._csv_writer = None
        self._pages_since_flush = 0

    # ------------------------------------------------------------------
    # Ciclo de vida principal
    # ------------------------------------------------------------------

    def run(self, max_pages: int = 5, headless: bool = True):
        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"  Iniciando scraper: {self.portal_name}")
        self.logger.info(f"{'=' * 60}")

        # 1. Cargar hashes (Prioridad S3, fallback Local)
        self._load_hashes()

        # 2. Preparar CSV local
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self._csv_path = self.batches_dir / f"{self.portal_name}_{timestamp}.csv"
        self._csv_file = open(self._csv_path, "w", newline="", encoding="utf-8")
        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=CSV_FIELDS, extrasaction="ignore")
        self._csv_writer.writeheader()
        self.logger.info(f"Local Buffer: {self._csv_path}")

        # 3. Lanzar browser
        ua = random.choice(_USER_AGENTS)
        width = random.choice([1280, 1366, 1440, 1920])
        height = random.choice([768, 800, 900, 1080])

        proxy_url = os.getenv("PROXY_URL")
        proxy_config = {"server": proxy_url} if proxy_url else None

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                proxy=proxy_config,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-infobars",
                    f"--window-size={width},{height}",
                ],
            )
            context = browser.new_context(
                viewport={"width": width, "height": height},
                user_agent=ua,
                locale="es-CO",
                timezone_id="America/Bogota",
                extra_http_headers={
                    "Accept-Language": "es-CO,es;q=0.9,en-US;q=0.8",
                    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                },
            )
            
            page = context.new_page()
            page.add_init_script(_STEALTH_SCRIPT)

            self.logger.info(f"Browser lanzado | UA=...{ua[-40:]}")

            try:
                self.scrape_pages(page, max_pages)
            except Exception as e:
                self.logger.error(f"Error crítico en {self.portal_name}: {e}")
            finally:
                context.close()
                browser.close()

        # 4. Finalizar persistencia
        self._finalize_run()

    def _load_hashes(self):
        self.logger.info("Cargando memoria de hashes...")
        try:
            self.historical_hashes = self.s3.download_hash_index(self.hash_index_key)
        except Exception as e:
            self.logger.warning(f"No se pudo cargar hashes de S3: {e}. Usando fallback local...")
            local_path = LOCAL_HASHES_DIR / f"{self.portal_name}_hash_index.txt"
            if local_path.exists():
                with open(local_path, "r") as f:
                    self.historical_hashes = set(line.strip() for line in f if line.strip())
        
        self.logger.info(f"Total hashes cargados: {len(self.historical_hashes)}")

    def _finalize_run(self):
        # Flush final a CSV
        self._flush_to_csv()
        if self._csv_file:
            self._csv_file.close()

        # Guardar hashes localmente (por si S3 falla tras el run)
        self._save_hashes_local()

        # Subir a S3 si hay data
        if self.total_processed_in_run:
            self.logger.info(f"Subiendo batch Parquet ({len(self.total_processed_in_run)} regs) a S3...")
            df = pd.DataFrame(self.total_processed_in_run)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            parquet_key = f"{self.prefix}/batches/{self.portal_name}_{timestamp}.parquet"
            
            if self.s3.upload_parquet(parquet_key, df):
                self.logger.info("Batch Parquet subido exitosamente.")
                self.logger.info("Actualizando índice de hashes en S3...")
                self.s3.upload_hash_index(self.hash_index_key, self.historical_hashes)
            else:
                self.logger.error("Fallo al subir Parquet a S3.")
        
        self.logger.info(f"Ingesta finalizada para {self.portal_name}")

    def _save_hashes_local(self):
        local_path = LOCAL_HASHES_DIR / f"{self.portal_name}_hash_index.txt"
        with open(local_path, "w") as f:
            for h in self.historical_hashes:
                f.write(h + "\n")
        self.logger.debug(f"Hashes locales guardados en {local_path}")

    # ------------------------------------------------------------------
    # Gestión de Flushes (Llamar desde el spider cada página)
    # ------------------------------------------------------------------

    def on_page_done(self):
        """Llamar al terminar cada página para gestión de resiliencia."""
        self._pages_since_flush += 1
        if self._pages_since_flush >= FLUSH_EVERY:
            self._flush_to_csv()
            self._save_hashes_local()
            self._pages_since_flush = 0

    def _flush_to_csv(self):
        if not self.scraped_data or not self._csv_writer:
            return
        
        self.logger.info(f"Flush parcial discu: {len(self.scraped_data)} registros.")
        for row in self.scraped_data:
            self._csv_writer.writerow(row)
            self.total_processed_in_run.append(row)
        
        self._csv_file.flush()
        self.scraped_data.clear()

    # ------------------------------------------------------------------
    # Lógica de scraping
    # ------------------------------------------------------------------

    @abstractmethod
    def scrape_pages(self, page: Page, max_pages: int):
        pass

    def process_and_upload(self, property_data: Dict[str, Any], property_id: str) -> bool:
        precio = property_data.get("precio_num", 0)
        unique_string = f"{property_id}_{precio}"
        price_hash = hashlib.md5(unique_string.encode("utf-8")).hexdigest()[:10]
        id_hash = f"{property_id}_{price_hash}"

        if id_hash in self.historical_hashes:
            self.logger.debug(f"[SKIP] {property_id} — SCD Type 2 activo.")
            return False

        self.historical_hashes.add(id_hash)
        self.scraped_data.append(property_data)
        self.logger.info(f"[NEW] {property_id} agregado al buffer.")
        return True

    # ------------------------------------------------------------------
    # Comportamiento humano
    # ------------------------------------------------------------------

    def human_delay(self, page: Page = None, min_ms: int = 2000, max_ms: int = 5000):
        delay_ms = random.randint(min_ms, max_ms)
        if page:
            try:
                for _ in range(random.randint(2, 5)):
                    scroll_px = random.randint(150, 400)
                    page.mouse.wheel(0, scroll_px)
                    page.wait_for_timeout(random.randint(200, 500))
                page.wait_for_timeout(delay_ms // 2)
            except Exception:
                time.sleep(delay_ms / 1000.0)
        else:
            time.sleep(delay_ms / 1000.0)

    @staticmethod
    def parse_price(raw: str) -> int:
        if not raw: return 0
        digits = re.sub(r"[^\d]", "", str(raw))
        return int(digits) if digits else 0
