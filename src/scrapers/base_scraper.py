"""
src/scrapers/base_scraper.py
============================
Clase Base Abstracta para todos los scrapers de Real Estate.
Define el patrón Factory/Strategy para estandarizar la extracción.

Flujo:
  1. Carga hashes históricos desde S3 (deduplicación SCD Type 2).
  2. Lanza Playwright con stealth, UA randomizado, locale es-CO.
  3. Delega a scrape_pages() (método abstracto de cada spider).
  4. Sube batch Parquet + actualiza hash index en S3.
"""

import hashlib
import random
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Set

import pandas as pd
from playwright.sync_api import Page, sync_playwright

from config.settings import S3_BRONZE_PREFIX
from src.utils.logger import get_logger
from src.utils.s3_connector import S3Connector

# ---------------------------------------------------------------------------
# User-Agents (Fijado a Chrome 122 para match exacto con Sec-Ch-Ua headers)
# ---------------------------------------------------------------------------
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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
    Maneja: S3 upload (Parquet), hash dedup (SCD Type 2), stealth browser.
    """

    def __init__(self, portal_name: str):
        self.portal_name = portal_name
        self.s3 = S3Connector()
        self.logger = get_logger(self.__class__.__name__)
        self.prefix = f"{S3_BRONZE_PREFIX}/{self.portal_name}"

        self.scraped_data: List[Dict[str, Any]] = []
        self.hash_index_key = f"{self.prefix}/_hash_index.txt"
        self.historical_hashes: Set[str] = set()

    # ------------------------------------------------------------------
    # Ciclo de vida principal
    # ------------------------------------------------------------------

    def run(self, max_pages: int = 5, headless: bool = True):
        """
        Ciclo de vida principal del scraper.

        Args:
            max_pages: Máximo de páginas / clicks por listado.
            headless:  False → abre ventana visible del navegador (debug).
        """
        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"  Iniciando scraper: {self.portal_name}")
        self.logger.info(f"{'=' * 60}")

        # 1. Cargar hashes previos desde S3
        self.logger.info("Descargando memoria de hashes previa de S3...")
        self.historical_hashes = self.s3.download_hash_index(self.hash_index_key)
        self.logger.info(f"Cargados {len(self.historical_hashes)} hashes históricos.")

        ua = random.choice(_USER_AGENTS)
        width = random.choice([1280, 1366, 1440, 1920])
        height = random.choice([768, 800, 900, 1080])

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
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
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-User": "?1"
                },
            )
            
            page = context.new_page()
            page.add_init_script(_STEALTH_SCRIPT)

            self.logger.info(
                f"Browser lanzado | UA=...{ua[-40:]} | viewport={width}x{height}"
            )

            try:
                self.scrape_pages(page, max_pages)
            except Exception as e:
                self.logger.error(
                    f"Error crítico durante el Scraping de {self.portal_name}: {e}"
                )
            finally:
                context.close()
                browser.close()

        # 2. Subir resultados a S3
        if self.scraped_data:
            self.logger.info(
                f"Preparando conversión a Parquet de "
                f"{len(self.scraped_data)} nuevos inmuebles/actualizaciones..."
            )
            df = pd.DataFrame(self.scraped_data)

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            parquet_key = (
                f"{self.prefix}/batches/{self.portal_name}_{timestamp}.parquet"
            )

            if self.s3.upload_parquet(parquet_key, df):
                self.logger.info(f"Subida masiva exitosa a S3: {parquet_key}")
                self.logger.info("Actualizando índice de Hashes Maestro...")
                self.s3.upload_hash_index(
                    self.hash_index_key, self.historical_hashes
                )
            else:
                self.logger.error(
                    "Fallo al subir el batch Parquet a S3. "
                    "Los hashes no se actualizarán."
                )
        else:
            self.logger.info("No hay inmuebles nuevos o cambios de precio para subir.")

        self.logger.info(f"Ingesta finalizada para {self.portal_name}")

    # ------------------------------------------------------------------
    # Método abstracto
    # ------------------------------------------------------------------

    @abstractmethod
    def scrape_pages(self, page: Page, max_pages: int):
        """
        Lógica de navegación y extracción específica del portal.
        Debe llamar a self.process_and_upload() por cada inmueble.
        """
        pass

    # ------------------------------------------------------------------
    # Deduplicación SCD Type 2
    # ------------------------------------------------------------------

    def process_and_upload(
        self, property_data: Dict[str, Any], property_id: str
    ) -> bool:
        """
        Valida deduplicación en RAM y acumula el registro.
        SCD Type 2: si el precio cambia → hash diferente → actualización.
        """
        precio = property_data.get("precio_num", 0)
        unique_string = f"{property_id}_{precio}"
        price_hash = hashlib.md5(unique_string.encode("utf-8")).hexdigest()[:10]
        id_hash = f"{property_id}_{price_hash}"

        if id_hash in self.historical_hashes:
            self.logger.debug(
                f"[SKIP] {property_id} — sin cambios de precio (SCD Type 2)."
            )
            return False

        self.historical_hashes.add(id_hash)
        self.scraped_data.append(property_data)
        self.logger.info(f"[NEW] {property_id} agregado al batch.")
        return True

    # ------------------------------------------------------------------
    # Comportamiento humano
    # ------------------------------------------------------------------

    def human_delay(
        self, page: Page = None, min_ms: int = 2000, max_ms: int = 5000
    ):
        """Pausa aleatoria + scroll suave para simular comportamiento humano."""
        delay = random.randint(min_ms, max_ms)
        self.logger.debug(f"human_delay → {delay}ms")

        if page:
            try:
                page.mouse.wheel(0, random.randint(100, 500))
                page.wait_for_timeout(delay)
            except Exception:
                time.sleep(delay / 1000.0)
        else:
            time.sleep(delay / 1000.0)

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    @staticmethod
    def parse_price(raw: str) -> int:
        """Extrae el valor numérico de un string de precio (COP)."""
        if not raw:
            return 0
        digits = re.sub(r"[^\d]", "", str(raw))
        return int(digits) if digits else 0
