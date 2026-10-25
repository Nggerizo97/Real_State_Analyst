"""
src/scrapers/base_scraper.py
============================
Clase Base Abstracta para todos los scrapers de Real Estate.
Define el patrón Factory/Strategy para estandarizar la extracción.

Híbrido Resiliente:
  - Guarda en CSV local (data/batches/) cada 10 páginas.
  - Sube batch Parquet final a S3 al terminar.
  - Sincroniza hashes SCD Type 2 con S3.
  - Sube los IDs vistos en el run a raw/_sightings/<portal>/ (vigencia).
"""

import csv
import hashlib
import json
import os
import random
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import unquote, urlparse

import pandas as pd
from playwright.sync_api import Page, sync_playwright
from playwright_stealth import Stealth

from config.settings import S3_BRONZE_PREFIX
from src.utils.logger import get_logger
from src.utils.s3_connector import S3Connector
from src.utils.checkpoint import CheckpointManager

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
    "habitaciones", "banos", "area", "garajes", "estado_inmueble",
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

    # Ajustes anti-bot que cada portal puede activar (MercadoLibre los usa):
    # - MATCH_REAL_UA: User-Agent y Sec-Ch-Ua con la versión real del Chromium.
    #   Forzar "Chrome/122" sobre un Chromium más nuevo deja el UA en
    #   desacuerdo con navigator.userAgentData, una señal clásica de bot.
    # - PROFILE_DIR_ENV: variable de entorno con un directorio de perfil
    #   persistente (cookies, storage) para volver como visitante recurrente.
    MATCH_REAL_UA = False
    PROFILE_DIR_ENV: Optional[str] = None
    # Días sin volver a intentar tras un bloqueo (0 = desactivado). Reintentar
    # contra un portal que ya bloqueó solo alarga la penalización a la IP.
    COOLDOWN_DAYS = 0

    def __init__(self, portal_name: str):
        self.portal_name = portal_name
        self.s3 = S3Connector()
        self.logger = get_logger(self.__class__.__name__)
        self.prefix = f"{S3_BRONZE_PREFIX}/{self.portal_name}"
        self.checkpoint = CheckpointManager(self.portal_name)

        # Estado en RAM
        self.scraped_data: List[Dict[str, Any]] = []
        self.total_processed_in_run: List[Dict[str, Any]] = [] # Para el Parquet final
        self.start_page = 1
        self.current_page = 1
        self.hash_index_loaded = False  # Para evitar sobreescribir con set vacío por error

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

        # Bloqueo anti-bot: el portal redirigió a login/CAPTCHA.
        # Si ocurre sin haber completado ninguna página, el run se reporta
        # como fallido (exit_code != 0) en vez de terminar "OK" con 0 registros.
        self.blocked = False
        self.block_reason = ""
        self.pages_scraped = 0

        # Avistamientos: todo aviso visto en este run, incluidos los que el
        # índice de hashes descarta por no tener cambios. Sin esto un aviso
        # estable que sigue publicado nunca actualiza su fecha y el ETL lo
        # marca como no vigente a los 30 días.
        self.seen_ids: Set[str] = set()

    # ------------------------------------------------------------------
    # Ciclo de vida principal
    # ------------------------------------------------------------------

    def run(self, max_pages: int = 5, headless: bool = True):
        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"  Iniciando scraper: {self.portal_name}")
        self.logger.info(f"{'=' * 60}")

        # 0. Cool-down tras un bloqueo reciente: no se toca el portal
        cooldown_until = self._cooldown_until()
        if cooldown_until:
            self.logger.warning(
                f"[COOLDOWN] {self.portal_name} bloqueó el acceso; no se reintenta "
                f"hasta {cooldown_until.isoformat(timespec='minutes')}."
            )
            return

        # 1. Cargar hashes y checkpoint
        self._load_hashes()
        last_completed = self.checkpoint.load()
        self.start_page = (last_completed + 1) if last_completed is not None else 1
        self.current_page = self.start_page
        self.logger.info(f"Punto de partida: Página {self.start_page}")

        # 2. Preparar CSV local
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self._csv_path = self.batches_dir / f"{self.portal_name}_{timestamp}.csv"
        self._csv_file = open(self._csv_path, "w", newline="", encoding="utf-8")
        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=CSV_FIELDS, extrasaction="ignore")
        self._csv_writer.writeheader()
        self.logger.info(f"Local Buffer: {self._csv_path}")

        # 3. Lanzar browser
        major = self._real_chromium_major() if self.MATCH_REAL_UA else None
        if major:
            ua = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"
            )
        else:
            ua = random.choice(_USER_AGENTS)
            major = re.search(r"Chrome/(\d+)", ua).group(1)
        width = random.choice([1280, 1366, 1440, 1920])
        height = random.choice([768, 800, 900, 1080])

        proxy_config = self._proxy_config(os.getenv("PROXY_URL"))
        profile_dir = os.getenv(self.PROFILE_DIR_ENV) if self.PROFILE_DIR_ENV else None

        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-infobars",
            f"--window-size={width},{height}",
        ]
        headers = {"Accept-Language": "es-CO,es;q=0.9,en-US;q=0.8"}
        if not self.MATCH_REAL_UA:
            # Con UA real se dejan los client hints nativos del navegador: los
            # forzados ("Google Chrome", "Not(A:Brand";v="24") no coinciden con
            # navigator.userAgentData de Chromium.
            headers.update({
                "Sec-Ch-Ua": f'"Chromium";v="{major}", "Not(A:Brand";v="24", "Google Chrome";v="{major}"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
            })
        context_options = dict(
            viewport={"width": width, "height": height},
            user_agent=ua,
            locale="es-CO",
            timezone_id="America/Bogota",
            extra_http_headers=headers,
        )

        stealth = Stealth(
            navigator_languages_override=("es-CO", "es", "en-US", "en"),
            navigator_platform_override="Win32",
            navigator_user_agent_override=ua,
        )
        with stealth.use_sync(sync_playwright()) as p:
            if profile_dir:
                # Perfil persistente: el contexto ES el navegador
                Path(profile_dir).mkdir(parents=True, exist_ok=True)
                browser = None
                context = p.chromium.launch_persistent_context(
                    profile_dir, headless=headless, proxy=proxy_config,
                    args=launch_args, **context_options,
                )
                self.logger.info(f"Perfil persistente: {profile_dir}")
            else:
                browser = p.chromium.launch(headless=headless, proxy=proxy_config, args=launch_args)
                context = browser.new_context(**context_options)

            page = context.new_page()
            page.add_init_script(_STEALTH_SCRIPT)

            self.logger.info(f"Browser lanzado | UA=...{ua[-40:]}")

            try:
                self.scrape_pages(page, max_pages)
            except Exception as e:
                self.logger.error(f"Error crítico en {self.portal_name}: {e}")
            finally:
                context.close()
                if browser:
                    browser.close()

        # 4. Finalizar persistencia
        self._finalize_run()
        if self.blocked and self.COOLDOWN_DAYS:
            self._start_cooldown()

    @staticmethod
    def _real_chromium_major() -> Optional[str]:
        """Versión mayor del Chromium que usa Playwright (p. ej. '141')."""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                version = browser.version
                browser.close()
            return version.split(".")[0]
        except Exception:
            return None

    @staticmethod
    def _proxy_config(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
        """Convierte PROXY_URL en la config de Playwright.

        Chromium ignora user:pass embebidos en `server`, así que se separan
        (necesario para proxies residenciales con autenticación).
        """
        if not proxy_url:
            return None
        parsed = urlparse(proxy_url)
        if not parsed.username:
            return {"server": proxy_url}
        server = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            server += f":{parsed.port}"
        return {
            "server": server,
            "username": unquote(parsed.username),
            "password": unquote(parsed.password or ""),
        }

    @property
    def _cooldown_key(self) -> str:
        # Dentro de raw/<portal>/state/: Bronze solo carga .parquet y el lector
        # de checkpoints ignora este nombre de archivo.
        return f"{self.prefix}/state/cooldown.json"

    def _cooldown_until(self) -> Optional[datetime]:
        """Fin del cool-down vigente, o None si se puede raspar."""
        if not self.COOLDOWN_DAYS:
            return None
        try:
            resp = self.s3.s3_client.get_object(Bucket=self.s3.bucket, Key=self._cooldown_key)
            until = datetime.fromisoformat(json.loads(resp["Body"].read())["until"])
        except Exception:
            return None  # sin marca (o ilegible): no hay cool-down
        return until if until > datetime.now() else None

    def _start_cooldown(self) -> None:
        until = datetime.now() + timedelta(days=self.COOLDOWN_DAYS)
        body = {
            "portal": self.portal_name,
            "blocked_at": datetime.now().isoformat(timespec="seconds"),
            "until": until.isoformat(timespec="seconds"),
            "reason": self.block_reason,
        }
        try:
            self.s3.s3_client.put_object(
                Bucket=self.s3.bucket, Key=self._cooldown_key,
                Body=json.dumps(body, indent=2).encode(), ContentType="application/json",
            )
            self.logger.warning(f"[COOLDOWN] Sin reintentos hasta {body['until']} ({self._cooldown_key}).")
        except Exception as e:
            self.logger.error(f"No se pudo guardar el cool-down: {e}")

    def mark_blocked(self, reason: str) -> None:
        """Registra que el portal bloqueó la sesión (login wall, CAPTCHA...)."""
        self.blocked = True
        self.block_reason = reason
        self.logger.error(f"[BLOCKED] {self.portal_name}: {reason}")

    @property
    def exit_code(self) -> int:
        """0 = OK; 2 = bloqueado antes de completar cualquier página."""
        return 2 if self.blocked and self.pages_scraped == 0 else 0

    def _load_hashes(self):
        self.logger.info("Cargando memoria de hashes...")
        try:
            self.historical_hashes = self.s3.download_hash_index(self.hash_index_key)
            self.hash_index_loaded = True
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
                
                # Sincronizar hashes
                if self.hash_index_loaded:
                    self.s3.upload_hash_index(self.hash_index_key, self.historical_hashes)
                else:
                    self.logger.warning("No se suben hashes porque no se cargaron correctamente al inicio.")

            else:
                self.logger.error("Fallo al subir Parquet a S3.")
        
        self._upload_sightings()
        self.logger.info(f"Ingesta finalizada para {self.portal_name}")

    def _upload_sightings(self):
        """Sube los IDs vistos en el run a raw/_sightings/<portal>/.

        Va fuera de raw/<portal>/ a propósito: Bronze carga cada parquet de
        esa carpeta como filas de avisos.
        """
        if not self.seen_ids:
            return
        seen_at = datetime.now().isoformat(timespec="seconds")
        df = pd.DataFrame({
            "id_inmueble": sorted(self.seen_ids),
            "portal": self.portal_name,
            "seen_at": seen_at,
        })
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        key = f"{S3_BRONZE_PREFIX}/_sightings/{self.portal_name}/{self.portal_name}_{timestamp}.parquet"
        if self.s3.upload_parquet(key, df):
            self.logger.info(f"Avistamientos subidos: {len(df)} IDs vistos -> {key}")
        else:
            self.logger.error("Fallo al subir avistamientos a S3.")

    def _save_hashes_local(self):
        local_path = LOCAL_HASHES_DIR / f"{self.portal_name}_hash_index.txt"
        with open(local_path, "w") as f:
            for h in self.historical_hashes:
                f.write(h + "\n")
        self.logger.debug(f"Hashes locales guardados en {local_path}")

    # ------------------------------------------------------------------
    # Gestión de Flushes (Llamar desde el spider cada página)
    # ------------------------------------------------------------------

    def on_page_done(self, page_num: int):
        """Llamar al terminar cada página para gestión de resiliencia."""
        self.current_page = page_num
        self.pages_scraped += 1
        self._pages_since_flush += 1
        if self._pages_since_flush >= FLUSH_EVERY:
            self._flush_to_csv()
            self._save_hashes_local()
            # Guardar checkpoint parcial en S3
            self.checkpoint.save(self.current_page, len(self.total_processed_in_run))
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

        self.seen_ids.add(str(property_data.get("id_inmueble") or property_id))

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
