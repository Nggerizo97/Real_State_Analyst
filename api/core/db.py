"""
api/core/db.py
==============
Singleton DuckDB sobre vistas en S3 con:
  - Lock único para thread-safety bajo Uvicorn/ThreadPoolExecutor
  - Refresco automático de credenciales temporales (ECS Task Role)
  - Cache de columnas disponibles (evita DESCRIBE en cada request)
"""
import logging
import threading
import time

import boto3
import duckdb

from .config import get_settings

logger = logging.getLogger(__name__)

_GOLD_TABLES = {
    "inmuebles":         "gold/app_inmuebles_scored",
    "mercado_analitica": "gold/mercado_analitica",
    "portal_operacion":  "gold/portal_operacion",
}

# Intervalo de refresco de credenciales temporales (ECS Task Role expira en ~60 min)
_CREDS_REFRESH_SECS = 25 * 60  # 25 minutos


class DuckDBManager:
    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection | None = None
        # Lock único: protege TODOS los conn.execute() — DDL, SET y queries.
        # Evita condiciones de carrera bajo el threadpool de Uvicorn.
        self._lock = threading.Lock()
        self._using_temp_creds: bool = False
        self._session_kwargs: dict = {}
        self._s3_region: str = ""
        # Columnas de la vista 'inmuebles' cacheadas en setup() — evita DESCRIBE por request
        self.available_cols: frozenset = frozenset()

    # ------------------------------------------------------------------
    def setup(self) -> None:
        """Inicializa la conexión, aplica credenciales y crea vistas.
        Se llama una sola vez en el lifespan de FastAPI.
        """
        s = get_settings()
        conn = duckdb.connect(":memory:")

        try:
            conn.execute("LOAD httpfs;")
        except Exception:
            try:
                conn.execute("INSTALL httpfs;")
                conn.execute("LOAD httpfs;")
            except Exception as e:
                raise RuntimeError(f"No se pudo cargar httpfs de DuckDB: {e}")

        self._s3_region = s.aws_region
        self._session_kwargs = {"region_name": s.aws_region}
        if s.aws_access_key_id and s.aws_secret_access_key:
            self._session_kwargs["aws_access_key_id"] = s.aws_access_key_id
            self._session_kwargs["aws_secret_access_key"] = s.aws_secret_access_key
            self._using_temp_creds = False
        else:
            # ECS Task Role → credenciales temporales que expiran en ~60 min
            self._using_temp_creds = True

        self._conn = conn
        self._apply_credentials()

        bucket = s.s3_bucket
        with self._lock:
            for view_name, s3_prefix in _GOLD_TABLES.items():
                s3_glob = f"s3://{bucket}/{s3_prefix}/**/*.parquet"
                conn.execute(
                    f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM read_parquet('{s3_glob}', hive_partitioning=false,
                                               union_by_name=true);
                    """
                )

        # Cachear columnas de la tabla principal (una sola vez)
        try:
            with self._lock:
                self.available_cols = frozenset(
                    row[0] for row in conn.execute("DESCRIBE inmuebles").fetchall()
                )
        except Exception as exc:
            logger.warning("No se pudo cachear columnas de inmuebles: %s", exc)
            self.available_cols = frozenset()

        if self._using_temp_creds:
            self._start_refresh_thread()

        logger.info("DuckDB listo. Vistas creadas sobre s3://%s/gold/", bucket)

    # ------------------------------------------------------------------
    def _apply_credentials(self) -> None:
        """Obtiene credenciales AWS frescas y las inyecta en DuckDB."""
        creds = boto3.Session(**self._session_kwargs).get_credentials()
        if creds is None:
            raise RuntimeError("No se pudieron resolver credenciales AWS.")
        frozen = creds.get_frozen_credentials()
        with self._lock:
            self._conn.execute(f"SET s3_region = '{self._s3_region}';")
            self._conn.execute(f"SET s3_access_key_id = '{frozen.access_key}';")
            self._conn.execute(f"SET s3_secret_access_key = '{frozen.secret_key}';")
            if frozen.token:
                self._conn.execute(f"SET s3_session_token = '{frozen.token}';")

    def _start_refresh_thread(self) -> None:
        """Lanza un daemon thread que renueva las credenciales cada 25 min."""
        def _loop() -> None:
            while True:
                time.sleep(_CREDS_REFRESH_SECS)
                try:
                    self._apply_credentials()
                    logger.info("Credenciales AWS refrescadas correctamente en DuckDB.")
                except Exception as exc:
                    logger.error("Error refrescando credenciales AWS: %s", exc)

        t = threading.Thread(target=_loop, daemon=True, name="duckdb-creds-refresh")
        t.start()
        logger.info(
            "Hilo de refresco de credenciales iniciado (cada %d min).",
            _CREDS_REFRESH_SECS // 60,
        )

    # ------------------------------------------------------------------
    def ping(self) -> bool:
        """Comprueba que DuckDB responde — usado por el health check de readiness."""
        try:
            with self._lock:
                self._conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("DuckDB no inicializado. Llama setup() en el lifespan.")
        return self._conn

    def query_df(self, sql: str, params: list | None = None):
        """Ejecuta SQL y devuelve pandas DataFrame. Thread-safe."""
        with self._lock:
            return self.conn.execute(sql, params or []).df()

    def query_one(self, sql: str, params: list | None = None):
        """Ejecuta SQL y devuelve la primera fila como tupla. Thread-safe."""
        with self._lock:
            return self.conn.execute(sql, params or []).fetchone()


# Singleton global
db = DuckDBManager()
