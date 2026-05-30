"""
api/core/db.py
==============
Singleton DuckDB que crea VISTAS directas sobre los Parquet de S3.

DuckDB con la extensión httpfs consulta S3 con predicate-pushdown completo
(filtros de precio, ciudad, etc. se ejecutan en C++ antes de descargar datos).
No se descarga ningún archivo al disco del servidor.

Thread-safety: DuckDB usa su propio mutex interno para lecturas simultáneas;
para escrituras/DDL usamos un Lock explícito.
"""
import threading

import boto3
import duckdb

from .config import get_settings

# Tablas Gold que la API necesita
_GOLD_TABLES = {
    "inmuebles":         "gold/app_inmuebles_scored",
    "mercado_analitica": "gold/mercado_analitica",
    "portal_operacion":  "gold/portal_operacion",
}


class DuckDBManager:
    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._ddl_lock = threading.Lock()

    # ------------------------------------------------------------------
    def setup(self) -> None:
        """Inicializa la conexión y crea las vistas sobre S3.
        Debe llamarse una sola vez en el lifespan de FastAPI.
        """
        s = get_settings()

        # Conexión en memoria (los datos viven en S3, no en RAM)
        conn = duckdb.connect(":memory:")

        # Instalar / cargar la extensión httpfs (incluida en DuckDB >= 0.8)
        conn.execute("INSTALL httpfs; LOAD httpfs;")

        # Resolver credenciales reales: usar explícitas sólo si existen;
        # de lo contrario boto3 obtiene las temporales del ECS Task Role.
        session_kwargs = {"region_name": s.aws_region}
        if s.aws_access_key_id and s.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = s.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = s.aws_secret_access_key

        creds = boto3.Session(**session_kwargs).get_credentials()
        if creds is None:
            raise RuntimeError("No se pudieron resolver credenciales AWS para DuckDB.")
        frozen = creds.get_frozen_credentials()

        conn.execute(f"SET s3_region = '{s.aws_region}';")
        conn.execute(f"SET s3_access_key_id = '{frozen.access_key}';")
        conn.execute(f"SET s3_secret_access_key = '{frozen.secret_key}';")
        if frozen.token:
            conn.execute(f"SET s3_session_token = '{frozen.token}';")

        bucket = s.s3_bucket
        with self._ddl_lock:
            for view_name, s3_prefix in _GOLD_TABLES.items():
                # Patrón glob para soportar múltiples particiones Parquet
                s3_glob = f"s3://{bucket}/{s3_prefix}/**/*.parquet"
                conn.execute(
                    f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM read_parquet('{s3_glob}', hive_partitioning=false,
                                               union_by_name=true);
                    """
                )

        self._conn = conn
        print(f"[DuckDB] Vistas creadas sobre s3://{bucket}/gold/", flush=True)

    # ------------------------------------------------------------------
    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("DuckDB no inicializado. Llama setup() en el lifespan.")
        return self._conn

    def query_df(self, sql: str, params: list | None = None):
        """Ejecuta SQL y devuelve pandas DataFrame."""
        return self.conn.execute(sql, params or []).df()

    def query_one(self, sql: str, params: list | None = None):
        """Ejecuta SQL y devuelve la primera fila como tupla."""
        return self.conn.execute(sql, params or []).fetchone()


# Singleton global
db = DuckDBManager()
