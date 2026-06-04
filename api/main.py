"""
api/main.py
===========
Entry-point FastAPI con:
  - Logging estructurado (JSON)
  - Middleware de API key (X-API-Key header; vac\u00edo = sin auth en dev)
  - Middleware de request logging (m\u00e9todo, path, status, ms)
  - CORS configurable v\u00eda env var ALLOWED_ORIGINS
  - Health check de readiness (DuckDB + modelo)

Rutas:
  GET  /health            \u2192 readiness check
  POST /predict           \u2192 valoraci\u00f3n individual
  POST /search            \u2192 b\u00fasqueda paginada
  GET  /search/metadata   \u2192 metadatos para filtros
  GET  /markets           \u2192 resumen por mercado
  GET  /markets/{market}  \u2192 detalle de mercado
"""
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .core.config import get_settings
from .core.db import db
from .core.model import model_registry
from .routers import markets, predict, search

# \u2500\u2500 Logging estructurado \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando API \u2014 cargando modelo XGBoost desde S3...")
    model_registry.load()

    logger.info("Inicializando DuckDB con vistas sobre Gold Parquet...")
    db.setup()

    logger.info("API lista para recibir tr\u00e1fico.")
    yield

    logger.info("API detenida.")


app = FastAPI(
    title="Real Estate Analyst API",
    description=(
        "Backend para la plataforma de inteligencia inmobiliaria colombiana. "
        "Proporciona valoraci\u00f3n ML, b\u00fasqueda sobre el cat\u00e1logo Gold y m\u00e9tricas de mercado."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# \u2500\u2500 Middleware: API Key \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    """Requiere X-API-Key si la variable API_KEY est\u00e1 configurada en el entorno.
    /health siempre accesible (para readiness probes de ECS/Docker sin credenciales).
    """
    s = get_settings()
    if s.api_key and request.url.path != "/health":
        provided = request.headers.get("X-API-Key", "")
        if provided != s.api_key:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "API key inv\u00e1lida o faltante."},
            )
    return await call_next(request)


# \u2500\u2500 Middleware: Request logging \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "method=%s path=%s status=%d duration_ms=%s",
        request.method, request.url.path, response.status_code, ms,
    )
    return response


# \u2500\u2500 CORS configurable por env var ALLOWED_ORIGINS \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_s = get_settings()
_origins = [o.strip() for o in _s.allowed_origins.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)

# \u2500\u2500 Routers \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
app.include_router(predict.router)
app.include_router(search.router)
app.include_router(markets.router)


# \u2500\u2500 Health check \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
@app.get("/health", tags=["Infra"])
def health():
    """Readiness check: verifica que el modelo est\u00e9 cargado y DuckDB responda."""
    model_ok = model_registry._bundle is not None
    db_ok = db.ping()
    ready = model_ok and db_ok
    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "ok" if ready else "degraded",
            "db": "ok" if db_ok else "error",
            "model": model_registry.model_key if model_ok else "not_loaded",
            "mape_pct": model_registry.mape if model_ok else None,
        },
    )
