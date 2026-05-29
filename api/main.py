"""
api/main.py
===========
Entry-point FastAPI.

Lifespan:
  1. Cargar bundle XGBoost desde S3 (una sola vez)
  2. Inicializar DuckDB con vistas sobre Gold Parquet en S3

Rutas:
  GET  /health            → liveness/readiness check
  POST /predict           → valoración individual
  POST /search            → búsqueda paginada
  GET  /search/metadata   → metadatos para filtros
  GET  /markets           → resumen por mercado
  GET  /markets/{market}  → detalle de mercado
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core.db import db
from .core.model import model_registry
from .routers import markets, predict, search


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialización pesada (I/O con S3) al arrancar una sola vez."""
    print("[Startup] Cargando modelo XGBoost desde S3...", flush=True)
    model_registry.load()

    print("[Startup] Inicializando DuckDB con vistas sobre Gold Parquet...", flush=True)
    db.setup()

    print("[Startup] API lista.", flush=True)
    yield

    # Teardown (opcional)
    print("[Shutdown] API detenida.", flush=True)


app = FastAPI(
    title="Real Estate Analyst API",
    description=(
        "Backend para la plataforma de inteligencia inmobiliaria colombiana. "
        "Proporciona valoración ML, búsqueda sobre el catálogo Gold y métricas de mercado."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS: en producción reemplaza "*" por el dominio del frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(predict.router)
app.include_router(search.router)
app.include_router(markets.router)


@app.get("/health", tags=["Infra"])
def health():
    """Liveness check."""
    return {
        "status": "ok",
        "model": model_registry.model_key,
        "mape_pct": model_registry.mape,
    }
