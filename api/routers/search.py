"""
api/routers/search.py
=====================
POST /search          → Búsqueda paginada de inmuebles en Gold Parquet (DuckDB)
GET  /search/metadata → Listas de valores únicos para filtros del frontend
"""
import logging
import math
import re
from typing import List

from fastapi import APIRouter, HTTPException

from ..core.db import db
from ..schemas.search import PropertyItem, SearchRequest, SearchResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["Search"])

# Columnas que se devuelven en los items (subset liviano del Gold)
_ITEM_COLS = [
    "id", "titulo", "ubicacion_clean", "ubicacion_norm", "tipo_inmueble", "estado_inmueble",
    "precio_num", "area_m2", "habitaciones", "banos", "garajes",
    "city_token", "market_token", "comuna_mercado", "sector_mercado",
    "fuente", "rentabilidad_potencial", "estado_inversion", "num_portales",
    "dispersion_pct_grupo", "precio_min_grupo", "precio_max_grupo",
    "score_inversion", "precio_predicho", "first_seen_date", "precio_cambio_pct", "url",
]

_SORTABLE = frozenset(["precio_num", "area_m2", "habitaciones", "score_inversion", "precio_predicho", "rentabilidad_potencial", "num_portales"])
_DIRECTIONS = frozenset(["asc", "desc"])
_TOKEN_RE = re.compile(r"^[\w\-]{1,64}$")


def _safe_token(value: str) -> str:
    if not _TOKEN_RE.match(value):
        raise HTTPException(status_code=422, detail=f"Token inválido: '{value}'")
    return value


@router.post("", response_model=SearchResponse)
def search(req: SearchRequest) -> SearchResponse:
    """Busca inmuebles con filtros opcionales + paginación."""

    # Usar columnas cacheadas en startup — evita DESCRIBE en cada request
    available_cols = db.available_cols if db.available_cols else frozenset(_ITEM_COLS)

    _DATE_COLS = {"first_seen_date"}
    col_list = ", ".join(
        (f"CAST({c} AS VARCHAR) AS {c}" if (c == "id" or c in _DATE_COLS) else c)
        if c in available_cols
        else f"NULL AS {c}"
        for c in _ITEM_COLS
    )

    conditions: List[str] = []
    params: List = []

    if req.city_token:
        conditions.append("city_token = ?")
        params.append(_safe_token(req.city_token))

    if req.market_token:
        conditions.append("market_token = ?")
        params.append(_safe_token(req.market_token))

    if req.tipo_inmueble:
        conditions.append("tipo_inmueble = ?")
        params.append(_safe_token(req.tipo_inmueble))

    if req.estado_inmueble:
        conditions.append("estado_inmueble = ?")
        params.append(_safe_token(req.estado_inmueble))

    if req.price_min is not None:
        conditions.append("precio_num >= ?")
        params.append(float(req.price_min))

    if req.price_max is not None:
        conditions.append("precio_num <= ?")
        params.append(float(req.price_max))

    if req.area_min is not None:
        conditions.append("area_m2 >= ?")
        params.append(float(req.area_min))

    if req.area_max is not None:
        conditions.append("area_m2 <= ?")
        params.append(float(req.area_max))

    if req.habitaciones_min is not None:
        conditions.append("habitaciones >= ?")
        params.append(int(req.habitaciones_min))

    if req.num_portales_min is not None:
        conditions.append("num_portales >= ?")
        params.append(int(req.num_portales_min))

    if req.fuentes:
        safe_fuentes = [_safe_token(f) for f in req.fuentes]
        placeholders = ", ".join("?" * len(safe_fuentes))
        conditions.append(f"fuente IN ({placeholders})")
        params.extend(safe_fuentes)

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    order_col = req.order_by if req.order_by in _SORTABLE else "precio_num"
    order_dir = req.order_dir.lower() if req.order_dir.lower() in _DIRECTIONS else "asc"

    # COUNT
    count_sql = f"SELECT COUNT(*) FROM inmuebles {where_sql}"
    try:
        total: int = db.query_one(count_sql, params)[0]
    except Exception as exc:
        logger.error("Error contando registros: %s", exc)
        raise HTTPException(status_code=500, detail="Error interno al contar registros.") from exc

    # DATA
    data_sql = f"""
        SELECT {col_list}
        FROM inmuebles
        {where_sql}
        ORDER BY {order_col} {order_dir} NULLS LAST
        LIMIT {req.limit}
        OFFSET {req.offset}
    """
    try:
        rows_df = db.query_df(data_sql, params)
    except Exception as exc:
        logger.error("Error consultando datos: %s", exc)
        raise HTTPException(status_code=500, detail="Error interno al consultar datos.") from exc

    def _safe_val(v):
        if v is None:
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    items = [
        PropertyItem(**{k: _safe_val(v) for k, v in row.items()})
        for row in rows_df.to_dict(orient="records")
    ]

    return SearchResponse(
        total=total,
        offset=req.offset,
        limit=req.limit,
        items=items,
    )


@router.get("/metadata")
def search_metadata():
    """Devuelve listas de valores únicos para poblar los selectores del frontend."""
    try:
        cities = db.query_df(
            "SELECT DISTINCT city_token FROM mercado_analitica WHERE analytics_level = 'city' AND city_token IS NOT NULL ORDER BY city_token"
        )["city_token"].tolist()

        markets = db.query_df(
            "SELECT DISTINCT market_token FROM mercado_analitica WHERE analytics_level = 'market' AND market_token IS NOT NULL ORDER BY market_token"
        )["market_token"].tolist()

        tipos = ["apartamento", "casa", "lote", "local_comercial", "oficina"]
        fuentes = ["bancolombia_tu360", "ciencuadras", "ciencuadras_nuevo", "ciencuadras_usado", "facebook", "fincaraiz", "mercadolibre", "metrocuadrado", "properati"]

        return {
            "cities": cities,
            "markets": markets,
            "tipos_inmueble": tipos,
            "fuentes": fuentes,
            "price_min": 50000000.0,
            "price_max": 5000000000.0,
        }
    except Exception as exc:
        logger.error("Error en /search/metadata: %s", exc)
        raise HTTPException(status_code=500, detail="Error interno al obtener metadatos.") from exc


@router.get("/summary")
def search_summary():
    """Resumen global liviano para el frontend Streamlit API-first."""
    try:
        try:
            latest_snapshot = db.query_one("SELECT MAX(gold_snapshot_at) FROM portal_operacion")[0]
            if latest_snapshot:
                portal_row = db.query_one(
                    "SELECT SUM(portal_ofertas_activas), COUNT(DISTINCT portal) FROM portal_operacion WHERE gold_snapshot_at = ?",
                    [latest_snapshot]
                )
            else:
                portal_row = db.query_one("SELECT SUM(portal_ofertas_activas), COUNT(DISTINCT portal) FROM portal_operacion")
            total_inmuebles = int(portal_row[0] or 0)
            n_portales = int(portal_row[1] or 0)
        except Exception:
            total_inmuebles = 0
            n_portales = 0

        try:
            n_mercados = int(db.query_one("SELECT COUNT(DISTINCT market_token) FROM mercado_analitica WHERE analytics_level = 'market' AND market_token IS NOT NULL")[0] or 0)
            n_ciudades = int(db.query_one("SELECT COUNT(DISTINCT city_token) FROM mercado_analitica WHERE analytics_level = 'city' AND city_token IS NOT NULL")[0] or 0)
        except Exception:
            n_mercados = 0
            n_ciudades = 0

        stale = (total_inmuebles == 0)

        return {
            "total_inmuebles": total_inmuebles,
            "n_mercados": n_mercados,
            "n_ciudades": n_ciudades,
            "n_portales": n_portales,
            "med_precio": 585000000.0,
            "med_precio_m2": 5150000.0,
            "n_oportunidades": 0,
            "stale_data": stale,
        }
    except Exception as exc:
        logger.error("Error en /search/summary: %s", exc)
        raise HTTPException(status_code=500, detail="Error interno al obtener resumen.") from exc
