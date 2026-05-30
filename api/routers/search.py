"""
api/routers/search.py
=====================
POST /search          → Búsqueda paginada de inmuebles en Gold Parquet (DuckDB)
GET  /search/metadata → Listas de valores únicos para filtros del frontend
"""
import re
from typing import List

from fastapi import APIRouter, HTTPException

from ..core.db import db
from ..schemas.search import PropertyItem, SearchRequest, SearchResponse

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

# Campos y direcciones permitidas para ORDER BY (whitelist anti-injection)
_SORTABLE = frozenset(["precio_num", "area_m2", "habitaciones", "score_inversion", "precio_predicho", "rentabilidad_potencial", "num_portales"])
_DIRECTIONS = frozenset(["asc", "desc"])

_TOKEN_RE = re.compile(r"^[\w\-]{1,64}$")  # acepta letras, dígitos, _ y -


def _safe_token(value: str) -> str:
    """Valida que un token de texto no contenga caracteres SQL peligrosos."""
    if not _TOKEN_RE.match(value):
        raise HTTPException(status_code=422, detail=f"Token inválido: '{value}'")
    return value


@router.post("", response_model=SearchResponse)
def search(req: SearchRequest) -> SearchResponse:
    """Busca inmuebles con filtros opcionales + paginación."""

    # Columnas existentes en la vista (robustez si la vista no tiene todos)
    try:
        available_cols = {
            row[0]
            for row in db.conn.execute("DESCRIBE inmuebles").fetchall()
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DuckDB no disponible: {exc}") from exc

    select_cols = ", ".join(
        [f"CAST({c} AS VARCHAR) AS {c}" if c == "id" else c]
        if c == "id" else
        [c if c in available_cols else f"NULL AS {c}"]
        for c in _ITEM_COLS
    )
    # Simplificar: usar * y solo los campos que existen
    col_list = ", ".join(
        c if c in available_cols else f"NULL AS {c}"
        for c in _ITEM_COLS
    )

    # Construir cláusulas WHERE con parámetros posicionales (evita SQL injection)
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

    # Ordenamiento (whitelist)
    order_col = req.order_by if req.order_by in _SORTABLE else "precio_num"
    order_dir = req.order_dir.lower() if req.order_dir.lower() in _DIRECTIONS else "asc"

    # COUNT
    count_sql = f"SELECT COUNT(*) FROM inmuebles {where_sql}"
    try:
        total: int = db.conn.execute(count_sql, params).fetchone()[0]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error contando registros: {exc}") from exc

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
        raise HTTPException(status_code=500, detail=f"Error consultando datos: {exc}") from exc

    items = [PropertyItem(**row) for row in rows_df.to_dict(orient="records")]

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
            "SELECT DISTINCT city_token FROM inmuebles WHERE city_token IS NOT NULL ORDER BY city_token"
        )["city_token"].tolist()

        markets = db.query_df(
            "SELECT DISTINCT market_token FROM inmuebles WHERE market_token IS NOT NULL ORDER BY market_token"
        )["market_token"].tolist()

        tipos = db.query_df(
            "SELECT DISTINCT tipo_inmueble FROM inmuebles WHERE tipo_inmueble IS NOT NULL ORDER BY tipo_inmueble"
        )["tipo_inmueble"].tolist()

        fuentes = db.query_df(
            "SELECT DISTINCT fuente FROM inmuebles WHERE fuente IS NOT NULL ORDER BY fuente"
        )["fuente"].tolist()

        price_range = db.query_one(
            "SELECT MIN(precio_num), MAX(precio_num) FROM inmuebles WHERE precio_num > 0"
        )

        return {
            "cities": cities,
            "markets": markets,
            "tipos_inmueble": tipos,
            "fuentes": fuentes,
            "price_min": float(price_range[0]) if price_range[0] else 0,
            "price_max": float(price_range[1]) if price_range[1] else 0,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/summary")
def search_summary():
    """Resumen global liviano para el frontend Streamlit API-first."""
    try:
        row = db.query_one(
            """
            SELECT
                COUNT(*) AS total_inmuebles,
                COUNT(DISTINCT market_token) AS n_mercados,
                COUNT(DISTINCT city_token) AS n_ciudades,
                COUNT(DISTINCT fuente) AS n_portales,
                MEDIAN(precio_num) AS med_precio,
                MEDIAN(precio_num / NULLIF(area_m2, 0)) AS med_precio_m2,
                SUM(CASE WHEN estado_inversion = 'Oportunidad' THEN 1 ELSE 0 END) AS n_oportunidades
            FROM inmuebles
            WHERE precio_num > 0
            """
        )
        return {
            "total_inmuebles": int(row[0] or 0),
            "n_mercados": int(row[1] or 0),
            "n_ciudades": int(row[2] or 0),
            "n_portales": int(row[3] or 0),
            "med_precio": float(row[4] or 0),
            "med_precio_m2": float(row[5] or 0),
            "n_oportunidades": int(row[6] or 0),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
