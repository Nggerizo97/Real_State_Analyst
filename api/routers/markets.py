"""
api/routers/markets.py
======================
GET /markets           → Resumen agregado por mercado
GET /markets/{market}  → Detalle de un mercado específico
"""
import logging
from typing import List

from fastapi import APIRouter, HTTPException, Path

from ..core.db import db
from ..schemas.search import MarketSummary

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/markets", tags=["Markets"])

_MARKET_SQL = """
    SELECT
        market_token,
        city_token,
        CAST(SUM(market_n) AS INTEGER)     AS n_inmuebles,
        MEDIAN(precio_mediano)             AS precio_mediano,
        MEDIAN(precio_m2_mediano)          AS precio_m2_mediano,
        MEDIAN(area_mediana)               AS area_mediana
    FROM mercado_analitica
    WHERE analytics_level = 'city'
      AND market_token IS NOT NULL
"""


@router.get("", response_model=List[MarketSummary])
def list_markets():
    """Devuelve métricas agregadas por mercado."""
    sql = _MARKET_SQL + " GROUP BY market_token, city_token ORDER BY n_inmuebles DESC"
    try:
        df = db.query_df(sql)
    except Exception as exc:
        logger.error("Error en list_markets: %s", exc)
        raise HTTPException(status_code=500, detail="Error interno al consultar mercados.") from exc
    return [MarketSummary(**row) for row in df.to_dict(orient="records")]


@router.get("/{market_token}", response_model=MarketSummary)
def get_market(
    market_token: str = Path(..., pattern=r"^[\w\-]{1,64}$"),
):
    """Devuelve métricas de un mercado específico."""
    sql = _MARKET_SQL + " AND market_token = ? GROUP BY market_token, city_token LIMIT 1"
    try:
        row = db.query_one(sql, [market_token])
    except Exception as exc:
        logger.error("Error en get_market(%s): %s", market_token, exc)
        raise HTTPException(status_code=500, detail="Error interno al consultar el mercado.") from exc

    if row is None:
        raise HTTPException(status_code=404, detail=f"Mercado '{market_token}' no encontrado.")

    cols = ["market_token", "city_token", "n_inmuebles", "precio_mediano", "precio_m2_mediano", "area_mediana"]
    return MarketSummary(**dict(zip(cols, row)))
