"""
api/routers/markets.py
======================
GET /markets           → Resumen agregado por mercado
GET /markets/{market}  → Detalle de un mercado específico
"""
from fastapi import APIRouter, HTTPException, Path
from typing import List

from ..core.db import db
from ..schemas.search import MarketSummary

router = APIRouter(prefix="/markets", tags=["Markets"])


@router.get("", response_model=List[MarketSummary])
def list_markets():
    """Devuelve métricas agregadas por mercado (25 mercados)."""
    sql = """
        SELECT
            market_token,
            city_token,
            COUNT(*)                           AS n_inmuebles,
            MEDIAN(precio_num)                 AS precio_mediano,
            MEDIAN(precio_num / NULLIF(area_m2, 0)) AS precio_m2_mediano,
            MEDIAN(area_m2)                    AS area_mediana
        FROM inmuebles
        WHERE precio_num > 0
          AND market_token IS NOT NULL
        GROUP BY market_token, city_token
        ORDER BY n_inmuebles DESC
    """
    try:
        df = db.query_df(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return [MarketSummary(**row) for row in df.to_dict(orient="records")]


@router.get("/{market_token}", response_model=MarketSummary)
def get_market(
    market_token: str = Path(..., pattern=r"^[\w\-]{1,64}$"),
):
    """Devuelve métricas de un mercado específico."""
    sql = """
        SELECT
            market_token,
            city_token,
            COUNT(*)                           AS n_inmuebles,
            MEDIAN(precio_num)                 AS precio_mediano,
            MEDIAN(precio_num / NULLIF(area_m2, 0)) AS precio_m2_mediano,
            MEDIAN(area_m2)                    AS area_mediana
        FROM inmuebles
        WHERE precio_num > 0
          AND market_token = ?
        GROUP BY market_token, city_token
        LIMIT 1
    """
    try:
        row = db.query_one(sql, [market_token])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if row is None:
        raise HTTPException(status_code=404, detail=f"Mercado '{market_token}' no encontrado.")

    cols = ["market_token", "city_token", "n_inmuebles", "precio_mediano", "precio_m2_mediano", "area_mediana"]
    return MarketSummary(**dict(zip(cols, row)))
