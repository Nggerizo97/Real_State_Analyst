"""
api/schemas/search.py
=====================
Modelos Pydantic para /search y /markets.
"""
from typing import Any, Dict, List, Optional
import pandas as pd
from pydantic import BaseModel, Field, field_validator


class SearchRequest(BaseModel):
    city_token: Optional[str] = None
    market_token: Optional[str] = None
    tipo_inmueble: Optional[str] = None
    estado_inmueble: Optional[str] = None
    price_min: Optional[float] = Field(None, ge=0)
    price_max: Optional[float] = Field(None, ge=0)
    area_min: Optional[float] = Field(None, ge=0)
    area_max: Optional[float] = Field(None)
    habitaciones_min: Optional[int] = Field(None, ge=0)
    num_portales_min: Optional[int] = Field(None, ge=1)
    fuentes: Optional[List[str]] = None
    # Paginación
    limit: int = Field(50, ge=1, le=500)
    offset: int = Field(0, ge=0)
    # Ordenamiento
    order_by: str = Field("precio_num", description="Campo por el que ordenar")
    order_dir: str = Field("asc", description="asc | desc")

    model_config = {"json_schema_extra": {
        "example": {
            "city_token": "bogota",
            "tipo_inmueble": "apartamento",
            "price_min": 200_000_000,
            "price_max": 600_000_000,
            "area_min": 50,
            "limit": 50,
            "offset": 0,
        }
    }}


class PropertyItem(BaseModel):
    id: Optional[str] = None
    titulo: Optional[str] = None
    ubicacion_clean: Optional[str] = None
    ubicacion_norm: Optional[str] = None
    tipo_inmueble: Optional[str] = None
    estado_inmueble: Optional[str] = None
    precio_num: Optional[float] = None
    area_m2: Optional[float] = None
    habitaciones: Optional[float] = None
    banos: Optional[float] = None
    garajes: Optional[float] = None
    city_token: Optional[str] = None
    market_token: Optional[str] = None
    comuna_mercado: Optional[str] = None
    sector_mercado: Optional[str] = None
    fuente: Optional[str] = None
    rentabilidad_potencial: Optional[float] = None
    estado_inversion: Optional[str] = None
    num_portales: Optional[float] = None
    dispersion_pct_grupo: Optional[float] = None
    precio_min_grupo: Optional[float] = None
    precio_max_grupo: Optional[float] = None
    score_inversion: Optional[float] = None
    precio_predicho: Optional[float] = None
    first_seen_date: Optional[str] = None
    precio_cambio_pct: Optional[float] = None
    url: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

    @field_validator("first_seen_date", mode="before")
    @classmethod
    def coerce_nat_to_none(cls, v):
        """DuckDB/pandas puede devolver NaT para fechas nulas; Pydantic no lo acepta como str."""
        if v is None:
            return None
        # pandas NaT y numpy NaN
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return str(v) if not isinstance(v, str) else v


class SearchResponse(BaseModel):
    total: int
    offset: int
    limit: int
    items: List[PropertyItem]


class MarketSummary(BaseModel):
    market_token: str
    city_token: str
    n_inmuebles: int
    precio_mediano: Optional[float] = None
    precio_m2_mediano: Optional[float] = None
    area_mediana: Optional[float] = None
