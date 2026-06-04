"""
api/schemas/search.py
=====================
Modelos Pydantic para /search y /markets.
"""
import math
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class SearchRequest(BaseModel):
    city_token: Optional[str] = Field(None, max_length=64)
    market_token: Optional[str] = Field(None, max_length=64)
    tipo_inmueble: Optional[str] = Field(None, max_length=64)
    estado_inmueble: Optional[str] = Field(None, max_length=32)
    price_min: Optional[float] = Field(None, ge=0)
    price_max: Optional[float] = Field(None, ge=0, le=100_000_000_000)  # 100 B COP máx
    area_min: Optional[float] = Field(None, ge=0)
    area_max: Optional[float] = Field(None, ge=0, le=50_000)            # 50 000 m² máx
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

    @field_validator("fuentes", mode="before")
    @classmethod
    def limit_fuentes_size(cls, v):
        if v is not None and len(v) > 20:
            raise ValueError("fuentes no puede tener más de 20 elementos")
        return v


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
        """Convierte pandas NaT, NaN float y valores no-string a None."""
        if v is None:
            return None
        # Detectar pandas NaT sin importar pandas
        if type(v).__name__ == "NaTType":
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
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
