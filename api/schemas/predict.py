"""
api/schemas/predict.py
======================
Modelos Pydantic para el endpoint /predict.
"""
from typing import Literal, Optional

from pydantic import BaseModel, Field

_TIPOS = Literal["apartamento", "casa", "lote", "oficina", "local_comercial", "otro"]
_ESTADOS = Literal["usado", "nuevo", "desconocido"]


class PredictRequest(BaseModel):
    area_m2: float = Field(..., gt=0, le=50_000, description="Área construida en m²")
    habitaciones: float = Field(2.0, ge=0, le=30)
    banos: float = Field(1.0, ge=0, le=20)
    garajes: float = Field(0.0, ge=0, le=20)
    tipo_inmueble: _TIPOS = Field("apartamento", description="Tipo de inmueble")
    estado_inmueble: _ESTADOS = Field("usado", description="Estado del inmueble")
    city_token: str = Field("bogota", max_length=64, description="Token de ciudad normalizado")
    market_token: Optional[str] = Field(None, max_length=64)
    comuna_mercado: str = Field("comuna_otra", max_length=64)
    sector_mercado: str = Field("sector_otra", max_length=64)
    fuente: str = Field("manual_input", max_length=64)

    model_config = {"json_schema_extra": {
        "example": {
            "area_m2": 85,
            "habitaciones": 3,
            "banos": 2,
            "garajes": 1,
            "tipo_inmueble": "apartamento",
            "estado_inmueble": "usado",
            "city_token": "medellin",
            "market_token": "valle_aburra",
            "comuna_mercado": "el_poblado",
            "sector_mercado": "sector_otra",
        }
    }}


class PredictResponse(BaseModel):
    valor_predicho: float
    rango_low: float
    rango_high: float
    precio_m2_pred: float
    mape_pct: float
    estado: Optional[str] = None
    model_key: Optional[str] = None
