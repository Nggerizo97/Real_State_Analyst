"""
api/schemas/predict.py
======================
Modelos Pydantic para el endpoint /predict.
"""
from typing import Optional
from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    area_m2: float = Field(..., gt=0, description="Área construida en m²")
    habitaciones: float = Field(2.0, ge=0)
    banos: float = Field(1.0, ge=0)
    garajes: float = Field(0.0, ge=0)
    tipo_inmueble: str = Field("apartamento", description="apartamento | casa | lote | oficina | local_comercial | otro")
    estado_inmueble: str = Field("usado", description="usado | nuevo | desconocido")
    city_token: str = Field("bogota", description="token de ciudad normalizado (ej. 'medellin')")
    market_token: Optional[str] = Field(None, description="Si no se envía, se deriva del city_token")
    comuna_mercado: str = Field("comuna_otra")
    sector_mercado: str = Field("sector_otra")
    fuente: str = Field("manual_input")

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
