"""
api/routers/predict.py
======================
POST /predict
  → Recibe features de un inmueble, devuelve valoración XGBoost + rango de confianza.
"""
import sys
import os

from fastapi import APIRouter, HTTPException

# Asegurarse de que el src/ del proyecto padre esté en el path
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.scorer import score_single  # type: ignore

from ..core.model import model_registry
from ..schemas.predict import PredictRequest, PredictResponse

router = APIRouter(prefix="/predict", tags=["Predict"])


@router.post("", response_model=PredictResponse)
def predict(req: PredictRequest) -> PredictResponse:
    """
    Valora un inmueble individual.

    - Los campos `titulo`, `ubicacion_norm`, `ubicacion_clean` no son requeridos
      en la petición (el modelo v8 no los usa como feature directa).
    - `market_token` se toma igual al `city_token` si no se envía.
    """
    row = req.model_dump()
    # Normalizar market_token
    if not row.get("market_token"):
        row["market_token"] = row["city_token"]

    # Campos dummy que score_single / score_dataframe esperan
    row.setdefault("precio_num", 0.0)
    row.setdefault("num_portales", 0.0)
    row.setdefault("dispersion_pct_grupo", 0.0)
    row.setdefault("precio_desviacion_grupo_pct", 0.0)
    row.setdefault("data_completeness", 0.0)
    row.setdefault("titulo", "")
    row.setdefault("ubicacion_norm", "")
    row.setdefault("ubicacion_clean", "")

    try:
        result = score_single(row, model_registry.bundle)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error de inferencia: {exc}") from exc

    if "error" in result:
        raise HTTPException(status_code=422, detail=result["error"])

    if "mape_pct" not in result:
        result["mape_pct"] = model_registry.mape
    if "model_key" not in result:
        result["model_key"] = model_registry.model_key

    return PredictResponse(**result)
