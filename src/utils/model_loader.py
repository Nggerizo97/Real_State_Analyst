"""
src/utils/model_loader.py
=========================
Carga el modelo campeón leyendo models/manifest.json (escrito por el orquestador).
Nunca usa paths hardcodeados — siempre consulta el manifest primero.
"""

import io
import json
import joblib
from typing import Any, Dict, Optional, Tuple

from src.utils.s3_connector import S3Connector
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Ruta única — debe coincidir con orchestrator.py MANIFEST_KEY
MANIFEST_KEY = "models/manifest.json"

# Pickles legacy como último recurso (orden de preferencia)
LEGACY_KEYS = [
    "models/modelo_xgboost_v2.pkl",
    "models/modelo_precios_v2.pkl",
    "models/modelo_precios_v1.pkl",
]


class ModelLoader:
    """
    Carga el modelo campeón desde S3 usando el manifest del orquestador.
    Provee fallback al modelo anterior si el campeón falla.
    """

    def __init__(self):
        self.s3 = S3Connector()

    # ------------------------------------------------------------------
    # Manifest
    # ------------------------------------------------------------------

    def get_manifest(self) -> Dict[str, Any]:
        """
        Lee models/manifest.json desde S3.
        Retorna dict vacío si no existe — el app muestra "N/A" en el badge.
        """
        try:
            response = self.s3.s3_client.get_object(
                Bucket=self.s3.bucket, Key=MANIFEST_KEY
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        except self.s3.s3_client.exceptions.NoSuchKey:
            logger.info("manifest.json no encontrado — primer deploy pendiente.")
            return {}
        except Exception as e:
            logger.warning(f"No se pudo leer el manifest: {e}")
            return {}

    # ------------------------------------------------------------------
    # Modelo
    # ------------------------------------------------------------------

    def load_latest_model(self) -> Tuple[Optional[Any], Dict[str, Any]]:
        """
        Carga el modelo campeón según el manifest del orquestador.
        Retorna (modelo, manifest_dict).

        Orden de resolución:
          1. models/manifest.json → champion_model_key
          2. Si falla: previous_champion del mismo manifest
          3. Si falla: pickles legacy en orden de preferencia
          4. Si todo falla: (None, {})
        """
        manifest = self.get_manifest()
        champion_key = manifest.get("champion_model_key")

        if champion_key:
            model = self._load_pickle(champion_key)
            if model is not None:
                logger.info(f"Campeón cargado: {champion_key}")
                return model, manifest

            # Fallback al modelo anterior
            prev_key = manifest.get("previous_champion")
            if prev_key:
                logger.warning(
                    f"Campeón falló ({champion_key}), intentando anterior: {prev_key}"
                )
                model = self._load_pickle(prev_key)
                if model is not None:
                    return model, {**manifest, "_fallback": True, "champion_model_key": prev_key}

        # Fallback legacy (primeros deploys o si el manifest está vacío)
        logger.warning("Sin manifest válido — intentando pickles legacy.")
        for key in LEGACY_KEYS:
            model = self._load_pickle(key)
            if model is not None:
                logger.info(f"Modelo legacy cargado: {key}")
                return model, {"champion_model_key": key, "_legacy": True}

        logger.error("No se pudo cargar ningún modelo.")
        return None, {}

    # ------------------------------------------------------------------
    # Helpers de UI — claves unificadas con el orquestador
    # ------------------------------------------------------------------

    @staticmethod
    def get_badge_data(manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrae los campos para el badge del sidebar.
        Usa las claves que escribe el orquestador (deployed_at, metrics.mape).
        """
        from datetime import datetime, timezone

        metrics = manifest.get("metrics", {})
        mape = metrics.get("mape")
        mape_str = f"{mape:.1f}%" if isinstance(mape, (int, float)) else "N/A"

        deployed_at = manifest.get("deployed_at", "")
        if deployed_at:
            try:
                dt = datetime.fromisoformat(deployed_at)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                delta_s = int((datetime.now(tz=timezone.utc) - dt).total_seconds())
                if delta_s < 3600:
                    freshness = f"hace {delta_s // 60}m"
                elif delta_s < 86400:
                    freshness = f"hace {delta_s // 3600}h"
                else:
                    freshness = f"hace {delta_s // 86400}d"
            except Exception:
                freshness = deployed_at[:10]
        else:
            freshness = "N/A"

        model_key = manifest.get("champion_model_key", "")
        model_name = model_key.split("/")[-1] if model_key else "N/A"

        return {
            "model_name": model_name,
            "mape": mape_str,
            "train_size": metrics.get("train_size", 0),
            "freshness": freshness,
            "is_fallback": manifest.get("_fallback", False),
            "is_legacy": manifest.get("_legacy", False),
        }

    # ------------------------------------------------------------------
    # Interno
    # ------------------------------------------------------------------

    def _load_pickle(self, key: str) -> Optional[Any]:
        try:
            response = self.s3.s3_client.get_object(Bucket=self.s3.bucket, Key=key)
            return joblib.load(io.BytesIO(response["Body"].read()))
        except Exception as e:
            logger.debug(f"No se pudo cargar {key}: {e}")
            return None
