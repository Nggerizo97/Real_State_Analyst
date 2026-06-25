"""
src/utils/model_loader.py
=========================
Carga el modelo campeón desde S3 de forma lazy y memory-safe.

Soporta dos formatos de bundle:
  1. JSON Bundle v8 (preferido): {model_json, preprocessor_pickle, feature_cols, ...}
  2. Pickle legacy (.pkl): joblib/pickle serializado completo

Usa @st.cache_resource para cargar el modelo UNA sola vez por sesión de la app.
"""

import io
import json
from typing import Any, Dict, Optional, Tuple

from src.utils.s3_connector import S3Connector
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Ruta única — debe coincidir con orchestrator.py MANIFEST_KEY
MANIFEST_KEY = "models/manifest.json"
MODELO_PATH = "models/"

# Pickles legacy como último recurso (orden de preferencia)
LEGACY_KEYS = [
    "models/modelo_xgboost_v2.pkl",
    "models/modelo_precios_v2.pkl",
    "models/modelo_precios_v1.pkl",
]


class ModelLoader:
    """
    Carga el modelo campeón desde S3 usando el manifest del orquestador.
    Soporta JSON Bundle v8 (nativo XGBoost Booster) y Pickle legacy.
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
    # Modelo — Carga Unificada
    # ------------------------------------------------------------------

    def load_latest_model(self) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """
        Carga el modelo campeón según el manifest del orquestador.
        Retorna (bundle_dict, manifest_dict).

        El bundle_dict contiene las claves necesarias para scorer.py:
          - "model": xgb.Booster o sklearn Pipeline
          - "preprocessor_pickle" / preprocessor ya deserializado
          - "feature_cols", "strategy", "city_stats", etc.

        Orden de resolución:
          1. manifest → champion_model_key (JSON o Pickle auto-detectado)
          2. Si falla: previous_champion del mismo manifest
          3. Si falla: discovery de bundles en S3 models/
          4. Si falla: pickles legacy en orden de preferencia
          5. Si todo falla: (None, {})
        """
        manifest = self.get_manifest()
        champion_key = manifest.get("champion_model_key")

        if champion_key:
            bundle = self._load_bundle(champion_key)
            if bundle is not None:
                logger.info(f"Campeón cargado: {champion_key}")
                return bundle, manifest

            # Fallback al modelo anterior
            prev_key = manifest.get("previous_champion")
            if prev_key:
                logger.warning(
                    f"Campeón falló ({champion_key}), intentando anterior: {prev_key}"
                )
                bundle = self._load_bundle(prev_key)
                if bundle is not None:
                    return bundle, {**manifest, "_fallback": True, "champion_model_key": prev_key}

        # Discovery: buscar el bundle más reciente en S3
        bundle, key = self._discover_latest_bundle()
        if bundle is not None:
            logger.info(f"Bundle descubierto: {key}")
            return bundle, {**manifest, "champion_model_key": key, "_discovered": True}

        # Fallback legacy (primeros deploys o si el manifest está vacío)
        logger.warning("Sin manifest válido — intentando pickles legacy.")
        for key in LEGACY_KEYS:
            bundle = self._load_pickle_as_bundle(key)
            if bundle is not None:
                logger.info(f"Modelo legacy cargado: {key}")
                return bundle, {"champion_model_key": key, "_legacy": True}

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
    # Internos
    # ------------------------------------------------------------------

    def _load_bundle(self, key: str) -> Optional[Dict[str, Any]]:
        """Auto-detecta formato (JSON vs Pickle) y carga el bundle."""
        try:
            response = self.s3.s3_client.get_object(Bucket=self.s3.bucket, Key=key)
            raw_data = response["Body"].read()

            # JSON Bundle v8 — preferido y más ligero
            if raw_data.startswith(b"{"):
                return self._parse_json_bundle(raw_data)

            # Pickle fallback
            return self._parse_pickle_bundle(raw_data)
        except Exception as e:
            logger.debug(f"No se pudo cargar bundle {key}: {e}")
            return None

    def _parse_json_bundle(self, raw_data: bytes) -> Optional[Dict[str, Any]]:
        """Parsea un JSON bundle v8 con Booster nativo."""
        import pickle
        import xgboost as xgb

        bundle = json.loads(raw_data)

        # Deserializar el Booster nativo
        model_json = bundle.get("model_json")
        if model_json:
            bst = xgb.Booster()
            if isinstance(model_json, str):
                model_bytes = model_json.encode("utf-8")
            else:
                model_bytes = model_json
            bst.load_model(bytearray(model_bytes))
            bundle["model"] = bst

        # Deserializar el preprocessor (pickle codificado en latin1)
        preprocessor_blob = bundle.get("preprocessor_pickle")
        if preprocessor_blob:
            try:
                if isinstance(preprocessor_blob, str):
                    preprocessor_blob = preprocessor_blob.encode("latin1")
                bundle["_preprocessor"] = pickle.loads(preprocessor_blob)
            except Exception as e:
                logger.warning(f"Error deserializando preprocessor: {e}")

        # Convertir list-of-dicts a DataFrames para stats
        import pandas as pd
        for stats_key in ["city_stats", "comuna_stats", "segment_stats",
                          "micro_stats", "sector_stats", "hab_stats",
                          "fuente_ratio_stats", "fuente_segmento_ratio_stats"]:
            val = bundle.get(stats_key)
            if isinstance(val, list) and val:
                bundle[stats_key] = pd.DataFrame(val)

        # Liberar el JSON crudo de la memoria inmediatamente
        del raw_data
        logger.info(f"JSON bundle parseado — keys: {list(bundle.keys())[:10]}")
        return bundle

    def _parse_pickle_bundle(self, raw_data: bytes) -> Optional[Dict[str, Any]]:
        """Parsea un bundle serializado con pickle/joblib."""
        import joblib
        bundle = joblib.load(io.BytesIO(raw_data))
        del raw_data

        if isinstance(bundle, dict) and "model" in bundle:
            return bundle

        # Wrap simple model object
        return {
            "model": bundle,
            "strategy": "absolute",
            "feature_cols": [],
        }

    def _load_pickle_as_bundle(self, key: str) -> Optional[Dict[str, Any]]:
        """Intenta cargar un pickle legacy como bundle."""
        try:
            response = self.s3.s3_client.get_object(Bucket=self.s3.bucket, Key=key)
            return self._parse_pickle_bundle(response["Body"].read())
        except Exception as e:
            logger.debug(f"No se pudo cargar legacy {key}: {e}")
            return None

    def _discover_latest_bundle(self) -> Tuple[Optional[Dict[str, Any]], str]:
        """Busca el bundle más reciente en S3 models/ si no hay manifest."""
        try:
            objs = self.s3.s3_client.list_objects_v2(
                Bucket=self.s3.bucket, Prefix=MODELO_PATH
            ).get("Contents", [])

            # Buscar JSON bundles primero, luego pickles
            bundles = sorted(
                [o["Key"] for o in objs
                 if "bundle" in o["Key"] and (o["Key"].endswith(".json") or o["Key"].endswith(".pkl"))],
                reverse=True,
            )
            for key in bundles:
                bundle = self._load_bundle(key)
                if bundle is not None:
                    return bundle, key
        except Exception as e:
            logger.debug(f"Discovery fallida: {e}")
        return None, ""
