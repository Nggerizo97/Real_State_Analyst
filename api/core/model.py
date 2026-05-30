"""
api/core/model.py
=================
Singleton que carga el bundle XGBoost una sola vez al arrancar la API.

El bundle contiene:
  - model          : XGBRegressor entrenado
  - feature_cols   : lista de features en el orden correcto
  - city_stats / segment_stats / ... : tablas de market features (para score_single)
  - metrics        : {"mape": 20.8, ...}
  - strategy       : "absolute" | "residual"
"""
import io
import json
import os
import pickle
import tempfile

import boto3

from .config import get_settings


class ModelRegistry:
    def __init__(self) -> None:
        self._bundle: dict | None = None
        self._model_key: str = ""

    # ------------------------------------------------------------------
    def load(self) -> None:
        """Descarga el bundle champion desde S3 y lo deja en memoria."""
        s = get_settings()
        client_kwargs = {"region_name": s.aws_region}
        if s.aws_access_key_id and s.aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = s.aws_access_key_id
            client_kwargs["aws_secret_access_key"] = s.aws_secret_access_key

        client = boto3.client("s3", **client_kwargs)

        # 1. Manifest → obtener la clave del modelo champion
        manifest_resp = client.get_object(Bucket=s.s3_bucket, Key=s.model_manifest_key)
        manifest: dict = json.loads(manifest_resp["Body"].read())
        model_key = manifest.get("champion_model_key", "")

        if not model_key:
            # Discovery fallback
            objs = client.list_objects_v2(
                Bucket=s.s3_bucket, Prefix="models/"
            ).get("Contents", [])
            bundles = sorted(
                [o["Key"] for o in objs if "bundle_v" in o["Key"] and o["Key"].endswith(".pkl")]
            )
            if not bundles:
                raise RuntimeError("No se encontró ningún bundle de modelo en S3.")
            model_key = bundles[-1]

        self._model_key = model_key

        # 2. Descargar bundle (JSON o Pickle)
        raw = client.get_object(Bucket=s.s3_bucket, Key=model_key)["Body"].read()

        if raw.lstrip()[:1] == b"{":
            # Formato JSON
            import xgboost as xgb

            bundle: dict = json.loads(raw)
            model_data = bundle.get("model")
            if isinstance(model_data, (str, dict)):
                model_json = model_data if isinstance(model_data, str) else json.dumps(model_data)
                reg = xgb.XGBRegressor()
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tf:
                    tf.write(model_json)
                    tf_path = tf.name
                reg.load_model(tf_path)
                os.remove(tf_path)
                bundle["model"] = reg
            self._bundle = bundle
        else:
            # Formato Pickle
            self._bundle = pickle.loads(raw)

        mape = self._bundle.get("metrics", {}).get("mape", "N/A")  # type: ignore
        print(
            f"[MODEL] Bundle cargado. key={model_key} | MAPE={mape}% | "
            f"features={len(self._bundle.get('feature_cols', []))}",  # type: ignore
            flush=True,
        )

    # ------------------------------------------------------------------
    @property
    def bundle(self) -> dict:
        if self._bundle is None:
            raise RuntimeError("Modelo no cargado. Llama load() en el lifespan.")
        return self._bundle

    @property
    def model_key(self) -> str:
        return self._model_key

    @property
    def mape(self) -> float:
        return float(self.bundle.get("metrics", {}).get("mape", 20.0))


# Singleton global
model_registry = ModelRegistry()
