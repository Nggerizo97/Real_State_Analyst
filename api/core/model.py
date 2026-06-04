"""
api/core/model.py
=================
Singleton XGBoost con carga desde S3 y:
  - Verificación de integridad SHA-256 (si el manifest la provee)
  - Carga de modelo JSON desde memoria (sin tempfile)
  - Logging estructurado
"""
import hashlib
import json
import logging
import pickle

import boto3

from .config import get_settings

logger = logging.getLogger(__name__)


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

        # 1. Manifest → clave del modelo champion + checksum opcional
        manifest_resp = client.get_object(Bucket=s.s3_bucket, Key=s.model_manifest_key)
        manifest: dict = json.loads(manifest_resp["Body"].read())
        model_key = manifest.get("champion_model_key", "")
        expected_checksum: str | None = manifest.get("champion_model_sha256")

        if not model_key:
            objs = client.list_objects_v2(
                Bucket=s.s3_bucket, Prefix="models/"
            ).get("Contents", [])
            bundles = sorted(
                [o["Key"] for o in objs if "bundle_v" in o["Key"] and o["Key"].endswith(".pkl")]
            )
            if not bundles:
                raise RuntimeError("No se encontró ningún bundle de modelo en S3.")
            model_key = bundles[-1]
            expected_checksum = None

        self._model_key = model_key

        # 2. Descargar bundle
        raw: bytes = client.get_object(Bucket=s.s3_bucket, Key=model_key)["Body"].read()

        # 3. Verificar integridad si el manifest provee checksum SHA-256
        if expected_checksum:
            actual = hashlib.sha256(raw).hexdigest()
            if actual != expected_checksum:
                raise RuntimeError(
                    f"Fallo de integridad del modelo: checksum no coincide. "
                    f"Esperado: {expected_checksum[:16]}… Obtenido: {actual[:16]}…"
                )
            logger.info("Checksum SHA-256 del modelo verificado.")

        # 4. Deserializar (JSON o Pickle)
        if raw.lstrip()[:1] == b"{":
            import xgboost as xgb

            bundle: dict = json.loads(raw)
            model_data = bundle.get("model")
            if isinstance(model_data, (str, dict)):
                model_json = model_data if isinstance(model_data, str) else json.dumps(model_data)
                reg = xgb.XGBRegressor()
                # Cargar desde bytearray en memoria — sin tempfile
                reg.load_model(bytearray(model_json.encode("utf-8")))
                bundle["model"] = reg
            self._bundle = bundle
        else:
            # Pickle: solo aceptable si checksum fue verificado
            self._bundle = pickle.loads(raw)  # noqa: S301

        mape = self._bundle.get("metrics", {}).get("mape", "N/A")  # type: ignore
        logger.info(
            "Bundle cargado. key=%s | MAPE=%s%% | features=%d",
            model_key, mape, len(self._bundle.get("feature_cols", [])),  # type: ignore
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
