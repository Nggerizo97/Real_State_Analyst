"""
api/core/config.py
==================
Configuración central leída desde variables de entorno.
En desarrollo usa un archivo .env; en producción las inyecta Docker/Lightsail.
"""
import os
from functools import lru_cache

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class Settings(BaseSettings):
        aws_access_key_id: str = ""
        aws_secret_access_key: str = ""
        aws_region: str = "us-east-1"
        s3_bucket: str = ""
        model_manifest_key: str = "models/manifest.json"
        api_workers: int = 2

        model_config = SettingsConfigDict(env_file=".env", extra="ignore")

except ImportError:
    # Fallback: leer directamente de os.environ si pydantic-settings no está instalado
    class Settings:  # type: ignore
        aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "")
        aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        aws_region: str = os.getenv("AWS_REGION", "us-east-1")
        s3_bucket: str = os.getenv("S3_BUCKET", "")
        model_manifest_key: str = os.getenv("MODEL_MANIFEST_KEY", "models/manifest.json")
        api_workers: int = int(os.getenv("API_WORKERS", "2"))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
