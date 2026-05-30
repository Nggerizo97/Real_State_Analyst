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
        s3_bucket_name: str = ""
        model_manifest_key: str = "models/manifest.json"
        api_workers: int = 2

        model_config = SettingsConfigDict(env_file=".env", extra="ignore")

        def model_post_init(self, __context) -> None:
            # Fallback a S3_BUCKET_NAME si s3_bucket está vacío
            if not self.s3_bucket:
                self.s3_bucket = self.s3_bucket_name or os.getenv("S3_BUCKET", os.getenv("S3_BUCKET_NAME", ""))


except ImportError:
    # Fallback: leer directamente de os.environ si pydantic-settings no está instalado
    class Settings:  # type: ignore
        def __init__(self):
            self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "")
            self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
            self.aws_region = os.getenv("AWS_REGION", "us-east-1")
            self.s3_bucket = os.getenv("S3_BUCKET", os.getenv("S3_BUCKET_NAME", ""))
            self.model_manifest_key = os.getenv("MODEL_MANIFEST_KEY", "models/manifest.json")
            self.api_workers = int(os.getenv("API_WORKERS", "2"))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

