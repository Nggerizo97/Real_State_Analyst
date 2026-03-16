"""
src/utils/checkpoint.py
=======================
Checkpoint de paginación por scraper.
Lee y escribe en S3: checkpoints/<portal>.json

Formato:
{
  "portal": "ciencuadras_usado",
  "last_page": 200,
  "total_scraped": 5600,
  "updated_at": "2026-03-16T20:00:00+00:00"
}

Si el scraper termina limpiamente (sin páginas restantes), borra el checkpoint
para que el próximo run empiece desde 1.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from src.utils.s3_connector import S3Connector

logger = logging.getLogger(__name__)
CHECKPOINT_PREFIX = "checkpoints"


class CheckpointManager:

    def __init__(self, portal_name: str):
        self.portal = portal_name
        self.s3 = S3Connector()
        self.key = f"{CHECKPOINT_PREFIX}/{portal_name}.json"

    def load(self) -> Optional[int]:
        """
        Retorna la última página completada, o None si no hay checkpoint.
        El scraper debe empezar desde load() + 1.
        """
        try:
            resp = self.s3.s3_client.get_object(Bucket=self.s3.bucket, Key=self.key)
            data = json.loads(resp["Body"].read())
            page = data.get("last_page", 0)
            logger.info(
                f"[{self.portal}] Checkpoint encontrado — última página: {page} "
                f"(raspadas: {data.get('total_scraped', '?')})"
            )
            return page
        except self.s3.s3_client.exceptions.NoSuchKey:
            logger.info(f"[{self.portal}] Sin checkpoint — empezando desde página 1.")
            return None
        except Exception as e:
            logger.warning(f"[{self.portal}] Error leyendo checkpoint: {e} — empezando desde 1.")
            return None

    def save(self, last_page: int, total_scraped: int) -> None:
        """Guarda el progreso al finalizar un batch."""
        data = {
            "portal": self.portal,
            "last_page": last_page,
            "total_scraped": total_scraped,
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            self.s3.s3_client.put_object(
                Bucket=self.s3.bucket,
                Key=self.key,
                Body=json.dumps(data, indent=2).encode(),
                ContentType="application/json",
            )
            logger.info(f"[{self.portal}] Checkpoint guardado — página {last_page}.")
        except Exception as e:
            logger.error(f"[{self.portal}] Error guardando checkpoint: {e}")

    def clear(self) -> None:
        """
        Borra el checkpoint cuando el scraper termina limpiamente
        (no hay más páginas). El próximo run empieza desde 1.
        """
        try:
            self.s3.s3_client.delete_object(Bucket=self.s3.bucket, Key=self.key)
            logger.info(f"[{self.portal}] Checkpoint borrado — ciclo completo.")
        except Exception as e:
            logger.warning(f"[{self.portal}] No se pudo borrar checkpoint: {e}")
