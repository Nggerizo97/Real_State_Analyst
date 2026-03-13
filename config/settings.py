import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env local
load_dotenv()

# ==================================================================
# AWS Credentials & S3 Settings
# ==================================================================
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "real-state-data-bronze")
S3_BRONZE_PREFIX = "raw"


# ==================================================================
# Top Portales Colombia - Target Configurations
# ==================================================================
# Dejamos placeholders listos para habilitar de forma modular.
PORTALS_CONFIG = {
    "fincaraiz": {
        "base_url": "https://www.fincaraiz.com.co",
        "enabled": True
    },
    "mercadolibre": {
        "base_url": "https://listado.mercadolibre.com.co",
        "enabled": True
    },
    "facebook": {
        "base_url": "https://web.facebook.com/marketplace",
        "enabled": False
    },
    "metrocuadrado": {
        "base_url": "https://www.metrocuadrado.com",
        "enabled": True
    },
    "ciencuadras": {
        "base_url": "https://www.ciencuadras.com",
        "enabled": True
    },
    "properati": {
        "base_url": "https://www.properati.com.co",
        "enabled": True
    },
    "lahaus": {
        "base_url": "https://www.lahaus.com",
        "enabled": False
    },
    "estrenar_vivienda": {
        "base_url": "https://www.estrenarvivienda.com",
        "enabled": False
    },
    "habi": {
        "base_url": "https://habi.co",
        "enabled": False
    },
    "olx": {
        "base_url": "https://www.olx.com.co",
        "enabled": False
    },
    "propiedades_com": {
        "base_url": "https://propiedades.com",
        "enabled": False
    },
    "mitula": {
        "base_url": "https://www.mitula.com.co",
        "enabled": False  # Stub — selectores CSS pendientes
    },
    "bancolombia_tu360": {
        "base_url": "https://inmobiliariotu360.bancolombia.com",
        "enabled": True
    },
    "davivienda": {
        "base_url": "https://www.davivienda.com",
        "enabled": False  # Stub — selectores CSS pendientes
    }
}
