import os
import streamlit as st
import pandas as pd
from .mock_data import _generate_demo_dataset
from .geo_utils import get_departamento, resolve_municipio_and_sector

def _get_aws_config() -> dict:
    try:
        if "aws" in st.secrets:
            return dict(st.secrets["aws"])
    except Exception:
        pass
    return {
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
        "s3_bucket_name": os.getenv("S3_BUCKET_NAME", ""),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    }

aws_config = _get_aws_config()

# Probar conexión básica
s3_active = False
try:
    import s3fs
    import pyarrow.dataset as ds
    import pyarrow.compute as pc
    import pyarrow as pa
    
    bucket_name = aws_config.get("s3_bucket_name")
    if bucket_name:
        fs = s3fs.S3FileSystem(
            key=aws_config.get("aws_access_key_id") or None,
            secret=aws_config.get("aws_secret_access_key") or None
        )
        if fs.exists(f"{bucket_name}/gold/"):
            s3_active = True
except Exception:
    s3_active = False

@st.cache_resource(show_spinner=False)
def load_inmuebles_data():
    """Carga los datos desde S3 si está disponible, o cae al mock de demostración."""
    if s3_active:
        try:
            import s3fs
            import pyarrow.dataset as ds
            bucket = aws_config.get("s3_bucket_name")
            fs = s3fs.S3FileSystem(
                key=aws_config.get("aws_access_key_id") or None,
                secret=aws_config.get("aws_secret_access_key") or None
            )
            s3_path = f"{bucket}/gold/app_inmuebles_scored/"
            
            # Cargar un subset inicial para no agotar la memoria en la versión light
            dataset = ds.dataset(s3_path, filesystem=fs, format="parquet")
            table = dataset.to_table(
                columns=["city_token", "market_token", "ubicacion_norm", "precio_num", 
                         "area_m2", "habitaciones", "banos", "garajes", "tipo_inmueble", 
                         "estado_inmueble", "fuente", "url", "titulo", "rentabilidad_potencial", "score_inversion"]
            )
            df = table.to_pandas()
            
            # Limpiar columnas
            if "ubicacion_norm" in df.columns:
                df["ubicacion_clean"] = df["ubicacion_norm"].astype(str).str.replace(r"\|.*", "", regex=True).str.strip()
            else:
                df["ubicacion_clean"] = "Desconocida"
                
            df["precio_m2"] = df["precio_num"] / df["area_m2"]
            df["departamento"] = df.apply(get_departamento, axis=1)
            
            # Resolver departamento, municipio y sector
            resolved = df.apply(resolve_municipio_and_sector, axis=1)
            df["departamento"] = resolved[0]
            df["municipio"] = resolved[1]
            df["sector"] = resolved[2]
            return df, "S3 Live"
        except Exception:
            pass
            
    # Fallback
    df = _generate_demo_dataset()
    df["departamento"] = df.apply(get_departamento, axis=1)
    
    # Resolver departamento, municipio y sector para demo
    resolved = df.apply(resolve_municipio_and_sector, axis=1)
    df["departamento"] = resolved[0]
    df["municipio"] = resolved[1]
    df["sector"] = resolved[2]
    return df, "Local Demo"
