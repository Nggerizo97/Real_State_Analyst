# -*- coding: utf-8 -*-
"""Carga del catalogo de inmuebles desde S3, con geografia y vigencia.

Cambios frente a la version anterior
------------------------------------
1. Ya no hay `except Exception: pass` que caiga en silencio al dataset de
   demostracion. Ese bloque ocultaba cualquier fallo de S3 y la app podia
   quedar mostrando inmuebles inventados con URLs que no existen. Ahora el
   error se muestra y el modo demo es una decision explicita.

2. Se piden solo las columnas que la tabla realmente tiene. Antes se pedia
   una lista fija que incluia `score_inversion`; si el ETL cambiaba el
   esquema, pyarrow lanzaba y la app se iba a demo sin avisar.

3. La geografia se resuelve con el mismo resolver DIVIPOLA del ETL, no con
   un diccionario propio. `src/utils/geo_utils.py` mantenia su propia tabla
   de municipios y se habia desincronizado del catalogo del lakehouse.

4. Se marca la vigencia del aviso y por defecto solo se muestran los
   vigentes, para no publicar enlaces que abren en "Este inmueble no esta
   disponible".
"""

import os

import pandas as pd
import streamlit as st

# La capa geografica se importa de forma perezosa y tolerante a fallos.
#
# Antes se importaba arriba, con lo que cualquier problema en ese subarbol
# (un modulo sin sincronizar, la dimension DIVIPOLA ausente porque .gitignore
# excluia *.parquet, una dependencia que no instalo) tumbaba la app entera en
# el `from src.utils.data_loader import ...` de app.py, con un ImportError
# que Streamlit Cloud muestra censurado y sin pista de la causa.
#
# Ahora el catalogo se carga igual y solo se degrada el enriquecimiento
# geografico, informando en pantalla que falta.
_GEO_IMPORT_ERROR = None

try:
    from src.geo.geo_resolver import get_resolver
except Exception as exc:                                  # pragma: no cover
    get_resolver = None
    _GEO_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

try:
    from src.quality.listing_lifecycle import add_lifecycle_columns
except Exception as exc:                                  # pragma: no cover
    add_lifecycle_columns = None
    _GEO_IMPORT_ERROR = _GEO_IMPORT_ERROR or f"{type(exc).__name__}: {exc}"


def geo_import_error():
    """Detalle del fallo al cargar la capa geografica, o None si todo bien."""
    return _GEO_IMPORT_ERROR

GOLD_TABLE = "gold/app_inmuebles_scored/"
URL_HEALTH_TABLE = "gold/listing_url_health/"

# Columnas deseadas. Se intersectan con el esquema real antes de leer, de
# modo que un cambio en el ETL no rompe la app.
WANTED_COLUMNS = [
    "id_original", "city_token", "market_token", "ubicacion_raw", "ubicacion_norm",
    "precio_num", "area_m2", "habitaciones", "banos", "garajes", "tipo_inmueble",
    "estado_inmueble", "fuente", "url", "titulo", "fecha_extraccion",
    "precio_m2", "rentabilidad_potencial", "score_inversion", "precio_predicho",
    "estado_inversion", "descuento_potencial_cop", "percentil_precio_ciudad",
    "cuota_mensual_est", "precio_m2_vs_mediana_pct", "num_portales",
    "dispersion_pct_grupo", "data_completeness", "first_seen_date",
    # Columnas nuevas del ETL; pueden no existir aun en la primera corrida.
    "cod_dpto", "departamento", "cod_mpio", "municipio", "cod_barrio", "barrio",
    "barrio_texto", "latitud", "longitud", "geo_match_level", "geo_confidence",
    "listing_status", "is_active", "dias_sin_ver",
]


def get_aws_config() -> dict:
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


def _filesystem(config):
    import s3fs

    return s3fs.S3FileSystem(
        key=config.get("aws_access_key_id") or None,
        secret=config.get("aws_secret_access_key") or None,
    )


def _read_table(config, relative, columns=None):
    """Lee una tabla parquet de S3 pidiendo solo columnas existentes."""
    import pyarrow.dataset as ds

    bucket = config.get("s3_bucket_name")
    dataset = ds.dataset(f"{bucket}/{relative}", filesystem=_filesystem(config), format="parquet")
    available = set(dataset.schema.names)
    selected = [name for name in (columns or dataset.schema.names) if name in available]
    return dataset.to_table(columns=selected).to_pandas()


@st.cache_data(ttl=3600, show_spinner="Resolviendo geografía oficial (DIVIPOLA)...")
def _resolve_geography(location_pairs: tuple) -> pd.DataFrame:
    """Resuelve municipio/barrio/coordenadas para pares unicos de texto.

    Se cachea sobre los pares distintos, no sobre las filas: en la tabla real
    hay ~75.000 filas pero muchas menos combinaciones de ubicacion+titulo.
    """
    if get_resolver is None:
        raise RuntimeError(f"Capa geográfica no disponible: {_GEO_IMPORT_ERROR}")
    resolver = get_resolver()
    resolved = [resolver.resolve(ubicacion, titulo) for ubicacion, titulo in location_pairs]
    frame = pd.DataFrame(resolved)
    frame["_ubicacion"] = [pair[0] for pair in location_pairs]
    frame["_titulo"] = [pair[1] for pair in location_pairs]
    return frame


# Marcadores de "sin dato" que llegan como texto y no como null.
_BLANKS = {"", "nan", "none", "null", "<na>", "nat"}


def _blank_to_na(series: pd.Series) -> pd.Series:
    """Convierte los vacios disfrazados de texto en NA de verdad.

    Hace falta porque 05_Batch_Inference escribe el Gold con
    `df[col].fillna("").astype(str)` sobre todas las columnas de tipo objeto.
    Eso convierte los nulls en cadenas vacias, y entonces `barrio.fillna(
    barrio_texto)` no sustituye nada: de 9.176 barrios distintos solo se
    veian 355, porque las 50.957 filas sin barrio oficial traian "" en vez
    de null y nunca caian al texto libre.
    """
    if series is None:
        return None
    cleaned = series.astype("object").where(series.notna(), None)
    return cleaned.map(lambda v: None if v is None or str(v).strip().lower() in _BLANKS else v)


def _display_case(value):
    """Capitaliza solo lo que viene en minusculas.

    Los barrios de DIVIPOLA ya traen su capitalizacion oficial
    ("Cerca de Piedra"); los de texto libre llegan normalizados en
    minusculas ("fonqueta"). Sin esto el selector mezclaba ambos estilos.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.title() if text.islower() else text


def _coalesce_geo(df: pd.DataFrame, primary: str, fallback: str) -> pd.Series:
    left = _blank_to_na(df[primary]) if primary in df.columns else None
    right = _blank_to_na(df[fallback]) if fallback in df.columns else None
    if left is None and right is None:
        return None
    if left is None:
        merged = right
    elif right is None:
        merged = left
    else:
        merged = left.fillna(right)
    return merged.map(_display_case)


def enrich_geography(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega geografia oficial si el ETL todavia no la trae."""
    already_resolved = (
        "municipio" in df.columns and _blank_to_na(df["municipio"]).notna().any()
    )
    if already_resolved:
        # El ETL ya resolvio. Se normalizan los vacios y se arma el nombre
        # que se muestra: barrio oficial DIVIPOLA, o el texto libre depurado.
        df = df.copy()
        for column in ("municipio", "departamento", "barrio", "barrio_texto"):
            if column in df.columns:
                df[column] = _blank_to_na(df[column])
        df["barrio_display"] = _coalesce_geo(df, "barrio", "barrio_texto")
        return df

    if get_resolver is None:
        # Sin resolver no hay geografia, pero el catalogo debe seguir siendo
        # usable: precios, areas y enlaces no dependen de esta capa.
        st.warning(
            "La capa geográfica no se pudo cargar, así que no hay municipio ni "
            f"barrio en esta sesión. Detalle: `{_GEO_IMPORT_ERROR}`"
        )
        df = df.copy()
        for column in ("departamento", "municipio", "barrio", "barrio_texto", "barrio_display"):
            if column not in df.columns:
                df[column] = None
        return df

    ubicacion = df.get("ubicacion_raw", df.get("ubicacion_norm", pd.Series("", index=df.index)))
    titulo = df.get("titulo", pd.Series("", index=df.index))
    pairs = list(dict.fromkeys(zip(ubicacion.astype(str), titulo.astype(str))))
    resolved = _resolve_geography(tuple(pairs))

    df = df.copy()
    df["_ubicacion"] = ubicacion.astype(str)
    df["_titulo"] = titulo.astype(str)
    df = df.merge(resolved, on=["_ubicacion", "_titulo"], how="left", suffixes=("", "_geo"))
    df = df.drop(columns=["_ubicacion", "_titulo"])
    df["barrio_display"] = _coalesce_geo(df, "barrio", "barrio_texto")

    # El resolver devuelve lat/lon; el ETL escribe latitud/longitud. Se
    # unifican aqui para que el mapa funcione venga de donde venga el dato.
    for source, target in (("lat", "latitud"), ("lon", "longitud")):
        if source in df.columns:
            df[target] = df[target].fillna(df[source]) if target in df.columns else df[source]
    return df


@st.cache_data(ttl=1800, show_spinner="Cargando catálogo desde S3...")
def load_inmuebles_data(include_inactive: bool = False):
    """Devuelve (df, origen). Lanza si S3 falla: no cae a demo en silencio."""
    config = get_aws_config()
    if not config.get("s3_bucket_name"):
        raise RuntimeError(
            "Falta s3_bucket_name. Configuralo en .streamlit/secrets.toml "
            "([aws]) o en la variable de entorno S3_BUCKET_NAME."
        )

    df = _read_table(config, GOLD_TABLE, WANTED_COLUMNS)
    if df.empty:
        raise RuntimeError(f"La tabla {GOLD_TABLE} llego vacia.")

    # Vigencia del aviso
    if (
        "is_active" not in df.columns
        and "fecha_extraccion" in df.columns
        and add_lifecycle_columns is not None
    ):
        df = add_lifecycle_columns(df)

    # Cruce con la verificacion de URLs, si el job ya corrio
    try:
        health = _read_table(config, URL_HEALTH_TABLE, ["url", "url_status", "url_checked_at"])
        df = df.merge(health, on="url", how="left")
        df["url_status"] = df["url_status"].fillna("sin_verificar")
        if "is_active" in df.columns:
            df["is_active"] = df["is_active"] & (df["url_status"] != "muerta")
    except Exception:
        df["url_status"] = "sin_verificar"

    df = enrich_geography(df)

    if "precio_m2" not in df.columns or df["precio_m2"].isna().all():
        df["precio_m2"] = pd.to_numeric(df["precio_num"], errors="coerce") / pd.to_numeric(
            df["area_m2"], errors="coerce"
        )

    # Se devuelve el catalogo COMPLETO y el filtrado de vigencia ocurre en la
    # vista (src/utils/filters.py, toggle "Solo avisos vigentes").
    #
    # Filtrar aqui rompia las estadisticas de precio: la pestana de Valoracion
    # construye los comparables por barrio sobre este DataFrame, y con solo los
    # avisos vigentes El Retiro pasaba de 590 registros a 70, con lo que ningun
    # barrio alcanzaba el minimo y la tasacion caia al promedio del municipio.
    # Un aviso retirado ya no sirve como enlace, pero sigue siendo evidencia
    # valida de cuanto vale el metro cuadrado en esa zona.
    total = len(df)
    activos = int(df["is_active"].fillna(False).sum()) if "is_active" in df.columns else total
    origen = f"S3 Live · {activos:,} vigentes de {total:,}"
    return df, origen


def load_demo_data():
    """Dataset sintetico. Solo bajo peticion explicita del usuario.

    Sus URLs son generadas y no resuelven: no debe usarse como respaldo
    automatico de un fallo de S3.
    """
    from .mock_data import _generate_demo_dataset

    df = _generate_demo_dataset()
    df = enrich_geography(df)
    df["is_active"] = True
    df["listing_status"] = "activa"
    df["url_status"] = "demo"
    return df, "Demo local (datos sintéticos)"
