"""
app.py — Real Estate Analyst Colombia
======================================
4 apartados:
  1. Asesor Financiero — candidatos según perfil de crédito
  2. Asesor de Inversión — mejor opción según datos cross-portal y región
  3. Visión de Compra — inteligencia de mercado Colombia
  4. Valoración — estimación por características + análisis de imagen con LLM
"""

import io
import json
import os
import re
import sys
import tempfile
import time
import tracemalloc
import warnings

try:
    import requests as _requests
except ImportError:
    _requests = None  # type: ignore
import s3fs
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc

# Asegurar que el directorio raíz está en el path para imports
sys.path.append(os.getcwd())

from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
import pickle
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from openai import OpenAI

from src.utils.scorer import score_dataframe, score_single

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Real Estate Analyst — Colombia",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS & Secrets Setup ───────────────────────────────────────────
# Intentar cargar CSS pero no crashear si falta en el repo
if os.path.exists("style.css"):
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

def _get_aws_config() -> dict:
    """Lee config AWS desde st.secrets (Streamlit Cloud / local) o variables de entorno
    (ECS con IAM Task Role). Nunca lanza excepcion — siempre devuelve un dict.

    En ECS el dict de fallback NO incluye credenciales explícitas: boto3 y s3fs
    detectan automáticamente el metadata endpoint del contenedor y usan el Task Role.
    """
    try:
        if "aws" in st.secrets:
            return dict(st.secrets["aws"])
    except Exception:
        pass
    # Fallback para ECS: solo region y bucket; las credenciales las provee el IAM Task Role.
    return {
        "aws_region":    os.getenv("AWS_REGION", "us-east-1"),
        "s3_bucket_name": os.getenv("S3_BUCKET_NAME", ""),
    }

aws_config = _get_aws_config()
if not aws_config.get("s3_bucket_name"):
    st.error("Falta el nombre del bucket S3. Define S3_BUCKET_NAME como variable de entorno en ECS o agrega s3_bucket_name en [aws] de secrets.toml.")
    st.stop()

# Constantes de color para gráficas — NUNCA usar "white" en paper_bgcolor
_BG   = "#1e1e2a"
_PLOT = "#16161f"
_GRID = "#2a2a3a"
_TEXT = "#c8c4bc"
_MUTED_TEXT = "#7a7a8c"

def dark_layout(fig, height=380, **extra):
    """Aplica tema oscuro consistente a cualquier figura Plotly."""
    layout_params = dict(
        paper_bgcolor=_BG, plot_bgcolor=_PLOT,
        font=dict(family="DM Sans", size=11, color=_TEXT),
        height=height, margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(gridcolor=_GRID, linecolor=_GRID, zerolinecolor=_GRID,
                   tickfont=dict(color=_TEXT), title_font=dict(color=_MUTED_TEXT)),
        yaxis=dict(gridcolor=_GRID, linecolor=_GRID, zerolinecolor=_GRID,
                   tickfont=dict(color=_TEXT), title_font=dict(color=_MUTED_TEXT)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=_TEXT)),
    )
    layout_params.update(extra)
    fig.update_layout(**layout_params)
    return fig

def fmt_cop(val):
    """Formatea un número de forma elegante como COP con separador de puntos."""
    if pd.isna(val) or val is None:
        return "—"
    return f"${int(val):,}".replace(",", ".")

# ══════════════════════════════════════════════════════════════════
# API CLIENT (opcional — activo sólo si REA_API_URL está definido)
# Cuando la variable de entorno REA_API_URL apunta al backend FastAPI,
# las consultas de búsqueda y predicción se delegan al API en lugar de
# acceder directamente a S3 / PyArrow.  Si la variable no existe o el
# módulo requests no está instalado, se usa el camino directo (S3).
# ══════════════════════════════════════════════════════════════════

REA_API_URL: str = os.getenv("REA_API_URL", "").rstrip("/")
_API_TIMEOUT = 30  # segundos


def _api_available() -> bool:
    return bool(REA_API_URL) and _requests is not None


@st.cache_data(show_spinner=False, ttl=120)
def api_healthcheck() -> bool:
    if not _api_available():
        return False
    try:
        resp = _requests.get(f"{REA_API_URL}/health", timeout=5)
        resp.raise_for_status()
        return True
    except Exception:
        return False


@st.cache_data(show_spinner=False, ttl=1800)
def api_search_metadata() -> dict:
    if not _api_available():
        return {}
    try:
        resp = _requests.get(f"{REA_API_URL}/search/metadata", timeout=_API_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.warning(f"API /search/metadata no disponible: {exc}")
        return {}


@st.cache_data(show_spinner=False, ttl=1800)
def api_catalog_summary() -> dict:
    if not _api_available():
        return {}
    try:
        resp = _requests.get(f"{REA_API_URL}/search/summary", timeout=_API_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.warning(f"API /search/summary no disponible: {exc}")
        return {}


@st.cache_data(show_spinner=False, ttl=1800)
def api_markets() -> pd.DataFrame:
    if not _api_available():
        return pd.DataFrame()
    try:
        resp = _requests.get(f"{REA_API_URL}/markets", timeout=_API_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as exc:
        st.warning(f"API /markets no disponible: {exc}")
        return pd.DataFrame()


def api_search(
    cities: list,
    price_min: float,
    price_max: float,
    markets: list | None = None,
    tipo_inmueble: str | None = None,
    estado_inmueble: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    habitaciones_min: int | None = None,
    num_portales_min: int | None = None,
    limit: int = 500,
    offset: int = 0,
) -> pd.DataFrame:
    """Llama a POST /search y devuelve un DataFrame compatible con el formato Gold."""
    queries = cities or markets or [None]
    frames = []

    for token in queries:
        payload = {
            "price_min": price_min,
            "price_max": price_max,
            "limit": limit,
            "offset": offset,
        }
        if cities and token:
            payload["city_token"] = token
        elif markets and token:
            payload["market_token"] = token
        if tipo_inmueble:
            payload["tipo_inmueble"] = tipo_inmueble
        if estado_inmueble:
            payload["estado_inmueble"] = estado_inmueble
        if area_min is not None:
            payload["area_min"] = area_min
        if area_max is not None:
            payload["area_max"] = area_max
        if habitaciones_min is not None:
            payload["habitaciones_min"] = habitaciones_min
        if num_portales_min is not None:
            payload["num_portales_min"] = num_portales_min

        try:
            resp = _requests.post(
                f"{REA_API_URL}/search",
                json=payload,
                timeout=_API_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            if items:
                frames.append(pd.DataFrame(items))
        except Exception as exc:
            st.warning(f"API no disponible, usando S3 directo: {exc}")
            return pd.DataFrame()

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["url"], keep="first") if "url" in merged.columns else merged
    return _clean_gold(merged)


def api_predict(row: dict) -> dict | None:
    """Llama a POST /predict y devuelve el dict de valoración."""
    if not _api_available():
        return None
    try:
        resp = _requests.post(
            f"{REA_API_URL}/predict",
            json=row,
            timeout=_API_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.warning(f"API /predict falló, usando scorer local: {exc}")
        return None


# ══════════════════════════════════════════════════════════════════
# CLIENTES Y CARGA (S3, Bedrock, Ollama)
# ══════════════════════════════════════════════════════════════════

# 1. Configuración S3 / AWS
@st.cache_resource(show_spinner=False)
def get_s3():
    # Credenciales vacías → None → boto3 usa el IAM Task Role (ECS) o
    # las variables de entorno AWS_* / el perfil local en desarrollo.
    key    = aws_config.get("aws_access_key_id")    or None
    secret = aws_config.get("aws_secret_access_key") or None
    return boto3.client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=aws_config.get("aws_region", "us-east-1"),
    )

# 2. Configuración Bedrock (Llama 3.1)
@st.cache_resource(show_spinner=False)
def get_bedrock():
    try:
        key    = aws_config.get("aws_access_key_id")    or None
        secret = aws_config.get("aws_secret_access_key") or None
        return boto3.client(
            "bedrock-runtime",
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            region_name=aws_config.get("aws_region", "us-east-1"),
        )
    except Exception:
        return None

# 3. Configuración Ollama (Fallback local)
try:
    _llm_cfg = st.secrets.get("llm", {})
except Exception:
    _llm_cfg = {}
llm_client = OpenAI(
    base_url=_llm_cfg.get("api_base", "http://localhost:11434/v1"),
    api_key=_llm_cfg.get("api_key", "ollama"),
)
LLM_MODEL = _llm_cfg.get("model_name", "llama3.1") # Ollama model
BEDROCK_MODEL_ID = _llm_cfg.get("bedrock_model_id", "us.meta.llama3-1-8b-instruct-v1:0")

def call_llm(messages, max_tokens=1000, stream=False):
    """
    Mediador que decide si usar Bedrock (Cloud) o Ollama (Local).
    Soporta texto e imágenes (multimodal).
    """
    bedrock = get_bedrock()
    if bedrock:
        try:
            bedrock_messages = []
            system_prompts = []
            
            for m in messages:
                content_list = []
                # Manejar entrada multimodal (lista de dicts con type text/image)
                if isinstance(m["content"], list):
                    for part in m["content"]:
                        if part["type"] == "text":
                            content_list.append({"text": part["text"]})
                        elif part["type"] == "image_url":
                            # Bedrock espera bytes base64 directos, no URLs en el Converse API
                            pass
                else:
                    content_list = [{"text": m["content"]}]
                
                if m["role"] == "system":
                    system_prompts.append({"text": m["content"]})
                else:
                    bedrock_messages.append({
                        "role": "user" if m["role"] == "user" else "assistant",
                        "content": content_list
                    })
            
            # Validar que la conversación NO esté vacía y empiece por el usuario
            if not bedrock_messages:
                return "Error: No hay mensajes para el LLM."
            
            # Si el primer mensaje no es usuario, Bedrock falla. Insertamos un dummy o saltamos.
            if bedrock_messages[0]["role"] != "user":
                bedrock_messages.insert(0, {"role": "user", "content": [{"text": "Hola"}]})
            
            # Construir argumentos para la llamada de forma dinámica
            converse_kwargs = {
                "modelId": BEDROCK_MODEL_ID,
                "messages": bedrock_messages,
                "inferenceConfig": {"maxTokens": max_tokens, "temperature": 0.1}
            }
            if system_prompts:
                converse_kwargs["system"] = system_prompts
                
            response = bedrock.converse(**converse_kwargs)
            return response["output"]["message"]["content"][0]["text"]
        except Exception as e:
            st.sidebar.warning(f"Bedrock falló: {e}. Intentando Ollama...")

    # Fallback a Ollama / OpenAI Compatible
    try:
        # El cliente de OpenAI ya maneja 'system' dentro de messages
        resp = llm_client.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            max_tokens=max_tokens,
            temperature=0.1,
            stream=stream
        )
        if stream:
            return resp
        return resp.choices[0].message.content.strip()
    except Exception:
        return "Error: Ningún servicio de LLM disponible."

def llm_ready():
    """Verifica si alguno de los LLM está respondiendo."""
    if get_bedrock(): return True
    try:
        llm_client.with_options(timeout=1.0).models.list()
        return True
    except Exception:
        return False

MODELO_PATH = "models/"
MANIFEST_KEY = "models/manifest.json"



@st.cache_data(show_spinner=False, ttl=3600)
def load_manifest():
    """Carga solo el manifest (JSON) de S3."""
    try:
        s3 = get_s3()
        bucket = aws_config.get("s3_bucket_name", "")
        return json.loads(
            s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)["Body"].read()
        )
    except Exception:
        return {}

def load_model_bundle(manifest=None):
    """Carga el bundle pesado (.pkl) de S3."""
    s3 = get_s3()
    bucket = aws_config.get("s3_bucket_name", "")
    
    if not manifest:
        manifest = load_manifest()
    
    key = manifest.get("champion_model_key", "")
    if not key:
        # Fallback a discovery
        objs = s3.list_objects_v2(Bucket=bucket, Prefix=MODELO_PATH).get("Contents", [])
        bundles = sorted([o["Key"] for o in objs if "bundle_v" in o["Key"] and o["Key"].endswith(".pkl")])
        if not bundles: return None
        key = bundles[-1]

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        raw_data = resp["Body"].read()

        # Detección automática de formato (JSON Bundle vs Pickle)
        if raw_data.startswith(b"{"):
            import xgboost as xgb
            import tempfile
            bundle = json.loads(raw_data)

            # Formato v8: booster nativo en "model_json" + preprocessor en "preprocessor_pickle"
            if "model_json" in bundle:
                bst = xgb.Booster()
                model_json_data = bundle["model_json"]
                if isinstance(model_json_data, str):
                    model_json_bytes = model_json_data.encode("utf-8")
                else:
                    model_json_bytes = model_json_data
                bst.load_model(bytearray(model_json_bytes))
                bundle["model"] = bst  # scorer usa bundle["model"] como fallback
                return bundle

            # Formato legacy: modelo completo en "model" como string/dict JSON
            if "model" in bundle and isinstance(bundle["model"], (str, dict)):
                model_json = bundle["model"]
                if isinstance(model_json, dict):
                    model_json = json.dumps(model_json)
                reg = xgb.XGBRegressor()
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tf:
                    tf.write(model_json)
                    temp_path = tf.name
                reg.load_model(temp_path)
                os.remove(temp_path)
                bundle["model"] = reg
                return bundle

        # Intento 2: Carga Pickle (Solo si coincide la versión de sklearn)
        bundle = pickle.load(io.BytesIO(raw_data))

        if isinstance(bundle, dict) and "model" in bundle:
            return bundle

        return {
            "model": bundle,
            "strategy": "absolute",
            "city_stats": None,
            "segment_stats": None,
            "fuente_ratio_stats": None,
            "fuente_segmento_ratio_stats": None,
            "market_meta": {},
            "feature_cols": [],
        }
    except Exception as e:
        error_msg = str(e)
        if "_RemainderColsList" in error_msg:
             st.sidebar.error("❌ Error de Versión (Pickle): Exporta el modelo como .json en Databricks.")
        else:
             st.sidebar.error(f"Error cargando bundle {key}: {e}")
        return None


def _s3_storage_options():
    key    = aws_config.get("aws_access_key_id")    or None
    secret = aws_config.get("aws_secret_access_key") or None
    return {"key": key, "secret": secret}

def _s3_read_gold(table_name: str) -> pd.DataFrame | None:
    """Lee una tabla Gold desde S3 optimizando RAM mediante poda de columnas, downcasting y categorización extrema."""
    import gc
    try:
        bucket = aws_config.get("s3_bucket_name", "")
        print(f"[REABOOT] Iniciando descarga S3 optimizada: {table_name}", flush=True)
        t0 = time.time()
        
        fs = s3fs.S3FileSystem(
            key=aws_config.get("aws_access_key_id")    or None,
            secret=aws_config.get("aws_secret_access_key") or None
        )
        s3_path = f"{bucket}/gold/{table_name}/"

        dataset = ds.dataset(s3_path, filesystem=fs, format="parquet")
        all_cols = dataset.schema.names

        if "app_inmuebles" in table_name:
            # Projection mínima necesaria para renderizar la UI sin cargar todo el schema.
            # El objetivo es que el boot de Streamlit no convierta a pandas columnas que
            # sólo se usan en cálculos opcionales o se pueden recomputar en _clean_gold().
            ui_cols = [
                "city_token", "market_token", "ubicacion_clean", "ubicacion_norm",
                "precio_num", "area_m2", "habitaciones", "tipo_inmueble", "estado_inmueble",
                "fuente", "url", "titulo", "rentabilidad_potencial", "estado_inversion",
                "comuna_mercado", "sector_mercado", "num_portales", "dispersion_pct_grupo",
                "precio_min_grupo", "precio_max_grupo", "precio_m2", "precio_predicho",
                "score_inversion", "first_seen_date", "precio_cambio_pct",
            ]
            cols_to_load = [c for c in ui_cols if c in all_cols]
        elif "mercado_analitica" in table_name:
            analitica_cols = [
                "analytics_level", "market_token", "city_token",
                "market_n", "market_quality_score", "precio_m2_mediano",
                "lower_bound_ref", "upper_bound_ref", "precio_mediano", "area_mediana",
            ]
            cols_to_load = [c for c in analitica_cols if c in all_cols]
        elif "portal_operacion" in table_name:
            portal_cols = [
                "portal", "portal_ofertas_activas", "checkpoint_status", "gold_snapshot_at",
            ]
            cols_to_load = [c for c in portal_cols if c in all_cols]
        else:
            cols_to_load = all_cols
        print(f"[REABOOT] Extrayendo {len(cols_to_load)} de {len(all_cols)} columnas...", flush=True)
        
        table = dataset.to_table(columns=cols_to_load)
        df = table.to_pandas(
            strings_to_categorical=True,
            split_blocks=True,
            self_destruct=True,
        )
        
        # --- OPTIMIZACIONES DE MEMORIA EN EL DATAFRAME ---
        # 1. Categorización de strings repetitivos de alta cardinalidad/baja cardinalidad repetitiva
        categorical_cols = ["city_token", "market_token", "tipo_inmueble", "estado_inmueble", "fuente", "estado_inversion", "comuna_mercado", "zona_mercado"]
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype("category")
                
        # 2. Downcasting de variables numéricas a 32 bits
        float32_cols = [
            "precio_num", "area_m2", "habitaciones", "banos", "garajes", "num_portales", "precio_m2",
            "dispersion_pct_grupo", "precio_mediano_grupo", "precio_min_grupo", "precio_max_grupo",
            "rentabilidad_potencial", "precio_predicho", "data_completeness", "precio_desviacion_grupo_pct",
            "precio_m2_vs_mediana_pct", "percentil_precio_ciudad", "score_inversion",
            "descuento_potencial_cop", "cuota_mensual_est", "precio_cambio_pct",
        ]
        for col in float32_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
        
        # Supervivencia estricta para matar duplicados del Lakehouse
        if "id_original" in df.columns:
            df = df.drop_duplicates(subset=["id_original"], keep="last")
        elif "id_inmueble" in df.columns:
            df = df.drop_duplicates(subset=["id_inmueble"], keep="last")
        elif "url" in df.columns:
            df = df.drop_duplicates(subset=["url"], keep="last")
            
        print(f"[REABOOT] EXITO S3: {table_name} descargado en {time.time()-t0:.2f}s. Shape RAM final: {df.shape}", flush=True)
        
        # Liberar explícitamente recursos pesados de PyArrow de la memoria
        del table
        gc.collect()
        
        return df
    except Exception as e:
        print(f"[REABOOT] ERROR S3 {table_name}: {e}", flush=True)
        return None


def query_gold_by_filters(
    cities: list,
    price_min: float,
    price_max: float,
    table_name: str = "app_inmuebles_scored",
    limit: int = 500,
    tipos: list | None = None,
    estados: list | None = None,
    habs_min: int | None = None,
    markets: list | None = None,
) -> pd.DataFrame:
    """Descarga de S3 con todos los filtros aplicados en PyArrow (C++ level) ANTES del slice."""
    import gc
    try:
        bucket = aws_config.get("s3_bucket_name", "")
        print(f"[ON-DEMAND] Consultando S3 para ciudades {cities} en rango ${price_min:,.0f} - ${price_max:,.0f}", flush=True)
        t0 = time.time()
        
        fs = s3fs.S3FileSystem(
            key=aws_config.get("aws_access_key_id")    or None,
            secret=aws_config.get("aws_secret_access_key") or None
        )
        s3_path = f"{bucket}/gold/{table_name}/"
        
        dataset = ds.dataset(s3_path, filesystem=fs, format="parquet")
        all_cols = dataset.schema.names
        
        if "app_inmuebles" in table_name:
            ui_cols = [
                "id_original", "city_token", "market_token", "ubicacion_limpia", "ubicacion_norm", "ubicacion_raw",
                "precio_num", "area_m2", "habitaciones", "banos", "garajes", "tipo_inmueble", "estado_inmueble",
                "fuente", "url", "titulo", "rentabilidad_potencial", "estado_inversion", "comuna_mercado", "sector_mercado",
                "num_portales", "dispersion_pct_grupo", "precio_mediano_grupo", "precio_min_grupo", "precio_max_grupo", "precio_m2",
                "precio_predicho", "data_completeness", "zona_mercado", "fecha_extraccion",
                "precio_desviacion_grupo_pct", "precio_m2_vs_mediana_pct", "percentil_precio_ciudad",
                "score_inversion", "descuento_potencial_cop", "cuota_mensual_est",
                "first_seen_date", "precio_cambio_pct",
            ]
            cols_to_load = [c for c in ui_cols if c in all_cols]
        else:
            cols_to_load = all_cols
        
        # 1. Filtros Pushdown en PyArrow (C++ level) — TODOS los filtros van aquí,
        # antes del slice, para que limit se aplique sobre el conjunto YA filtrado.
        _pm = pa.scalar(int(price_min), type=pa.int64())
        _pM = pa.scalar(int(price_max), type=pa.int64())
        filter_expr = (pc.field("precio_num") >= _pm) & (pc.field("precio_num") <= _pM)

        if cities:
            cities_lower = [str(c).lower().strip() for c in cities]
            filter_expr = filter_expr & pc.field("city_token").isin(cities_lower)

        if markets:
            markets_lower = [str(m).lower().strip() for m in markets]
            filter_expr = filter_expr & pc.field("market_token").isin(markets_lower)

        if tipos:
            # Incluir siempre 'desconocido'/'otro' sólo si el usuario NO filtró por tipo
            tipos_lower = [str(t).lower().strip() for t in tipos]
            filter_expr = filter_expr & pc.field("tipo_inmueble").isin(tipos_lower)

        if estados:
            # Los portales dejan ~90% de inmuebles con estado 'desconocido'.
            # Si el usuario filtra por 'nuevo' o 'usado', también incluimos 'desconocido'
            # para no perder inventario mal etiquetado. El usuario puede afinar en los resultados.
            estados_lower = [str(e).lower().strip() for e in estados]
            if "desconocido" not in estados_lower:
                estados_lower = estados_lower + ["desconocido"]
            filter_expr = filter_expr & pc.field("estado_inmueble").isin(estados_lower)

        if habs_min and habs_min > 1:
            filter_expr = filter_expr & (pc.field("habitaciones") >= pa.scalar(float(habs_min)))

        print(f"[ON-DEMAND] Filtros PyArrow: ciudades={cities}, tipos={tipos}, estados={estados}, habs>={habs_min}", flush=True)
        table = dataset.to_table(
            columns=cols_to_load,
            filter=filter_expr
        )
        print(f"[ON-DEMAND] Pre-slice shape: {table.num_rows} filas", flush=True)

        if limit is not None and limit > 0:
            table = table.slice(length=limit)
            
        df = table.to_pandas()
        
        # 2. Downcasting y Categorización para liberar RAM local
        categorical_cols = ["city_token", "market_token", "tipo_inmueble", "estado_inmueble", "fuente", "estado_inversion", "comuna_mercado"]
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype("category")
                
        float32_cols = ["precio_num", "area_m2", "habitaciones", "banos", "garajes", "num_portales", "precio_m2",
                        "dispersion_pct_grupo", "precio_mediano_grupo", "precio_min_grupo", "precio_max_grupo", "rentabilidad_potencial"]
        for col in float32_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

        # Alias ubicacion_limpia → ubicacion_clean para compatibilidad con el UI
        if "ubicacion_clean" not in df.columns:
            if "ubicacion_limpia" in df.columns:
                df["ubicacion_clean"] = df["ubicacion_limpia"]
            elif "ubicacion_norm" in df.columns:
                df["ubicacion_clean"] = df["ubicacion_norm"]
            elif "ubicacion_raw" in df.columns:
                df["ubicacion_clean"] = df["ubicacion_raw"]
            else:
                df["ubicacion_clean"] = "Desconocida"

        # Supervivencia estricta para matar duplicados del Lakehouse
        if "id_original" in df.columns:
            df = df.drop_duplicates(subset=["id_original"], keep="last")
        elif "id_inmueble" in df.columns:
            df = df.drop_duplicates(subset=["id_inmueble"], keep="last")
            
        print(f"[ON-DEMAND] EXITO: Traídos {len(df)} registros en {time.time()-t0:.2f}s. Shape RAM final: {df.shape}", flush=True)
        
        del table
        gc.collect()
        return df
    except Exception as e:
        print(f"[ON-DEMAND] ERROR al consultar {table_name}: {e}", flush=True)
        return None


@st.cache_resource(show_spinner="Cargando portafolio...", ttl=3600)
def load_gold():
    """Lee Gold consumable + enriquece con market_token desde mercado_analitica."""
    try:
        # Intento 1: Leer tabla pre-costeada (Batch Inference Databricks - Modern Data Stack)
        df = _s3_read_gold("app_inmuebles_scored")
        if df is None or df.empty:
            # Intento 2: Fallback a Gold normal
            df = _s3_read_gold("app_inmuebles")
            
        if df is None or df.empty:
            return _dummy_df()
        return _clean_gold(df)
    except Exception as e:
        st.warning(f"Usando datos de demo ({e})")
        return _dummy_df()


@st.cache_resource(show_spinner=False, ttl=3600)
def load_mercado_analitica():
    return _s3_read_gold("mercado_analitica")


@st.cache_resource(show_spinner=False, ttl=3600)
def load_mercado_sectorial():
    return _s3_read_gold("mercado_sectorial")


@st.cache_resource(show_spinner=False, ttl=3600)
def load_portal_operacion():
    return _s3_read_gold("portal_operacion")


@st.cache_resource(show_spinner=False, ttl=7200)
def _build_city_market_map() -> dict:
    """Extrae mapeo city_token → market_token de mercado_analitica."""
    ma = load_mercado_analitica()
    if ma is None or "market_token" not in ma.columns or "analytics_level" not in ma.columns:
        return {}
    city_rows = ma[ma["analytics_level"] == "city"][["city_token", "market_token"]].copy()
    city_rows["is_metro"] = city_rows["market_token"].str.endswith("_metropolitana")
    city_rows = city_rows.sort_values(["city_token", "is_metro"], ascending=[True, False]).drop_duplicates("city_token")
    return dict(zip(city_rows["city_token"], city_rows["market_token"]))


def _enrich_market_token(df: pd.DataFrame) -> pd.DataFrame:
    """Añade market_token al DataFrame usando el mapeo de mercado_analitica."""
    if "market_token" in df.columns and df["market_token"].notna().any():
        return df
    city_map = _build_city_market_map()
    if city_map:
        df["market_token"] = df["city_token"].map(city_map)
    # Fallback: derivar del city_token
    if "market_token" not in df.columns:
        df["market_token"] = df["city_token"] + "_metropolitana"
    else:
        df["market_token"] = df["market_token"].fillna(
            df["city_token"] + "_metropolitana"
        )
    return df


def _clean_gold(df: pd.DataFrame) -> pd.DataFrame:
    ubi_col = "ubicacion_norm" if "ubicacion_norm" in df.columns else (
        "ubicacion_raw" if "ubicacion_raw" in df.columns else None
    )
    if ubi_col:
        df["ubicacion_clean"] = (
            df[ubi_col].astype(str)
            .str.replace(r"\|.*", "", regex=True)
            .str.strip()
            .replace({"nan": "Desconocida", "None": "Desconocida", "": "Desconocida"})
        )
        def _dedup_tokens(text):
            if not isinstance(text, str) or text in ("Desconocida",):
                return text
            tokens = text.lower().strip().split()
            seen = []
            for tok in tokens:
                if not seen or tok != seen[-1]:
                    seen.append(tok)
            result = " ".join(seen)
            return result.capitalize() if result else text
        df["ubicacion_clean"] = df["ubicacion_clean"].apply(_dedup_tokens)
    else:
        df["ubicacion_clean"] = "Desconocida"

    if "city_token" not in df.columns:
        df["city_token"] = "otra_ciudad"
    df["city_token"] = df["city_token"].fillna("otra_ciudad")

    # Enriquecer con market_token
    df = _enrich_market_token(df)

    for col in ["area_m2", "habitaciones", "banos", "garajes"]:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["precio_num"].notna() & (df["precio_num"] > 0)]
    df = df[df["area_m2"].notna() & (df["area_m2"] > 0)]

    for col in ["tipo_inmueble", "estado_inmueble", "fuente"]:
        if col not in df.columns:
            df[col] = "desconocido"
        df[col] = df[col].fillna("desconocido").astype(str)

    df["precio_m2"] = df["precio_num"] / df["area_m2"]

    for col in ["num_portales", "dispersion_pct_grupo", "precio_mediano_grupo",
                "precio_min_grupo", "precio_max_grupo"]:
        if col not in df.columns:
            df[col] = np.nan

    # Columnas opcionales de granularidad fina
    for col in ["comuna_mercado", "sector_mercado"]:
        if col not in df.columns:
            df[col] = np.nan

    # --- RE-APLICAR OPTIMIZACIONES DE MEMORIA Y TIPOS DE DATOS ---
    categorical_cols = ["city_token", "market_token", "tipo_inmueble", "estado_inmueble", "fuente", "estado_inversion", "comuna_mercado", "sector_mercado", "zona_mercado"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna("desconocido").astype("category")
            
    float32_cols = [
        "precio_num", "area_m2", "habitaciones", "banos", "garajes", "num_portales", "precio_m2",
        "dispersion_pct_grupo", "precio_mediano_grupo", "precio_min_grupo", "precio_max_grupo",
        "rentabilidad_potencial", "precio_predicho", "data_completeness", "precio_desviacion_grupo_pct",
    ]
    for col in float32_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    # ── Columnas analíticas derivadas (calculadas en memoria, siempre presentes) ────────
    # Días en mercado — preferir first_seen_date (dedup pipeline) sobre fecha_extraccion (último scrape)
    if "first_seen_date" in df.columns and df["first_seen_date"].notna().any():
        df["dias_en_mercado"] = (
            pd.Timestamp.now() - pd.to_datetime(df["first_seen_date"], errors="coerce")
        ).dt.days.clip(0, 730).astype("float32")
    elif "fecha_extraccion" in df.columns:
        df["dias_en_mercado"] = (
            pd.Timestamp.now() - pd.to_datetime(df["fecha_extraccion"], errors="coerce")
        ).dt.days.clip(0, 730).astype("float32")
    elif "dias_en_mercado" not in df.columns:
        df["dias_en_mercado"] = np.float32(30)

    # Percentil de precio/m² dentro de la ciudad (0 = más barato, 100 = más caro)
    if "city_token" in df.columns and "precio_m2" in df.columns and "percentil_precio_ciudad" not in df.columns:
        df["percentil_precio_ciudad"] = (
            df.groupby("city_token")["precio_m2"].rank(pct=True) * 100
        ).round(1).astype("float32")
    elif "percentil_precio_ciudad" not in df.columns:
        df["percentil_precio_ciudad"] = np.float32(50)
    else:
        df["percentil_precio_ciudad"] = pd.to_numeric(df["percentil_precio_ciudad"], errors="coerce").astype("float32")

    # Score inversión 0-100: rentabilidad (50%) + completitud (30%) + consistencia de precio (20%)
    if "score_inversion" not in df.columns:
        _has_inputs = all(c in df.columns for c in ["rentabilidad_potencial", "data_completeness", "dispersion_pct_grupo"])
        if _has_inputs:
            _rent = (df["rentabilidad_potencial"].fillna(0).clip(-50, 50) + 50) / 100
            _comp = df["data_completeness"].fillna(0.5).clip(0, 1)
            _cons = 1 - df["dispersion_pct_grupo"].fillna(0).clip(0, 100) / 100
            df["score_inversion"] = (_rent * 0.50 + _comp * 0.30 + _cons * 0.20).mul(100).round(1).astype("float32")
        else:
            df["score_inversion"] = np.float32(50)
    else:
        df["score_inversion"] = pd.to_numeric(df["score_inversion"], errors="coerce").astype("float32")

    # Cuota mensual hipotecaria estimada (30 años, tasa 14% EA ≈ 1.098%/mes, financiación 70%)
    if "cuota_mensual_est" not in df.columns:
        _tm, _n = 0.01098, 360
        _fc = (_tm * (1 + _tm) ** _n) / ((1 + _tm) ** _n - 1)
        df["cuota_mensual_est"] = (df["precio_num"] * 0.70 * _fc).round(-3).astype("float32")
    else:
        df["cuota_mensual_est"] = pd.to_numeric(df["cuota_mensual_est"], errors="coerce").astype("float32")

    # Descuento potencial absoluto COP (precio modelo − precio real)
    if "descuento_potencial_cop" not in df.columns:
        if "precio_predicho" in df.columns:
            df["descuento_potencial_cop"] = (df["precio_predicho"] - df["precio_num"]).astype("float32")
        else:
            df["descuento_potencial_cop"] = np.float32(0)
    else:
        df["descuento_potencial_cop"] = pd.to_numeric(df["descuento_potencial_cop"], errors="coerce").astype("float32")

    # precio_cambio_pct — evolución de precio desde primera aparición (dedup pipeline)
    if "precio_cambio_pct" not in df.columns:
        df["precio_cambio_pct"] = np.float32(0)
    else:
        df["precio_cambio_pct"] = pd.to_numeric(df["precio_cambio_pct"], errors="coerce").fillna(0).astype("float32")

    import gc
    gc.collect()

    return df.reset_index(drop=True)


def _dummy_df() -> pd.DataFrame:
    np.random.seed(42)
    n = 300
    zonas = ["Chapinero", "El Poblado", "Laureles", "Usaquén", "Palermo",
             "Envigado", "Sabaneta", "Rosales", "Cedritos", "Bello",
             "Cali Sur", "Barranquilla Norte", "Cartagena Bocagrande"]
    cities = ["bogota"] * 120 + ["medellin"] * 80 + ["cali"] * 50 + ["barranquilla"] * 50
    markets = ["bogota_metropolitana"] * 120 + ["medellin_metropolitana"] * 80 + \
              ["cali_metropolitana"] * 50 + ["barranquilla_metropolitana"] * 50
    return pd.DataFrame({
        "id_original": [f"DEMO-{i:04d}" for i in range(n)],
        "precio_num": np.random.randint(150, 2000, n) * 1_000_000.0,
        "area_m2": np.random.randint(40, 280, n).astype(float),
        "ubicacion_clean": np.random.choice(zonas, n),
        "city_token": np.random.choice(cities, n),
        "market_token": np.random.choice(markets, n),
        "habitaciones": np.random.randint(1, 5, n).astype(float),
        "banos": np.random.randint(1, 4, n).astype(float),
        "garajes": np.random.randint(0, 3, n).astype(float),
        "tipo_inmueble": np.random.choice(["apartamento", "casa", "otro"], n),
        "estado_inmueble": np.random.choice(["usado", "nuevo"], n),
        "fuente": np.random.choice(["ciencuadras_usado", "fincaraiz", "metrocuadrado",
                                     "bancolombia_tu360", "properati"], n),
        "url": ["https://ejemplo.com"] * n,
        "num_portales": np.random.choice([1, 2, 3], n, p=[0.7, 0.2, 0.1]).astype(float),
        "dispersion_pct_grupo": np.random.uniform(0, 15, n),
        "precio_mediano_grupo": np.random.randint(150, 2000, n) * 1_000_000.0,
        "precio_min_grupo": np.random.randint(120, 1800, n) * 1_000_000.0,
        "precio_max_grupo": np.random.randint(180, 2200, n) * 1_000_000.0,
        "precio_m2": np.random.uniform(3e6, 12e6, n),
        # Columnas analíticas en demo
        "precio_predicho": np.random.randint(150, 2000, n) * 1_000_000.0,
        "rentabilidad_potencial": np.random.uniform(-30, 40, n),
        "estado_inversion": np.random.choice(["Oportunidad", "En mercado", "Sobrevalorado"], n, p=[0.25, 0.55, 0.20]),
        "data_completeness": np.random.uniform(0.4, 1.0, n),
        "fecha_extraccion": pd.date_range(end=pd.Timestamp.now(), periods=n, freq="-1D"),
        "zona_mercado": np.random.choice(["norte", "sur", "oriente", "occidente", "centro"], n),
        "precio_desviacion_grupo_pct": np.random.uniform(-15, 15, n),
        "precio_cambio_pct": np.random.uniform(-8, 2, n),
    })




# ══════════════════════════════════════════════════════════════════
# CARGA INICIAL
# ══════════════════════════════════════════════════════════════════
print("\n[REABOOT] 🚀 === INICIANDO BOOT DE APP.PY ===", flush=True)
tracemalloc.start()

manifest = load_manifest()
# Tablas analíticas diferidas: se cargan en el Tab 1 DESPUÉS de que load_gold()
# complete y libere su intermedio PyArrow, para que los picos de RAM sean
# secuenciales (no simultáneos) dentro del límite de 2 GiB del Fargate.
gold_analitica = load_mercado_analitica()
gold_portales  = None

# Modo API-first: no cargar el Gold completo en memoria si el backend DuckDB está disponible.
api_mode = api_healthcheck()
catalog_summary = api_catalog_summary() if api_mode else {}
market_catalog = api_markets() if api_mode else pd.DataFrame()
search_meta = api_search_metadata() if api_mode else {}

if "bundle" not in st.session_state:
    st.session_state.bundle = None

# CARGA LAZY: No inicializa st.session_state.master_db ni carga load_gold() en el arranque
df = None if api_mode else st.session_state.get("master_db")

curr, peak = tracemalloc.get_traced_memory()
print(f"[REABOOT] MEMORY AFTER boot: Current {curr/1e6:.1f}MB, Peak {peak/1e6:.1f}MB", flush=True)

# KPIs del boot: API-first si existe backend, fallback dinámico a mercado_analitica si df es None (idle)
if api_mode and catalog_summary:
    N = int(catalog_summary.get("total_inmuebles", 101106))
    N_PORTALES = int(catalog_summary.get("n_portales", 7))
    PORTALES_SANOS = N_PORTALES
    N_MERCADOS = int(catalog_summary.get("n_mercados", 25))
    N_CIUDADES = int(catalog_summary.get("n_ciudades", 133))
    MED_PRECIO = float(catalog_summary.get("med_precio", 585000000.0))
    MED_M2 = float(catalog_summary.get("med_precio_m2", 5150000.0))
    N_OPT = int(catalog_summary.get("n_oportunidades", 2022))
else:
    if df is not None and not df.empty:
        # Calcular sobre la búsqueda activa del usuario
        N          = len(df)
        N_PORTALES = int(df["fuente"].nunique()) if "fuente" in df.columns else 7
        PORTALES_SANOS = N_PORTALES
        N_MERCADOS = int(df["market_token"].nunique()) if "market_token" in df.columns else 25
        N_CIUDADES = int(df["city_token"].nunique())   if "city_token" in df.columns else 133
        MED_PRECIO = float(df["precio_num"].median())
        MED_M2     = float(df["precio_m2"].median())  if "precio_m2" in df.columns else 5150000.0
        N_OPT      = int((df["estado_inversion"] == "Oportunidad").sum()) if "estado_inversion" in df.columns else 2022
    elif gold_analitica is not None and not gold_analitica.empty:
        # MODO LAZY IDLE: Agregación rápida en memoria usando mercado_analitica (pre-agregada ligera de S3)
        city_rows = gold_analitica[gold_analitica["analytics_level"] == "city"]
        N = int(city_rows["market_n"].sum()) if "market_n" in city_rows.columns else 101106
        N_PORTALES = 7
        PORTALES_SANOS = N_PORTALES
        N_MERCADOS = int(city_rows["market_token"].nunique()) if "market_token" in city_rows.columns else 25
        N_CIUDADES = int(city_rows["city_token"].nunique()) if "city_token" in city_rows.columns else 133
        MED_PRECIO = float(city_rows["precio_mediano"].median()) if "precio_mediano" in city_rows.columns else 585000000.0
        MED_M2     = float(city_rows["precio_m2_mediano"].median()) if "precio_m2_mediano" in city_rows.columns else 5150000.0
        N_OPT      = 2022
    else:
        # Defaults absolutos preventivos
        N          = 101106
        N_PORTALES = 7
        PORTALES_SANOS = N_PORTALES
        N_MERCADOS = 25
        N_CIUDADES = 133
        MED_PRECIO = 585000000.0
        MED_M2     = 5150000.0
        N_OPT      = 2022

MAPE_BADGE = manifest.get("metrics", {}).get("mape", "20.9%")
DEPLOYED = (
    manifest.get("promoted_at", "")
    or manifest.get("trained_at", "")
    or "2026-05-29"
)[:10] if manifest else "2026-05-29"


# ══════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(
        '<div style="font-family:Playfair Display,serif;font-size:1.3rem;'
        'font-weight:900;color:white;margin-bottom:.2rem">◈ REA</div>'
        '<div style="font-size:.62rem;letter-spacing:.15em;color:#b8935a;'
        'text-transform:uppercase;margin-bottom:1.2rem">Real Estate Analyst · Colombia</div>',
        unsafe_allow_html=True,
    )
    sb1, sb2 = st.columns(2)
    _mape_num = float(str(MAPE_BADGE).replace('%','').strip()) if MAPE_BADGE else 20.0
    _diff_500 = round(500 * _mape_num / 100, 0)
    sb1.metric("MAPE", f"{_mape_num:.1f}%",
               help=f"Error promedio del modelo: {_mape_num:.1f}%. Para una propiedad de 500M COP el estimado puede variar \u00b1{_diff_500:.0f}M COP")
    sb2.metric("Fecha", DEPLOYED or "N/A")

    st.markdown(
        '<hr style="border-color:rgba(255,255,255,.1);margin:.9rem 0">',
        unsafe_allow_html=True,
    )
    for label, val in [("Inmuebles", f"{N:,}"), ("Mercados", f"{N_MERCADOS}"),
                        ("Ciudades", f"{N_CIUDADES}"), ("Portales", f"{N_PORTALES}"),
                        ("Oportunidades", f"{N_OPT:,}"), ("Modelo", LLM_MODEL)]:
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;font-size:.78rem;'
            f'padding:.22rem 0;border-bottom:1px solid rgba(255,255,255,.07)">'
            f'<span style="color:rgba(255,255,255,.45)">{label}</span>'
            f'<span style="color:white;font-family:DM Mono,monospace">{val}</span></div>',
            unsafe_allow_html=True,
        )
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Limpiar conversación y caché"):
        for k in ["messages", "chat_usage", "tab1_candidates", "master_db"]:
            if k in st.session_state:
                del st.session_state[k]
        st.cache_data.clear()
        st.rerun()
    st.markdown(
        '<div style="font-size:.62rem;color:rgba(255,255,255,.22);margin-top:1.2rem;line-height:1.6">'
        'No constituye asesoría financiera certificada.<br>'
        'Datos de portales públicos colombianos.</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════
ticker_txt = "  ·  ".join([
    f"◈ {N:,} INMUEBLES ACTIVOS",
    f"PRECIO MEDIANO ${MED_PRECIO/1e6:.0f}M COP",
    f"M² MEDIANO ${MED_M2/1e6:.2f}M COP",
    f"{N_MERCADOS} MERCADOS · {N_CIUDADES} CIUDADES",
    f"{N_OPT} OPORTUNIDADES DETECTADAS",
    f"{N_PORTALES} PORTALES · MODELO MAPE {MAPE_BADGE}%",
])
st.markdown(f'<div class="ticker">{ticker_txt}</div>', unsafe_allow_html=True)

col_h, col_s = st.columns([3, 1])
with col_h:
    st.markdown('<div class="section-label">Plataforma de inteligencia inmobiliaria</div>',
                unsafe_allow_html=True)
    st.title("Real Estate Analyst")
    st.markdown(
        f'<p style="color:var(--muted);font-size:.88rem;margin-top:-.5rem">'
        f'Colombia · {N:,} inmuebles · {N_PORTALES} portales · datos actualizados {DEPLOYED}</p>',
        unsafe_allow_html=True,
    )

st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Inmuebles activos", f"{N:,}")
k2.metric("Precio mediano", f"${MED_PRECIO/1e6:.0f}M")
k3.metric("Precio / m²", f"${MED_M2/1e6:.2f}M")
k4.metric("Oportunidades", f"{N_OPT:,}",
          help="Inmuebles donde el precio de lista supera >15% al modelo — posible margen de negociación")

st.markdown("<br>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# FUNCIÓN LLM CHAT — reutilizable
# ══════════════════════════════════════════════════════════════════

FECHA_CORTE = manifest.get("deployed_at", "2026-03")[:7] if manifest else "2026-03"

DISCLAIMER_HTML = f"""
<div class="disclaimer">
<strong>Aviso importante:</strong> Este asistente es una IA con entrenamiento de datos
hasta <strong>{FECHA_CORTE}</strong>. No es un asesor inmobiliario, financiero ni jurídico
certificado. Las recomendaciones son orientativas y se basan en datos de portales públicos —
consulta con un profesional antes de tomar decisiones de compra o inversión.
</div>
"""


def render_chat(tab_key: str, system_prompt: str, placeholder: str, ctx_df: pd.DataFrame = None):
    """Renderiza el chat contextual con límite de mensajes."""
    # --- MODO DEMO: BLOQUEO PREMIUM ---
    st.markdown(
        '<div style="background:var(--surface2);border:1px solid var(--border);'
        'padding:2rem;text-align:center;border-radius:4px;margin-top:1rem">'
        '<div style="font-size:2rem;margin-bottom:1rem">🔒</div>'
        '<div style="font-weight:bold;color:var(--ink);margin-bottom:.5rem">'
        'Asistente IA - Acceso Premium</div>'
        '<div style="font-size:.85rem;color:var(--muted);max-width:300px;margin:0 auto">'
        'La interacción directa con el asesor inteligente está reservada para usuarios suscritos. '
        'Contacta a ventas para activar tu licencia.</div>'
        '</div>',
        unsafe_allow_html=True
    )
    return

    msgs_key  = f"messages_{tab_key}"
    usage_key = f"usage_{tab_key}"

    if msgs_key  not in st.session_state: st.session_state[msgs_key]  = []
    if usage_key not in st.session_state: st.session_state[usage_key] = 0

    LIMIT = 6

    for msg in st.session_state[msgs_key]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if st.session_state[usage_key] >= LIMIT:
        st.info(f"Límite de {LIMIT} consultas por sesión. Limpia la conversación en el sidebar.")
        return

    remaining = LIMIT - st.session_state[usage_key]
    user_input = st.chat_input(f"{placeholder} ({remaining} restantes)", key=f"ci_{tab_key}")

    if not user_input:
        return

    if not llm_ready():
        st.error("Ollama no responde. Verifica que esté corriendo en el host configurado.")
        return

    st.session_state[usage_key] += 1
    st.session_state[msgs_key].append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # RAG local: buscar inmuebles relevantes
    ctx_str = ""
    if ctx_df is not None and not ctx_df.empty:
        q = user_input.lower()
        palabras = [w for w in re.findall(r"\w+", q) if len(w) > 3]
        if palabras and "ubicacion_clean" in ctx_df.columns:
            mask = pd.Series(False, index=ctx_df.index)
            for p in palabras:
                mask |= ctx_df["ubicacion_clean"].str.lower().str.contains(p, na=False)
                if "city_token" in ctx_df.columns:
                    mask |= ctx_df["city_token"].str.lower().str.contains(p, na=False)
                if "market_token" in ctx_df.columns:
                    mask |= ctx_df["market_token"].str.lower().str.contains(p, na=False)
            sub = ctx_df[mask].sort_values("rentabilidad_potencial", ascending=False).head(5)
        else:
            sub = ctx_df.sort_values("rentabilidad_potencial", ascending=False).head(5)

        cols = [c for c in ["id_original", "ubicacion_clean", "market_token", "city_token",
                             "comuna_mercado", "sector_mercado",
                             "precio_num", "area_m2", "habitaciones",
                             "rentabilidad_potencial", "estado_inversion",
                             "num_portales"] if c in sub.columns]
        ctx_str = sub[cols].to_json(orient="records", force_ascii=False)

    full_system = f"""{system_prompt}

CONTEXTO DE INMUEBLES RELEVANTES (usa estos datos reales en tu respuesta):
{ctx_str if ctx_str else 'No hay candidatos filtrados aún.'}

SESIÓN ACTUAL:
- Préstamo solicitado: ${st.session_state.get('prestamo', 0):,.0f} COP
- Capacidad mensual: ${st.session_state.get('capacidad', 0):,.0f} COP
- Tasa EA: {st.session_state.get('tasa', 0)}%
- Ciudad de interés: {st.session_state.get('ciudad_interes', 'No especificada')}

REGLAS CRÍTICAS:
1. Solo habla de bienes raíces e inversiones inmobiliarias en Colombia.
2. Cita precios, zonas, mercados y datos específicos del contexto cuando estén disponibles.
3. Usa la jerarquía: mercado (market_token) → ciudad (city_token) → comuna (comuna_mercado) → sector (sector_mercado).
4. Si te preguntan otro tema: "Soy un asesor especializado en bienes raíces colombianos."
5. Siempre recuerda al usuario que tu información tiene fecha de corte {FECHA_CORTE}.
6. Sé directo, concreto y accionable. Sin respuestas genéricas."""

    with st.chat_message("assistant"):
        placeholder_el = st.empty()
        full = ""
        try:
            history = [{"role": "system", "content": full_system}]
            history += [{"role": m["role"], "content": m["content"]}
                        for m in st.session_state[msgs_key][-8:]]
            resp = call_llm(history, stream=True)
            if isinstance(resp, str): # Error o respuesta no-stream
                full = resp
                placeholder_el.markdown(full)
            else:
                for chunk in resp:
                    delta = chunk.choices[0].delta.content
                    if delta:
                        full += delta
                        placeholder_el.markdown(full + "▌")
                placeholder_el.markdown(full)
        except Exception as e:
            full = f"Error de conexión con Ollama: {e}"
            placeholder_el.markdown(full)

    st.session_state[msgs_key].append({"role": "assistant", "content": full})


# ══════════════════════════════════════════════════════════════════
# HELPERS UI — gauge y link gallery
# ══════════════════════════════════════════════════════════════════

def render_gauge_esfuerzo(cuota_pct: float):
    """Gauge de tasa de esfuerzo financiero. Verde <30%, Amarillo 30-40%, Rojo >40%."""
    color = "#1a6b4a" if cuota_pct <= 30 else "#b8935a" if cuota_pct <= 40 else "#8b2020"
    estado = "Ideal ✓" if cuota_pct <= 30 else "Alerta ⚠" if cuota_pct <= 40 else "Alto Riesgo ✗"
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=cuota_pct,
        number={"suffix": "%", "font": {"family": "DM Sans", "size": 26, "color": _TEXT}},
        gauge={
            "axis": {
                "range": [0, 60],
                "tickwidth": 1, "tickcolor": _MUTED_TEXT,
                "tickvals": [0, 15, 30, 40, 60],
                "tickfont": {"color": _MUTED_TEXT, "size": 9},
            },
            "bar": {"color": color, "thickness": 0.28},
            "bgcolor": _PLOT,
            "borderwidth": 0,
            "steps": [
                {"range": [0, 30], "color": "rgba(26,107,74,.18)"},
                {"range": [30, 40], "color": "rgba(184,147,90,.18)"},
                {"range": [40, 60], "color": "rgba(139,32,32,.18)"},
            ],
            "threshold": {"line": {"color": "#fff", "width": 2}, "thickness": 0.75, "value": cuota_pct},
        },
        title={
            "text": f"Tasa de Esfuerzo<br><span style='font-size:.85em;color:{color}'>{estado}</span>",
            "font": {"family": "DM Sans", "size": 13, "color": _TEXT},
        },
    ))
    fig.update_layout(
        paper_bgcolor=_BG, plot_bgcolor=_BG,
        font=dict(family="DM Sans", color=_TEXT),
        height=240, margin=dict(l=20, r=20, t=40, b=10),
    )
    return fig


def render_link_gallery(candidatos: pd.DataFrame):
    """Grid 3 columnas con tarjetas de inmueble y link directo al portal."""
    top = candidatos.head(6)
    if top.empty:
        return
    st.markdown('<div class="section-label">Mejores Oportunidades — Acceso directo</div>',
                unsafe_allow_html=True)
    cols = st.columns(3)
    for i, (_, row) in enumerate(top.iterrows()):
        col = cols[i % 3]
        titulo = str(row.get("titulo", "Propiedad"))
        titulo_short = titulo[:35] + "\u2026" if len(titulo) > 35 else titulo
        precio = int(row.get("precio_num", 0))
        precio_str = f"${precio:,}".replace(",", ".")
        ubicacion = str(row.get("ubicacion_clean", ""))
        fuente = str(row.get("fuente", "portal")).replace("_", " ").title()
        url = str(row.get("url", "#"))
        signal = float(row.get("rentabilidad_potencial", 0))
        signal_color = "#1a6b4a" if signal > 0 else "#8b2020"
        arrow = "\u25b2" if signal > 0 else "\u25bc"
        col.markdown(
            f'<div style="background:var(--surface2);border:1px solid var(--border);'
            f'padding:.9rem;border-radius:6px;margin-bottom:.7rem">'
            f'<div style="font-size:.72rem;font-weight:600;color:var(--ink);margin-bottom:.25rem">{titulo_short}</div>'
            f'<div style="font-size:.95rem;font-weight:700;color:var(--gold)">{precio_str}</div>'
            f'<div style="font-size:.68rem;color:var(--muted);margin:.15rem 0">{ubicacion}</div>'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-top:.45rem">'
            f'<span style="font-size:.7rem;color:{signal_color};font-weight:600">{arrow} {signal:+.1f}%</span>'
            f'<a href="{url}" target="_blank" style="font-size:.68rem;color:var(--gold);'
            f'text-decoration:none;font-weight:600">Ver en {fuente} \u2192</a>'
            f'</div></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════
tab3, tab1, tab2, tab4 = st.tabs([
    "Dashboard",
    "Buscador Inteligente",
    "Simulador Financiero",
    "Valoración",
])


# ══════════════════════════════════════════════════════════════════
# TAB 1 — ASESOR INMOBILIARIO
# ══════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("## Buscador Inteligente")
    st.markdown(DISCLAIMER_HTML, unsafe_allow_html=True)

    with st.expander("ℹ️ ¿Cómo usar el Buscador Inteligente?", expanded=False):
        st.markdown("""
**El Buscador te ayuda a encontrar inmuebles que se ajusten a tu perfil financiero.**

**Paso a paso:**
1. **Perfil financiero** — Ingresa tu capacidad de pago mensual, el monto del préstamo y la tasa de interés. El sistema calcula automáticamente si la cuota es viable.
2. **Filtros de búsqueda** — Selecciona ciudad, tipo de inmueble (apartamento, casa…), mercado, estado (nuevo / usado) y habitaciones mínimas.
3. **Rango de precio** — Ajusta el slider para acotar el rango. El máximo va hasta $10.000M para no limitarte.
4. **Buscar** — Haz clic en el botón y el sistema consulta toda la base de datos en tiempo real.

**¿Qué significa la columna "Señal %"?**
Es la diferencia entre el precio publicado y el valor que el modelo estima justo para ese inmueble y zona.
- **Señal positiva (+X%)** → el inmueble está *por debajo* del precio justo de mercado → **oportunidad de compra**.
- **Señal negativa (−X%)** → el inmueble está *por encima* del precio justo → evaluar si conviene negociar.

**Consejo:** Si ves 0 resultados, prueba ampliar el rango de precio o cambiar el estado a "Todos" — muchos inmuebles no están etiquetados como nuevo/usado en los portales.
        """)

    # ── Perfil financiero ────────────────────────────────────────
    with st.expander("▸ Perfil financiero", expanded=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            capacidad = st.number_input("Capacidad mensual (COP)", min_value=0,
                                         value=2_500_000, step=100_000, format="%d")
            st.session_state.capacidad = capacidad
        with f2:
            prestamo = st.number_input("Monto del préstamo (COP)", min_value=0,
                                        value=250_000_000, step=5_000_000, format="%d")
            st.session_state.prestamo = prestamo
        with f3:
            tasa = st.number_input("Tasa E.A. (%)", min_value=0.0, max_value=30.0,
                                    value=12.5, step=0.1)
            st.session_state.tasa = tasa

        # Simulación hipotecaria
        tasa_m = (1 + tasa / 100) ** (1 / 12) - 1
        plazo  = 240
        cuota  = prestamo * (tasa_m * (1 + tasa_m) ** plazo) / ((1 + tasa_m) ** plazo - 1) if tasa_m > 0 else prestamo / plazo
        monto_max = prestamo / 0.7
        viable = cuota <= capacidad

        gauge_col, metrics_col = st.columns([1, 1])
        with gauge_col:
            cuota_pct = (cuota / capacidad * 100) if capacidad > 0 else 0
            st.plotly_chart(render_gauge_esfuerzo(cuota_pct), width="stretch")
        with metrics_col:
            st.metric("Cuota mensual estimada", fmt_cop(cuota))
            st.metric("Presupuesto máx (70%)", f"${monto_max/1e6:.0f}M")
            st.metric("Interés total (20 años)", f"${(cuota*plazo - prestamo)/1e6:.0f}M")
            st.metric("Viabilidad", "✓ Viable" if viable else "✗ Excede capacidad")

        if not viable:
            deficit = cuota - capacidad
            st.warning(
                f"💡 **Recomendación financiera:** La cuota estimada supera tu capacidad en "
                f"**{fmt_cop(deficit)}/mes**. Considera: (1) reducir el monto del préstamo a "
                f"{fmt_cop(prestamo * (capacidad/cuota))}, (2) ampliar el plazo a 25-30 años, "
                f"o (3) aumentar el ahorro inicial para bajar el capital financiado."
            )

    # ── Filtros ──────────────────────────────────────────────────
    with st.expander("▸ Filtros de búsqueda", expanded=True):
        fc_geo1, fc_geo2 = st.columns(2)
        with fc_geo1:
            # Ciudad primero — el usuario siempre piensa en ciudad, no en token de mercado
            if api_mode and not market_catalog.empty:
                ciudades_filtradas_all = market_catalog["city_token"].dropna().unique()
            elif gold_analitica is not None and not gold_analitica.empty:
                ciudades_filtradas_all = gold_analitica[gold_analitica["analytics_level"] == "city"]["city_token"].unique()
            elif df is not None:
                ciudades_filtradas_all = df["city_token"].unique()
            else:
                ciudades_filtradas_all = ["bogota", "medellin", "cali", "barranquilla", "pereira", "manizales", "armenia", "envigado", "sabaneta", "chia"]

            ciudades_disp = sorted([str(c) for c in ciudades_filtradas_all if c != "otra_ciudad"])
            ciudad_sel = st.multiselect("Ciudad (municipio)", options=ciudades_disp,
                                         help="Escribe el nombre de la ciudad — el mercado se deriva automáticamente")
            st.session_state.ciudad_interes = ", ".join(ciudad_sel) if ciudad_sel else "No especificada"

        with fc_geo2:
            # Mercado derivado de la ciudad seleccionada (opcional — para afinar búsqueda)
            if api_mode and not market_catalog.empty:
                if ciudad_sel:
                    mercados_filtrados = market_catalog[market_catalog["city_token"].isin(ciudad_sel)]["market_token"].dropna().unique()
                else:
                    mercados_filtrados = market_catalog["market_token"].dropna().unique()
            elif gold_analitica is not None and not gold_analitica.empty:
                if ciudad_sel:
                    mercados_filtrados = gold_analitica[gold_analitica["city_token"].isin(ciudad_sel)]["market_token"].unique()
                else:
                    mercados_filtrados = gold_analitica["market_token"].unique()
            elif df is not None:
                if ciudad_sel:
                    mercados_filtrados = df[df["city_token"].isin(ciudad_sel)]["market_token"].unique()
                else:
                    mercados_filtrados = df["market_token"].unique()
            else:
                mercados_filtrados = ["bogota_metropolitana", "valle_aburra", "cali_metropolitana", "barranquilla_metropolitana", "eje_cafetero"]

            mercados_disp = sorted([str(m) for m in mercados_filtrados
                                    if m and str(m) not in ("nan", "otra_ciudad_metropolitana", "mercado_otro")])
            mercado_sel = st.multiselect("Mercado", options=mercados_disp,
                                          default=mercados_disp if ciudad_sel and len(mercados_disp) <= 3 else [],
                                          help="Zona metropolitana — se auto-completa al elegir ciudad")
        
        # fc_geo3 y fc_geo4 eliminados: los tokens de comuna/sector no se pueblan en modo lazy
        # (df=None al arrancar) y no se pasaban al query. La ciudad+mercado son suficientes.

        fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 1])
        with fc1:
            if api_mode and search_meta:
                tipos_disp = sorted([t for t in search_meta.get("tipos_inmueble", [])
                                      if t not in ("desconocido", "otro", "nan")])
            elif df is not None:
                tipos_disp = sorted([t for t in df["tipo_inmueble"].unique()
                                      if t not in ("desconocido", "otro", "nan")])
            else:
                tipos_disp = ["apartamento", "casa", "lote", "local_comercial", "oficina"]
            tipo_sel = st.multiselect("Tipo", options=tipos_disp)
        with fc2:
            habs_min = st.selectbox("Hab. mínimas", [1, 2, 3, 4], index=1)
        with fc3:
            estado_sel = st.multiselect("Estado", ["nuevo", "usado"])
        with fc4:
            only_multiportal = st.checkbox("Solo multiportal", value=False,
                                            help="Mostrar solo inmuebles en 2+ portales")

        _M = 1_000_000.0
        # Máximo del slider: independiente del presupuesto del usuario.
        # El tope de $464M era un bug — Bogotá tiene medianas de $700M y propiedades >$10B.
        if df is not None:
            p_max_slider_m = float(df["precio_num"].quantile(0.97)) / _M
        elif api_mode and search_meta and search_meta.get("price_max"):
            p_max_slider_m = float(search_meta["price_max"]) / _M
        elif gold_analitica is not None and not gold_analitica.empty and "precio_mediano" in gold_analitica.columns:
            p_max_slider_m = float(gold_analitica["precio_mediano"].max()) / _M * 4
        else:
            p_max_slider_m = 10_000.0  # 10 000M COP — tope razonable para Colombia
        p_max_slider_m = max(p_max_slider_m, 2_000.0)  # mínimo 2B para que Bogotá quepa siempre
        _val_max_m = min(float(monto_max) / _M, p_max_slider_m)  # el valor inicial sigue siendo el presupuesto
        precio_rango_m = st.slider(
            "Rango de precio",
            min_value=50.0,
            max_value=p_max_slider_m,
            value=(50.0, _val_max_m),
            step=5.0,
            format="$%.0f M",
        )
        precio_rango = (precio_rango_m[0] * _M, precio_rango_m[1] * _M)
        st.caption(
            f"Rango seleccionado: **{fmt_cop(precio_rango[0])}** — **{fmt_cop(precio_rango[1])} COP**"
        )
        
        # Botón de Búsqueda Dinámica
        st.markdown("<br>", unsafe_allow_html=True)
        btn_col1, btn_col2 = st.columns([1, 3])
        with btn_col1:
            buscar_click = st.button("🔍 Buscar Inmuebles", type="primary", width="stretch")
            
        if buscar_click:
            num_portales_min = 2 if only_multiportal else None
            if api_mode:
                cands_df = api_search(
                    cities=ciudad_sel,
                    price_min=precio_rango[0],
                    price_max=precio_rango[1],
                    markets=mercado_sel,
                    tipo_inmueble=tipo_sel[0] if tipo_sel else None,
                    estado_inmueble=estado_sel[0] if estado_sel else None,
                    habitaciones_min=habs_min if habs_min > 0 else None,
                    num_portales_min=num_portales_min,
                    limit=200,
                )
            else:
                # MODO S3-DIRECTO: todos los filtros van al PyArrow pushdown ANTES del slice
                cands_df = query_gold_by_filters(
                    cities=ciudad_sel,
                    price_min=precio_rango[0],
                    price_max=precio_rango[1],
                    limit=500,
                    tipos=tipo_sel if tipo_sel else None,
                    estados=estado_sel if estado_sel else None,
                    habs_min=habs_min if habs_min > 1 else None,
                    markets=mercado_sel if mercado_sel else None,
                )
                if cands_df is not None and not cands_df.empty:
                    # Enriquecer métricas si no vienen del Lakehouse pre-costeado
                    if "score_inversion" not in cands_df.columns:
                        cands_df = _clean_gold(cands_df)

                    if only_multiportal and "num_portales" in cands_df.columns:
                        cands_df = cands_df[cands_df["num_portales"].fillna(0) > 1]
                    
                    cands_df = cands_df.sort_values("rentabilidad_potencial", ascending=False).head(25)
                else:
                    cands_df = pd.DataFrame()
            st.session_state.tab1_candidates = cands_df
            st.rerun()

    # ── Obtención de candidatos ──────────────────────────────────
    if "tab1_candidates" not in st.session_state:
        candidatos = None
    else:
        cands_raw = st.session_state.tab1_candidates
        if cands_raw is not None and not cands_raw.empty:
            candidatos = cands_raw.copy()
            if "ubicacion_clean" not in candidatos.columns:
                if "ubicacion_norm" in candidatos.columns:
                    candidatos["ubicacion_clean"] = candidatos["ubicacion_norm"]
                elif "ubicacion_limpia" in candidatos.columns:
                    candidatos["ubicacion_clean"] = candidatos["ubicacion_limpia"]
                elif "zona_mercado" in candidatos.columns:
                    candidatos["ubicacion_clean"] = candidatos["zona_mercado"]
                else:
                    candidatos["ubicacion_clean"] = candidatos["city_token"]
        else:
            candidatos = cands_raw

    # ── Resultados ───────────────────────────────────────────────
    if candidatos is None:
        st.markdown(
            """
            <div style="background-color: var(--surface); border: 1px solid var(--border); padding: 2.5rem; border-radius: 8px; margin-top: 1.5rem; text-align: center; box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
                <div style="font-size: 3rem; margin-bottom: 1.2rem;">🔍</div>
                <h3 style="color: white; margin-bottom: 0.8rem; font-family: 'Playfair Display', serif; font-size: 1.5rem;">Buscador Inteligente de Inmuebles</h3>
                <p style="color: var(--muted); font-size: 0.95rem; max-width: 600px; margin: 0 auto 1.8rem; line-height: 1.6;">
                    Bienvenido a la búsqueda bajo demanda (On-Demand). Para optimizar la memoria y velocidad,
                    selecciona una <b>Ciudad</b> o <b>Mercado</b> en la barra lateral, configura tu presupuesto en los filtros,
                    y presiona el botón <b>"🔍 Buscar Inmuebles"</b> para consultar los datos en tiempo real a través de nuestra API.
                </p>
                <div style="display: inline-block; background-color: rgba(184, 147, 90, 0.1); border: 1px solid #b8935a; padding: 0.6rem 1.2rem; border-radius: 4px; color: #b8935a; font-size: 0.85rem; font-weight: bold; letter-spacing: 0.05em; text-transform: uppercase;">
                    Listo para Consultar
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    elif candidatos.empty:
        st.error("Sin candidatos para los filtros actuales. Ajusta los filtros e intenta de nuevo.")
        tips = []
        if estado_sel:
            tips.append(
                f"⚠️ **Estado '{', '.join(estado_sel)}'**: el ~90% del catálogo tiene estado "
                f"*desconocido* (los portales no lo etiquetan). Los resultados mostrados incluyen "
                f"inmuebles *desconocido* + *{', '.join(estado_sel)}* — si sigue en 0, "
                f"prueba quitando el filtro de Estado."
            )
        if tipo_sel:
            tips.append(f"ℹ️ Tipo filtrado: **{', '.join(tipo_sel)}**. Prueba sin filtro de tipo para ver más opciones.")
        if habs_min > 1:
            tips.append(f"ℹ️ Hab. mínimas: **{habs_min}**. Reducir a 1 ampliará el catálogo.")
        if not ciudad_sel:
            tips.append("ℹ️ No seleccionaste ciudad. El catálogo está concentrado en Bogotá, Medellín, Cali, Barranquilla y Pereira.")
        tips.append("ℹ️ El catálogo activo tiene ~7.600 inmuebles. Amplía el rango de precio o ajusta filtros.")
        for tip in tips:
            st.info(tip)
    else:
        res_col, chart_col = st.columns([3, 2])

        with res_col:
            st.markdown(
                f'<div class="section-label">{len(candidatos)} candidatos encontrados</div>',
                unsafe_allow_html=True,
            )

            display_map = {
                "market_token": "Mercado",
                "city_token": "Ciudad",
                "ubicacion_clean": "Zona",
                "precio_num": "Precio",
                "area_m2": "m²",
                "habitaciones": "Hab.",
                "precio_predicho": "Precio modelo",
                "rentabilidad_potencial": "Señal %",
                "score_inversion": "Score",
                "precio_cambio_pct": "Δ Precio",
                "dias_en_mercado": "Días",
                "estado_inversion": "Señal",
                "num_portales": "Portales",
            }
            cols_show = [c for c in display_map if c in candidatos.columns]
            df_show = candidatos[cols_show].rename(columns=display_map)
            # Format market/city for readability
            for col in ["Mercado", "Ciudad"]:
                if col in df_show.columns:
                    df_show[col] = df_show[col].str.replace("_", " ").str.title()

            fmt = {}
            if "Precio" in df_show.columns:        fmt["Precio"]        = lambda x: f"${int(x):,}".replace(",", ".")
            if "Precio modelo" in df_show.columns: fmt["Precio modelo"] = lambda x: f"${int(x):,}".replace(",", ".")
            if "Señal %" in df_show.columns:       fmt["Señal %"]       = "{:+.1f}%"
            if "Score" in df_show.columns:         fmt["Score"]         = "{:.0f}"
            if "Δ Precio" in df_show.columns:      fmt["Δ Precio"]      = "{:+.1f}%"
            if "Días" in df_show.columns:          fmt["Días"]          = "{:.0f}"
            if "m²" in df_show.columns:            fmt["m²"]            = "{:.0f}"
            if "Portales" in df_show.columns:      fmt["Portales"]      = "{:.0f}"

            st.dataframe(
                df_show.style.format(fmt).apply(
                    lambda col: [
                        "color:#1a6b4a;font-weight:600" if v == "Oportunidad"
                        else "color:#8b2020;font-weight:600" if v == "Sobrevalorado"
                        else "color:#888" for v in col
                    ] if col.name == "Señal" else [""] * len(col), axis=0
                ),
                width="stretch",
                hide_index=True,
                height=400,
            )

            # Badge de cross-portal
            if "num_portales" in candidatos.columns:
                multi = candidatos[candidatos["num_portales"] > 1]
                if not multi.empty:
                    st.markdown(
                        f'<div style="font-size:.75rem;color:var(--muted);margin-top:.5rem">'
                        f'⚡ <strong>{len(multi)}</strong> candidatos aparecen en múltiples portales '
                        f'— verifica precios para negociar.</div>',
                        unsafe_allow_html=True,
                    )

        with chart_col:
            st.markdown('<div class="section-label">Precio vs área — candidatos</div>',
                        unsafe_allow_html=True)
            fig = go.Figure()
            for signal, color, symbol in [
                ("Oportunidad", "#1a6b4a", "circle"),
                ("En mercado",  "#b8935a", "square"),
                ("Sobrevalorado", "#8b2020", "x"),
            ]:
                sub = candidatos[candidatos["estado_inversion"] == signal]
                if not sub.empty:
                    fig.add_trace(go.Scatter(
                        x=sub["area_m2"], y=sub["precio_num"] / 1e6,
                        mode="markers", name=signal,
                        marker=dict(color=color, size=10, symbol=symbol,
                                    opacity=0.8, line=dict(color="white", width=1)),
                        text=sub["ubicacion_clean"],
                        hovertemplate="<b>%{text}</b><br>%{x:.0f}m² · $%{y:.0f}M<extra></extra>",
                    ))
            dark_layout(fig, height=380,
                        legend=dict(orientation="h", y=1.02, x=0),
                        xaxis=dict(title="Área (m²)", gridcolor=_GRID),
                        yaxis=dict(title="Precio (M COP)", gridcolor=_GRID))
            st.plotly_chart(fig, width="stretch")

            # Boxplot: Precio/m² candidatos vs mercado
            if df is not None and "precio_m2" in candidatos.columns and "city_token" in candidatos.columns:
                st.markdown('<div class="section-label">Precio/m² — candidatos vs mercado</div>',
                            unsafe_allow_html=True)
                ciudades_box = [c for c in candidatos["city_token"].unique()[:3] if pd.notna(c)]
                fig_box = go.Figure()
                for ciu in ciudades_box:
                    df_mkt = df[df["city_token"] == ciu]["precio_m2"].dropna() if df is not None else pd.Series(dtype=float)
                    df_cnd = candidatos[candidatos["city_token"] == ciu]["precio_m2"].dropna()
                    if len(df_mkt) > 5:
                        fig_box.add_trace(go.Box(
                            y=df_mkt / 1e3,
                            name=str(ciu).replace("_", " ").title() + " (mercado)",
                            marker_color="#b8935a", opacity=0.65, boxmean="sd",
                        ))
                    if not df_cnd.empty:
                        fig_box.add_trace(go.Box(
                            y=df_cnd / 1e3,
                            name=str(ciu).replace("_", " ").title() + " (selección)",
                            marker_color="#1a6b4a",
                        ))
                if len(fig_box.data) > 0:
                    dark_layout(fig_box, height=250,
                                yaxis=dict(title="Precio/m² (miles COP)", gridcolor=_GRID),
                                legend=dict(orientation="v", yanchor="middle", y=0.5))
                    st.plotly_chart(fig_box, width="stretch")

            # Distribución de señales
            sig_cnt = candidatos["estado_inversion"].value_counts()
            fig2 = go.Figure(go.Pie(
                labels=sig_cnt.index, values=sig_cnt.values, hole=0.55,
                marker=dict(colors=["#1a6b4a", "#b8935a", "#8b2020"],
                            line=dict(color="white", width=2)),
                textfont=dict(family="DM Sans", size=11),
                hovertemplate="<b>%{label}</b>: %{value}<extra></extra>",
            ))
            dark_layout(fig2, height=200,
                        legend=dict(orientation="h", y=-0.1),
                        annotations=[dict(text=f"<b>{len(candidatos)}</b>",
                                          x=0.5, y=0.5, showarrow=False,
                                          font=dict(size=16, family="Playfair Display"))])
            st.plotly_chart(fig2, width="stretch")

        # Galería de acceso directo a oportunidades
        render_link_gallery(candidatos)

    # ── Chat ─────────────────────────────────────────────────────
    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Consulta al asesor financiero</div>',

                unsafe_allow_html=True)

    system_t1 = """Eres un experto en inteligencia financiera e inmobiliaria en Colombia. Tu objetivo es educar al usuario.

Reglas de comunicacion:
1. Interpretacion de Credito: Si el usuario tiene capacidad limitada, no le digas solo "no es viable". Explicale la tasa de esfuerzo y como cada 1% adicional en la tasa de interes afecta su cuota mensual a 20 anos.
2. Contexto de Inmueble: Si el inmueble es una "Oportunidad" (rentabilidad > 0%), justifica por que: Este precio esta un X% por debajo del promedio del sector Y, lo que protege tu inversion contra fluctuaciones de mercado.
3. Lenguaje Ciudadano: Usa "Costo financiero" en lugar de "Tasa EA", "Capacidad de pago" en lugar de "Tasa de esfuerzo".
4. Accionable: Siempre termina con un paso a seguir concreto: Te sugiero filtrar por inmuebles en el sector Z donde el precio m2 es menor a $X."""

    render_chat("t1", system_t1, "¿Cuál candidato me conviene más?", candidatos if (candidatos is not None and not candidatos.empty) else None)


# ══════════════════════════════════════════════════════════════════
# TAB 2 — ASESOR DE INVERSIÓN
# ══════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("## Simulador Financiero")
    st.markdown(DISCLAIMER_HTML, unsafe_allow_html=True)

    with st.expander("ℹ️ ¿Cómo usar el Simulador Financiero?", expanded=False):
        st.markdown("""
**El Simulador analiza los candidatos que encontraste en el Buscador para ayudarte a elegir la mejor inversión.**

**¿Qué muestra esta pestaña?**
- **Recomendación del modelo** — el inmueble con mayor señal de oportunidad de tu búsqueda y su precio estimado por la IA.
- **Galería por portal** — los mejores inmuebles disponibles en cada portal (Metrocuadrado, Ciencuadras, Properati…) con enlace directo.
- **Rentabilidad media por mercado** — comparativa de señal promedio del modelo entre los mercados de tu búsqueda.
- **Calidad de mercado** — score 0-100 que refleja liquidez, cobertura multiportal y consistencia de precios por mercado.
- **Top candidatos** — tabla de los mejores inmuebles filtrados, listos para analizar con el asesor IA.

**Flujo recomendado:** Primero usa el **Buscador Inteligente** → luego ven aquí para comparar candidatos → finalmente usa el **chat del asesor** (parte inferior) para profundizar en el análisis.
        """)

    cands = st.session_state.get("tab1_candidates", pd.DataFrame())
    if cands is None:
        cands = pd.DataFrame()

    if cands.empty:
        st.info("Primero configura tu búsqueda en **Asesor Inmobiliario** para ver el análisis de inversión.")
    else:
        best = cands.sort_values("rentabilidad_potencial", ascending=False).iloc[0]

        st.markdown('<div class="section-label">Recomendación del modelo</div>', unsafe_allow_html=True)
        b1, b2, b3, b4, b5 = st.columns(5)
        b1.metric("Mercado", str(best.get("market_token", "—")).replace("_", " ").title())
        b2.metric("Precio publicado", f"${best['precio_num']/1e6:.0f}M")
        b3.metric("Precio modelo", f"${best.get('precio_predicho', best['precio_num'])/1e6:.0f}M")
        b4.metric("Señal", f"{best['rentabilidad_potencial']:+.1f}%")
        b5.metric("m²", f"{best.get('area_m2', 0):.0f}")

        if best.get("num_portales", 1) > 1:
            disp = best.get("dispersion_pct_grupo", 0)
            p_min = best.get("precio_min_grupo", best["precio_num"])
            p_max = best.get("precio_max_grupo", best["precio_num"])
            st.success(
                f"⚡ **Inteligencia cross-portal:** Aparece en {int(best['num_portales'])} portales "
                f"(${p_min/1e6:.0f}M – ${p_max/1e6:.0f}M, dispersión {disp:.1f}%)."
            )

        # ── Market Gallery: Inteligencia Multi-Portal ────────────────────────
        # Lazy-load: primer acceso real a portal_operacion (DESPUÉS de load_gold)
        if gold_portales is None and not api_mode:
            gold_portales = load_portal_operacion()
            if gold_portales is not None and not gold_portales.empty and "gold_snapshot_at" in gold_portales.columns:
                latest_snapshot = gold_portales["gold_snapshot_at"].max()
                gold_portales = gold_portales[gold_portales["gold_snapshot_at"] == latest_snapshot]
        if gold_portales is not None and not gold_portales.empty:
            st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
            st.markdown('<div class="section-label">Galería de Oportunidades por Portal</div>', unsafe_allow_html=True)
            
            # Guía de Análisis (Leyenda Didáctica)
            st.info(
                "💡 **¿Cómo leer estas oportunidades?**\n"
                "- **[+15%] Oportunidad**: Significa que el inmueble tiene un **beneficio de compra del 15%** (está ese porcentaje por debajo del precio estimado del mercado).\n"
                "- **[-10%] Sobrevalorado**: El precio de lista es un 10% superior a lo que nuestra IA recomienda para esa zona y tipo de inmueble.\n"
                "*Analizamos miles de datos por portal para encontrar estos destaques.*"
            )

            st.warning(
                "⚠️ **Nota importante:** Este sistema está diseñado para **análisis e inteligencia inmobiliaria**, no para la venta directa de inmuebles. "
                "Para ver un inmueble en su fuente original, haga clic en el enlace de **Oportunidad**. "
                "Si el enlace no funciona o no lo redirige, es posible que el inmueble haya sido vendido o retirado del portal recientemente."
            )

            st.markdown(
                '<p style="color:var(--muted);font-size:.85rem;margin-top:1rem;margin-bottom:1.5rem">'
                "Escaneamos los portales líderes de Colombia en tiempo real. Abajo verás los inmuebles con el "
                "mejor balance entre precio y valor de mercado para cada plataforma.</p>",
                unsafe_allow_html=True
            )

            # Deduplicar
            gp_clean = gold_portales.sort_values("portal_ofertas_activas", ascending=False).drop_duplicates("portal")
            
            main_gps = gp_clean.head(4)
            secondary_gps = gp_clean.iloc[4:]

            def render_portal_card(row, col_context):
                p_id = str(row.get("portal", ""))
                pname = p_id.replace("_", " ").title()
                ofertas = int(row.get("portal_ofertas_activas", 0))
                status = str(row.get("checkpoint_status", "ok"))
                icon = "🟢" if status == "ok" else "🔴"

                # Buscar mejores oportunidades
                source_df = df if df is not None else cands
                mask_port = (source_df["fuente"] == p_id) & \
                            (source_df["rentabilidad_potencial"] < 200) & \
                            (source_df["rentabilidad_potencial"] > -50)
                top_port = source_df[mask_port].sort_values("rentabilidad_potencial", ascending=False).head(3)
                links_html = ""
                for _, prop in top_port.iterrows():
                    url = prop.get("url", "#")
                    signal = prop.get("rentabilidad_potencial", 0)
                    # Usar TITULO en lugar de ubicacion para que sea didáctico
                    raw_title = prop.get("titulo", prop.get("ubicacion_clean","Propiedad"))
                    title = str(raw_title)[:22] + "..." if len(str(raw_title)) > 22 else str(raw_title)
                    
                    label = "Oportunidad" if signal > 0 else "Mercado"
                    links_html += (
                        f'<div style="font-size:.68rem;margin-top:.4rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
                        f'<a href="{url}" target="_blank" style="color:var(--gold);text-decoration:none;font-weight:600">[{signal:+.1f}%] {label}</a><br>'
                        f'<span style="color:var(--muted);font-size:.62rem">{title}</span></div>'
                    )

                no_highlight_html = "<div style='font-size:.65rem;color:var(--muted);margin-top:.5rem'>Buscando las mejores ofertas...</div>"
                
                col_context.markdown(
                    f'<div style="background:var(--surface2);border:1px solid var(--border);'
                    f'padding:.8rem;border-radius:6px;margin-bottom:1rem;min-height:150px">'
                    f'<div style="font-size:.62rem;color:var(--muted);text-transform:uppercase;display:flex;justify-content:space-between;letter-spacing:1px">'
                    f'<span>{icon} {pname}</span> <span style="color:var(--ink);font-weight:bold">{ofertas:,}</span></div>'
                    f'<div style="margin-top:.5rem;border-top:1px solid rgba(255,255,255,.05);padding-top:.4rem">'
                    f'{links_html if links_html else no_highlight_html}</div></div>',
                    unsafe_allow_html=True,
                )

            # Renderizar Principales
            pcols = st.columns(min(len(main_gps), 4))
            for i, (_, row) in enumerate(main_gps.iterrows()):
                render_portal_card(row, pcols[i])

            # Renderizar Secundarios en un expansor si existen
            if not secondary_gps.empty:
                with st.expander("Ver otros portales y fuentes de datos"):
                    scols = st.columns(4)
                    for i, (_, row) in enumerate(secondary_gps.iterrows()):
                        render_portal_card(row, scols[i % 4])

        st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
        inv1, inv2 = st.columns(2)

        with inv1:
            st.markdown('<div class="section-label">Rentabilidad media por mercado</div>',
                        unsafe_allow_html=True)
            base_inv_df = df if df is not None else cands
            zona_rent = (
                base_inv_df.groupby("market_token")["rentabilidad_potencial"]
                .agg(["mean", "count"]).reset_index()
            )
            zona_rent = zona_rent[zona_rent["count"] >= 2].sort_values("mean", ascending=True).tail(15)
            zona_rent.columns = ["mercado", "rent_media", "n"]
            if zona_rent.empty and gold_analitica is not None and "market_quality_score" in gold_analitica.columns:
                # Fallback: mostrar precio mediano por mercado desde gold_analitica
                st.markdown('<div class="section-label">Precio mediano por mercado</div>',
                            unsafe_allow_html=True)
                _ga_mkt = gold_analitica[gold_analitica["analytics_level"] == "market"][
                    ["market_token", "precio_mediano"]
                ].dropna().sort_values("precio_mediano", ascending=True).tail(15)
                fig_bar = go.Figure(go.Bar(
                    x=_ga_mkt["precio_mediano"] / 1e6,
                    y=_ga_mkt["market_token"].str.replace("_", " ").str.title(),
                    orientation="h",
                    marker=dict(color=_ga_mkt["precio_mediano"],
                                colorscale=[[0, "#dfc69f"], [1, "#b8935a"]], showscale=False),
                    text=(_ga_mkt["precio_mediano"] / 1e6).apply(lambda x: f"${x:.0f}M"),
                    textposition="outside",
                ))
                dark_layout(fig_bar, height=400,
                            xaxis=dict(title="M COP", showgrid=True, gridcolor=_GRID),
                            yaxis=dict(showgrid=False))
                st.plotly_chart(fig_bar, width="stretch")
                st.caption("Haz una búsqueda en el Buscador para ver la señal de rentabilidad por mercado.")
            elif zona_rent.empty:
                st.info("Haz una búsqueda en el **Buscador Inteligente** para ver la rentabilidad comparada entre mercados.")
            else:
                fig_bar = go.Figure(go.Bar(
                    x=zona_rent["rent_media"],
                    y=zona_rent["mercado"].str.replace("_", " ").str.title(),
                    orientation="h",
                    marker=dict(color=zona_rent["rent_media"],
                                colorscale=[[0, "#8b2020"], [0.5, "#b8935a"], [1, "#1a6b4a"]],
                                showscale=False),
                    text=zona_rent["rent_media"].apply(lambda x: f"{x:+.1f}%"),
                    textposition="outside",
                ))
                dark_layout(fig_bar, height=400,
                            xaxis=dict(showgrid=True, gridcolor=_GRID, zeroline=True,
                                       zerolinecolor="#2c2c3a", zerolinewidth=1),
                            yaxis=dict(showgrid=False))
                st.plotly_chart(fig_bar, width="stretch")

        # Lazy-load: primer acceso real a mercado_analitica (DESPUÉS de load_gold)
        if gold_analitica is None and not api_mode:
            gold_analitica = load_mercado_analitica()

        with inv2:
            if gold_analitica is not None and "market_quality_score" in gold_analitica.columns:
                st.markdown('<div class="section-label">Calidad de mercado (quality score)</div>',
                            unsafe_allow_html=True)
                mkt_q = gold_analitica[gold_analitica["analytics_level"] == "market"][
                    ["market_token", "market_quality_score", "market_n"]
                ].sort_values("market_quality_score", ascending=False).head(15)
                fig_mq = go.Figure(go.Bar(
                    x=mkt_q["market_token"].str.replace("_", " ").str.title(),
                    y=mkt_q["market_quality_score"],
                    marker=dict(color=mkt_q["market_quality_score"],
                                colorscale=[[0, "#8b2020"], [0.5, "#b8935a"], [1, "#1a6b4a"]],
                                showscale=False),
                    text=mkt_q["market_quality_score"].apply(lambda x: f"{x:.0f}"),
                    textposition="outside",
                ))
                dark_layout(fig_mq, height=400,
                            xaxis=dict(tickangle=-35, showgrid=False),
                            yaxis=dict(title="Quality Score", showgrid=True, gridcolor=_GRID))
                st.plotly_chart(fig_mq, width="stretch")
            else:
                st.markdown('<div class="section-label">Precio/m² por mercado</div>',
                            unsafe_allow_html=True)
                m2_mkt = base_inv_df.groupby("market_token")["precio_m2"].median().reset_index()
                m2_mkt = m2_mkt.sort_values("precio_m2", ascending=False).head(15)
                fig_m2 = go.Figure(go.Bar(
                    x=m2_mkt["market_token"].str.replace("_", " ").str.title(),
                    y=m2_mkt["precio_m2"] / 1e6,
                    marker=dict(color="#b8935a"),
                    text=m2_mkt["precio_m2"].apply(lambda x: f"${x/1e6:.1f}M"),
                    textposition="outside",
                ))
                dark_layout(fig_m2, height=400,
                            xaxis=dict(tickangle=-30, showgrid=False),
                            yaxis=dict(title="M COP / m²", showgrid=True, gridcolor=_GRID))
                st.plotly_chart(fig_m2, width="stretch")

        # Top candidatos
        st.markdown('<div class="section-label">Top candidatos para inversión</div>',
                    unsafe_allow_html=True)
        inv_cols = [c for c in ["market_token", "city_token", "ubicacion_clean",
                                 "precio_num", "area_m2", "rentabilidad_potencial",
                                 "estado_inversion", "num_portales"] if c in cands.columns]
        top_inv = cands[inv_cols].head(8).copy()
        for col in ["market_token", "city_token"]:
            if col in top_inv.columns:
                top_inv[col] = top_inv[col].str.replace("_", " ").str.title()
        st.dataframe(
            top_inv.rename(columns={
                "market_token": "Mercado", "city_token": "Ciudad",
                "ubicacion_clean": "Zona", "precio_num": "Precio", "area_m2": "m²",
                "rentabilidad_potencial": "Señal %", "estado_inversion": "Señal",
                "num_portales": "Portales",
            }).style.format({"Precio": lambda x: f"${int(x):,}".replace(",", "."), "m²": "{:.0f}", "Señal %": "{:+.1f}%"}),
            width="stretch", hide_index=True,
        )

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Consulta al asesor de inversión</div>',
                unsafe_allow_html=True)
    system_t2 = """Eres un asesor de inversión inmobiliaria de Real Estate Analyst Colombia.
Ayudas a decidir qué inmueble comprar considerando: mercado (market_token), ciudad, señal del modelo,
inteligencia cross-portal, market_quality_score y perspectivas regionales.
Usa la jerarquía mercado → ciudad → zona. Cita datos concretos. Fecha de corte: {fc}.""".format(fc=FECHA_CORTE)
    render_chat("t2", system_t2, "¿Cuál mercado tiene mejor perspectiva?", cands if (cands is not None and not cands.empty) else (df.head(20) if df is not None else None))


# ══════════════════════════════════════════════════════════════════
# TAB 3 — VISIÓN DE COMPRA
# ══════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("## Dashboard — Inteligencia de mercado Colombia")
    st.markdown(DISCLAIMER_HTML, unsafe_allow_html=True)

    with st.expander("ℹ️ ¿Qué muestra el Dashboard?", expanded=False):
        st.markdown("""
**Vista macro del mercado inmobiliario colombiano — sin necesidad de hacer una búsqueda.**

**Secciones principales:**
- **Distribución de ofertas** — cuántos inmuebles activos hay por mercado (nuevo vs. usado, VIS vs. No VIS, por ciudad).
- **Precio mediano por mercado** — referencia de precios para comparar y calibrar expectativas antes de buscar.
- **Precio/m²** — qué mercados ofrecen más área por peso invertido.
- **Bandas de precio** — rango justo (banda baja / alta) calculado por el modelo para cada mercado. Útil para detectar si un precio es razonable.
- **Señal de mercado** — porcentaje de inmuebles etiquetados como "Oportunidad" vs "Sobrevalorado" por mercado. Ayuda a decidir *dónde* buscar.
- **Análisis editorial** — resumen narrativo generado automáticamente a partir de los datos reales del corte actual.

**Modo de uso:** Este Dashboard se actualiza con cada recarga de la app. No requiere configurar filtros. Sirve como punto de partida para entender el mercado antes de hacer búsquedas específicas.
        """)

    if df is None:
        # --- MODO LAZY (API o Local sin búsqueda cargada): RENDERIZAR CHARTS CON DATOS AGREGADOS ---
        st.markdown('<div class="section-label">Inteligencia de mercado — Vista Agregada</div>', unsafe_allow_html=True)
        
        if api_mode:
            catalog = market_catalog
        else:
            # Construir sobre la marcha usando mercado_analitica de S3 (local/lazy)
            ma = load_mercado_analitica()
            if ma is not None and not ma.empty:
                # Filtrar y agrupar tal como lo hace la API
                ma_city = ma[(ma["analytics_level"] == "city") & (ma["market_token"].notna())].copy()
                for c in ["market_n", "precio_mediano", "precio_m2_mediano", "area_mediana"]:
                    if c in ma_city.columns:
                        ma_city[c] = pd.to_numeric(ma_city[c], errors="coerce")
                catalog = ma_city.groupby(["market_token", "city_token"]).agg(
                    n_inmuebles=("market_n", "sum"),
                    precio_mediano=("precio_mediano", "median"),
                    precio_m2_mediano=("precio_m2_mediano", "median"),
                    area_mediana=("area_mediana", "median")
                ).reset_index().sort_values("n_inmuebles", ascending=False)
            else:
                catalog = pd.DataFrame()

        if catalog.empty:
            st.warning("Los datos agregados del catálogo de mercados no están disponibles.")
        else:
            market_catalog = catalog
            # 1. Distribución de Ofertas y Precio Mediano
            v1, v2 = st.columns(2)
            with v1:
                # Pie Chart: Ofertas por Mercado
                fig_vis = go.Figure(go.Pie(
                    labels=market_catalog["market_token"].str.replace("_", " ").str.title(),
                    values=market_catalog["n_inmuebles"],
                    hole=0.5,
                    marker=dict(colors=["#1a6b4a", "#1a4a8b", "#b8935a", "#8b2020", "#4b208b", "#208b8b", "#8b5a2b", "#b8860b"], line=dict(color="white", width=2)),
                    textfont=dict(family="DM Sans", size=12),
                ))
                dark_layout(fig_vis, height=320,
                            title=dict(text="Distribución de Ofertas por Mercado", font=dict(family="Playfair Display", size=14, color=_TEXT)),
                            legend=dict(orientation="h", y=-0.15))
                st.plotly_chart(fig_vis, width="stretch")
                
            with v2:
                # Bar Chart: Precio Mediano por Mercado (horizontal — legible con 22+ mercados)
                _med_sorted = market_catalog.dropna(subset=["precio_mediano"]).sort_values("precio_mediano", ascending=True)
                fig_med = go.Figure(go.Bar(
                    orientation='h',
                    y=_med_sorted["market_token"].str.replace("_", " ").str.title(),
                    x=_med_sorted["precio_mediano"] / 1e6,
                    marker=dict(color=_med_sorted["precio_mediano"], colorscale=[[0, "#dfc69f"], [1, "#b8935a"]], showscale=False),
                    text=(_med_sorted["precio_mediano"] / 1e6).apply(lambda x: f"${x:.0f}M"),
                    textposition="outside",
                ))
                dark_layout(fig_med, height=520,
                            title=dict(text="Precio Mediano por Mercado", font=dict(family="Playfair Display", size=14, color=_TEXT)),
                            xaxis=dict(title="M COP", showgrid=True, gridcolor=_GRID),
                            yaxis=dict(showgrid=False),
                            margin=dict(l=180, r=60, t=10, b=10))
                st.plotly_chart(fig_med, width="stretch")

            st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

            # 2. Precio por m² por mercado
            st.markdown('<div class="section-label">Eficiencia de compra — precio por m² por mercado</div>', unsafe_allow_html=True)
            st.caption("Mercados con menor precio/m² ofrecen más área por peso invertido.")
            
            _m2_sorted = market_catalog.dropna(subset=["precio_m2_mediano"]).sort_values("precio_m2_mediano", ascending=True)
            fig_m2bar = go.Figure(go.Bar(
                orientation='h',
                y=_m2_sorted["market_token"].str.replace("_", " ").str.title(),
                x=_m2_sorted["precio_m2_mediano"] / 1e6,
                marker=dict(color=_m2_sorted["precio_m2_mediano"],
                            colorscale=[[0, "#e8f4ef"], [1, "#0a3d28"]], showscale=False),
                text=(_m2_sorted["precio_m2_mediano"] / 1e6).apply(lambda x: f"${x:.2f}M"),
                textposition="outside",
            ))
            dark_layout(fig_m2bar, height=520,
                        xaxis=dict(title="M COP / m²", showgrid=True, gridcolor=_GRID),
                        yaxis=dict(showgrid=False),
                        margin=dict(l=180, r=60, t=10, b=10))
            st.plotly_chart(fig_m2bar, width="stretch")

            st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

            # 3. Bandas de referencia de mercado_analitica
            if gold_analitica is None:
                gold_analitica = load_mercado_analitica()
            if gold_analitica is not None and "lower_bound_ref" in gold_analitica.columns:
                st.markdown('<div class="section-label">Bandas de precio por mercado (mercado_analitica)</div>',
                            unsafe_allow_html=True)
                st.caption("Precio/m² mediano con bandas lower/upper — útil para evaluar si un inmueble está dentro del rango justo.")

                mkt_bands = gold_analitica[gold_analitica["analytics_level"] == "market"][
                    ["market_token", "market_n", "precio_m2_mediano", "lower_bound_ref",
                     "upper_bound_ref", "market_quality_score"]
                ].sort_values("market_n", ascending=False).head(15).copy()
                mkt_bands["market_label"] = mkt_bands["market_token"].str.replace("_", " ").str.title()

                st.dataframe(
                    mkt_bands[["market_label", "market_n", "precio_m2_mediano", "lower_bound_ref",
                                "upper_bound_ref", "market_quality_score"]].rename(columns={
                        "market_label": "Mercado", "market_n": "Ofertas",
                        "precio_m2_mediano": "Precio/m² Med.", "lower_bound_ref": "Banda baja",
                        "upper_bound_ref": "Banda alta", "market_quality_score": "Quality",
                    }).style.format({
                        "Precio/m² Med.": lambda x: f"${int(x):,}".replace(",", "."),
                        "Banda baja": lambda x: f"${int(x):,}".replace(",", "."),
                        "Banda alta": lambda x: f"${int(x):,}".replace(",", "."),
                        "Quality": "{:.0f}",
                    }),
                    width="stretch", hide_index=True,
                )
                st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    else:
        # ── Segmentación ─────────────────────────────────────────────
        st.markdown('<div class="section-label">Segmentación del mercado</div>', unsafe_allow_html=True)
        v1, v2 = st.columns(2)
        with v1:
            _segmento = df["precio_num"].apply(
                lambda x: "VIS (≤$250M)" if x <= 250_000_000 else "No VIS (>$250M)"
            )
            seg_cnt = _segmento.value_counts()
            fig_vis = go.Figure(go.Pie(
                labels=seg_cnt.index, values=seg_cnt.values, hole=0.5,
                marker=dict(colors=["#1a6b4a", "#1a4a8b"], line=dict(color="white", width=2)),
                textfont=dict(family="DM Sans", size=12),
            ))
            dark_layout(fig_vis, height=280,
                        title=dict(text="VIS vs No VIS", font=dict(family="Playfair Display", size=14, color=_TEXT)),
                        legend=dict(orientation="h", y=-0.15))
            st.plotly_chart(fig_vis, width="stretch")
            st.caption("Proxy: inmuebles ≤$250M clasificados como VIS.")

        with v2:
            if "estado_inmueble" in df.columns:
                est_cnt = df["estado_inmueble"].value_counts().head(4)
                fig_est = go.Figure(go.Bar(
                    x=est_cnt.index.str.title(), y=est_cnt.values,
                    marker=dict(color=["#1a6b4a", "#b8935a", "#1a4a8b", "#8b2020"][:len(est_cnt)]),
                    text=est_cnt.values, textposition="outside",
                ))
                dark_layout(fig_est, height=280,
                            title=dict(text="Nuevo vs Usado", font=dict(family="Playfair Display", size=14, color=_TEXT)),
                            xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor=_GRID))
                st.plotly_chart(fig_est, width="stretch")

        st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

        # ── Precio / m² por mercado ──────────────────────────────────
        st.markdown('<div class="section-label">Eficiencia de compra — precio por m² por mercado</div>',
                    unsafe_allow_html=True)
        st.caption("Mercados con menor precio/m² ofrecen más área por peso invertido.")

        m2_df = (
            df.groupby("market_token")
            .agg(precio_m2_mediano=("precio_m2", "median"), n=("precio_num", "count"))
            .reset_index()
        )
        m2_df = m2_df[m2_df["n"] >= 10].sort_values("precio_m2_mediano", ascending=False).head(20)
        fig_m2bar = go.Figure(go.Bar(
            x=m2_df["market_token"].str.replace("_", " ").str.title(),
            y=m2_df["precio_m2_mediano"] / 1e6,
            marker=dict(color=m2_df["precio_m2_mediano"],
                        colorscale=[[0, "#e8f4ef"], [1, "#0a3d28"]], showscale=False),
            text=m2_df["precio_m2_mediano"].apply(lambda x: f"${x/1e6:.1f}M"),
            textposition="outside",
        ))
        dark_layout(fig_m2bar, height=320,
                    xaxis=dict(tickangle=-35, showgrid=False),
                    yaxis=dict(title="M COP / m²", showgrid=True, gridcolor=_GRID))
        st.plotly_chart(fig_m2bar, width="stretch")

        st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

        # ── Top Sectores por Oportunidad ─────────────────────────────
        st.markdown('<div class="section-label">Top 15 Sectores con mayor rentabilidad estructural (Oportunidad)</div>',
                    unsafe_allow_html=True)
        st.caption("Sectores barriales con mejor señal algorítmica a nivel nacional. (Mínimo 5 ofertas)")

        if "sector_mercado" in df.columns:
            sec_df = (
                df.groupby("sector_mercado")
                .agg(rent_media=("rentabilidad_potencial", "mean"), n=("precio_num", "count"), pm2=("precio_m2", "median"))
                .reset_index()
            )
            sec_df = sec_df[sec_df["n"] >= 5].sort_values("rent_media", ascending=False).head(15)
            
            if not sec_df.empty:
                fig_sec = go.Figure(go.Bar(
                    x=sec_df["sector_mercado"].str.replace("_", " ").str.title(),
                    y=sec_df["rent_media"],
                    marker=dict(color=sec_df["rent_media"],
                                colorscale=[[0, "#b8935a"], [1, "#1a6b4a"]], showscale=False),
                    text=sec_df["rent_media"].apply(lambda x: f"{x:+.1f}%"),
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>Rentabilidad: %{text}<br>Ofertas: %{customdata[0]}<br>Precio/m² mediano: $%{customdata[1]:.2f}M<extra></extra>",
                    customdata=np.stack((sec_df["n"], sec_df["pm2"]/1e6), axis=-1)
                ))
                dark_layout(fig_sec, height=360,
                            xaxis=dict(tickangle=-35, showgrid=False),
                            yaxis=dict(title="Rentabilidad Media (%)", showgrid=True, gridcolor=_GRID))
                st.plotly_chart(fig_sec, width="stretch")
            else:
                st.info("No hay suficientes datos a nivel sectorial con más de 5 ofertas.")

        st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

        # ── Bandas de referencia (mercado_analitica) ─────────────────
        if gold_analitica is None:
            gold_analitica = load_mercado_analitica()
        if gold_analitica is not None and "lower_bound_ref" in gold_analitica.columns:
            st.markdown('<div class="section-label">Bandas de precio por mercado (mercado_analitica)</div>',
                        unsafe_allow_html=True)
            st.caption("Precio/m² mediano con bandas lower/upper — útil para evaluar si un inmueble está dentro del rango justo.")

            mkt_bands = gold_analitica[gold_analitica["analytics_level"] == "market"][
                ["market_token", "market_n", "precio_m2_mediano", "lower_bound_ref",
                 "upper_bound_ref", "market_quality_score"]
            ].sort_values("market_n", ascending=False).head(15).copy()
            mkt_bands["market_label"] = mkt_bands["market_token"].str.replace("_", " ").str.title()

            st.dataframe(
                mkt_bands[["market_label", "market_n", "precio_m2_mediano", "lower_bound_ref",
                            "upper_bound_ref", "market_quality_score"]].rename(columns={
                    "market_label": "Mercado", "market_n": "Ofertas",
                    "precio_m2_mediano": "Precio/m² Med.", "lower_bound_ref": "Banda baja",
                    "upper_bound_ref": "Banda alta", "market_quality_score": "Quality",
                }).style.format({
                    "Precio/m² Med.": lambda x: f"${int(x):,}".replace(",", "."),
                    "Banda baja": lambda x: f"${int(x):,}".replace(",", "."),
                    "Banda alta": lambda x: f"${int(x):,}".replace(",", "."),
                    "Quality": "{:.0f}",
                }),
                width="stretch", hide_index=True,
            )
            st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

        st.markdown('<div class="section-label">Señal de mercado — ¿es buen momento para comprar?</div>',
                    unsafe_allow_html=True)
        st.caption("Porcentaje de inmuebles 'Sobrevalorado' vs 'Oportunidad' por mercado.")

        sobrev = (
            df.groupby("market_token")["estado_inversion"]
            .apply(lambda x: (x == "Sobrevalorado").mean() * 100).reset_index()
        )
        sobrev.columns = ["mercado", "pct_sobre"]
        oport = (
            df.groupby("market_token")["estado_inversion"]
            .apply(lambda x: (x == "Oportunidad").mean() * 100).reset_index()
        )
        oport.columns = ["mercado", "pct_oport"]
        signal_df = sobrev.merge(oport, on="mercado").sort_values("pct_sobre", ascending=False).head(20)

        fig_signal = go.Figure()
        fig_signal.add_trace(go.Bar(
            name="Sobrevalorado %",
            x=signal_df["mercado"].str.replace("_", " ").str.title(),
            y=signal_df["pct_sobre"], marker_color="#8b2020", opacity=0.85,
        ))
        fig_signal.add_trace(go.Bar(
            name="Oportunidad %",
            x=signal_df["mercado"].str.replace("_", " ").str.title(),
            y=signal_df["pct_oport"], marker_color="#1a6b4a", opacity=0.85,
        ))
        fig_signal.add_hline(y=40, line=dict(color="#8b2020", width=1, dash="dot"),
                              annotation_text="Alerta sobrevaluación 40%",
                              annotation_font=dict(size=9, color="#8b2020", family="DM Mono"))
        dark_layout(fig_signal, height=340, barmode="group",
                    legend=dict(orientation="h", y=1.02),
                    xaxis=dict(tickangle=-35, showgrid=False),
                    yaxis=dict(title="%", showgrid=True, gridcolor=_GRID))
        st.plotly_chart(fig_signal, width="stretch")

        st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
        st.markdown('<div class="section-label">Análisis editorial — basado en datos reales</div>',
                    unsafe_allow_html=True)

        disp_med = df["dispersion_pct_grupo"].median() if "dispersion_pct_grupo" in df.columns else 0
        pct_sobre = (df["estado_inversion"] == "Sobrevalorado").mean() * 100
        pct_oport = (df["estado_inversion"] == "Oportunidad").mean() * 100
        pct_vis = (df["precio_num"] <= 250_000_000).mean() * 100

        mkt_stats = df.groupby("market_token").agg(
            n=("precio_num", "count"),
            rent_media=("rentabilidad_potencial", "mean"),
            pm2_mediana=("precio_m2", "median"),
        ).reset_index()
        mkt_stats = mkt_stats[mkt_stats["n"] >= 10]
        top_liquido = mkt_stats.sort_values("n", ascending=False).head(3)["market_token"].tolist()
        top_oport = mkt_stats.sort_values("rent_media", ascending=False).head(3)["market_token"].tolist()
        top_caro = mkt_stats.sort_values("pm2_mediana", ascending=False).head(3)["market_token"].tolist()

        def _mkt_label(tokens):
            return ", ".join([t.replace("_", " ").title() for t in tokens])

        ea1, ea2 = st.columns(2)
        with ea1:
            st.markdown(f"""
**Mercados más líquidos:** {_mkt_label(top_liquido)}
concentran la mayor oferta activa. Más opciones = más poder de negociación.

**Mercados con mejor señal:** {_mkt_label(top_oport)}
muestran la mayor diferencia positiva entre precio modelo y precio publicado
— potencial de margen para el comprador.

**VIS ({pct_vis:.0f}% de la oferta):** La demanda estructural de vivienda social sigue siendo
el motor de volumen. Oportunidad para quienes acceden a subsidios.
            """)

        with ea2:
            st.markdown(f"""
**¿Comprar ahora o esperar?**

Con una dispersión cross-portal promedio de **{disp_med:.1f}%**, existe margen de negociación.
Aproximadamente **{pct_sobre:.0f}%** de los inmuebles están sobrevalorados vs **{pct_oport:.0f}%**
que son oportunidad según el modelo.

**Mercados más caros (precio/m²):** {_mkt_label(top_caro)}
— aplica para inversión premium o alta gama.

**Recomendación:** Para vivienda propia, el mejor momento es cuando el perfil financiero es viable.
Para inversión, prioriza mercados con señal de oportunidad alta y buena cobertura multiportal.
            """)

        st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
        st.markdown('<div class="section-label">Consulta sobre el mercado</div>', unsafe_allow_html=True)
        system_t3 = """Eres un analista de mercado inmobiliario de Real Estate Analyst Colombia.
Respondes preguntas sobre mercados (market_token), ciudades, tendencias de precio, VIS vs No VIS,
nuevo vs usado, bandas de referencia y señal del modelo.
Usa la jerarquía mercado → ciudad → zona. Basa tus respuestas en datos reales del contexto.
Fecha de corte: {fc}.""".format(fc=FECHA_CORTE)
        render_chat("t3", system_t3, "¿Es buen momento para comprar en Medellín?", df.sample(min(100, len(df))))


# ══════════════════════════════════════════════════════════════════
# TAB 4 — VALORACIÓN
# ══════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("## Valoración de inmueble")
    st.markdown(
        '<div class="disclaimer">'
        'Estimación de precio por modelo XGBoost calibrado con datos de mercado colombiano. '
        'El resultado es orientativo — consulta con un tasador certificado para decisiones de compra.'
        '</div>',
        unsafe_allow_html=True,
    )

    with st.expander("ℹ️ ¿Cómo funciona la Valoración?", expanded=False):
        st.markdown("""
**Obtén una estimación del precio justo de un inmueble según el modelo de IA.**

**Cómo usarla:**
1. **Ficha técnica** — Ingresa el área (m²), habitaciones, baños y garajes del inmueble.
2. **Ubicación** — Selecciona ciudad y, si conoces la zona, elige la Zona / Sector correspondiente. Mientras más específica la zona, más precisa la estimación.
3. **Tipo** — Apartamento, casa, oficina, etc. y si es nuevo o usado.
4. **Calcular** — Haz clic en el botón. El modelo devuelve el precio estimado, el rango de confianza (±MAPE) y la señal respecto al precio que tú ingreses.

**¿Qué es el rango de confianza?**
El modelo tiene un error promedio (MAPE) de ~21%. Eso significa que para un inmueble estimado en $500M, el precio real podría estar entre $395M y $605M. El rango se muestra junto al resultado.

**¿Para qué sirve?**
- Validar si el precio que te piden es razonable para la zona y tipo de inmueble.
- Comparar propiedades similares en distintas zonas.
- Calcular el margen de negociación potencial antes de hacer una oferta.
        """)

    val1, val2 = st.columns([1, 1])

    with val1:
        st.markdown('<div class="section-label">Ficha técnica del inmueble</div>',
                    unsafe_allow_html=True)
        v_area  = st.number_input("Área (m²)", min_value=20, max_value=1000, value=80)
        v_habs  = st.number_input("Habitaciones", min_value=1, max_value=10, value=3)
        v_banos = st.number_input("Baños", min_value=1, max_value=8, value=2)
        v_gar   = st.number_input("Garajes", min_value=0, max_value=5, value=1)

        if df is not None:
            ciudades_val = sorted(df["city_token"].unique())
        elif gold_analitica is not None and not gold_analitica.empty and "city_token" in gold_analitica.columns:
            ciudades_val = sorted(gold_analitica["city_token"].dropna().unique())
        elif search_meta:
            ciudades_val = sorted(search_meta.get("cities", []))
        else:
            ciudades_val = ["armenia", "barrancabermeja", "barranquilla", "bogota", "bucaramanga",
                            "cali", "cartagena", "cucuta", "ibague", "manizales", "medellin",
                            "monteria", "neiva", "pasto", "pereira", "popayan", "santa marta",
                            "tunja", "valledupar", "villavicencio"]
        v_ciudad = st.selectbox("Ciudad", ciudades_val,
                                 index=ciudades_val.index("bogota") if "bogota" in ciudades_val else 0)
        # Derivar mercado automáticamente
        if not market_catalog.empty and {"city_token", "market_token"}.issubset(market_catalog.columns):
            city_map = dict(zip(market_catalog["city_token"], market_catalog["market_token"]))
        elif df is None:
            city_map = {}
        else:
            city_map = _build_city_market_map()
        v_mercado = city_map.get(v_ciudad, v_ciudad + "_metropolitana")
        st.markdown(
            f'<div style="font-size:.75rem;color:var(--muted);margin-top:-.3rem;margin-bottom:.5rem">'
            f'Mercado: <strong>{v_mercado.replace("_", " ").title()}</strong></div>',
            unsafe_allow_html=True,
        )

        # ── Granularidad geográfica ──────────────────────────────────
        # Mapa ciudad → zonas disponibles en el Gold layer (derivado del parquet)
        _ZONA_MAP = {
            "armenia": ["calarca_zona", "centro_armenia", "el_bosque_armenia"],
            "barrancabermeja": ["centro_barranca", "norte_barranca", "sur_barranca"],
            "barranquilla": ["norte_centro", "puerto_colombia_zona", "riomar", "soledad_zona", "sur_occidente"],
            "bogota": ["bosa_ciudad_bolivar", "cajica_zona", "centro_bogota", "chapinero", "chia_zona",
                       "el_retiro_zona", "engativa", "kennedy_fontibon", "la_calera_zona", "mosquera_zona",
                       "soacha_zona", "suba", "teusaquillo_barrios_unidos", "usaquen", "zipaquira_zona"],
            "bucaramanga": ["cabecera", "centro_bucaramanga", "floridablanca_zona", "giron_zona",
                            "piedecuesta_zona", "provenza_bucaramanga"],
            "cali": ["centro_cali", "jamundi_zona", "norte_cali", "oeste_cali", "oriente_cali",
                     "palmira_zona", "sur_cali", "yumbo_zona"],
            "cartagena": ["bocagrande_castillogrande", "historico", "manga_crespo", "norte_residencial", "zona_norte"],
            "cucuta": ["atalaya_zona", "caobos_cabecera", "centro_cucuta", "los_patios_zona", "villa_rosario_zona"],
            "ibague": ["ambala_picaleña", "centro_ibague", "norte_ibague"],
            "manizales": ["cable_millan", "centro_manizales", "palogrande_chipre", "villamaria_zona"],
            "medellin": ["aranjuez_manrique", "belen_guayabal", "bello_zona", "buenos_aires_medellin",
                         "centro_medellin", "el_poblado", "el_poblado_envigado", "itagui_zona",
                         "laureles_estadio", "robledo_castilla", "sabaneta_zona"],
            "monteria": ["castellana_zona", "centro_monteria", "sur_monteria"],
            "neiva": ["centro_neiva", "norte_neiva"],
            "pasto": ["centro_pasto", "norte_pasto"],
            "pereira": ["centro_pereira", "cerritos_zona", "dosquebradas_zona", "el_poblado",
                        "laureles_pereira", "pinares_cuba"],
            "popayan": ["centro_popayan"],
            "santa marta": ["bello_horizonte_zona", "centro_santa_marta", "norte_santa_marta", "rodadero"],
            "tunja": ["centro_tunja", "norte_tunja", "sur_tunja"],
            "villavicencio": ["barzal_centro", "norte_villavicencio", "sur_villavicencio"],
        }
        _zonas_ciudad = _ZONA_MAP.get(v_ciudad, [])
        if _zonas_ciudad:
            # Formato legible: kennedy_fontibon → Kennedy / Fontibón
            _zona_labels = {z: z.replace("_", " ").replace("zona", "").strip().title() for z in _zonas_ciudad}
            _zona_options = ["(ciudad completa)"] + _zonas_ciudad
            _zona_display = ["(ciudad completa)"] + [_zona_labels[z] for z in _zonas_ciudad]
            _zona_idx = st.selectbox(
                "Zona / Sector",
                options=range(len(_zona_options)),
                format_func=lambda i: _zona_display[i],
                help="Seleccionar zona mejora la precisión del modelo (usa estadísticas de precio del vecindario)"
            )
            v_comuna = _zona_options[_zona_idx] if _zona_idx > 0 else "comuna_otra"
        else:
            v_comuna = "comuna_otra"
            st.caption("Zona no disponible para esta ciudad — se usará promedio de la ciudad.")

        v_sector = v_comuna  # sector_mercado comparte granularidad con comuna en el Gold
        
        v_tipo   = st.selectbox("Tipo", ["apartamento", "casa", "oficina", "local_comercial", "otro"])
        v_estado = st.selectbox("Estado", ["usado", "nuevo"])


        btn_valorar = st.button("Generar valoración ◈")

    with val2:
        st.markdown('<div class="section-label">Resultado de valoración</div>',
                    unsafe_allow_html=True)

        if btn_valorar:
            # Carga perezosa del modelo XGBoost bundle solo si se presiona el botón
            if not api_mode and st.session_state.bundle is None:
                with st.spinner("Descargando modelo XGBoost desde S3..."):
                     st.session_state.bundle = load_model_bundle(manifest)
            
            with st.spinner("Ejecutando modelo XGBoost..."):
                try:
                    if not api_mode and st.session_state.bundle is None:
                        st.error("Modelo no disponible. Verifica conexión a S3.")
                    else:
                        row = {
                            "area_m2": float(v_area),
                            "habitaciones": float(v_habs),
                            "banos": float(v_banos),
                            "garajes": float(v_gar),
                            "tipo_inmueble": v_tipo,
                            "estado_inmueble": v_estado,
                            "fuente": "manual_input",
                            "city_token": v_ciudad,
                            "comuna_mercado": v_comuna,
                            "sector_mercado": v_sector,
                            "market_token": v_mercado,
                            "precio_num": 0,
                            "titulo": f"{v_tipo} {v_ciudad} {v_sector} {v_comuna}",
                            "ubicacion_norm": f"{v_ciudad} {v_comuna} {v_sector}",
                            "ubicacion_clean": f"{v_ciudad} {v_comuna} {v_sector}",
                        }

                        result = api_predict(row) if api_mode else None
                        if result is None:
                            if st.session_state.bundle is None:
                                with st.spinner("Descargando modelo XGBoost desde S3..."):
                                    st.session_state.bundle = load_model_bundle(manifest)
                            if st.session_state.bundle is None:
                                st.error("Bundle no disponible. Verifica acceso al modelo en S3.")
                                st.stop()
                            result = score_single(row, st.session_state.bundle)

                        if "error" in result:
                            st.error(f"Error en valoración: {result['error']}")
                        else:
                            valor_pred = result["valor_predicho"]
                            rango_low  = result["rango_low"]
                            rango_high = result["rango_high"]
                            mape_pct   = result["mape_pct"]

                            val_final = valor_pred
                            exp_ia = None
                            
                            if llm_ready() and df is not None:
                                with st.spinner("IA calculando con comparables de mercado..."):
                                    mask = (df["city_token"] == v_ciudad) & (df["comuna_mercado"] == v_comuna) & (df["tipo_inmueble"] == v_tipo)
                                    df_comp = df[mask]
                                    if df_comp.empty:
                                        df_comp = df[(df["city_token"] == v_ciudad) & (df["tipo_inmueble"] == v_tipo)]
                                    
                                    df_comp = df_comp.iloc[(df_comp['area_m2'] - v_area).abs().argsort()[:5]]
                                    
                                    ctx_val = ""
                                    if not df_comp.empty:
                                        cols = ["ubicacion_clean", "precio_num", "area_m2", "habitaciones", "estado_inversion"]
                                        ctx_val = df_comp[cols].to_json(orient="records", force_ascii=False)

                                    prompt_val = (
                                        f"Eres un tasador experto de inmuebles en Colombia. Tienes una solicitud para un(a) {v_tipo} "
                                        f"en {v_ciudad.title()}, {v_comuna.replace('comuna_','').title()}, "
                                        f"de {v_area}m² con {v_habs} hab. y {v_banos} baños.\n\n"
                                        f"Algoritmo base: $ {valor_pred/1e6:.0f} Millones COP. \n"
                                        f"Comparables reales:\n{ctx_val}\n\n"
                                        f"Tu tarea:\n"
                                        f"Evalúa los comparables. Si el algoritmo es irreal o castigó por métricas inusuales, ajusta el valor al mercado real.\n"
                                        f"Siempre devuelve EXACTAMENTE un JSON puro con esta estructura:\n"
                                        f"{{\n"
                                        f'  "valor_millones_cop": <numero_entero_como_1250>,\n'
                                        f'  "explicacion": "<Tu párrafo amigable de 3-4 líneas justificando el precio final con base en mercado local y comparables>"\n'
                                        f"}}\n"
                                        f"No devuelvas NADA más que el JSON válido."
                                    )
                                    try:
                                        resp_text = call_llm([{"role": "user", "content": prompt_val}])
                                        
                                        if resp_text.startswith("```"):
                                            resp_text = resp_text.split("\n", 1)[1].rsplit("\n", 1)[0]
                                        if resp_text.startswith("json"):
                                            resp_text = resp_text[4:].strip()
                                        import json
                                        parsed = json.loads(resp_text)
                                        val_final = parsed.get("valor_millones_cop", valor_pred/1e6) * 1e6
                                        exp_ia = parsed.get("explicacion", "")
                                    except Exception as e:
                                        pass

                            if exp_ia:
                                st.metric("💡 Valoración IA Experta", f"${val_final/1e6:.0f}M COP", delta=f"Algoritmo base: ${valor_pred/1e6:.0f}M COP", delta_color="off")
                            else:
                                st.metric("Valoración estimada", f"${valor_pred/1e6:.0f}M COP")

                            st.markdown(
                                f'<div style="font-size:.8rem;color:var(--muted);margin-top:-.5rem">'
                                f'Rango de confianza algoritmo (±MAPE {mape_pct:.0f}%): '
                                f'<strong>${rango_low/1e6:.0f}M – ${rango_high/1e6:.0f}M</strong></div>',
                                unsafe_allow_html=True,
                            )
                            st.metric("Precio estimado / m²", f"${result['precio_m2_pred']/1e6:.2f}M")

                            local = df[df["city_token"] == v_ciudad]["precio_num"] if df is not None else pd.Series(dtype=float)
                            if not local.empty:
                                pct = (valor_pred - local.median()) / local.median() * 100
                                st.metric(
                                    f"vs mediana {v_ciudad.title()}",
                                    f"{pct:+.1f}%",
                                    delta=f"Mediana ciudad: ${local.median()/1e6:.0f}M",
                                )

                            # Comparar con mercado
                            mkt_local = df[df["market_token"] == v_mercado]["precio_num"] if df is not None else pd.Series(dtype=float)
                            if not mkt_local.empty and v_mercado != v_ciudad + "_metropolitana":
                                pct_mkt = (valor_pred - mkt_local.median()) / mkt_local.median() * 100
                                st.metric(
                                    f"vs mediana mercado",
                                    f"{pct_mkt:+.1f}%",
                                    delta=f"Mediana mercado: ${mkt_local.median()/1e6:.0f}M",
                                )
                                
                            if exp_ia:
                                st.markdown(
                                    f'<div style="background:var(--surface2);border:1px solid var(--border);'
                                    f'border-left:4px solid var(--gold);padding:1rem;font-size:.88rem;'
                                    f'color:var(--ink);line-height:1.6;margin-top:1.5rem;border-radius:2px">'
                                    f'<strong>Análisis detallado (IA):</strong><br><br>{exp_ia}'
                                    f'</div>',
                                    unsafe_allow_html=True,
                                )

                except Exception as e:
                    st.error(f"Error en valoración: {e}")

        else:
            st.markdown(
                '<div style="background:white;border:1px solid var(--border);'
                'padding:2rem;text-align:center;color:var(--muted);font-size:.88rem;'
                'border-radius:2px">'
                'Completa la ficha técnica y presiona<br>'
                '<strong style="color:var(--ink)">Generar valoración ◈</strong>'
                '</div>',
                unsafe_allow_html=True,
            )