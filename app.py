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
import sys
import tempfile
import re
import warnings
import time
import tracemalloc
import s3fs
import pyarrow.dataset as ds

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

# Validar secretos mínimos antes de arrancar
if "aws" not in st.secrets:
    st.error("🔑 **Faltan los Secretos en Streamlit Cloud.** Ve a Settings -> Secrets y pega tu .streamlit/secrets.toml local.")
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

# ══════════════════════════════════════════════════════════════════
# CLIENTES Y CARGA (S3, Bedrock, Ollama)
# ══════════════════════════════════════════════════════════════════

# 1. Configuración S3 / AWS
@st.cache_resource(show_spinner=False)
def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
        region_name=st.secrets["aws"].get("aws_region", "us-east-1"),
    )

# 2. Configuración Bedrock (Llama 3.1)
@st.cache_resource(show_spinner=False)
def get_bedrock():
    try:
        return boto3.client(
            "bedrock-runtime",
            aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
            region_name=st.secrets["aws"].get("aws_region", "us-east-1"),
        )
    except Exception:
        return None

# 3. Configuración Ollama (Fallback local)
_llm_cfg = st.secrets.get("llm", {})
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



@st.cache_resource(show_spinner=False)
def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
        region_name=st.secrets["aws"].get("aws_region", "us-east-1"),
    )


@st.cache_data(show_spinner=False, ttl=3600)
def load_manifest():
    """Carga solo el manifest (JSON) de S3."""
    try:
        s3 = get_s3()
        bucket = st.secrets["aws"]["s3_bucket_name"]
        return json.loads(
            s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)["Body"].read()
        )
    except Exception:
        return {}

def load_model_bundle(manifest=None):
    """Carga el bundle pesado (.pkl) de S3."""
    s3 = get_s3()
    bucket = st.secrets["aws"]["s3_bucket_name"]
    
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
            bundle = json.loads(raw_data)
            
            # Si el bundle tiene el modelo como string/dict JSON, lo cargamos en XGBRegressor
            if "model" in bundle and isinstance(bundle["model"], (str, dict)):
                model_json = bundle["model"]
                if isinstance(model_json, dict):
                    model_json = json.dumps(model_json)
                
                reg = xgb.XGBRegressor()
                # Crear un archivo temporal para cargar (XGBRegressor.load_model prefiere archivos)
                import tempfile
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
    return {
        "key": st.secrets["aws"]["aws_access_key_id"],
        "secret": st.secrets["aws"]["aws_secret_access_key"],
    }

def _s3_read_gold(table_name: str) -> pd.DataFrame | None:
    """Lee una tabla Gold desde S3 optimizando RAM mediante poda de columnas e instancias duplicadas."""
    try:
        bucket = st.secrets["aws"]["s3_bucket_name"]
        print(f"[REABOOT] Iniciando descarga S3: {table_name}", flush=True)
        t0 = time.time()
        
        fs = s3fs.S3FileSystem(
            key=st.secrets["aws"]["aws_access_key_id"],
            secret=st.secrets["aws"]["aws_secret_access_key"]
        )
        s3_path = f"{bucket}/gold/{table_name}/"
        
        dataset = ds.dataset(s3_path, filesystem=fs, format="parquet")
        all_cols = dataset.schema.names
        
        ui_cols = [
            "id_original", "id_inmueble", "city_token", "market_token", "ubicacion_norm", "ubicacion_raw", "ubicacion_clean",
            "precio_num", "area_m2", "habitaciones", "banos", "garajes", "tipo_inmueble", "estado_inmueble",
            "fuente", "url", "titulo", "rentabilidad_potencial", "estado_inversion", "comuna_mercado", "sector_mercado",
            "num_portales", "dispersion_pct_grupo", "precio_mediano_grupo", "precio_min_grupo", "precio_max_grupo", "precio_m2"
        ]
        
        cols_to_load = [c for c in ui_cols if c in all_cols]
        print(f"[REABOOT] Extrayendo {len(cols_to_load)} de {len(all_cols)} columnas...", flush=True)
        
        table = dataset.to_table(columns=cols_to_load)
        df = table.to_pandas()
        
        # Supervivencia estricta para matar duplicados del Lakehouse
        if "id_original" in df.columns:
            df = df.drop_duplicates(subset=["id_original"], keep="last")
        elif "id_inmueble" in df.columns:
            df = df.drop_duplicates(subset=["id_inmueble"], keep="last")
            
        print(f"[REABOOT] EXITO S3: {table_name} descargado en {time.time()-t0:.2f}s. Shape RAM final: {df.shape}", flush=True)
        return df
    except Exception as e:
        print(f"[REABOOT] ERROR S3 {table_name}: {e}", flush=True)
        return None


@st.cache_data(show_spinner="Cargando portafolio...", ttl=3600)
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


@st.cache_data(show_spinner=False, ttl=3600)
def load_mercado_analitica():
    return _s3_read_gold("mercado_analitica")


@st.cache_data(show_spinner=False, ttl=3600)
def load_mercado_sectorial():
    return _s3_read_gold("mercado_sectorial")


@st.cache_data(show_spinner=False, ttl=3600)
def load_portal_operacion():
    return _s3_read_gold("portal_operacion")


@st.cache_data(show_spinner=False, ttl=7200)
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
    })




# ══════════════════════════════════════════════════════════════════
# CARGA INICIAL
# ══════════════════════════════════════════════════════════════════
print("\n[REABOOT] 🚀 === INICIANDO BOOT DE APP.PY ===", flush=True)
tracemalloc.start()

raw_df = load_gold()
curr, peak = tracemalloc.get_traced_memory()
print(f"[REABOOT] MEMORY AFTER load_gold(): Current {curr/1e6:.1f}MB, Peak {peak/1e6:.1f}MB", flush=True)

manifest = load_manifest()
gold_analitica = load_mercado_analitica()
gold_sectorial = load_mercado_sectorial()
gold_portales  = load_portal_operacion()

curr, peak = tracemalloc.get_traced_memory()
print(f"[REABOOT] MEMORY AFTER ALL Caches: Current {curr/1e6:.1f}MB, Peak {peak/1e6:.1f}MB", flush=True)

# Solo cargar modelo pesado si la data NO viene pre-costeada de Databricks
pre_scored = "rentabilidad_potencial" in raw_df.columns and "estado_inversion" in raw_df.columns

if "master_db" not in st.session_state:
    if pre_scored:
        st.session_state.master_db = raw_df
    else:
        bundle = load_model_bundle(manifest)
        with st.spinner("Calculando señales (Fallback a XGBoost local)..."):
            st.session_state.master_db = score_dataframe(raw_df, bundle)

# El bundle se carga como None inicialmente si ya hay pre-scored (ahorro RAM)
if "bundle" not in st.session_state:
    st.session_state.bundle = None

df = st.session_state.master_db

# KPIs globales
N          = len(df)
MED_PRECIO = df["precio_num"].median()
MED_M2     = df["precio_m2"].median() if "precio_m2" in df.columns else 0
N_MERCADOS = df["market_token"].nunique() if "market_token" in df.columns else 0
N_CIUDADES = (df["city_token"].value_counts() >= 5).sum()
N_OPT      = (df["estado_inversion"] == "Oportunidad").sum()
MAPE_BADGE = manifest.get("metrics", {}).get("mape", "N/A")
DEPLOYED = (
    manifest.get("promoted_at", "")
    or manifest.get("trained_at", "")
    or ""
)[:10] if manifest else ""

# Portales activos — dinámico desde portal_operacion
if gold_portales is not None and "portal" in gold_portales.columns:
    N_PORTALES = len(gold_portales)
    PORTALES_SANOS = int((gold_portales.get("checkpoint_status", pd.Series(["ok"])) == "ok").sum()) \
        if "checkpoint_status" in gold_portales.columns else N_PORTALES
else:
    N_PORTALES = df["fuente"].nunique() if "fuente" in df.columns else 0
    PORTALES_SANOS = N_PORTALES


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
    sb1.metric("MAPE modelo", f"{MAPE_BADGE}%" if isinstance(MAPE_BADGE, (int, float)) else MAPE_BADGE)
    sb2.metric("Actualizado", DEPLOYED or "N/A")

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
# TABS
# ══════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs([
    "Asesor Inmobiliario",
    "Asesor de Inversión",
    "Visión de Compra",
    "Valoración  ⚒",
])


# ══════════════════════════════════════════════════════════════════
# TAB 1 — ASESOR INMOBILIARIO
# ══════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("## Encuentra tu inmueble ideal")
    st.markdown(DISCLAIMER_HTML, unsafe_allow_html=True)

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

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Cuota mensual estimada", f"${cuota:,.0f}")
        m2.metric("Presupuesto máx (70%)", f"${monto_max/1e6:.0f}M")
        m3.metric("Interés total (20 años)", f"${(cuota*plazo - prestamo)/1e6:.0f}M")
        m4.metric("Viabilidad", "✓ Viable" if viable else "✗ Excede capacidad")

        if not viable:
            deficit = cuota - capacidad
            st.warning(
                f"💡 **Recomendación financiera:** La cuota estimada supera tu capacidad en "
                f"**${deficit:,.0f} COP/mes**. Considera: (1) reducir el monto del préstamo a "
                f"${prestamo * (capacidad/cuota)/1e6:.0f}M, (2) ampliar el plazo a 25-30 años, "
                f"o (3) aumentar el ahorro inicial para bajar el capital financiado."
            )

    # ── Filtros ──────────────────────────────────────────────────
    with st.expander("▸ Filtros de búsqueda", expanded=True):
        fc_geo1, fc_geo2, fc_geo3, fc_geo4 = st.columns(4)
        with fc_geo1:
            mercados_disp = sorted([m for m in df["market_token"].unique()
                                     if m and str(m) not in ("nan", "otra_ciudad_metropolitana")])
            mercado_sel = st.multiselect("Mercado", options=mercados_disp,
                                          help="Mercado comercial / metropolitano")
        with fc_geo2:
            # Ciudades filtradas por mercado seleccionado
            if mercado_sel:
                ciudades_filtradas = df[df["market_token"].isin(mercado_sel)]["city_token"].unique()
            else:
                ciudades_filtradas = df["city_token"].unique()
            ciudades_disp = sorted([c for c in ciudades_filtradas if c != "otra_ciudad"])
            ciudad_sel = st.multiselect("Ciudad (municipio)", options=ciudades_disp)
            st.session_state.ciudad_interes = ", ".join(ciudad_sel) if ciudad_sel else (
                ", ".join(mercado_sel) if mercado_sel else "No especificada"
            )
        
        with fc_geo3:
            if ciudad_sel and "comuna_mercado" in df.columns:
                comunas_f = df[df["city_token"].isin(ciudad_sel)]["comuna_mercado"].unique()
                comuna_disp = sorted([c for c in comunas_f if pd.notna(c) and c != "comuna_otra"])
            else:
                comuna_disp = []
            comuna_sel = st.multiselect("Comuna / Zona", options=comuna_disp)

        with fc_geo4:
            if comuna_sel and "sector_mercado" in df.columns:
                sectores_f = df[df["comuna_mercado"].isin(comuna_sel)]["sector_mercado"].unique()
                sector_disp = sorted([s for s in sectores_f if pd.notna(s) and s != "sector_otra"])
            else:
                sector_disp = []
            sector_sel = st.multiselect("Sector / Barrio", options=sector_disp)

        fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 1])
        with fc1:
            tipos_disp = sorted([t for t in df["tipo_inmueble"].unique()
                                  if t not in ("desconocido", "otro", "nan")])
            tipo_sel = st.multiselect("Tipo", options=tipos_disp)
        with fc2:
            habs_min = st.selectbox("Hab. mínimas", [1, 2, 3, 4], index=1)
        with fc3:
            estado_sel = st.multiselect("Estado", ["nuevo", "usado"])
        with fc4:
            only_multiportal = st.checkbox("Solo multiportal", value=False,
                                            help="Mostrar solo inmuebles en 2+ portales")

        p_max_slider = float(min(monto_max * 1.3, df["precio_num"].quantile(0.97)))
        precio_rango = st.slider(
            "Rango de precio (COP)",
            min_value=50_000_000.0,
            max_value=p_max_slider,
            value=(50_000_000.0, float(monto_max)),
            step=5_000_000.0,
            format="$%.0f",
        )

    # ── Filtrado ─────────────────────────────────────────────────
    mask = (df["precio_num"].between(*precio_rango))
    if mercado_sel:
        mask &= df["market_token"].isin(mercado_sel)
    if ciudad_sel:
        mask &= df["city_token"].isin(ciudad_sel)
    if 'comuna_sel' in locals() and comuna_sel:
        mask &= df["comuna_mercado"].isin(comuna_sel)
    if 'sector_sel' in locals() and sector_sel:
        mask &= df["sector_mercado"].isin(sector_sel)
    if tipo_sel:
        mask &= df["tipo_inmueble"].isin(tipo_sel)
    if estado_sel:
        mask &= df["estado_inmueble"].isin(estado_sel)
    if "habitaciones" in df.columns:
        mask &= df["habitaciones"].fillna(0) >= habs_min
    if only_multiportal and "num_portales" in df.columns:
        mask &= df["num_portales"].fillna(0) > 1

    candidatos = df[mask].sort_values("rentabilidad_potencial", ascending=False).head(25)
    st.session_state.tab1_candidates = candidatos

    # ── Resultados ───────────────────────────────────────────────
    if candidatos.empty:
        # --- DIAGNÓSTICO TEMPORAL ---
        mask_diag = df["precio_num"].between(*precio_rango)
        c2 = mask_diag.sum()
        if mercado_sel: mask_diag &= df["market_token"].isin(mercado_sel)
        c3 = mask_diag.sum()
        if tipo_sel: mask_diag &= df["tipo_inmueble"].isin(tipo_sel)
        c4 = mask_diag.sum()
        if estado_sel: mask_diag &= df["estado_inmueble"].isin(estado_sel)
        c5 = mask_diag.sum()
        
        st.error("Sin candidatos para los filtros actuales. Ajusta el rango de precio, mercado o ciudad.")
        st.warning(f"🔍 **DIAGNÓSTICO DETALLADO: ¿Por qué hay 0?**\n\n"
                   f"1. En rango de precio (Toda Colombia): **{c2}**\n"
                   f"2. En el mercado seleccionado: **{c3}**\n"
                   f"3. Que sean tipo '{tipo_sel}': **{c4}**\n"
                   f"4. Que ADEMÁS tengan etiqueta explicita de estado '{estado_sel}': **{c5}**\n\n"
                   f"👉 Si el paso 4 cae a **0**, significa que los portales inmobiliarios NO etiquetaron correctamente el estado de esos inmuebles (los dejaron vacíos o 'desconocido'). **Quita la 'X' en Estado** para verlos.")
        # ---------------------------
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
            if "Precio" in df_show.columns:       fmt["Precio"]        = "${:,.0f}"
            if "Precio modelo" in df_show.columns: fmt["Precio modelo"] = "${:,.0f}"
            if "Señal %" in df_show.columns:       fmt["Señal %"]       = "{:+.1f}%"
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

    # ── Chat ─────────────────────────────────────────────────────
    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Consulta al asesor financiero</div>',
                unsafe_allow_html=True)

    system_t1 = """Eres un asesor financiero inmobiliario de Real Estate Analyst Colombia.
Tu rol es ayudar al usuario a encontrar el inmueble que mejor se ajuste a su perfil de crédito.
Analiza los candidatos del contexto y da recomendaciones concretas: mercado, ciudad, zona, precio, área, señal del modelo.
Usa la jerarquía: mercado (market_token) → ciudad (city_token) → zona (ubicación).
Si el perfil financiero no alcanza, da recomendaciones específicas para mejorar su capacidad de compra."""

    render_chat("t1", system_t1, "¿Cuál candidato me conviene más?", candidatos if not candidatos.empty else None)


# ══════════════════════════════════════════════════════════════════
# TAB 2 — ASESOR DE INVERSIÓN
# ══════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("## Asesor de inversión")
    st.markdown(DISCLAIMER_HTML, unsafe_allow_html=True)

    cands = st.session_state.get("tab1_candidates", pd.DataFrame())

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
                mask_port = (df["fuente"] == p_id) & \
                            (df["rentabilidad_potencial"] < 200) & \
                            (df["rentabilidad_potencial"] > -50)
                
                top_port = df[mask_port].sort_values("rentabilidad_potencial", ascending=False).head(3)
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
            zona_rent = (
                df.groupby("market_token")["rentabilidad_potencial"]
                .agg(["mean", "count"]).reset_index()
            )
            zona_rent = zona_rent[zona_rent["count"] >= 10].sort_values("mean", ascending=True).tail(15)
            zona_rent.columns = ["mercado", "rent_media", "n"]
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
                m2_mkt = df.groupby("market_token")["precio_m2"].median().reset_index()
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
            }).style.format({"Precio": "${:,.0f}", "m²": "{:.0f}", "Señal %": "{:+.1f}%"}),
            width="stretch", hide_index=True,
        )

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Consulta al asesor de inversión</div>',
                unsafe_allow_html=True)
    system_t2 = """Eres un asesor de inversión inmobiliaria de Real Estate Analyst Colombia.
Ayudas a decidir qué inmueble comprar considerando: mercado (market_token), ciudad, señal del modelo,
inteligencia cross-portal, market_quality_score y perspectivas regionales.
Usa la jerarquía mercado → ciudad → zona. Cita datos concretos. Fecha de corte: {fc}.""".format(fc=FECHA_CORTE)
    render_chat("t2", system_t2, "¿Cuál mercado tiene mejor perspectiva?", cands if not cands.empty else df.head(20))


# ══════════════════════════════════════════════════════════════════
# TAB 3 — VISIÓN DE COMPRA
# ══════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("## Inteligencia de mercado Colombia")
    st.markdown(DISCLAIMER_HTML, unsafe_allow_html=True)

    # ── Segmentación ─────────────────────────────────────────────
    st.markdown('<div class="section-label">Segmentación del mercado</div>', unsafe_allow_html=True)
    v1, v2 = st.columns(2)
    with v1:
        df["segmento"] = df["precio_num"].apply(
            lambda x: "VIS (≤$250M)" if x <= 250_000_000 else "No VIS (>$250M)"
        )
        seg_cnt = df["segmento"].value_counts()
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
                "Precio/m² Med.": "${:,.0f}", "Banda baja": "${:,.0f}",
                "Banda alta": "${:,.0f}", "Quality": "{:.0f}",
            }),
            width="stretch", hide_index=True,
        )
        st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

    # ── Señal de sobrecalentamiento por mercado ──────────────────
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

    # ── Análisis editorial dinámico ──────────────────────────────
    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Análisis editorial — basado en datos reales</div>',
                unsafe_allow_html=True)

    # Generar editorial dinámico de agregados
    disp_med = df["dispersion_pct_grupo"].median() if "dispersion_pct_grupo" in df.columns else 0
    pct_sobre = (df["estado_inversion"] == "Sobrevalorado").mean() * 100
    pct_oport = (df["estado_inversion"] == "Oportunidad").mean() * 100
    pct_vis = (df["precio_num"] <= 250_000_000).mean() * 100

    # Mercados más líquidos y con mejor señal
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
        '<strong>Apartado en construcción ⚒</strong> — La valoración por características ya funciona. '
        'El análisis de imágenes usa el LLM para descripción cualitativa de acabados; '
        'el número de valoración viene del modelo XGBoost, no de la imagen.'
        '</div>',
        unsafe_allow_html=True,
    )

    val1, val2 = st.columns([1, 1])

    with val1:
        st.markdown('<div class="section-label">Ficha técnica del inmueble</div>',
                    unsafe_allow_html=True)
        v_area  = st.number_input("Área (m²)", min_value=20, max_value=1000, value=80)
        v_habs  = st.number_input("Habitaciones", min_value=1, max_value=10, value=3)
        v_banos = st.number_input("Baños", min_value=1, max_value=8, value=2)
        v_gar   = st.number_input("Garajes", min_value=0, max_value=5, value=1)

        ciudades_val = sorted(df["city_token"].unique())
        v_ciudad = st.selectbox("Ciudad", ciudades_val,
                                 index=ciudades_val.index("bogota") if "bogota" in ciudades_val else 0)
        # Derivar mercado automáticamente
        city_map = _build_city_market_map()
        v_mercado = city_map.get(v_ciudad, v_ciudad + "_metropolitana")
        st.markdown(
            f'<div style="font-size:.75rem;color:var(--muted);margin-top:-.3rem;margin-bottom:.5rem">'
            f'Mercado: <strong>{v_mercado.replace("_", " ").title()}</strong></div>',
            unsafe_allow_html=True,
        )

        # ── Granularidad geográfica ──────────────────────────────────
        df_ciudad = df[df["city_token"] == v_ciudad]
        if "comuna_mercado" in df.columns:
            comunas_disp = sorted([c for c in df_ciudad["comuna_mercado"].unique() if pd.notna(c) and c != "comuna_otra"])
            v_comuna = st.selectbox("Comuna / Zona", ["comuna_otra"] + comunas_disp)
        else:
            v_comuna = "comuna_otra"
            
        df_comuna = df_ciudad[df_ciudad["comuna_mercado"] == v_comuna] if v_comuna != "comuna_otra" else df_ciudad
        v_sector = "sector_otra" # Simplificación UX sugerida por usuario
        
        v_tipo   = st.selectbox("Tipo", ["apartamento", "casa", "oficina", "local_comercial", "otro"])
        v_estado = st.selectbox("Estado", ["usado", "nuevo"])

        st.markdown('<div class="section-label" style="margin-top:1rem">Imágenes (opcional)</div>',
                    unsafe_allow_html=True)
        imgs = st.file_uploader(
            "Sube 1-5 fotos del inmueble para análisis de acabados",
            type=["jpg", "jpeg", "png", "webp"],
            accept_multiple_files=True,
        )
        if imgs:
            cols_imgs = st.columns(min(len(imgs), 3))
            for i, img in enumerate(imgs[:3]):
                cols_imgs[i].image(img, width="stretch")

        btn_valorar = st.button("Generar valoración ◈")

    with val2:
        st.markdown('<div class="section-label">Resultado de valoración</div>',
                    unsafe_allow_html=True)

        if btn_valorar:
            # Carga perezosa del modelo XGBoost bundle solo si se presiona el botón
            if st.session_state.bundle is None:
                with st.spinner("Descargando modelo XGBoost desde S3..."):
                     st.session_state.bundle = load_model_bundle(manifest)
            
            with st.spinner("Ejecutando modelo XGBoost..."):
                try:
                    if st.session_state.bundle is None:
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
                            
                            if llm_ready():
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

                            local = df[df["city_token"] == v_ciudad]["precio_num"]
                            if not local.empty:
                                pct = (valor_pred - local.median()) / local.median() * 100
                                st.metric(
                                    f"vs mediana {v_ciudad.title()}",
                                    f"{pct:+.1f}%",
                                    delta=f"Mediana ciudad: ${local.median()/1e6:.0f}M",
                                )

                            # Comparar con mercado
                            mkt_local = df[df["market_token"] == v_mercado]["precio_num"]
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

            # ── Análisis de imagen con LLM ───────────────────────
            if imgs and llm_ready():
                with st.spinner("Analizando acabados con IA..."):
                    import base64
                    img_contents = []
                    for img in imgs[:3]:
                        b64 = base64.b64encode(img.read()).decode()
                        ext = img.name.split(".")[-1].lower()
                        mime = f"image/{'jpeg' if ext in ('jpg','jpeg') else ext}"
                        img_contents.append({"type": "image_url",
                                              "image_url": {"url": f"data:{mime};base64,{b64}"}})

                    img_contents.append({
                        "type": "text",
                        "text": (
                            f"Analiza estas imágenes de un {v_tipo} en {v_ciudad.title()} Colombia "
                            f"de {v_area}m², {v_habs} hab., {v_banos} baños. "
                            "Describe: 1) Calidad de acabados (pisos, paredes, cocina, baños), "
                            "2) Estado de conservación, "
                            "3) Estimación de estrato socioeconómico (1-6), "
                            "4) Si los acabados son consistentes con el rango de precio estimado. "
                            "Sé concreto y objetivo. Máximo 150 palabras."
                        )
                    })

                    try:
                        analysis = call_llm([{"role": "user", "content": img_contents}])
                        st.markdown('<div class="section-label" style="margin-top:1rem">Análisis de acabados (IA)</div>',
                                    unsafe_allow_html=True)
                        st.markdown(
                            f'<div style="background:var(--surface2);border:1px solid var(--border);'
                            f'border-left:4px solid var(--gold);padding:1rem;font-size:.85rem;'
                            f'line-height:1.7;border-radius:0 2px 2px 0">{analysis}</div>',
                            unsafe_allow_html=True,
                        )
                        st.caption(
                            "⚠️ El análisis de imágenes es orientativo. "
                            "El valor estimado proviene del modelo estadístico, no de las fotos."
                        )
                    except Exception as e:
                        st.warning(f"Análisis de imagen no disponible: {e}")

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