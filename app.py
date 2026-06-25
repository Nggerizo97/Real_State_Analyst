"""
app.py — Real Estate Analyst Colombia (Versión Light)
======================================================
Esta es una versión ligera diseñada para correr gratuitamente en Streamlit Cloud.
Incluye modularización y componentes separados para un mejor mantenimiento y escalabilidad.
"""

import os
import warnings
import dotenv
dotenv.load_dotenv()
import streamlit as st

# Evitar advertencias molestas
warnings.filterwarnings("ignore")

# Configuración de página con look premium
st.set_page_config(
    page_title="Real Estate Analyst — Colombia",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Cargar CSS personalizado
if os.path.exists("style.css"):
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Importaciones de utilidades y componentes del proyecto
from src.utils.data_loader import load_inmuebles_data
from src.utils.formatters import fmt_cop
from src.components.tab_finance import render_tab_finance
from src.components.tab_market import render_tab_market
from src.components.tab_val_teaser import render_tab_val_teaser

# Carga de datos unificada
df_master, source_name = load_inmuebles_data()

# ══════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(
        '<div style="font-family:Playfair Display,serif;font-size:1.3rem;'
        'font-weight:900;color:white;margin-bottom:.2rem">◈ REA Light</div>'
        '<div style="font-size:.62rem;letter-spacing:.15em;color:#b8935a;'
        'text-transform:uppercase;margin-bottom:1.2rem">Real Estate Analyst · Colombia</div>',
        unsafe_allow_html=True,
    )
    
    # Status de conexión
    status_color = "#2a9b6a" if source_name == "S3 Live" else "#b8935a"
    st.markdown(
        f'<div style="display:inline-block;background:{status_color}22;border:1px solid {status_color};'
        f'padding:.3rem .8rem;border-radius:2px;font-size:.7rem;color:{status_color};'
        f'font-weight:bold;letter-spacing:1px;text-transform:uppercase;margin-bottom:1.5rem">'
        f'🔌 Origen: {source_name}'
        f'</div>',
        unsafe_allow_html=True
    )
    
    # KPIs rápidos en el sidebar
    st.markdown('<div class="section-label">Estadísticas del Catálogo</div>', unsafe_allow_html=True)
    N_total = len(df_master)
    N_ciudades = df_master["city_token"].nunique()
    med_precio = df_master["precio_num"].median()
    
    # Indicador de modelo — solo verifica el manifest (no carga el modelo)
    model_status = "Disponible ◈" if source_name == "S3 Live" else "Demo"
    
    for label, val in [
        ("Inmuebles Activos", f"{N_total:,}"),
        ("Ciudades Cubiertas", f"{N_ciudades}"),
        ("Precio Mediano", f"${med_precio/1e6:.0f}M COP"),
        ("Modelo IA", model_status),
    ]:
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;font-size:.78rem;'
            f'padding:.22rem 0;border-bottom:1px solid rgba(255,255,255,.07)">'
            f'<span style="color:rgba(255,255,255,.45)">{label}</span>'
            f'<span style="color:white;font-family:DM Mono,monospace">{val}</span></div>',
            unsafe_allow_html=True,
        )
        
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Limpiar Caché local", key="btn_clear_cache"):
        st.cache_data.clear()
        st.rerun()
        
    st.markdown(
        '<div style="font-size:.62rem;color:rgba(255,255,255,.22);margin-top:2.5rem;line-height:1.6">'
        'No constituye asesoría financiera certificada.<br>'
        'Propiedades extraídas de portales inmobiliarios de acceso público.</div>',
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════════
# HEADER & TICKER
# ══════════════════════════════════════════════════════════════════
modelo_ticker = "MODELO IA: ACTIVO" if source_name == "S3 Live" else "MODELO IA: MODO DEMO"
ticker_txt = "  ·  ".join([
    f"◈ {N_total:,} INMUEBLES DISPONIBLES",
    f"PRECIO MEDIANO ${med_precio/1e6:.0f}M COP",
    f"{N_ciudades} CIUDADES COLOMBIANAS",
    modelo_ticker,
])
st.markdown(f'<div class="ticker">{ticker_txt}</div>', unsafe_allow_html=True)

col_h, col_s = st.columns([3, 1])
with col_h:
    st.markdown('<div class="section-label">Plataforma Inmobiliaria — Versión Ultra-Light</div>', unsafe_allow_html=True)
    st.title("Real Estate Analyst Colombia")
    st.markdown(
        f'<p style="color:var(--muted);font-size:.88rem;margin-top:-.5rem">'
        f'Buscador optimizado y simulador financiero para corredores e inversionistas</p>',
        unsafe_allow_html=True,
    )

st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# TABS DEFINITION
# ══════════════════════════════════════════════════════════════════
tab_finance, tab_market, tab_val_teaser = st.tabs([
    "Simulador Financiero y Propiedades",
    "Análisis de Mercados",
    "Valoración IA (Premium Teaser)"
])

# Renders de cada tab usando componentes separados
with tab_finance:
    render_tab_finance(df_master)

with tab_market:
    render_tab_market(df_master)

with tab_val_teaser:
    render_tab_val_teaser(df_master)