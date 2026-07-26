# -*- coding: utf-8 -*-
"""app.py — Real Estate Analyst Colombia

Estructura:
  · Un solo estado de filtros en la barra lateral, compartido por todas las
    pestanas (src/utils/filters.py).
  · Los datos se cargan una vez y se filtran por vista, en lugar de que cada
    pestana arme su propio subconjunto.
  · Por defecto solo se muestran avisos vigentes: los que ya no aparecen en
    su portal se conservan en el lakehouse para el modelo, pero no se
    publican como si fueran ofertas activas.
"""

import os
import warnings

import dotenv
import streamlit as st

dotenv.load_dotenv()
warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="Real Estate Analyst — Colombia",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as handle:
        st.markdown(f"<style>{handle.read()}</style>", unsafe_allow_html=True)

from src.utils.data_loader import load_demo_data, load_inmuebles_data  # noqa: E402
from src.utils.filters import apply_filters, render_sidebar_filters  # noqa: E402
from src.components.tab_explorer import render_tab_explorer  # noqa: E402
from src.components.tab_finance import render_tab_finance  # noqa: E402
from src.components.tab_market import render_tab_market  # noqa: E402
from src.components.tab_val_teaser import render_tab_val_teaser  # noqa: E402


# ══════════════════════════════════════════════════════════════════
# CARGA DE DATOS
# ══════════════════════════════════════════════════════════════════
# Si S3 falla se dice explicitamente. La version anterior capturaba el error
# y servia el dataset sintetico sin avisar, asi que la app podia estar
# mostrando inmuebles inventados con enlaces que nunca resolvieron.

df_master, source_name = None, None
try:
    df_master, source_name = load_inmuebles_data()
except Exception as exc:
    st.error(
        "**No se pudo cargar el catálogo desde S3.**\n\n"
        f"`{type(exc).__name__}: {exc}`\n\n"
        "Revisa las credenciales en `.streamlit/secrets.toml` (sección `[aws]`) "
        "y que la tabla `gold/app_inmuebles_scored/` exista."
    )
    if st.button("Continuar con datos de demostración"):
        df_master, source_name = load_demo_data()
        st.warning(
            "Modo demostración: los inmuebles son sintéticos y sus enlaces no existen."
        )
    else:
        st.stop()

if df_master is None or df_master.empty:
    st.warning(
        "El catálogo quedó vacío. Si acabas de activar el filtro de vigencia, "
        "puede que el ETL no haya corrido aún con las columnas de ciclo de vida."
    )
    st.stop()


# ══════════════════════════════════════════════════════════════════
# SIDEBAR — identidad, estado y filtros compartidos
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(
        '<div style="font-family:Playfair Display,serif;font-size:1.3rem;'
        'font-weight:900;color:white;margin-bottom:.2rem">◈ REA</div>'
        '<div style="font-size:.62rem;letter-spacing:.15em;color:#b8935a;'
        'text-transform:uppercase;margin-bottom:1rem">Real Estate Analyst · Colombia</div>',
        unsafe_allow_html=True,
    )

    is_live = str(source_name).startswith("S3")
    status_color = "#16a34a" if is_live else "#d97706"
    st.markdown(
        f'<div style="display:inline-block;background:{status_color}22;'
        f'border:1px solid {status_color};padding:.3rem .8rem;border-radius:2px;'
        f'font-size:.68rem;color:{status_color};font-weight:bold;letter-spacing:.5px;'
        f'margin-bottom:1rem">{source_name}</div>',
        unsafe_allow_html=True,
    )

    filter_state = render_sidebar_filters(df_master)

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Limpiar caché de datos", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        '<div style="font-size:.62rem;color:rgba(255,255,255,.22);margin-top:1.5rem;'
        'line-height:1.6">No constituye asesoría financiera certificada.<br>'
        'Propiedades extraídas de portales inmobiliarios de acceso público.</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════
# CABECERA
# ══════════════════════════════════════════════════════════════════
df_view = apply_filters(df_master, filter_state)

precio_mediano = df_view["precio_num"].median() if len(df_view) else 0
n_municipios = df_view["municipio"].nunique() if "municipio" in df_view.columns else 0

ticker = "  ·  ".join([
    f"◈ {len(df_view):,} INMUEBLES EN LA SELECCIÓN",
    f"PRECIO MEDIANO ${precio_mediano / 1e6:,.0f}M COP",
    f"{n_municipios} MUNICIPIOS",
    "GEOGRAFÍA OFICIAL DIVIPOLA · DANE",
])
st.markdown(f'<div class="ticker">{ticker}</div>', unsafe_allow_html=True)

st.markdown('<div class="section-label">Plataforma Inmobiliaria</div>', unsafe_allow_html=True)
st.title("Real Estate Analyst Colombia")
st.markdown(
    '<p style="color:var(--muted);font-size:.88rem;margin-top:-.5rem">'
    'Explorador de mercado, simulador financiero y valoración asistida por modelo</p>',
    unsafe_allow_html=True,
)
st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# PESTANAS
# ══════════════════════════════════════════════════════════════════
tab_explorer, tab_finance, tab_market, tab_val = st.tabs([
    "Explorador Interactivo",
    "Simulador Financiero",
    "Análisis de Mercado",
    "Valoración IA",
])

with tab_explorer:
    render_tab_explorer(df_master)

with tab_finance:
    render_tab_finance(df_view)

with tab_market:
    render_tab_market(df_view)

with tab_val:
    # La valoración recibe el catálogo completo, no la vista filtrada: es una
    # herramienta de tasación puntual y sus selectores de departamento,
    # municipio y barrio deben ofrecer todo el país aunque el explorador esté
    # filtrado a una sola zona.
    render_tab_val_teaser(df_master)
