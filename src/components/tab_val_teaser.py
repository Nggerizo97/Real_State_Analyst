"""
src/components/tab_val_teaser.py
================================
Tab de Valoración IA con predicción real usando XGBoost.
Carga el modelo de forma lazy (solo al primer click) con @st.cache_resource.
"""

import streamlit as st
import pandas as pd
import gc


@st.cache_resource(show_spinner="Cargando modelo de valoración...")
def _get_model_bundle():
    """
    Carga el bundle del modelo campeón desde S3.
    Se ejecuta UNA sola vez por sesión de la app gracias a @st.cache_resource.
    Retorna (bundle, manifest) o (None, {}) si falla.
    """
    try:
        from src.utils.model_loader import ModelLoader
        loader = ModelLoader()
        bundle, manifest = loader.load_latest_model()
        gc.collect()
        return bundle, manifest
    except Exception as e:
        print(f"[MODEL] Error cargando modelo: {e}")
        return None, {}


def _render_model_badge(manifest: dict):
    """Renderiza un mini-badge con info del modelo cargado."""
    from src.utils.model_loader import ModelLoader
    badge = ModelLoader.get_badge_data(manifest)

    fallback_tag = ""
    if badge["is_fallback"]:
        fallback_tag = ' <span style="color:#e8a838">⚠ FALLBACK</span>'
    elif badge["is_legacy"]:
        fallback_tag = ' <span style="color:#e8a838">⚠ LEGACY</span>'

    st.markdown(
        f'<div style="background:rgba(42,155,106,.08);border:1px solid rgba(42,155,106,.25);'
        f'padding:.6rem 1rem;border-radius:4px;margin-bottom:1rem;font-size:.72rem">'
        f'<span style="color:#2a9b6a;font-weight:bold">✓ MODELO ACTIVO</span>{fallback_tag}<br>'
        f'<span style="color:rgba(255,255,255,.5)">Versión:</span> '
        f'<span style="color:white;font-family:DM Mono,monospace">{badge["model_name"]}</span><br>'
        f'<span style="color:rgba(255,255,255,.5)">MAPE:</span> '
        f'<span style="color:white;font-family:DM Mono,monospace">{badge["mape"]}</span> · '
        f'<span style="color:rgba(255,255,255,.5)">Entrenado:</span> '
        f'<span style="color:white;font-family:DM Mono,monospace">{badge["freshness"]}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_result_card(result: dict):
    """Renderiza la tarjeta de resultado de la valoración."""
    if "error" in result:
        st.error(f"❌ Error en la predicción: {result['error']}")
        return

    valor = result["valor_predicho"]
    rango_low = result["rango_low"]
    rango_high = result["rango_high"]
    pm2 = result["precio_m2_pred"]
    mape = result["mape_pct"]
    estado = result["estado"]

    # Color según estado
    estado_colors = {
        "Oportunidad": ("#2a9b6a", "▲"),
        "Sobrevalorado": ("#e05252", "▼"),
        "En mercado": ("#b8935a", "●"),
    }
    color, icon = estado_colors.get(estado, ("#b8935a", "●"))

    st.markdown(
        f'<div style="background:var(--surface2);border:1px solid var(--border);'
        f'border-left:4px solid {color};padding:1.5rem;border-radius:4px">'

        # Precio predicho principal
        f'<div style="font-size:.65rem;color:rgba(255,255,255,.4);text-transform:uppercase;'
        f'letter-spacing:.15em;margin-bottom:.3rem">Valor Estimado por IA</div>'
        f'<div style="font-size:2rem;font-weight:900;color:white;font-family:DM Mono,monospace;'
        f'margin-bottom:.5rem">${valor:,.0f} <span style="font-size:.7rem;color:rgba(255,255,255,.4)">'
        f'COP</span></div>'

        # Rango de confianza
        f'<div style="font-size:.72rem;color:rgba(255,255,255,.5);margin-bottom:.8rem">'
        f'Rango de confianza (±{mape:.0f}%): '
        f'<span style="color:white;font-family:DM Mono,monospace">'
        f'${rango_low:,.0f} — ${rango_high:,.0f}</span></div>'

        # Precio por m²
        f'<div style="display:flex;gap:1.5rem;margin-bottom:.8rem">'
        f'<div><div style="font-size:.6rem;color:rgba(255,255,255,.35);text-transform:uppercase;'
        f'letter-spacing:.1em">Precio/m²</div>'
        f'<div style="font-size:1rem;color:white;font-family:DM Mono,monospace">'
        f'${pm2:,.0f}</div></div>'
        f'<div><div style="font-size:.6rem;color:rgba(255,255,255,.35);text-transform:uppercase;'
        f'letter-spacing:.1em">Precisión Modelo</div>'
        f'<div style="font-size:1rem;color:white;font-family:DM Mono,monospace">'
        f'MAPE {mape:.1f}%</div></div></div>'

        # Estado de inversión
        f'<div style="background:{color}15;border:1px solid {color}40;padding:.5rem .8rem;'
        f'border-radius:2px;display:inline-block;font-size:.78rem;color:{color};font-weight:bold">'
        f'{icon} {estado}</div>'

        f'</div>',
        unsafe_allow_html=True,
    )


def _render_no_model_card():
    """Muestra un mensaje informativo cuando no hay modelo disponible."""
    st.markdown(
        '<div style="background:var(--surface2);border:1px solid var(--border);'
        'border-left:4px solid #e8a838;padding:1.5rem;border-radius:4px;text-align:center">'
        '<div style="font-size:2rem;margin-bottom:.5rem">⚡</div>'
        '<div style="font-weight:bold;color:white;font-size:1rem;margin-bottom:.5rem">'
        'Modelo No Disponible</div>'
        '<p style="font-size:.78rem;color:var(--muted);line-height:1.6;max-width:380px;margin:0 auto">'
        'No se encontró un modelo XGBoost entrenado en S3. '
        'Ejecuta el pipeline de entrenamiento en el Lakehouse para generar el bundle del modelo, '
        'o verifica la conexión a AWS.</p></div>',
        unsafe_allow_html=True,
    )


def render_tab_val_teaser(df_master):
    """Renderiza el tab de Valoración IA con predicción real."""
    st.markdown("## Valoración Estimada por Inteligencia Artificial")
    st.markdown(
        '<div class="disclaimer">'
        'Nuestro modelo XGBoost cruza información geográfica, '
        'características del inmueble y estadísticas de mercado para calcular '
        'el precio justo estimado de un inmueble.'
        '</div>',
        unsafe_allow_html=True,
    )

    t_col1, t_col2 = st.columns([1, 1])

    with t_col1:
        st.markdown('<div class="section-label">Ficha del Inmueble a tasar</div>', unsafe_allow_html=True)
        t_area = st.number_input("Área en m²", min_value=20, max_value=800, value=85, key="teaser_area")
        t_habs = st.number_input("Habitaciones", min_value=1, max_value=8, value=3, key="teaser_habs")
        t_banos = st.number_input("Baños", min_value=1, max_value=6, value=2, key="teaser_banos")
        t_garajes = st.number_input("Garajes", min_value=0, max_value=4, value=1, key="teaser_garajes")

        # Departamento
        deps_disp = sorted(list(df_master["departamento"].dropna().unique()))
        default_idx = deps_disp.index("Bogotá D.C.") if "Bogotá D.C." in deps_disp else 0
        t_depto = st.selectbox(
            "Departamento",
            options=deps_disp,
            index=default_idx,
            key="teaser_depto",
        )

        # Filtrar municipios por departamento
        df_temp_mun = df_master[df_master["departamento"] == t_depto]
        mun_mapping = df_temp_mun.groupby("municipio")["city_token"].first().to_dict()
        mun_options = sorted(list(mun_mapping.keys()))

        t_municipio = st.selectbox(
            "Ciudad / Municipio",
            options=mun_options,
            key="teaser_municipio",
        )

        t_ciudad = mun_mapping.get(t_municipio, "bogota")

        t_tipo = st.selectbox(
            "Tipo de Propiedad",
            options=["apartamento", "casa"],
            key="teaser_type",
        )

        btn_calcular = st.button("Calcular Tasación Inteligente ◈", key="teaser_btn", type="primary")

    with t_col2:
        st.markdown('<div class="section-label">Resultado de la Tasación</div>', unsafe_allow_html=True)

        if btn_calcular:
            # ── Lazy load del modelo ──
            with st.spinner("Conectando con el modelo de IA..."):
                bundle, manifest = _get_model_bundle()

            if bundle is None:
                _render_no_model_card()
            else:
                # Mostrar badge del modelo
                _render_model_badge(manifest)

                # Construir la fila de input para scorer
                row = {
                    "area_m2": float(t_area),
                    "habitaciones": float(t_habs),
                    "banos": float(t_banos),
                    "garajes": float(t_garajes),
                    "city_token": t_ciudad,
                    "tipo_inmueble": t_tipo,
                    "estado_inmueble": "usado",
                    "fuente": "manual",
                    "precio_num": 0.0,
                    "num_portales": 1,
                    "dispersion_pct_grupo": 0.0,
                    "precio_desviacion_grupo_pct": 0.0,
                    "data_completeness": 0.8,
                    "comuna_mercado": "comuna_otra",
                    "sector_mercado": "sector_otra",
                }

                # Ejecutar predicción
                from src.utils.scorer import score_single
                result = score_single(row, bundle)
                _render_result_card(result)
        else:
            st.markdown(
                '<div style="background:var(--surface2);border:1px solid var(--border);'
                'padding:2.5rem;text-align:center;color:var(--muted);font-size:.82rem;'
                'border-radius:4px">'
                'Completa la ficha técnica a la izquierda y presiona<br>'
                '<strong style="color:white">Calcular Tasación Inteligente ◈</strong><br>'
                'para obtener la valoración del modelo XGBoost.'
                '</div>',
                unsafe_allow_html=True,
            )
