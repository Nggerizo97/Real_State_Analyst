"""
src/components/tab_val_teaser.py
================================
Tab de Valoración IA con predicción real usando XGBoost.
Carga el modelo de forma lazy (solo al primer click) con @st.cache_resource.
"""

import streamlit as st
import pandas as pd
import gc


@st.cache_data(ttl=300, show_spinner=False)
def _champion_key() -> str:
    """Clave del modelo campeón según el manifest. Se consulta cada 5 min.

    Es una lectura de pocos bytes y sirve de llave de caché para el bundle:
    cuando el pipeline entrena un modelo nuevo, esta clave cambia y el bundle
    se recarga solo.
    """
    try:
        from src.utils.model_loader import ModelLoader
        return str(ModelLoader().get_manifest().get("champion_model_key", ""))
    except Exception:
        return ""


@st.cache_resource(show_spinner="Cargando modelo de valoración...")
def _load_bundle_for(champion_key: str):
    """Carga el bundle. La clave del campeón entra como argumento a propósito.

    Antes esto era @st.cache_resource sin argumento ni TTL: el bundle quedaba
    fijo para toda la vida del proceso de Streamlit. Tras reentrenar, la app
    seguía tasando con el modelo viejo — se veía la versión 033321 en pantalla
    cuando el campeón real ya era 044143, y por eso una casa en El Retiro
    salía en $322M en vez de ~$494M. Al pasar la clave como argumento,
    un campeón nuevo produce una entrada de caché nueva.
    """
    try:
        from src.utils.model_loader import ModelLoader
        bundle, manifest = ModelLoader().load_latest_model()
        gc.collect()
        return bundle, manifest
    except Exception as exc:
        print(f"[MODEL] Error cargando modelo: {exc}")
        return None, {}


def _get_model_bundle():
    return _load_bundle_for(_champion_key())


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


def _render_barrio_block(result, stats_barrio, comparables, barrio, municipio, area,
                         modelo_conoce_barrio):
    """Comparables del barrio y, si hace falta, la valoración ajustada.

    `comparables` sale de src.utils.comparables.buscar_comparables(): el grupo
    más parecido al inmueble (mismo tipo y área) que tenga datos suficientes,
    con el nivel usado dicho en pantalla. `stats_barrio` es todo el barrio y
    solo da el factor frente al municipio.

    Se distinguen tres situaciones y se dicen en voz alta, porque cambian el
    significado del número:

    * El modelo se entrenó con barrios y conoce este -> el valor de arriba ya
      tiene el barrio dentro y aquí solo se muestran los comparables.
    * El modelo se entrenó con barrios pero no conoce este -> el valor de
      arriba es el del municipio; se avisa y se muestra el ajuste.
    * El modelo solo ve la ciudad -> se muestra además una estimación
      ajustada por el factor del barrio, etiquetada como cálculo aparte y no
      como salida del modelo.

    `modelo_conoce_barrio` sale de bundle_conoce_barrio(), no de mirar si
    barrio_key aparece en feature_cols: el bundle v8 la listaba aunque se
    entrenó con barrio_key constante, y aquí se afirmaba que la valoración
    "ya tenía en cuenta" un barrio cuyo efecto era cero.
    """
    if "error" in result:
        return

    if not barrio:
        st.caption(
            "Sin barrio seleccionado: la tasación corresponde al promedio del municipio."
        )
        return

    barrio_en_modelo = bool(modelo_conoce_barrio and result.get("barrio_en_modelo"))
    if modelo_conoce_barrio and not barrio_en_modelo:
        st.warning(
            f"{barrio} no está entre los barrios del modelo (tuvo muy pocos avisos "
            f"al entrenar), así que la valoración usa el promedio de {municipio}."
        )

    if stats_barrio is None or comparables is None:
        st.caption(f"Sin comparables suficientes en {barrio}.")
        return

    n = int(comparables["n"])
    pm2_barrio = comparables["precio_m2_mediano"]
    p25 = comparables["precio_m2_p25"]
    p75 = comparables["precio_m2_p75"]
    factor = float(stats_barrio["factor"])
    n_barrio = int(stats_barrio["n"])

    st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)
    st.markdown(
        f'<div class="section-label">Comparables en {barrio}</div>',
        unsafe_allow_html=True,
    )

    cells = st.columns(3)
    cells[0].metric("Inmuebles comparables", f"{n:,}", help=comparables["descripcion"].capitalize())
    cells[1].metric("Precio/m² mediano", f"${pm2_barrio:,.0f}", help=comparables["descripcion"].capitalize())
    cells[2].metric(
        "Frente al municipio",
        f"{(factor - 1) * 100:+.0f}%",
        help=f"Mediana de todo {barrio} ({n_barrio:,} avisos, todos los tipos) contra la de {municipio}.",
    )

    # El simbolo $ se escapa: Streamlit interpreta $...$ como LaTeX y se comia
    # los signos de peso, dejando "6,569,145 — 10,179,398 por m2" sin moneda.
    st.caption(
        f"Comparables: {comparables['descripcion']}. "
        f"Rango intercuartil: \\${p25:,.0f} — \\${p75:,.0f} por m². "
        f"Para {area:,.0f} m² eso equivale a \\${p25 * area:,.0f} — \\${p75 * area:,.0f}."
    )
    if comparables["nota"]:
        st.caption(comparables["nota"])

    # Aviso cuando el modelo se sale del rango real del barrio. Es la senal de
    # que la valoracion no es fiable ahi. No se da cuando el rango mezcla tipos
    # (barrio_todos): una casa fuera del rango de los apartamentos no dice nada.
    pm2_modelo = result.get("precio_m2_pred", 0)
    if (comparables["nivel"] != "barrio_todos" and pm2_modelo
            and (pm2_modelo < p25 * 0.75 or pm2_modelo > p75 * 1.25)):
        direccion = "por debajo" if pm2_modelo < p25 else "por encima"
        motivo = (
            "el inmueble se aleja de sus comparables en el barrio."
            if barrio_en_modelo
            else f"el modelo no está usando el barrio dentro de {municipio}."
        )
        st.warning(
            f"El modelo estima \\${pm2_modelo:,.0f}/m², muy {direccion} del rango "
            f"de {comparables['descripcion']} (\\${p25:,.0f} — \\${p75:,.0f}/m²). "
            f"Toma la valoración con reserva: {motivo}"
        )

    if barrio_en_modelo:
        st.caption(
            "El modelo activo incluye el barrio entre sus variables, así que la "
            "valoración de arriba ya lo tiene en cuenta."
        )
        return

    valor_ajustado = result["valor_predicho"] * factor
    motivo_ajuste = (
        "El modelo activo no conoce este barrio y lo valoró con el promedio del municipio."
        if modelo_conoce_barrio
        else "El modelo activo <strong>no</strong> usa el barrio como variable: solo llega al "
             "nivel de municipio."
    )
    st.markdown(
        f'<div style="background:var(--surface2);border:1px solid var(--border);'
        f'border-left:4px solid #0891b2;padding:1rem 1.2rem;border-radius:4px;margin-top:.6rem">'
        f'<div style="font-size:.65rem;color:rgba(255,255,255,.4);text-transform:uppercase;'
        f'letter-spacing:.15em;margin-bottom:.3rem">Estimación ajustada por barrio</div>'
        f'<div style="font-size:1.5rem;font-weight:900;color:white;font-family:DM Mono,monospace">'
        f'${valor_ajustado:,.0f} <span style="font-size:.65rem;color:rgba(255,255,255,.4)">COP</span></div>'
        f'<p style="font-size:.72rem;color:var(--muted);line-height:1.5;margin:.5rem 0 0">'
        f'{motivo_ajuste} Esta cifra aplica sobre su resultado el factor {factor:.2f} observado en '
        f'{barrio} ({n_barrio:,} avisos). No es una salida del modelo, es un ajuste con datos del '
        f'catálogo.'
        f'</p></div>',
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
        # DIVIPOLA escribe "Bogotá, D.c.", no "Bogotá D.C.". Con el nombre
        # antiguo la busqueda fallaba y el selector abria en Amazonas, que es
        # el primero alfabéticamente. Se busca por prefijo y se cae a la
        # jurisdicción con más inmuebles, no al índice 0.
        _preferidos = [d for d in deps_disp if str(d).lower().startswith("bogot")]
        if _preferidos:
            default_idx = deps_disp.index(_preferidos[0])
        else:
            _top = df_master["departamento"].value_counts()
            default_idx = deps_disp.index(_top.index[0]) if len(_top) else 0
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

        # ── Barrio / sector ──────────────────────────────────────────
        # El barrio es el factor que más mueve el precio dentro de una misma
        # ciudad. Las opciones se limitan a barrios con comparables
        # suficientes; con menos de 8 avisos la mediana es ruido.
        from src.utils.barrio_stats import barrios_de, build_barrio_stats, lookup

        barrio_stats = build_barrio_stats(df_master)
        barrio_opciones = barrios_de(barrio_stats, t_municipio)

        if barrio_opciones:
            t_barrio = st.selectbox(
                f"Barrio / sector ({len(barrio_opciones)} con comparables)",
                options=["— Sin especificar —"] + barrio_opciones,
                key="teaser_barrio",
                help=(
                    "Solo aparecen barrios con al menos 8 inmuebles comparables "
                    "en el catálogo."
                ),
            )
            if t_barrio == "— Sin especificar —":
                t_barrio = ""
        else:
            t_barrio = ""
            st.caption(
                f"Sin barrios con comparables suficientes en {t_municipio}. "
                "La tasación usará el promedio del municipio."
            )

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
                    # El scorer arma barrio_key con src.geo.normalize.barrio_key
                    # y toma las medianas de barrio del bundle, igual que el
                    # entrenamiento. Antes se armaba aquí a mano y con medianas
                    # propias del catálogo, y la llave no coincidía con la del
                    # modelo.
                    "barrio": t_barrio,
                    "municipio": t_municipio,
                    # Mismo formato que gold/app_inmuebles: el texto del aviso
                    # también entra al modelo (TF-IDF de texto_completo).
                    "zona_mercado": f"{t_municipio} | {t_barrio or 'Sin barrio'}",
                }

                stats_barrio = lookup(barrio_stats, t_municipio, t_barrio) if t_barrio else None
                from src.utils.comparables import buscar_comparables
                comparables = buscar_comparables(df_master, t_municipio, t_barrio, t_tipo, float(t_area))

                # Ejecutar predicción
                from src.utils.scorer import bundle_conoce_barrio, score_single
                result = score_single(row, bundle)

                modelo_conoce_barrio = bundle_conoce_barrio(bundle)
                _render_result_card(result)
                _render_barrio_block(result, stats_barrio, comparables, t_barrio, t_municipio,
                                     t_area, modelo_conoce_barrio)
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
