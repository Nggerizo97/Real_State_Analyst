# -*- coding: utf-8 -*-
"""Explorador interactivo tipo Power BI.

Lo que lo hace interactivo de verdad y no un tablero de imagenes:

* Un solo estado de filtros (src/utils/filters.py) que comparten todas las
  vistas, de modo que dos paneles nunca muestran poblaciones distintas.
* Drill-down Departamento -> Municipio -> Barrio: al hacer clic en una barra
  se baja de nivel, con migas de pan para volver.
* Filtro cruzado: seleccionar en cualquier grafica filtra las demas.
* Mapa con coordenadas reales de DIVIPOLA, que antes no existian.
* Vista de tabla siempre disponible, para que la lectura no dependa del
  color, y descarga a CSV.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.utils.filters import apply_filters, set_cross_filter
from src.utils.plotly_theme import (
    GOLD, MUTED_TEXT, SEQUENTIAL, chart_title, color_map, dark_layout,
)

DRILL_KEY = "rea_drill"
DRILL_LEVELS = ["departamento", "municipio", "barrio_display"]
DRILL_LABELS = {
    "departamento": "Departamento",
    "municipio": "Municipio",
    "barrio_display": "Barrio / sector",
}


# ══════════════════════════════════════════════════════════════════
# Estado del drill-down
# ══════════════════════════════════════════════════════════════════

def _drill_state() -> dict:
    if DRILL_KEY not in st.session_state:
        st.session_state[DRILL_KEY] = {"level": 0, "path": {}}
    return st.session_state[DRILL_KEY]


def _drill_down(column, value):
    state = _drill_state()
    state["path"][column] = value
    state["level"] = min(state["level"] + 1, len(DRILL_LEVELS) - 1)


def _drill_to(level):
    state = _drill_state()
    for column in DRILL_LEVELS[level:]:
        state["path"].pop(column, None)
    state["level"] = level


def _apply_drill(df):
    state = _drill_state()
    result = df
    for column, value in state["path"].items():
        if column in result.columns:
            result = result[result[column].astype(str) == str(value)]
    return result


def _render_breadcrumb():
    """Migas de pan del drill-down.

    Las columnas se dimensionan por numero de migas, no con un peso fijo de 1
    frente a un relleno de 6: con una sola miga esa proporcion dejaba el boton
    en ~40px y "Colombia" se partia en vertical ("Col om bia").
    """
    state = _drill_state()
    crumbs = ["Colombia"] + list(state["path"].values())

    if len(crumbs) == 1:
        st.caption(
            "Vista nacional · arrastra sobre una barra (o usa el selector) "
            "para bajar a municipio y barrio"
        )
        return

    widths = [3] * len(crumbs)
    filler = max(1, 12 - 3 * len(crumbs))
    columns = st.columns(widths + [filler])
    for index, crumb in enumerate(crumbs):
        with columns[index]:
            label = str(crumb)
            if len(label) > 18:
                label = label[:17] + "…"
            is_current = index == len(crumbs) - 1
            if st.button(
                label,
                key=f"crumb_{index}",
                width="stretch",
                type="primary" if is_current else "secondary",
                disabled=is_current,
                help=str(crumb),
            ):
                _drill_to(index)
                st.rerun()


# ══════════════════════════════════════════════════════════════════
# Piezas de la vista
# ══════════════════════════════════════════════════════════════════

def _kpi_row(df, total_catalog):
    precio = pd.to_numeric(df.get("precio_num"), errors="coerce")
    precio_m2 = pd.to_numeric(df.get("precio_m2"), errors="coerce")
    area = pd.to_numeric(df.get("area_m2"), errors="coerce")

    cells = st.columns(5)
    cells[0].metric("Inmuebles", f"{len(df):,}", f"{len(df) / max(total_catalog, 1) * 100:.0f}% del catálogo")
    cells[1].metric(
        "Precio mediano",
        f"${precio.median() / 1e6:,.0f}M" if precio.notna().any() else "—",
    )
    cells[2].metric(
        "Precio / m² mediano",
        f"${precio_m2.median() / 1e6:,.2f}M" if precio_m2.notna().any() else "—",
    )
    cells[3].metric("Área mediana", f"{area.median():,.0f} m²" if area.notna().any() else "—")
    cells[4].metric(
        "Municipios",
        f"{df['municipio'].nunique():,}" if "municipio" in df.columns else "—",
    )


def _geography_bar(df, container):
    """Barra del nivel geografico actual. Al hacer clic, baja un nivel."""
    state = _drill_state()
    column = DRILL_LEVELS[state["level"]]
    if column not in df.columns:
        container.info("Sin geografía disponible en este nivel.")
        return

    grouped = (
        df.groupby(df[column].astype(str))
        .agg(ofertas=("precio_num", "size"), precio_m2=("precio_m2", "median"))
        .reset_index()
        .rename(columns={column: "categoria"})
    )
    # astype(str) convierte los nulos en 'nan'/'None'; sin filtrarlos aparecia
    # una barra llamada "None" entre los departamentos.
    grouped = grouped[~grouped["categoria"].str.lower().isin(["nan", "none", "", "<na>"])]
    if grouped.empty:
        container.info("Sin datos para graficar.")
        return

    grouped = grouped.nlargest(15, "ofertas").sort_values("ofertas")

    figure = go.Figure(
        go.Bar(
            x=grouped["ofertas"],
            y=grouped["categoria"],
            orientation="h",
            marker=dict(
                color=grouped["precio_m2"],
                colorscale=SEQUENTIAL,
                colorbar=dict(
                    title=dict(text="COP/m²", font=dict(color=MUTED_TEXT, size=10)),
                    tickfont=dict(color=MUTED_TEXT, size=9),
                    thickness=10,
                ),
                line=dict(color="#16161f", width=2),   # separador de 2px
            ),
            customdata=grouped[["precio_m2"]],
            hovertemplate=(
                "<b>%{y}</b><br>Ofertas: %{x:,}"
                "<br>Precio/m² mediano: $%{customdata[0]:,.0f}<extra></extra>"
            ),
        )
    )
    dark_layout(
        figure,
        height=430,
        xaxis=dict(title="Ofertas"),
        yaxis=dict(showgrid=False),
        showlegend=False,
        # Sin clickmode="event+select" el on_select de Streamlit no se dispara
        # con un clic simple: el dragmode por defecto es "zoom" y solo un
        # arrastre de caja cuenta como seleccion. Con esto, un clic sobre la
        # barra ya emite el evento de seleccion de punto.
        clickmode="event+select",
        dragmode="select",
    )
    is_drillable = state["level"] < len(DRILL_LEVELS) - 1
    hint = "arrastra sobre una barra para bajar de nivel" if is_drillable else "último nivel"
    chart_title(figure, f"Oferta por {DRILL_LABELS[column].lower()} — {hint}")

    event = container.plotly_chart(
        figure, width="stretch", on_select="rerun", key=f"geo_bar_{state['level']}"
    )

    # Alternativa explicita al gesto sobre la grafica. Streamlit solo emite
    # on_select ante una seleccion (arrastre), no ante un clic simple, asi que
    # sin esto el drill-down dependeria de descubrir el gesto correcto. Ademas
    # es operable con teclado.
    if is_drillable:
        choice = container.selectbox(
            f"…o elige un {DRILL_LABELS[column].lower()} para bajar de nivel",
            ["—"] + grouped.sort_values("ofertas", ascending=False)["categoria"].tolist(),
            key=f"geo_pick_{state['level']}",
        )
        if choice and choice != "—":
            _drill_down(column, choice)
            st.rerun()

    points = (event.selection or {}).get("points") if event else None
    if points and is_drillable:
        _drill_down(column, points[0].get("y"))
        st.rerun()


def _price_distribution(df, container):
    precio = pd.to_numeric(df.get("precio_num"), errors="coerce").dropna()
    if precio.empty:
        container.info("Sin precios para graficar.")
        return
    cap = precio.quantile(0.98)
    values = precio[precio <= cap] / 1e6

    figure = go.Figure(
        go.Histogram(
            x=values,
            nbinsx=40,
            marker=dict(color="#0891b2", line=dict(color="#16161f", width=1)),
            hovertemplate="$%{x:.0f}M<br>%{y} inmuebles<extra></extra>",
        )
    )
    figure.add_vline(
        x=float(values.median()),
        line=dict(color=GOLD, width=2, dash="dash"),
        annotation_text=f"Mediana ${values.median():,.0f}M",
        annotation_font=dict(size=10, color=GOLD),
    )
    dark_layout(
        figure, height=300,
        xaxis=dict(title="Precio (millones COP)"),
        yaxis=dict(title="Inmuebles"),
        showlegend=False, bargap=0.04,
    )
    chart_title(figure, "Distribución de precios (recortada al p98)")
    container.plotly_chart(figure, width="stretch")


def _portal_bar(df, container):
    if "fuente" not in df.columns:
        return
    grouped = (
        df.groupby(df["fuente"].astype(str))
        .agg(ofertas=("precio_num", "size"), precio_m2=("precio_m2", "median"))
        .reset_index().rename(columns={"fuente": "portal"})
        .sort_values("ofertas", ascending=True)
    )
    if grouped.empty:
        return

    palette = color_map(grouped["portal"])
    figure = go.Figure(
        go.Bar(
            x=grouped["ofertas"],
            y=grouped["portal"].str.replace("_", " ").str.title(),
            orientation="h",
            marker=dict(
                color=[palette[p] for p in grouped["portal"]],
                line=dict(color="#16161f", width=2),
            ),
            text=[f"{v:,}" for v in grouped["ofertas"]],
            textposition="outside",
            textfont=dict(color=MUTED_TEXT, size=10),
            customdata=grouped[["precio_m2"]],
            hovertemplate=(
                "<b>%{y}</b><br>Ofertas: %{x:,}"
                "<br>Precio/m² mediano: $%{customdata[0]:,.0f}<extra></extra>"
            ),
        )
    )
    dark_layout(
        figure, height=300, xaxis=dict(title="Ofertas"),
        yaxis=dict(showgrid=False), showlegend=False,
        clickmode="event+select", dragmode="select",
    )
    chart_title(figure, "Oferta por portal — arrastra sobre una barra para filtrar")

    event = container.plotly_chart(
        figure, width="stretch", on_select="rerun", key="portal_bar"
    )
    points = (event.selection or {}).get("points") if event else None
    if points:
        chosen = [str(p.get("y", "")).lower().replace(" ", "_") for p in points]
        matches = [p for p in grouped["portal"] if p.lower() in chosen]
        if matches:
            set_cross_filter("fuente", matches)
            st.rerun()


def _map(df, container):
    """Mapa con coordenadas reales de DIVIPOLA."""
    if not {"latitud", "longitud"}.issubset(df.columns):
        container.info(
            "El mapa necesita las coordenadas de DIVIPOLA. "
            "Ejecuta tools/build_geo_dimension.py y vuelve a correr el ETL."
        )
        return

    frame = df.dropna(subset=["latitud", "longitud"]).copy()
    if frame.empty:
        container.info("Sin coordenadas en la selección actual.")
        return

    level = DRILL_LEVELS[min(_drill_state()["level"], len(DRILL_LEVELS) - 1)]
    group_col = level if level in frame.columns else "municipio"

    grouped = (
        frame.groupby(group_col)
        .agg(
            ofertas=("precio_num", "size"),
            precio_m2=("precio_m2", "median"),
            lat=("latitud", "median"),
            lon=("longitud", "median"),
        )
        .reset_index()
        .dropna(subset=["lat", "lon"])
    )
    if grouped.empty:
        container.info("Sin coordenadas agregables.")
        return

    sizes = np.sqrt(grouped["ofertas"].clip(lower=1))
    sizes = 8 + 34 * (sizes - sizes.min()) / max(sizes.max() - sizes.min(), 1e-9)

    figure = go.Figure(
        go.Scattermap(
            lat=grouped["lat"], lon=grouped["lon"],
            mode="markers",
            marker=dict(
                size=sizes, color=grouped["precio_m2"],
                colorscale=SEQUENTIAL, opacity=0.85,
                colorbar=dict(
                    title=dict(text="COP/m²", font=dict(color=MUTED_TEXT, size=10)),
                    tickfont=dict(color=MUTED_TEXT, size=9), thickness=10,
                ),
            ),
            text=grouped[group_col],
            customdata=grouped[["ofertas", "precio_m2"]],
            hovertemplate=(
                "<b>%{text}</b><br>Ofertas: %{customdata[0]:,}"
                "<br>Precio/m² mediano: $%{customdata[1]:,.0f}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        map=dict(
            style="carto-darkmatter",
            center=dict(lat=float(grouped["lat"].median()), lon=float(grouped["lon"].median())),
            zoom=4.2 if len(grouped) > 30 else 8,
        ),
        height=520, margin=dict(l=0, r=0, t=36, b=0),
        paper_bgcolor="#1e1e2a",
        font=dict(family="DM Sans", color="#c8c4bc"),
        hoverlabel=dict(bgcolor="#252535", bordercolor="#2a2a3a"),
    )
    chart_title(figure, "Mapa de mercado — tamaño = oferta, color = precio/m²")
    container.plotly_chart(figure, width="stretch")


def _price_area_scatter(df, container):
    frame = df.dropna(subset=["area_m2", "precio_num"]).copy()
    frame = frame[(frame["area_m2"] > 20) & (frame["area_m2"] < 600)]
    if frame.empty:
        return
    cap = frame["precio_num"].quantile(0.98)
    frame = frame[frame["precio_num"] <= cap]
    if len(frame) > 3000:
        frame = frame.sample(3000, random_state=42)

    tipos = sorted(frame["tipo_inmueble"].dropna().astype(str).unique())
    palette = color_map(tipos)

    figure = go.Figure()
    for tipo in tipos:
        mask = frame["tipo_inmueble"].astype(str) == tipo
        figure.add_trace(
            go.Scattergl(
                x=frame.loc[mask, "area_m2"],
                y=frame.loc[mask, "precio_num"] / 1e6,
                mode="markers",
                name=tipo.title(),
                marker=dict(
                    color=palette[tipo], size=6, opacity=0.55,
                    line=dict(color="#16161f", width=1),   # anillo de 2px
                ),
                hovertemplate=f"<b>{tipo.title()}</b><br>%{{x:.0f}} m²<br>$%{{y:.0f}}M<extra></extra>",
            )
        )
    dark_layout(
        figure, height=380,
        xaxis=dict(title="Área (m²)"), yaxis=dict(title="Precio (millones COP)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    chart_title(figure, "Precio frente a área por tipo")
    container.plotly_chart(figure, width="stretch")


def _table_view(df):
    """Vista de tabla: la lectura nunca debe depender solo del color."""
    columns = [
        c for c in [
            "titulo", "departamento", "municipio", "barrio_display", "tipo_inmueble",
            "precio_num", "area_m2", "precio_m2", "habitaciones", "banos",
            "fuente", "listing_status", "url",
        ] if c in df.columns
    ]
    table = df[columns].copy()
    renames = {
        "titulo": "Título", "departamento": "Departamento", "municipio": "Municipio",
        "barrio_display": "Barrio", "tipo_inmueble": "Tipo", "precio_num": "Precio",
        "area_m2": "Área m²", "precio_m2": "COP/m²", "habitaciones": "Hab.",
        "banos": "Baños", "fuente": "Portal", "listing_status": "Estado", "url": "Enlace",
    }
    table = table.rename(columns=renames)

    st.dataframe(
        table.head(500),
        width="stretch", height=420, hide_index=True,
        column_config={
            "Precio": st.column_config.NumberColumn(format="$ %d"),
            "COP/m²": st.column_config.NumberColumn(format="$ %d"),
            "Enlace": st.column_config.LinkColumn("Enlace", display_text="Ver aviso"),
        },
    )
    st.download_button(
        "Descargar selección (CSV)",
        table.to_csv(index=False).encode("utf-8"),
        file_name="inmuebles_seleccion.csv",
        mime="text/csv",
    )


# ══════════════════════════════════════════════════════════════════
# Entrada principal
# ══════════════════════════════════════════════════════════════════

def render_tab_explorer(df_master: pd.DataFrame):
    st.markdown("## Explorador de mercado")
    st.caption(
        "Los filtros de la barra lateral y los clics en las gráficas se aplican "
        "a todos los paneles a la vez."
    )

    df = apply_filters(df_master)
    df = _apply_drill(df)

    _render_breadcrumb()
    st.markdown("<div style='height:.6rem'></div>", unsafe_allow_html=True)

    if df.empty:
        st.warning("Ningún inmueble cumple los filtros actuales. Ajusta la selección.")
        return

    _kpi_row(df, len(df_master))
    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

    left, right = st.columns([3, 2])
    _geography_bar(df, left)
    with right:
        _price_distribution(df, right)
        _portal_bar(df, right)

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    _map(df, st.container())

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    _price_area_scatter(df, st.container())

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Detalle</div>', unsafe_allow_html=True)
    _table_view(df)
