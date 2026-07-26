# -*- coding: utf-8 -*-
"""Análisis de mercado.

Recibe el DataFrame YA filtrado por la barra lateral, así que todo lo que se
ve aquí corresponde a la misma selección que el resto de la aplicación.

Se redujo de siete gráficas fijas a cuatro que responden preguntas
concretas, más una comparación de municipios bajo demanda. La versión
anterior mostraba todo a la vez sobre el catálogo completo, sin importar lo
que el usuario estuviera mirando.

Todas las series usan `city`/`municipio` de DIVIPOLA. Antes agrupaban por
`city_token`, que mezclaba 52 etiquetas escritas a mano y dejaba fuera los
municipios que no estaban en el diccionario.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.utils.plotly_theme import (
    CATEGORICAL, GOLD, MUTED_TEXT, SEQUENTIAL, STATUS, chart_title,
    color_map, dark_layout,
)

GEO_COLUMN_PREFERENCE = ["municipio", "city_token"]
MIN_OBSERVATIONS = 20


def _geo_column(df):
    for column in GEO_COLUMN_PREFERENCE:
        if column in df.columns:
            return column
    return None


def _price_per_m2_by_city(df, geo_column, container):
    grouped = (
        df.groupby(df[geo_column].astype(str))
        .agg(ofertas=("precio_num", "size"), precio_m2=("precio_m2", "median"))
        .reset_index().rename(columns={geo_column: "lugar"})
    )
    grouped = grouped[grouped["ofertas"] >= MIN_OBSERVATIONS].dropna(subset=["precio_m2"])
    if grouped.empty:
        container.info(
            f"Ningún municipio alcanza {MIN_OBSERVATIONS} avisos en esta selección. "
            "Amplía los filtros para comparar mercados."
        )
        return

    grouped = grouped.nlargest(14, "ofertas").sort_values("precio_m2")

    figure = go.Figure(
        go.Bar(
            x=grouped["precio_m2"] / 1e6,
            y=grouped["lugar"],
            orientation="h",
            marker=dict(
                color=grouped["precio_m2"], colorscale=SEQUENTIAL,
                showscale=False, line=dict(color="#16161f", width=2),
            ),
            text=[f"${v / 1e6:.2f}M" for v in grouped["precio_m2"]],
            textposition="outside",
            textfont=dict(color=MUTED_TEXT, size=10),
            customdata=grouped[["ofertas"]],
            hovertemplate="<b>%{y}</b><br>Precio/m²: $%{x:.2f}M<br>Ofertas: %{customdata[0]:,}<extra></extra>",
        )
    )
    median_market = df["precio_m2"].median()
    if pd.notna(median_market):
        figure.add_vline(
            x=median_market / 1e6,
            line=dict(color=GOLD, width=2, dash="dash"),
            annotation_text=f"Mediana selección ${median_market / 1e6:.2f}M",
            annotation_font=dict(size=10, color=GOLD),
        )
    dark_layout(
        figure, height=440,
        xaxis=dict(title="Millones COP / m²"), yaxis=dict(showgrid=False),
        showlegend=False,
    )
    chart_title(figure, f"Precio por m² — municipios con ≥{MIN_OBSERVATIONS} avisos")
    container.plotly_chart(figure, width="stretch")


def _price_spread(df, geo_column, container):
    """Dispersión de precios en los municipios de mayor oferta."""
    counts = df[geo_column].astype(str).value_counts()
    top = counts[counts >= MIN_OBSERVATIONS].head(6).index.tolist()
    if not top:
        container.info("Sin municipios con oferta suficiente para comparar dispersión.")
        return

    frame = df[df[geo_column].astype(str).isin(top)].copy()
    cap = frame["precio_num"].quantile(0.98)
    frame = frame[frame["precio_num"] <= cap]

    palette = color_map(top)
    figure = go.Figure()
    for lugar in sorted(top):
        values = frame.loc[frame[geo_column].astype(str) == lugar, "precio_num"] / 1e6
        figure.add_trace(
            go.Box(
                y=values, name=lugar,
                marker_color=palette[lugar],
                line=dict(width=2),
                boxmean=True,
                hovertemplate=f"<b>{lugar}</b><br>$%{{y:.0f}}M<extra></extra>",
            )
        )
    dark_layout(
        figure, height=400, yaxis=dict(title="Precio (millones COP)"),
        showlegend=False, xaxis=dict(tickangle=-20),
    )
    chart_title(figure, "Dispersión de precios por municipio")
    container.plotly_chart(figure, width="stretch")


def _portal_comparison(df, container):
    if "fuente" not in df.columns:
        return
    grouped = (
        df.dropna(subset=["precio_m2"])
        .groupby(df["fuente"].astype(str))
        .agg(precio_m2=("precio_m2", "median"), ofertas=("precio_num", "size"))
        .reset_index().rename(columns={"fuente": "portal"})
    )
    grouped = grouped[grouped["ofertas"] >= MIN_OBSERVATIONS].sort_values("precio_m2")
    if grouped.empty:
        container.info("Sin portales con oferta suficiente en esta selección.")
        return

    market_median = df["precio_m2"].median()
    # Polaridad frente a la mediana del mercado: divergente, no categórico.
    colors = [
        STATUS["good"] if value < market_median else STATUS["serious"]
        for value in grouped["precio_m2"]
    ]
    figure = go.Figure(
        go.Bar(
            x=grouped["precio_m2"] / 1e6,
            y=grouped["portal"].str.replace("_", " ").str.title(),
            orientation="h",
            marker=dict(color=colors, line=dict(color="#16161f", width=2)),
            text=[
                f"${v / 1e6:.2f}M · {n:,} avisos"
                for v, n in zip(grouped["precio_m2"], grouped["ofertas"])
            ],
            textposition="outside",
            textfont=dict(color=MUTED_TEXT, size=10),
            hovertemplate="<b>%{y}</b><br>Precio/m² mediano: $%{x:.2f}M<extra></extra>",
        )
    )
    figure.add_vline(
        x=market_median / 1e6,
        line=dict(color=GOLD, width=2, dash="dash"),
        annotation_text="Mediana del mercado",
        annotation_font=dict(size=10, color=GOLD),
    )
    dark_layout(
        figure, height=340, xaxis=dict(title="Precio mediano / m² (millones COP)"),
        yaxis=dict(showgrid=False), showlegend=False,
    )
    chart_title(figure, "Precio por m² según portal (verde = bajo la mediana)")
    container.plotly_chart(figure, width="stretch")
    container.caption(
        "Verde: por debajo de la mediana del mercado. Naranja: por encima. "
        "El color acompaña al valor numérico impreso en cada barra."
    )


def _freshness(df, container):
    """Vigencia del catálogo: cuánto de lo que se ve sigue publicado."""
    if "listing_status" not in df.columns:
        return
    counts = df["listing_status"].value_counts()
    labels = {
        "activa": "Vigente",
        "en_revision": "En revisión",
        "probable_retirada": "Probablemente retirada",
        "retirada": "Retirada",
    }
    order = ["activa", "en_revision", "probable_retirada", "retirada"]
    colors = {
        "activa": STATUS["good"],
        "en_revision": STATUS["warning"],
        "probable_retirada": STATUS["serious"],
        "retirada": STATUS["critical"],
    }
    present = [status for status in order if status in counts.index]
    if not present:
        return

    figure = go.Figure(
        go.Bar(
            x=[counts[status] for status in present],
            y=[labels[status] for status in present],
            orientation="h",
            marker=dict(
                color=[colors[status] for status in present],
                line=dict(color="#16161f", width=2),
            ),
            text=[f"{counts[status]:,}" for status in present],
            textposition="outside",
            textfont=dict(color=MUTED_TEXT, size=10),
            hovertemplate="<b>%{y}</b>: %{x:,} avisos<extra></extra>",
        )
    )
    dark_layout(
        figure, height=250, xaxis=dict(title="Avisos"),
        yaxis=dict(showgrid=False), showlegend=False,
    )
    chart_title(figure, "Vigencia de los avisos en la selección")
    container.plotly_chart(figure, width="stretch")


def render_tab_market(df_master):
    st.markdown("## Inteligencia de mercado")
    st.caption(
        "Todo el panel responde a los filtros de la barra lateral. "
        "Geografía oficial DIVIPOLA (DANE)."
    )

    df = df_master.copy()
    if df.empty:
        st.warning("Ningún inmueble cumple los filtros actuales.")
        return

    df["precio_m2"] = pd.to_numeric(df.get("precio_m2"), errors="coerce")
    if df["precio_m2"].isna().all():
        df["precio_m2"] = pd.to_numeric(df["precio_num"], errors="coerce") / pd.to_numeric(
            df["area_m2"], errors="coerce"
        )
    df["precio_m2"] = df["precio_m2"].replace([np.inf, -np.inf], np.nan)

    geo_column = _geo_column(df)
    if geo_column is None:
        st.error("El catálogo no trae columna de municipio.")
        return

    left, right = st.columns(2)
    _price_per_m2_by_city(df, geo_column, left)
    with right:
        _price_spread(df, geo_column, right)

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

    left2, right2 = st.columns(2)
    _portal_comparison(df, left2)
    _freshness(df, right2)

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

    with st.expander("Ver tabla comparativa de municipios"):
        table = (
            df.groupby(df[geo_column].astype(str))
            .agg(
                Avisos=("precio_num", "size"),
                Precio_mediano=("precio_num", "median"),
                Precio_m2_mediano=("precio_m2", "median"),
                Area_mediana=("area_m2", "median"),
            )
            .reset_index().rename(columns={geo_column: "Municipio"})
            .sort_values("Avisos", ascending=False)
        )
        st.dataframe(
            table, width="stretch", hide_index=True, height=360,
            column_config={
                "Precio_mediano": st.column_config.NumberColumn("Precio mediano", format="$ %d"),
                "Precio_m2_mediano": st.column_config.NumberColumn("COP/m²", format="$ %d"),
                "Area_mediana": st.column_config.NumberColumn("Área m²", format="%.0f"),
            },
        )
