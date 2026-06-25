"""
src/components/tab_market.py
============================
Inteligencia de Mercado Inmobiliario con 7 visualizaciones analíticas.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from src.utils.plotly_theme import dark_layout


def render_tab_market(df_master):
    st.markdown("## Inteligencia de Mercado Inmobiliario")
    st.markdown(
        '<div class="disclaimer">'
        'Panel analítico con métricas de mercado para corredores e inversionistas. '
        'Cruza oferta, precios, portales y tipologías en las principales ciudades de Colombia.'
        '</div>',
        unsafe_allow_html=True,
    )

    # ──────────────────────────────────────────────────────────────
    # Pre-cálculos comunes
    # ──────────────────────────────────────────────────────────────
    df = df_master.copy()
    df["precio_m2"] = pd.to_numeric(df["precio_num"], errors="coerce") / pd.to_numeric(df["area_m2"], errors="coerce")
    df["precio_m2"] = df["precio_m2"].replace([np.inf, -np.inf], np.nan)

    stats_ciudad = df.groupby("city_token").agg(
        ofertas=("precio_num", "count"),
        precio_mediano=("precio_num", "median"),
        precio_m2_mediano=("precio_m2", "median"),
    ).reset_index().sort_values("precio_m2_mediano", ascending=True)

    gold_scale = [[0, "#dfc69f"], [1, "#b8935a"]]

    # ──────────────────────────────────────────────────────────────
    # ROW 1: Precio/m² por Ciudad + Distribución de Ofertas
    # ──────────────────────────────────────────────────────────────
    m_col1, m_col2 = st.columns(2)

    with m_col1:
        st.markdown('<div class="section-label">Precio Mediano por m² por Ciudad</div>', unsafe_allow_html=True)
        fig_m2 = go.Figure(go.Bar(
            y=stats_ciudad["city_token"].str.replace("_", " ").str.title(),
            x=stats_ciudad["precio_m2_mediano"] / 1e6,
            orientation="h",
            marker=dict(color=stats_ciudad["precio_m2_mediano"], colorscale=gold_scale, showscale=False),
            text=(stats_ciudad["precio_m2_mediano"] / 1e6).apply(lambda x: f"${x:.2f}M"),
            textposition="outside",
        ))
        dark_layout(fig_m2, height=400, xaxis=dict(title="Millones COP / m²"), yaxis=dict(showgrid=False))
        st.plotly_chart(fig_m2, width="stretch")

    with m_col2:
        st.markdown('<div class="section-label">Distribución de Ofertas por Ciudad</div>', unsafe_allow_html=True)
        fig_pie = go.Figure(go.Pie(
            labels=stats_ciudad["city_token"].str.replace("_", " ").str.title(),
            values=stats_ciudad["ofertas"],
            hole=0.45,
            marker=dict(colors=["#b8935a", "#d4aa72", "#1a6b4a", "#2a6ab8", "#8b2020", "#4b208b",
                                "#c4784a", "#5a8b3a", "#3a5a8b", "#8b5a3a"]),
            textinfo="percent+label",
            textfont=dict(size=10),
        ))
        dark_layout(fig_pie, height=400)
        st.plotly_chart(fig_pie, width="stretch")

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────
    # ROW 2: Box Plot de Distribución de Precios + Scatter Precio vs Área
    # ──────────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Análisis de Distribución y Relación Precio-Área</div>',
                unsafe_allow_html=True)
    r2_col1, r2_col2 = st.columns(2)

    with r2_col1:
        # ── GRÁFICA 1: Box Plot distribución de precios por ciudad ──
        top_cities = stats_ciudad.nlargest(8, "ofertas")["city_token"].tolist()
        df_box = df[df["city_token"].isin(top_cities)].copy()
        df_box["ciudad"] = df_box["city_token"].str.replace("_", " ").str.title()
        # Limitar a precios razonables para que no explote el box plot
        q99 = df_box["precio_num"].quantile(0.99)
        df_box = df_box[df_box["precio_num"] <= q99]

        fig_box = go.Figure()
        palette = ["#b8935a", "#d4aa72", "#1a6b4a", "#2a6ab8", "#e05252", "#4b208b", "#c4784a", "#5a8b3a"]
        for i, city in enumerate(sorted(df_box["ciudad"].unique())):
            city_data = df_box[df_box["ciudad"] == city]["precio_num"] / 1e6
            fig_box.add_trace(go.Box(
                y=city_data,
                name=city,
                marker_color=palette[i % len(palette)],
                boxmean="sd",
            ))
        dark_layout(fig_box, height=450, yaxis=dict(title="Precio (Millones COP)"),
                    showlegend=False)
        fig_box.update_layout(title=dict(text="Distribución de Precios por Ciudad",
                                         font=dict(size=13, color="rgba(255,255,255,.6)")))
        st.plotly_chart(fig_box, width="stretch")

    with r2_col2:
        # ── GRÁFICA 2: Scatter Precio vs Área por tipo ──
        df_scatter = df[df["city_token"].isin(top_cities)].copy()
        df_scatter = df_scatter.dropna(subset=["area_m2", "precio_num"])
        df_scatter = df_scatter[(df_scatter["area_m2"] > 20) & (df_scatter["area_m2"] < 500)]
        df_scatter = df_scatter[df_scatter["precio_num"] <= q99]
        # Sample para rendimiento
        if len(df_scatter) > 2000:
            df_scatter = df_scatter.sample(2000, random_state=42)

        df_scatter["tipo"] = df_scatter["tipo_inmueble"].astype(str).str.title()
        df_scatter["precio_M"] = df_scatter["precio_num"] / 1e6

        fig_scatter = go.Figure()
        tipo_colors = {"Apartamento": "#b8935a", "Casa": "#2a6ab8"}
        for tipo in df_scatter["tipo"].unique():
            mask = df_scatter["tipo"] == tipo
            fig_scatter.add_trace(go.Scatter(
                x=df_scatter.loc[mask, "area_m2"],
                y=df_scatter.loc[mask, "precio_M"],
                mode="markers",
                name=tipo,
                marker=dict(
                    color=tipo_colors.get(tipo, "#888"),
                    size=5,
                    opacity=0.5,
                ),
                hovertemplate="<b>%{text}</b><br>Área: %{x:.0f}m²<br>Precio: $%{y:.0f}M<extra></extra>",
                text=df_scatter.loc[mask, "city_token"].str.replace("_", " ").str.title(),
            ))

        # Línea de tendencia global
        valid = df_scatter.dropna(subset=["area_m2", "precio_M"])
        if len(valid) > 10:
            z = np.polyfit(valid["area_m2"], valid["precio_M"], 1)
            x_line = np.linspace(valid["area_m2"].min(), valid["area_m2"].max(), 50)
            y_line = np.polyval(z, x_line)
            fig_scatter.add_trace(go.Scatter(
                x=x_line, y=y_line, mode="lines",
                name="Tendencia lineal",
                line=dict(color="#e8a838", width=2, dash="dash"),
            ))

        dark_layout(fig_scatter, height=450,
                    xaxis=dict(title="Área (m²)"),
                    yaxis=dict(title="Precio (Millones COP)"))
        fig_scatter.update_layout(
            title=dict(text="Relación Precio vs Área por Tipo",
                       font=dict(size=13, color="rgba(255,255,255,.6)")),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_scatter, width="stretch")

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────
    # ROW 3: Heatmap Fuente × Ciudad + Comparativa de Portales
    # ──────────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Análisis de Portales Inmobiliarios</div>', unsafe_allow_html=True)
    r3_col1, r3_col2 = st.columns(2)

    with r3_col1:
        # ── GRÁFICA 3: Heatmap oferta por Fuente × Ciudad ──
        df_heat = df[df["city_token"].isin(top_cities)].copy()
        df_heat["ciudad"] = df_heat["city_token"].str.replace("_", " ").str.title()
        df_heat["portal"] = df_heat["fuente"].astype(str).str.replace("_", " ").str.title()

        pivot = df_heat.pivot_table(
            index="portal", columns="ciudad", values="precio_num",
            aggfunc="count", fill_value=0,
        )
        # Ordenar por total de ofertas
        pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=True).index]

        fig_heat = go.Figure(go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0, "#1a1a2e"], [0.3, "#2a4a3a"], [0.7, "#b8935a"], [1, "#f0d090"]],
            text=pivot.values,
            texttemplate="%{text:,}",
            textfont=dict(size=10),
            hovertemplate="<b>%{y}</b> en %{x}<br>Ofertas: %{z:,}<extra></extra>",
        ))
        dark_layout(fig_heat, height=420)
        fig_heat.update_layout(
            title=dict(text="Mapa de Calor: Ofertas por Portal × Ciudad",
                       font=dict(size=13, color="rgba(255,255,255,.6)")),
            xaxis=dict(tickangle=-30),
        )
        st.plotly_chart(fig_heat, width="stretch")

    with r3_col2:
        # ── GRÁFICA 4: Comparativa precio/m² mediano por portal ──
        df_portal = df[df["city_token"].isin(top_cities)].copy()
        df_portal["portal"] = df_portal["fuente"].astype(str).str.replace("_", " ").str.title()
        df_portal = df_portal.dropna(subset=["precio_m2"])

        portal_stats = df_portal.groupby("portal").agg(
            precio_m2_med=("precio_m2", "median"),
            n_ofertas=("precio_num", "count"),
        ).reset_index()
        # Solo portales con al menos 20 ofertas para estadística relevante
        portal_stats = portal_stats[portal_stats["n_ofertas"] >= 20].sort_values("precio_m2_med", ascending=True)

        if not portal_stats.empty:
            global_median = df_portal["precio_m2"].median()

            fig_portal = go.Figure()
            colors = ["#2a9b6a" if v < global_median else "#e05252"
                       for v in portal_stats["precio_m2_med"]]

            fig_portal.add_trace(go.Bar(
                y=portal_stats["portal"],
                x=portal_stats["precio_m2_med"] / 1e6,
                orientation="h",
                marker=dict(color=colors),
                text=[f"${v/1e6:.2f}M ({n:,})" for v, n in
                      zip(portal_stats["precio_m2_med"], portal_stats["n_ofertas"])],
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>Precio/m²: $%{x:.2f}M<extra></extra>",
            ))
            # Línea de referencia del mercado
            fig_portal.add_vline(
                x=global_median / 1e6,
                line=dict(color="#b8935a", width=2, dash="dash"),
                annotation_text=f"Mediana mercado: ${global_median/1e6:.2f}M",
                annotation_position="top",
                annotation_font=dict(size=10, color="#b8935a"),
            )
            dark_layout(fig_portal, height=420,
                        xaxis=dict(title="Precio Mediano / m² (Millones COP)"),
                        yaxis=dict(showgrid=False))
            fig_portal.update_layout(
                title=dict(text="Precio/m² Mediano por Portal (vs Mercado)",
                           font=dict(size=13, color="rgba(255,255,255,.6)")),
            )
            st.plotly_chart(fig_portal, width="stretch")
        else:
            st.info("No hay suficientes datos por portal para generar la comparativa.")

    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────
    # ROW 4: KPIs de Calidad + Segmentación VIS
    # ──────────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Calidad de Datos y Segmentación VIS</div>', unsafe_allow_html=True)
    r4_col1, r4_col2 = st.columns(2)

    with r4_col1:
        # ── GRÁFICA 5: KPIs de calidad de datos ──
        total = len(df)
        pct_precio = (df["precio_num"].notna().sum() / total * 100) if total > 0 else 0
        pct_area = (df["area_m2"].notna().sum() / total * 100) if total > 0 else 0
        pct_habs = (df["habitaciones"].notna().sum() / total * 100) if total > 0 else 0
        pct_ubicacion = (df["ubicacion_clean"].notna().sum() / total * 100) if total > 0 else 0 if "ubicacion_clean" in df.columns else 0

        n_portales = df["fuente"].nunique()
        n_ciudades = df["city_token"].nunique()

        metrics = [
            ("Precio", pct_precio),
            ("Área m²", pct_area),
            ("Habitaciones", pct_habs),
            ("Ubicación", pct_ubicacion),
        ]

        fig_quality = go.Figure()
        cats = [m[0] for m in metrics]
        vals = [m[1] for m in metrics]
        bar_colors = ["#2a9b6a" if v >= 90 else "#b8935a" if v >= 70 else "#e05252" for v in vals]

        fig_quality.add_trace(go.Bar(
            x=cats,
            y=vals,
            marker=dict(color=bar_colors),
            text=[f"{v:.1f}%" for v in vals],
            textposition="outside",
        ))
        # Línea de referencia 90%
        fig_quality.add_hline(
            y=90, line=dict(color="rgba(255,255,255,.2)", width=1, dash="dot"),
            annotation_text="Meta: 90%",
            annotation_position="top right",
            annotation_font=dict(size=9, color="rgba(255,255,255,.3)"),
        )
        dark_layout(fig_quality, height=350,
                    yaxis=dict(title="% Completitud", range=[0, 110]))
        fig_quality.update_layout(
            title=dict(text="Completitud de Datos por Campo",
                       font=dict(size=13, color="rgba(255,255,255,.6)")),
        )
        st.plotly_chart(fig_quality, width="stretch")

        # KPIs complementarios en texto
        st.markdown(
            f'<div style="display:flex;gap:1rem;margin-top:.5rem">'
            f'<div style="flex:1;background:var(--surface2);border:1px solid var(--border);'
            f'padding:.8rem;border-radius:4px;text-align:center">'
            f'<div style="font-size:.6rem;color:rgba(255,255,255,.4);text-transform:uppercase;'
            f'letter-spacing:.1em">Portales</div>'
            f'<div style="font-size:1.3rem;font-weight:900;color:var(--gold)">{n_portales}</div></div>'
            f'<div style="flex:1;background:var(--surface2);border:1px solid var(--border);'
            f'padding:.8rem;border-radius:4px;text-align:center">'
            f'<div style="font-size:.6rem;color:rgba(255,255,255,.4);text-transform:uppercase;'
            f'letter-spacing:.1em">Ciudades</div>'
            f'<div style="font-size:1.3rem;font-weight:900;color:var(--gold)">{n_ciudades}</div></div>'
            f'<div style="flex:1;background:var(--surface2);border:1px solid var(--border);'
            f'padding:.8rem;border-radius:4px;text-align:center">'
            f'<div style="font-size:.6rem;color:rgba(255,255,255,.4);text-transform:uppercase;'
            f'letter-spacing:.1em">Inmuebles</div>'
            f'<div style="font-size:1.3rem;font-weight:900;color:var(--gold)">{total:,}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    with r4_col2:
        # Segmentación VIS vs No-VIS
        df["segmento"] = df["precio_num"].apply(
            lambda x: "VIS (≤$200M)" if x <= 200_000_000 else "No VIS (>$200M)"
        )
        seg_counts = df["segmento"].value_counts()
        N_total = len(df)

        fig_seg = go.Figure(go.Pie(
            labels=seg_counts.index,
            values=seg_counts.values,
            hole=0.5,
            marker=dict(colors=["#2a6ab8", "#b8935a"]),
            textinfo="percent+value",
        ))
        dark_layout(fig_seg, height=300)
        fig_seg.update_layout(
            title=dict(text="Segmentación VIS / No-VIS",
                       font=dict(size=13, color="rgba(255,255,255,.6)")),
        )
        st.plotly_chart(fig_seg, width="stretch")

        st.markdown(
            '<div style="background:var(--surface2);border:1px solid var(--border);padding:1rem;'
            'border-radius:4px">'
            '  <div style="font-weight:bold;color:white;font-size:.85rem;margin-bottom:.4rem">'
            '  Contexto Regulatorio</div>'
            f'  <p style="font-size:.75rem;color:var(--ink);line-height:1.6;margin:0">'
            f'    <strong>VIS</strong> (≤150 SMMLV ≈ $200M COP): '
            f'{(seg_counts.get("VIS (≤$200M)", 0)/N_total*100):.1f}% del catálogo. '
            f'    Acceso a subsidio Mi Casa Ya y tasa preferencial.<br>'
            f'    <strong>No VIS</strong>: Mayor variedad y liquidez en Bogotá, Medellín y Cali. '
            f'    Margen de negociación estimado del 8-12% entre portales.'
            '  </p>'
            '</div>',
            unsafe_allow_html=True,
        )
