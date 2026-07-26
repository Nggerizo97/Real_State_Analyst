# -*- coding: utf-8 -*-
"""Tema y paleta de las graficas.

La paleta anterior no era legible. Verificada con el validador de la
skill dataviz sobre la superficie oscura #16161f, fallaba las cinco pruebas:

    #5a8b3a <-> #c4784a   ΔE 2.0  (protanopia)  -> indistinguibles
    #d4aa72 <-> #b8935a   ΔE 7.9  (vision normal, umbral 15)
    #4b208b, #8b2020      contraste 1.6:1 y 2.0:1 contra el fondo

Es decir: verde y naranja eran el mismo color para una parte de los
usuarios, los dos dorados eran casi iguales para todos, y el morado y el
vino apenas se despegaban del fondo.

La paleta de abajo pasa las cinco pruebas (banda de luminosidad, piso de
croma, separacion CVD, piso de vision normal y contraste). Verificar con:

    python scripts/validate_palette.py \
      "#d97706,#0891b2,#ec4899,#16a34a,#6366f1,#ea580c" \
      --mode dark --surface "#16161f"

Los colores se asignan SIEMPRE en este orden y nunca se reciclan: la serie
numero 7 se agrupa en "Otros", no recibe un color inventado.
"""

import plotly.graph_objects as go

# ── Superficies ──────────────────────────────────────────────────
BG = "#1e1e2a"
PLOT = "#16161f"
GRID = "#2a2a3a"
TEXT = "#c8c4bc"
MUTED_TEXT = "#7a7a8c"
GOLD = "#b8935a"          # color de marca: titulos y acentos, no series

# ── Categorico: orden fijo, verificado ───────────────────────────
CATEGORICAL = [
    "#d97706",  # ambar
    "#0891b2",  # cian
    "#ec4899",  # rosa
    "#16a34a",  # verde
    "#6366f1",  # indigo
    "#ea580c",  # naranja
]
OTHER_COLOR = "#6b7280"   # gris reservado para la categoria "Otros"

# ── Secuencial: un solo tono, claro -> oscuro (magnitud) ─────────
SEQUENTIAL = [
    [0.00, "#2a2118"],
    [0.25, "#6b4d1f"],
    [0.50, "#a37324"],
    [0.75, "#d19a35"],
    [1.00, "#f0c674"],
]

# ── Divergente: dos tonos + gris neutro al centro (polaridad) ────
DIVERGING = [
    [0.0, "#0891b2"],
    [0.5, "#4a4a55"],
    [1.0, "#ea580c"],
]

# ── Estado: reservados, nunca se usan como serie ────────────────
STATUS = {
    "good": "#16a34a",
    "warning": "#d97706",
    "serious": "#ea580c",
    "critical": "#dc2626",
}


def categorical_colors(n: int) -> list:
    """n colores en orden fijo. A partir del septimo, gris de 'Otros'."""
    if n <= len(CATEGORICAL):
        return CATEGORICAL[:n]
    return CATEGORICAL + [OTHER_COLOR] * (n - len(CATEGORICAL))


def color_map(categories) -> dict:
    """Mapa categoria -> color, estable respecto al orden alfabetico.

    El color sigue a la entidad, no a su posicion en el ranking: al filtrar,
    las series que sobreviven conservan su color.
    """
    ordered = sorted(str(c) for c in categories)
    return {name: color for name, color in zip(ordered, categorical_colors(len(ordered)))}


def dark_layout(fig, height=380, **extra):
    """Tema oscuro consistente para cualquier figura Plotly."""
    layout_params = dict(
        paper_bgcolor=BG,
        plot_bgcolor=PLOT,
        font=dict(family="DM Sans", size=11, color=TEXT),
        height=height,
        margin=dict(l=40, r=20, t=36, b=40),
        xaxis=dict(
            gridcolor=GRID, linecolor=GRID, zerolinecolor=GRID,
            tickfont=dict(color=TEXT), title_font=dict(color=MUTED_TEXT),
        ),
        yaxis=dict(
            gridcolor=GRID, linecolor=GRID, zerolinecolor=GRID,
            tickfont=dict(color=TEXT), title_font=dict(color=MUTED_TEXT),
        ),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT)),
        hoverlabel=dict(
            bgcolor="#252535", bordercolor=GRID,
            font=dict(color=TEXT, family="DM Sans", size=11),
        ),
    )
    layout_params.update(extra)
    fig.update_layout(**layout_params)
    return fig


def chart_title(fig, text):
    fig.update_layout(
        title=dict(text=text, font=dict(size=13, color=MUTED_TEXT), x=0, xanchor="left")
    )
    return fig
