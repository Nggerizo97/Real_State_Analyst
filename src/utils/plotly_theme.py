import plotly.graph_objects as go

# Constantes de diseño para Plotly
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
        height=height, margin=dict(l=40, r=20, t=30, b=40),
        xaxis=dict(gridcolor=_GRID, linecolor=_GRID, zerolinecolor=_GRID,
                   tickfont=dict(color=_TEXT), title_font=dict(color=_MUTED_TEXT)),
        yaxis=dict(gridcolor=_GRID, linecolor=_GRID, zerolinecolor=_GRID,
                   tickfont=dict(color=_TEXT), title_font=dict(color=_MUTED_TEXT)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=_TEXT)),
    )
    layout_params.update(extra)
    fig.update_layout(**layout_params)
    return fig
