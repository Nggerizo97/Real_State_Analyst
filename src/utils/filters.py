# -*- coding: utf-8 -*-
"""Estado de filtros compartido por todas las pestanas.

Antes cada pestana tenia sus propios controles y recalculaba su subconjunto,
asi que dos paneles podian estar mostrando poblaciones distintas al mismo
tiempo. Aqui el filtro se define una sola vez, vive en session_state y todas
las vistas leen el mismo DataFrame filtrado, que es como se comporta un
tablero de Power BI.
"""

import pandas as pd
import streamlit as st

FILTER_KEY = "rea_filters"

DEFAULTS = {
    "departamento": [],
    "municipio": [],
    "barrio": [],
    "tipo_inmueble": [],
    "fuente": [],
    "estado_inmueble": [],
    "precio": None,
    "area": None,
    "habitaciones": None,
    "solo_vigentes": True,
    "cross": {},          # filtros originados al hacer clic en una grafica
}


def get_state() -> dict:
    if FILTER_KEY not in st.session_state:
        st.session_state[FILTER_KEY] = dict(DEFAULTS)
    return st.session_state[FILTER_KEY]


def reset_state():
    st.session_state[FILTER_KEY] = dict(DEFAULTS)


def set_cross_filter(column: str, values):
    """Fija un filtro cruzado desde la seleccion en una grafica."""
    state = get_state()
    cross = dict(state.get("cross", {}))
    if values:
        cross[column] = list(values)
    else:
        cross.pop(column, None)
    state["cross"] = cross


def clear_cross_filters():
    get_state()["cross"] = {}


def _numeric_bounds(series):
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.min()), float(values.max())


_NULL_LABELS = {"nan", "none", "", "<na>", "null"}


def _options(df, column):
    """Valores distintos de una columna, sin los nulos disfrazados de texto."""
    if column not in df.columns:
        return []
    values = df[column].dropna().astype(str).unique().tolist()
    return sorted(v for v in values if v.strip().lower() not in _NULL_LABELS)


def render_sidebar_filters(df: pd.DataFrame) -> dict:
    """Dibuja los controles en cascada y devuelve el estado actualizado.

    La cascada importa: al elegir un departamento, la lista de municipios se
    limita a ese departamento, y los barrios a esos municipios. Sin esto el
    selector de barrios tendria miles de entradas irrelevantes.
    """
    state = get_state()

    st.markdown('<div class="section-label">Filtros</div>', unsafe_allow_html=True)

    if "is_active" in df.columns:
        state["solo_vigentes"] = st.toggle(
            "Solo avisos vigentes",
            value=state["solo_vigentes"],
            help=(
                "Oculta los avisos que ya no aparecen en el portal de origen. "
                "Los datos se conservan para el modelo, solo no se muestran."
            ),
        )

    scope = df
    if state["solo_vigentes"] and "is_active" in df.columns:
        scope = df[df["is_active"].fillna(False)]

    # ── Cascada geografica ────────────────────────────────────────
    dep_options = _options(scope, "departamento")
    state["departamento"] = st.multiselect(
        "Departamento", dep_options,
        default=[v for v in state["departamento"] if v in dep_options],
        key="flt_departamento",
    )
    if state["departamento"]:
        scope = scope[scope["departamento"].astype(str).isin(state["departamento"])]

    mun_options = _options(scope, "municipio")
    state["municipio"] = st.multiselect(
        "Municipio", mun_options,
        default=[v for v in state["municipio"] if v in mun_options],
        key="flt_municipio",
    )
    if state["municipio"]:
        scope = scope[scope["municipio"].astype(str).isin(state["municipio"])]

    barrio_col = "barrio_display" if "barrio_display" in scope.columns else "barrio"
    barrio_options = _options(scope, barrio_col)
    if barrio_options:
        state["barrio"] = st.multiselect(
            f"Barrio / sector ({len(barrio_options)})", barrio_options,
            default=[v for v in state["barrio"] if v in barrio_options],
            key="flt_barrio",
        )

    # ── Atributos del inmueble ────────────────────────────────────
    with st.expander("Características", expanded=False):
        tipo_options = _options(scope, "tipo_inmueble")
        state["tipo_inmueble"] = st.multiselect(
            "Tipo", tipo_options,
            default=[v for v in state["tipo_inmueble"] if v in tipo_options],
            key="flt_tipo",
        )

        estado_options = _options(scope, "estado_inmueble")
        state["estado_inmueble"] = st.multiselect(
            "Estado", estado_options,
            default=[v for v in state["estado_inmueble"] if v in estado_options],
            key="flt_estado",
        )

        precio_bounds = _numeric_bounds(scope.get("precio_num", pd.Series(dtype=float)))
        if precio_bounds and precio_bounds[0] < precio_bounds[1]:
            low, high = precio_bounds
            state["precio"] = st.slider(
                "Precio (millones COP)",
                min_value=float(low / 1e6), max_value=float(high / 1e6),
                value=(float(low / 1e6), float(high / 1e6)),
                step=10.0, key="flt_precio",
            )

        area_bounds = _numeric_bounds(scope.get("area_m2", pd.Series(dtype=float)))
        if area_bounds and area_bounds[0] < area_bounds[1]:
            low, high = area_bounds
            state["area"] = st.slider(
                "Área (m²)",
                min_value=float(low), max_value=float(high),
                value=(float(low), float(high)),
                step=5.0, key="flt_area",
            )

    with st.expander("Portales", expanded=False):
        fuente_options = _options(scope, "fuente")
        state["fuente"] = st.multiselect(
            "Portal de origen", fuente_options,
            default=[v for v in state["fuente"] if v in fuente_options],
            key="flt_fuente",
        )

    if state.get("cross"):
        st.caption("Filtros activos desde gráficas:")
        for column, values in state["cross"].items():
            st.caption(f"· {column}: {', '.join(map(str, values[:3]))}")
        if st.button("Quitar filtros de gráficas", width="stretch"):
            clear_cross_filters()
            st.rerun()

    if st.button("Limpiar todos los filtros", width="stretch"):
        reset_state()
        for key in list(st.session_state.keys()):
            if key.startswith("flt_"):
                del st.session_state[key]
        st.rerun()

    return state


def apply_filters(df: pd.DataFrame, state: dict = None) -> pd.DataFrame:
    """Aplica el estado de filtros a cualquier DataFrame del catalogo."""
    state = state or get_state()
    result = df

    if state.get("solo_vigentes") and "is_active" in result.columns:
        result = result[result["is_active"].fillna(False)]

    categorical = [
        ("departamento", "departamento"),
        ("municipio", "municipio"),
        ("tipo_inmueble", "tipo_inmueble"),
        ("estado_inmueble", "estado_inmueble"),
        ("fuente", "fuente"),
    ]
    for state_key, column in categorical:
        selected = state.get(state_key)
        if selected and column in result.columns:
            result = result[result[column].astype(str).isin(selected)]

    if state.get("barrio"):
        column = "barrio_display" if "barrio_display" in result.columns else "barrio"
        if column in result.columns:
            result = result[result[column].astype(str).isin(state["barrio"])]

    if state.get("precio") and "precio_num" in result.columns:
        low, high = state["precio"]
        precio = pd.to_numeric(result["precio_num"], errors="coerce")
        result = result[precio.between(low * 1e6, high * 1e6)]

    if state.get("area") and "area_m2" in result.columns:
        low, high = state["area"]
        area = pd.to_numeric(result["area_m2"], errors="coerce")
        result = result[area.between(low, high)]

    for column, values in (state.get("cross") or {}).items():
        if column in result.columns and values:
            result = result[result[column].astype(str).isin([str(v) for v in values])]

    return result
