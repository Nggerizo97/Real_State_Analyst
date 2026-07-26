# -*- coding: utf-8 -*-
"""Estadisticas de precio a nivel de barrio, calculadas sobre el catalogo.

Sirven para dos cosas en la pestana de Valoracion:

1. Mostrar comparables reales del barrio (cuantos avisos hay, mediana de
   precio por m2, rango intercuartil). Es lo que un tasador miraria primero.

2. Ajustar la estimacion del modelo mientras el bundle en produccion todavia
   no incluya `barrio_key` como feature. El modelo actual solo ve la ciudad,
   asi que un apartamento en El Poblado y otro en Robledo reciben la misma
   valoracion base pese a estar ambos en Medellin. El factor de barrio
   corrige eso con evidencia del propio catalogo.

El ajuste es explicito y se muestra por separado del valor del modelo: no se
presenta como si el modelo hubiera aprendido el barrio.
"""

import pandas as pd
import streamlit as st

# Debajo de este numero de comparables la mediana del barrio es ruido.
MIN_COMPARABLES = 8

# Topes de seguridad para el factor de ajuste. Un barrio no puede valer 4x su
# ciudad; si el dato dice eso, casi siempre es una muestra sucia.
FACTOR_MIN = 0.55
FACTOR_MAX = 1.85

_BLANKS = {"", "nan", "none", "null", "<na>"}


def _clean(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    return text.mask(text.str.lower().isin(_BLANKS), "")


def barrio_column(df: pd.DataFrame) -> pd.Series:
    """Nombre de barrio a usar: el oficial DIVIPOLA, si no el texto depurado."""
    if "barrio_display" in df.columns:
        return _clean(df["barrio_display"])
    oficial = _clean(df["barrio"]) if "barrio" in df.columns else pd.Series("", index=df.index)
    texto = _clean(df["barrio_texto"]) if "barrio_texto" in df.columns else pd.Series("", index=df.index)
    return oficial.where(oficial.ne(""), texto)


@st.cache_data(ttl=1800, show_spinner=False)
def build_barrio_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Tabla municipio × barrio con precio/m2 y su factor frente al municipio."""
    work = pd.DataFrame({
        "municipio": df.get("municipio", pd.Series("", index=df.index)).astype(str),
        "barrio": barrio_column(df),
        "precio_m2": pd.to_numeric(df.get("precio_m2"), errors="coerce"),
        "precio_num": pd.to_numeric(df.get("precio_num"), errors="coerce"),
        "area_m2": pd.to_numeric(df.get("area_m2"), errors="coerce"),
    })
    work = work[work["barrio"].ne("") & work["precio_m2"].notna()]
    if work.empty:
        return pd.DataFrame(
            columns=["municipio", "barrio", "n", "precio_m2_mediano", "precio_m2_p25",
                     "precio_m2_p75", "precio_mediano", "area_mediana", "factor"]
        )

    grouped = work.groupby(["municipio", "barrio"]).agg(
        n=("precio_m2", "size"),
        precio_m2_mediano=("precio_m2", "median"),
        precio_m2_p25=("precio_m2", lambda s: s.quantile(0.25)),
        precio_m2_p75=("precio_m2", lambda s: s.quantile(0.75)),
        precio_mediano=("precio_num", "median"),
        area_mediana=("area_m2", "median"),
    ).reset_index()

    ciudad = work.groupby("municipio")["precio_m2"].median().rename("precio_m2_municipio")
    grouped = grouped.join(ciudad, on="municipio")
    grouped["factor"] = (
        grouped["precio_m2_mediano"] / grouped["precio_m2_municipio"]
    ).clip(FACTOR_MIN, FACTOR_MAX)
    return grouped


def barrios_de(stats: pd.DataFrame, municipio: str, min_comparables=MIN_COMPARABLES) -> list:
    """Barrios de un municipio con comparables suficientes, de mayor a menor."""
    if stats.empty:
        return []
    subset = stats[(stats["municipio"] == municipio) & (stats["n"] >= min_comparables)]
    return subset.sort_values("n", ascending=False)["barrio"].tolist()


def lookup(stats: pd.DataFrame, municipio: str, barrio: str):
    """Fila de estadisticas del barrio, o None si no hay comparables."""
    if stats.empty or not barrio:
        return None
    match = stats[(stats["municipio"] == municipio) & (stats["barrio"] == barrio)]
    if match.empty:
        return None
    return match.iloc[0]
