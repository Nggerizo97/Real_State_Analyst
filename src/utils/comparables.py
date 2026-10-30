# -*- coding: utf-8 -*-
"""Comparables de la pestana de Valoracion, con respaldo por niveles.

Comparar una casa de 221 m2 contra la mediana de TODO el barrio mezcla
apartamentos y casas de cualquier tamano. En Medellin los apartamentos tienen
un precio/m2 mas alto, asi que el modelo parecia quedar 16-35 % "por debajo"
del barrio sin que eso dijera nada del modelo.

Filtrar sin mas por tipo y area tampoco sirve: hay sectores y ciudades casi
sin casas publicadas, y una mediana de 3 avisos es ruido. Por eso se usa el
grupo mas especifico que tenga al menos MIN_COMPARABLES avisos:

1. mismo barrio, mismo tipo y area parecida (±30 %)
2. mismo barrio y mismo tipo, cualquier area
3. todo el barrio, corregido por lo que vale ese tipo frente al total del
   municipio (p. ej. casas = 0.80 del precio/m2 de Medellin). Si el municipio
   tampoco tiene suficientes avisos de ese tipo, sin correccion.

El nivel usado se devuelve siempre para mostrarlo en pantalla: un rango de 9
casas no pesa lo mismo que uno de 1.800 inmuebles.

No importa streamlit para poder probarse en CI.
"""

import pandas as pd

# Debajo de este numero de comparables la mediana es ruido.
MIN_COMPARABLES = 8

# Avisos de un tipo que necesita el municipio para estimar el factor de tipo.
MIN_TIPO_MUNICIPIO = 30

# "Area parecida": ±30 % del area del inmueble a tasar.
AREA_TOLERANCIA = 0.30

_BLANKS = {"", "nan", "none", "null", "<na>"}
_PLURAL = {"casa": "casas", "apartamento": "apartamentos"}


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


def _columna(df: pd.DataFrame, nombre: str) -> pd.Series:
    return df[nombre] if nombre in df.columns else pd.Series(None, index=df.index, dtype=object)


def _resumen(sub: pd.DataFrame, nivel: str, descripcion: str, ajuste: float = 1.0, nota: str = "") -> dict:
    pm2 = sub["precio_m2"]
    return {
        "nivel": nivel,
        "descripcion": descripcion,
        "n": int(len(sub)),
        "precio_m2_mediano": float(pm2.median()) * ajuste,
        "precio_m2_p25": float(pm2.quantile(0.25)) * ajuste,
        "precio_m2_p75": float(pm2.quantile(0.75)) * ajuste,
        "ajuste_tipo": ajuste,
        "nota": nota,
    }


def buscar_comparables(df: pd.DataFrame, municipio: str, barrio: str, tipo: str, area: float):
    """Comparables del inmueble en el nivel mas especifico con datos suficientes.

    Devuelve un dict con nivel, descripcion, n, mediana y rango intercuartil de
    precio/m2 (ya corregidos por tipo si aplica), o None si el barrio no llega
    a MIN_COMPARABLES avisos de ningun tipo.
    """
    if not barrio:
        return None

    base = pd.DataFrame({
        "municipio": _columna(df, "municipio").astype(str),
        "barrio": barrio_column(df),
        "tipo": _columna(df, "tipo_inmueble").astype(str),
        "area": pd.to_numeric(_columna(df, "area_m2"), errors="coerce"),
        "precio_m2": pd.to_numeric(_columna(df, "precio_m2"), errors="coerce"),
    })
    base = base[base["precio_m2"].notna() & base["municipio"].eq(municipio)]

    en_barrio = base["barrio"].eq(barrio)
    mismo_tipo = base["tipo"].eq(tipo)
    lo, hi = area * (1 - AREA_TOLERANCIA), area * (1 + AREA_TOLERANCIA)
    area_parecida = base["area"].between(lo, hi)
    plural = _PLURAL.get(tipo, f"inmuebles tipo {tipo}")

    sub = base[en_barrio & mismo_tipo & area_parecida]
    if len(sub) >= MIN_COMPARABLES:
        return _resumen(sub, "barrio_tipo_area", f"{plural} de {lo:,.0f}–{hi:,.0f} m² en {barrio}")

    sub = base[en_barrio & mismo_tipo]
    if len(sub) >= MIN_COMPARABLES:
        return _resumen(sub, "barrio_tipo", f"{plural} de cualquier área en {barrio}")

    sub = base[en_barrio]
    if len(sub) < MIN_COMPARABLES:
        return None

    n_tipo_barrio = int((en_barrio & mismo_tipo).sum())
    del_tipo = base.loc[mismo_tipo, "precio_m2"]
    if len(del_tipo) >= MIN_TIPO_MUNICIPIO:
        ajuste = float(del_tipo.median() / base["precio_m2"].median())
        return _resumen(
            sub, "barrio_ajustado", f"todos los inmuebles de {barrio}, corregidos por tipo", ajuste,
            nota=(
                f"Solo hay {n_tipo_barrio} {plural} en {barrio}, así que se usa todo el barrio "
                f"corregido por el factor {ajuste:.2f} ({plural} frente al total en {municipio}, "
                f"{len(del_tipo):,} avisos)."
            ),
        )
    return _resumen(
        sub, "barrio_todos", f"todos los inmuebles de {barrio}",
        nota=(
            f"Hay muy pocos avisos de {plural} en {barrio} ({n_tipo_barrio}) y en {municipio} "
            f"({len(del_tipo)}) para corregir por tipo: el rango mezcla tipos de inmueble "
            f"y sirve solo como referencia."
        ),
    )
