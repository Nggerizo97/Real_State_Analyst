# -*- coding: utf-8 -*-
"""Comparables por niveles: barrios y ciudades con pocas casas.

Ejecutar:  python -m pytest tests/test_comparables.py -q
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from src.utils.comparables import buscar_comparables  # noqa: E402


def _avisos(municipio, barrio, tipo, n, precio_m2, area=100.0):
    return [{"municipio": municipio, "barrio_display": barrio, "tipo_inmueble": tipo,
             "area_m2": area, "precio_m2": precio_m2} for _ in range(n)]


def _catalogo(*grupos):
    return pd.DataFrame([fila for grupo in grupos for fila in grupo])


# Medellin: muchas casas a 4M/m2 en otros barrios y apartamentos a 6M/m2.
MEDELLIN = (
    _avisos("Medellín", "Belén", "casa", 40, 4.0e6, area=200)
    + _avisos("Medellín", "Belén", "apartamento", 60, 6.0e6)
)


def test_mismo_tipo_y_area_parecida_cuando_hay_datos():
    df = _catalogo(MEDELLIN, _avisos("Medellín", "Laureles", "casa", 10, 5.0e6, area=210),
                   _avisos("Medellín", "Laureles", "apartamento", 50, 7.0e6))
    c = buscar_comparables(df, "Medellín", "Laureles", "casa", 221)
    assert c["nivel"] == "barrio_tipo_area"
    assert c["n"] == 10
    assert c["precio_m2_mediano"] == 5.0e6


def test_mismo_tipo_cualquier_area_si_no_hay_de_area_parecida():
    df = _catalogo(MEDELLIN, _avisos("Medellín", "Laureles", "casa", 10, 5.0e6, area=80),
                   _avisos("Medellín", "Laureles", "apartamento", 50, 7.0e6))
    c = buscar_comparables(df, "Medellín", "Laureles", "casa", 221)
    assert c["nivel"] == "barrio_tipo"
    assert c["n"] == 10


def test_barrio_casi_sin_casas_usa_todo_el_barrio_corregido_por_tipo():
    # 3 casas en Laureles: no alcanza. Se usa todo el barrio con el factor
    # casa/total de Medellin.
    df = _catalogo(MEDELLIN, _avisos("Medellín", "Laureles", "casa", 3, 5.0e6, area=220),
                   _avisos("Medellín", "Laureles", "apartamento", 50, 7.0e6))
    c = buscar_comparables(df, "Medellín", "Laureles", "casa", 221)
    assert c["nivel"] == "barrio_ajustado"
    assert c["n"] == 53
    factor = 4.0e6 / 6.0e6  # mediana casas / mediana total de Medellin
    assert c["ajuste_tipo"] == pytest.approx(factor)
    assert c["precio_m2_mediano"] == pytest.approx(7.0e6 * factor)
    assert "Solo hay 3 casas" in c["nota"]


def test_ciudad_casi_sin_casas_no_inventa_factor():
    df = _catalogo(_avisos("Santa Marta", "Rodadero", "apartamento", 40, 5.0e6),
                   _avisos("Santa Marta", "Rodadero", "casa", 2, 3.0e6))
    c = buscar_comparables(df, "Santa Marta", "Rodadero", "casa", 221)
    assert c["nivel"] == "barrio_todos"
    assert c["ajuste_tipo"] == 1.0
    assert "mezcla tipos" in c["nota"]


def test_sin_datos_suficientes_en_el_barrio():
    df = _catalogo(MEDELLIN, _avisos("Medellín", "Robledo", "casa", 5, 3.0e6))
    assert buscar_comparables(df, "Medellín", "Robledo", "casa", 221) is None
    assert buscar_comparables(df, "Medellín", "", "casa", 221) is None


def test_el_mismo_barrio_en_otra_ciudad_no_cuenta():
    df = _catalogo(MEDELLIN, _avisos("Envigado", "Laureles", "casa", 20, 9.0e6, area=220),
                   _avisos("Medellín", "Laureles", "apartamento", 10, 7.0e6))
    c = buscar_comparables(df, "Medellín", "Laureles", "casa", 221)
    assert c["nivel"] == "barrio_ajustado"
    assert c["n"] == 10
