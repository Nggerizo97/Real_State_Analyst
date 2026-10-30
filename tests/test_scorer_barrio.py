# -*- coding: utf-8 -*-
"""El barrio tiene que mover la valoracion.

Regresion del bundle v8: barrio_key estaba en feature_cols, pero el
entrenamiento nunca leia la columna barrio y la llave era la constante
"barrio_otro". La misma casa en La America, Santa Monica o La Candelaria
salia con el mismo valor, y la app decia que el modelo "incluye el barrio".

Se entrena un modelo pequeno sobre datos sinteticos con las features que
calcula el propio scorer y se prueba la inferencia completa.

Ejecutar:  python -m pytest tests/test_scorer_barrio.py -q
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402
import xgboost as xgb  # noqa: E402
from sklearn.compose import ColumnTransformer  # noqa: E402
from sklearn.feature_extraction.text import TfidfVectorizer  # noqa: E402
from sklearn.impute import SimpleImputer  # noqa: E402
from sklearn.pipeline import Pipeline  # noqa: E402
from sklearn.preprocessing import OneHotEncoder  # noqa: E402

from src.geo.normalize import BARRIO_OTRO, barrio_key  # noqa: E402
from src.utils.scorer import build_features, bundle_conoce_barrio, score_single  # noqa: E402

# feature_cols de 04_Model_Training (bundle v8 con barrio)
FEATURE_COLS = [
    "area_m2", "log_area_m2", "habitaciones", "banos", "garajes",
    "banos_por_habitacion", "num_portales", "dispersion_pct_grupo",
    "precio_desviacion_grupo_pct", "data_completeness",
    "precio_mediano_ciudad", "precio_m2_mediano_ciudad",
    "precio_m2_mediano_segmento", "precio_m2_mediano_habs",
    "precio_estimado_ciudad_area", "precio_estimado_segmento_area",
    "precio_estimado_segmento_area_ajustado",
    "fuente_factor", "fuente_segmento_factor", "ajuste_fuente_pct",
    "area_vs_ciudad_ratio", "tipo_inmueble", "estado_inmueble",
    "fuente", "city_token", "texto_completo",
    "barrio_key", "precio_m2_mediano_barrio", "precio_estimado_barrio_area",
    "barrio_vs_ciudad_ratio",
]
CATEGORICAS = ["tipo_inmueble", "estado_inmueble", "fuente", "city_token", "barrio_key"]

PM2_BARRIO = {
    "La América": 6.0e6,
    "Santa Mónica": 5.0e6,
    "La Candelaria (Centro)": 3.5e6,
    "El Poblado": 11.0e6,
}

CASA = dict(area_m2=230.0, habitaciones=6.0, banos=2.0, garajes=1.0,
            city_token="Medellín", tipo_inmueble="casa", estado_inmueble="usado",
            fuente="manual", municipio="Medellín")


def _avisos(n=1500, seed=3):
    rng = np.random.default_rng(seed)
    nombres = list(PM2_BARRIO)
    filas = []
    for _ in range(n):
        barrio = nombres[rng.integers(len(nombres))]
        area = float(rng.uniform(45, 300))
        filas.append({
            "area_m2": area, "habitaciones": float(rng.integers(1, 6)), "banos": 2.0,
            "garajes": 1.0, "num_portales": 1.0, "dispersion_pct_grupo": 10.0,
            "precio_desviacion_grupo_pct": 5.0, "data_completeness": 0.8,
            "tipo_inmueble": "casa" if rng.random() < 0.5 else "apartamento",
            "estado_inmueble": "usado", "fuente": "fincaraiz", "city_token": "Medellín",
            # Gold escribe el barrio ausente como la cadena "None".
            "barrio": barrio if rng.random() < 0.8 else "None",
            "titulo": "inmueble en venta",
            "precio_num": area * PM2_BARRIO[barrio] * rng.lognormal(0, 0.05),
        })
    return pd.DataFrame(filas)


def _entrenar(con_barrio: bool) -> dict:
    """Bundle con la forma del de 04: estadisticas, preprocesador y booster.

    con_barrio=False reproduce el bug: barrio_stats solo trae barrio_otro.
    """
    avisos = _avisos()
    avisos["precio_m2"] = avisos["precio_num"] / avisos["area_m2"]
    city_stats = avisos.groupby("city_token").agg(
        precio_mediano_ciudad=("precio_num", "median"),
        precio_m2_mediano_ciudad=("precio_m2", "median"),
        area_mediana_ciudad=("area_m2", "median"),
    ).reset_index()
    if con_barrio:
        keys = pd.Series([barrio_key(c, b) for c, b in zip(avisos["city_token"], avisos["barrio"])])
        barrio_stats = (
            avisos.assign(barrio_key=keys)[keys.ne(BARRIO_OTRO)]
            .groupby("barrio_key")["precio_m2"]
            .agg(precio_m2_mediano_barrio="median", n_barrio="count")
            .reset_index()
        )
    else:
        barrio_stats = pd.DataFrame({
            "barrio_key": [BARRIO_OTRO],
            "precio_m2_mediano_barrio": [avisos["precio_m2"].median()],
        })

    bundle = {
        "strategy": "absolute",
        "feature_cols": FEATURE_COLS,
        "city_stats": city_stats,
        "barrio_stats": barrio_stats,
        "market_meta": {
            "global_price_median": float(avisos["precio_num"].median()),
            "global_pm2_median": float(avisos["precio_m2"].median()),
            "global_area_median": float(avisos["area_m2"].median()),
            "global_fuente_factor": 1.0,
        },
        "metrics": {"mape": 10.0},
    }

    features = build_features(avisos, bundle)[FEATURE_COLS]
    numericas = [c for c in FEATURE_COLS if c not in CATEGORICAS and c != "texto_completo"]
    pre = ColumnTransformer(transformers=[
        ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numericas),
        ("cat", Pipeline([
            ("imputer", SimpleImputer(strategy="constant", fill_value="desconocido")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]), CATEGORICAS),
        ("txt", TfidfVectorizer(max_features=50), "texto_completo"),
    ])
    matriz = pre.fit_transform(features)
    bst = xgb.train(
        {"objective": "reg:squarederror", "learning_rate": 0.1, "max_depth": 4, "verbosity": 0},
        xgb.DMatrix(matriz, label=np.log1p(avisos["precio_num"])),
        num_boost_round=120,
    )
    bundle.update(model=bst, _preprocessor=pre)
    return bundle


@pytest.fixture(scope="module")
def bundle():
    return _entrenar(con_barrio=True)


def _valor(bundle, barrio):
    resultado = score_single({**CASA, "barrio": barrio}, bundle)
    assert "error" not in resultado, resultado
    return resultado


def test_barrio_mueve_prediccion(bundle):
    poblado = _valor(bundle, "El Poblado")["valor_predicho"]
    candelaria = _valor(bundle, "La Candelaria (Centro)")["valor_predicho"]
    assert candelaria < poblado
    assert abs(poblado - candelaria) / poblado > 0.05, "El barrio no está afectando la predicción"


def test_los_tres_barrios_del_reporte_dan_valores_distintos(bundle):
    valores = {b: _valor(bundle, b)["valor_predicho"]
               for b in ["La América", "Santa Mónica", "La Candelaria (Centro)"]}
    assert len(set(valores.values())) == 3, valores


def test_la_llave_no_depende_de_tildes_ni_mayusculas(bundle):
    referencia = _valor(bundle, "Santa Mónica")
    assert referencia["barrio_en_modelo"]
    for variante in ["SANTA MONICA", "santa mónica", "  Santa  Monica "]:
        assert _valor(bundle, variante)["valor_predicho"] == referencia["valor_predicho"]


def test_barrio_desconocido_cae_al_municipio_y_se_avisa(bundle, caplog):
    sin_barrio = _valor(bundle, "")
    with caplog.at_level(logging.WARNING):
        inventado = _valor(bundle, "Barrio Inventado")
    assert not inventado["barrio_en_modelo"]
    assert inventado["barrio_key"] == BARRIO_OTRO
    assert inventado["valor_predicho"] == sin_barrio["valor_predicho"]
    assert "barrio_no_encontrado" in caplog.text


def test_ninguna_feature_se_rellena_con_constante(bundle, caplog):
    with caplog.at_level(logging.WARNING):
        _valor(bundle, "La América")
    assert "features_sin_calcular" not in caplog.text

    filas = pd.DataFrame([{**CASA, "barrio": b} for b in ["La América", "El Poblado"]])
    features = build_features(filas, bundle)
    for col in ["barrio_key", "precio_m2_mediano_barrio", "precio_estimado_barrio_area",
                "barrio_vs_ciudad_ratio"]:
        assert features[col].nunique() == 2, col


def test_bundle_sin_barrio_real_no_anuncia_barrio(bundle):
    assert bundle_conoce_barrio(bundle)
    # El bundle v8 publicado: barrio_key en feature_cols, pero solo barrio_otro.
    assert not bundle_conoce_barrio(_entrenar(con_barrio=False))
    sin_columna = {**bundle, "feature_cols": [c for c in FEATURE_COLS if c != "barrio_key"]}
    assert not bundle_conoce_barrio(sin_columna)
