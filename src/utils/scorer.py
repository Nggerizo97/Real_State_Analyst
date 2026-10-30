import logging
import pandas as pd
import numpy as np
import pickle
import io
import traceback
import xgboost as xgb

try:
    from src.geo.normalize import BARRIO_OTRO, BLANK_MARKERS, barrio_key
except ImportError:  # la API agrega src/ al path, no la raiz del repo
    from geo.normalize import BARRIO_OTRO, BLANK_MARKERS, barrio_key

logger = logging.getLogger(__name__)

# Mismas columnas y orden que cols_text_candidates en 04_Model_Training.
TEXT_COLS = ["ubicacion_norm", "zona_mercado", "ubicacion_raw", "titulo"]

BARRIO_NUM_FEATURES = [
    "precio_m2_mediano_barrio", "precio_estimado_barrio_area", "barrio_vs_ciudad_ratio",
]


def _to_df(v):
    """Convierte list-of-dicts (formato JSON bundle) a DataFrame; pasa DataFrames sin cambio."""
    if v is None: return None
    if isinstance(v, list): return pd.DataFrame(v) if v else None
    return v


def _barrio_nombre(frame: pd.DataFrame) -> pd.Series:
    """Barrio oficial DIVIPOLA o, si falta, el texto libre. Igual que en 04."""
    def limpio(col):
        if col not in frame.columns:
            return pd.Series("", index=frame.index)
        texto = frame[col].astype(str).str.strip()
        return texto.mask(texto.str.lower().isin(BLANK_MARKERS), "")

    oficial, texto = limpio("barrio"), limpio("barrio_texto")
    return oficial.where(oficial.ne(""), texto)


def bundle_conoce_barrio(bundle: dict) -> bool:
    """True solo si el modelo se entreno con barrios reales.

    No basta con que barrio_key este en feature_cols: el bundle v8 la listaba
    aunque el entrenamiento nunca leyo la columna barrio, asi que barrio_key
    era la constante "barrio_otro" y su efecto en la prediccion era cero.
    """
    if not bundle or "barrio_key" not in (bundle.get("feature_cols") or []):
        return False
    stats = _to_df(bundle.get("barrio_stats"))
    if stats is None or stats.empty or "barrio_key" not in stats.columns:
        return False
    return bool(stats["barrio_key"].ne(BARRIO_OTRO).any())


def add_barrio_features(frame: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Features de barrio con la misma regla que 04_Model_Training.

    La llave sale de src.geo.normalize.barrio_key (la misma funcion que usa el
    entrenamiento) y las medianas de bundle["barrio_stats"], la tabla que
    alimento la feature. Antes la pestana armaba f"{ciudad} | {barrio}" a mano
    y traia sus propias medianas del catalogo: la llave no coincidia con la
    del modelo y el OneHotEncoder(handle_unknown="ignore") la volvia ceros sin
    avisar. Un barrio que el modelo no conoce pasa a barrio_otro y toma la
    mediana de su ciudad, igual que en el entrenamiento; `barrio_en_modelo`
    lo deja dicho. Requiere precio_m2_mediano_ciudad ya calculado.
    """
    stats = _to_df(bundle.get("barrio_stats"))
    if stats is not None and not stats.empty and "barrio_key" in stats.columns:
        medianas = stats.drop_duplicates("barrio_key").set_index("barrio_key")["precio_m2_mediano_barrio"]
    else:
        medianas = pd.Series(dtype=float)

    nombres = _barrio_nombre(frame)
    keys = pd.Series(
        [barrio_key(ciudad, barrio) for ciudad, barrio in zip(frame["city_token"], nombres)],
        index=frame.index,
    )
    frame = frame.drop(columns=[c for c in ["barrio_key", "barrio_en_modelo", *BARRIO_NUM_FEATURES]
                                if c in frame.columns])

    conocido = keys.isin(medianas.index)
    frame["barrio_en_modelo"] = conocido & keys.ne(BARRIO_OTRO)
    frame["barrio_key"] = keys.where(conocido, BARRIO_OTRO)
    frame["precio_m2_mediano_barrio"] = (
        frame["barrio_key"].map(medianas).fillna(frame["precio_m2_mediano_ciudad"])
    )
    frame["precio_estimado_barrio_area"] = frame["area_m2"] * frame["precio_m2_mediano_barrio"]
    frame["barrio_vs_ciudad_ratio"] = (
        frame["precio_m2_mediano_barrio"] / frame["precio_m2_mediano_ciudad"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(1.0)

    if bundle_conoce_barrio(bundle):
        perdidos = nombres.ne("") & ~frame["barrio_en_modelo"]
        if perdidos.any():
            logger.warning(
                "barrio_no_encontrado n=%d ejemplos=%s",
                int(perdidos.sum()), keys[perdidos].unique()[:5].tolist(),
            )
    return frame


def build_features(df: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Calcula todas las features del modelo con la logica de 04_Model_Training.

    Devuelve el DataFrame enriquecido; `resultado[bundle["feature_cols"]]` es
    exactamente lo que recibe el preprocesador. Sirve tambien para depurar:
    si una columna no cambia entre dos entradas, el modelo tampoco puede.
    """
    city_stats = _to_df(bundle.get("city_stats"))
    comuna_stats = _to_df(bundle.get("comuna_stats"))
    segment_stats = _to_df(bundle.get("segment_stats"))
    micro_stats = _to_df(bundle.get("micro_stats"))
    sector_stats = _to_df(bundle.get("sector_stats"))
    fuente_ratio_stats = _to_df(bundle.get("fuente_ratio_stats"))
    fuente_segmento_ratio_stats = _to_df(bundle.get("fuente_segmento_ratio_stats"))
    hab_stats = _to_df(bundle.get("hab_stats"))
    market_meta = bundle.get("market_meta", {})

    df_pred = df.copy()

    # 1. Limpieza base e imputación geométrica
    for col in ["area_m2", "habitaciones", "banos", "garajes", "num_portales",
                "dispersion_pct_grupo", "precio_desviacion_grupo_pct", "data_completeness"]:
        if col not in df_pred.columns: df_pred[col] = np.nan
        df_pred[col] = pd.to_numeric(df_pred[col], errors="coerce")

    for col, default in [("tipo_inmueble", "otro"), ("estado_inmueble", "desconocido"), 
                        ("fuente", "desconocido"), ("city_token", "otra_ciudad"),
                        ("comuna_mercado", "comuna_otra"), ("sector_mercado", "sector_otra")]:
        if col not in df_pred.columns: df_pred[col] = default
        df_pred[col] = df_pred[col].fillna(default).astype(str)

    df_pred["log_area_m2"] = np.log1p(df_pred["area_m2"].clip(lower=0))
    df_pred["banos_por_habitacion"] = (
        df_pred["banos"] / df_pred["habitaciones"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan)
    text_cols = [c for c in TEXT_COLS if c in df_pred.columns]
    df_pred["texto_completo"] = (
        df_pred[text_cols].fillna("").astype(str).agg(" ".join, axis=1) if text_cols else ""
    )
    df_pred["habitaciones_bucket"] = df_pred["habitaciones"].fillna(-1).clip(-1, 6)
    df_pred["market_segment"] = df_pred["city_token"] + "__" + df_pred["tipo_inmueble"]
    df_pred["micro_market_segment"] = df_pred["market_segment"] + "__" + df_pred["comuna_mercado"]
    df_pred["sector_market_segment"] = df_pred["market_segment"] + "__" + df_pred["sector_mercado"]

    # 2. Merges de Estadísticas (Market Features)
    if city_stats is not None: df_pred = df_pred.merge(city_stats, on="city_token", how="left")
    if comuna_stats is not None: df_pred = df_pred.merge(comuna_stats, on=["city_token", "comuna_mercado"], how="left")
    if segment_stats is not None: df_pred = df_pred.merge(segment_stats, on="market_segment", how="left")
    if micro_stats is not None: df_pred = df_pred.merge(micro_stats, on="micro_market_segment", how="left")
    if sector_stats is not None: df_pred = df_pred.merge(sector_stats, on="sector_market_segment", how="left")
    if hab_stats is not None: df_pred = df_pred.merge(hab_stats, on=["city_token","habitaciones_bucket"], how="left")
    if fuente_ratio_stats is not None: df_pred = df_pred.merge(fuente_ratio_stats, on="fuente", how="left")
    if fuente_segmento_ratio_stats is not None: df_pred = df_pred.merge(fuente_segmento_ratio_stats, on=["fuente","market_segment"], how="left")

    # 3. Cálculo de Features Estimadas (Lógica Paridad v8)
    gpm = market_meta.get("global_price_median", 0.0)
    gpm2 = market_meta.get("global_pm2_median", 0.0)
    gam = market_meta.get("global_area_median", np.nan)
    gff = market_meta.get("global_fuente_factor", 1.0)

    # Fallbacks
    r = df_pred
    r["precio_mediano_ciudad"] = r.get("precio_mediano_ciudad", pd.Series(gpm, index=r.index)).fillna(gpm)
    r["precio_m2_mediano_ciudad"] = r.get("precio_m2_mediano_ciudad", pd.Series(gpm2, index=r.index)).fillna(gpm2)
    r["area_mediana_ciudad"] = r.get("area_mediana_ciudad", pd.Series(gam, index=r.index)).fillna(gam)
    r["precio_m2_mediano_comuna"] = r.get("precio_m2_mediano_comuna", r["precio_m2_mediano_ciudad"]).fillna(r["precio_m2_mediano_ciudad"])
    r["precio_m2_mediano_segmento"] = r.get("precio_m2_mediano_segmento", r["precio_m2_mediano_comuna"]).fillna(r["precio_m2_mediano_comuna"])
    r["precio_m2_mediano_microsegmento"] = r.get("precio_m2_mediano_microsegmento", r["precio_m2_mediano_segmento"]).fillna(r["precio_m2_mediano_segmento"])
    r["precio_m2_mediano_sector"] = r.get("precio_m2_mediano_sector", r["precio_m2_mediano_comuna"]).fillna(r["precio_m2_mediano_comuna"])
    r["precio_m2_p25_sector"] = r.get("precio_m2_p25_sector", r["precio_m2_mediano_sector"]).fillna(r["precio_m2_mediano_sector"])
    r["precio_m2_p75_sector"] = r.get("precio_m2_p75_sector", r["precio_m2_mediano_sector"]).fillna(r["precio_m2_mediano_sector"])
    
    r["fuente_factor"] = r.get("fuente_factor", pd.Series(gff, index=r.index)).fillna(gff)
    r["fuente_segmento_factor"] = r.get("fuente_segmento_factor", r["fuente_factor"]).fillna(r["fuente_factor"])
    # En 04 la mediana por habitaciones cae a la del segmento; dejarla en NaN
    # la mandaba a la mediana global del imputer.
    r["precio_m2_mediano_habs"] = r.get("precio_m2_mediano_habs", r["precio_m2_mediano_segmento"]).fillna(r["precio_m2_mediano_segmento"])

    # Estimaciones Finales
    r["precio_estimado_ciudad_area"] = r["area_m2"] * r["precio_m2_mediano_ciudad"]
    r["precio_estimado_comuna_area"] = r["area_m2"] * r["precio_m2_mediano_comuna"]
    r["precio_estimado_segmento_area"] = r["area_m2"] * r["precio_m2_mediano_segmento"]
    r["precio_estimado_microsegmento_area"] = r["area_m2"] * r["precio_m2_mediano_microsegmento"]
    r["precio_estimado_sector_area"] = r["area_m2"] * r["precio_m2_mediano_sector"]
    r["precio_sector_rango_bajo_area"] = r["area_m2"] * r["precio_m2_p25_sector"]
    r["precio_sector_rango_alto_area"] = r["area_m2"] * r["precio_m2_p75_sector"]
    
    # El baseline principal del modelo v8
    r["precio_estimado_segmento_area_ajustado"] = (
        (r["precio_estimado_microsegmento_area"] * r["fuente_segmento_factor"])
        .fillna(r["precio_estimado_microsegmento_area"])
        .fillna(r["precio_estimado_sector_area"])
        .fillna(r["precio_estimado_comuna_area"])
        .fillna(r["precio_estimado_ciudad_area"])
    )
    # Estas tres no se calculaban aqui y entraban al modelo como 0.0
    # (area_vs_ciudad_ratio = 0 no existe en el entrenamiento).
    r["area_vs_ciudad_ratio"] = (
        r["area_m2"] / r["area_mediana_ciudad"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    r["ajuste_fuente_pct"] = (r["fuente_segmento_factor"] - 1.0) * 100.0
    r = add_barrio_features(r, bundle)

    # 4. Preparación de Features para el Booster
    feature_cols = bundle.get("feature_cols", [])
    if not feature_cols: raise ValueError("Bundle no contiene feature_cols.")

    faltantes = [c for c in feature_cols if c not in r.columns]
    if faltantes:
        # Una feature rellenada con una constante no puede mover la
        # prediccion: se avisa en vez de ocultarlo.
        logger.warning("features_sin_calcular rellenadas con constante: %s", faltantes)
    for col in faltantes:
        if col in ["tipo_inmueble", "estado_inmueble", "fuente", "city_token", "texto_completo"]:
            r[col] = "desconocido" if col != "texto_completo" else ""
        else:
            r[col] = 0.0
    return r


def score_dataframe(df: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Aplica el bundle al dataframe completo usando Safe-Load Hybrid y Market Features v8."""
    if bundle is None:
        df["precio_predicho"] = df["precio_num"]
        df["rentabilidad_potencial"] = 0.0
        df["estado_inversion"] = "Sin modelo"
        return df

    strategy = bundle.get("strategy", "absolute")
    market_meta = bundle.get("market_meta", {})
    gpm = market_meta.get("global_price_median", 0.0)

    try:
        r = build_features(df, bundle)
        X = r[bundle["feature_cols"]].copy()

        # 5. Predicción Safe-Load
        # 5a. Recuperar preprocesador (prioridad: _preprocessor ya deserializado por model_loader)
        preprocessor = bundle.get("_preprocessor")

        if preprocessor is None:
            preprocessor_blob = bundle.get("preprocessor_pickle")
            if preprocessor_blob:
                try:
                    # El bundle JSON almacena el pickle codificado en latin1
                    if isinstance(preprocessor_blob, str):
                        preprocessor_blob = preprocessor_blob.encode('latin1')
                    preprocessor = pickle.loads(preprocessor_blob)
                except Exception as e:
                    unpickle_error = str(e)
                    print(f"Error unpickling preprocessor: {e}")

        if preprocessor is None:
            old_pipe = bundle.get("model")
            if hasattr(old_pipe, 'named_steps'):
                preprocessor = old_pipe.named_steps.get('preprocessor')

        # 5b. Recuperar Booster (prioridad: ya cargado por model_loader → raw model_json)
        bst = None
        model_obj = bundle.get("model")
        if isinstance(model_obj, xgb.Booster):
            bst = model_obj

        if bst is None:
            model_json = bundle.get("model_json")
            if model_json:
                bst = xgb.Booster()
                # model_json es JSON plano de XGBoost, NO base64
                if isinstance(model_json, str):
                    model_json = model_json.encode('utf-8')
                bst.load_model(bytearray(model_json))

        if bst is not None:
            # Intento de recuperación final del preprocesador
            if not preprocessor:
                for k in ["preprocessor", "transformer", "proc"]:
                    if k in bundle:
                        preprocessor = bundle[k]
                        break

            if not preprocessor:
                avail_keys = list(bundle.keys())
                err_ext = f" Error unpickling: {unpickle_error}" if 'unpickle_error' in locals() else ""
                raise ValueError(f"Preprocesador no disponible.{err_ext} Llaves en bundle: {avail_keys}")

            X_proc = preprocessor.transform(X)
            precio_pred = bst.predict(xgb.DMatrix(X_proc))
            precio_pred = np.expm1(precio_pred)  # Inverse Log

            if strategy == "residual":
                precio_pred = precio_pred * r["precio_estimado_segmento_area_ajustado"].fillna(gpm).to_numpy()
        else:
            pipeline = bundle.get("model")
            if not pipeline: raise ValueError("No hay modelo disponible.")
            precio_pred = pipeline.predict(X)
            if strategy == "residual" or "Residual" in str(type(pipeline)):  # v8 unified fallback
                precio_pred = precio_pred * r["precio_estimado_segmento_area_ajustado"].fillna(gpm).to_numpy()

        df["precio_predicho"] = precio_pred.astype("float32")
        df["barrio_en_modelo"] = r["barrio_en_modelo"].to_numpy()
        df["barrio_key"] = r["barrio_key"].to_numpy()
        df["rentabilidad_potencial"] = (
            ((df["precio_predicho"] - df["precio_num"]) / df["precio_num"].replace(0, np.nan) * 100)
            .replace([np.inf, -np.inf], 0).fillna(0).round(1).astype("float32")
        )

        mape_modelo = bundle.get("metrics", {}).get("mape", 20.0)
        signal_threshold = max(12.0, min(25.0, float(mape_modelo) * 0.75))
        df["estado_inversion"] = df["rentabilidad_potencial"].apply(
            lambda x: "Oportunidad" if x > signal_threshold else ("Sobrevalorado" if x < -signal_threshold else "En mercado")
        ).astype("category")

        # Liberación explícita de copias y temporales pesados de RAM
        import gc
        if 'r' in locals(): del r
        if 'X' in locals(): del X
        if 'X_proc' in locals(): del X_proc
        gc.collect()

    except Exception as e:
        traceback.print_exc()
        df["precio_predicho"] = np.nan
        df["estado_inversion"] = f"Error Crítico: {str(e)[:50]}"
        if "estado_inversion" in df.columns:
            df["estado_inversion"] = df["estado_inversion"].astype("category")

    return df

def score_single(row: dict, bundle: dict) -> dict:
    """Valora un único inmueble manually."""
    if bundle is None: return {"error": "Bundle no disponible"}
    df_temp = pd.DataFrame([row])
    for col in ["precio_num", "num_portales", "dispersion_pct_grupo", "precio_desviacion_grupo_pct", "data_completeness"]:
        if col not in df_temp.columns: df_temp[col] = 0.0
            
    scored = score_dataframe(df_temp, bundle)
    r = scored.iloc[0]
    if pd.isna(r["precio_predicho"]): return {"error": r["estado_inversion"]}

    mape_pct = bundle.get("metrics", {}).get("mape", 20.0)
    valor = float(r["precio_predicho"])
    return {
        "valor_predicho": valor,
        "precio_m2_pred": valor / max(1, row.get("area_m2", 1)),
        "rango_low": valor * (1 - mape_pct/100),
        "rango_high": valor * (1 + mape_pct/100),
        "mape_pct": mape_pct,
        "estado": r["estado_inversion"],
        "barrio_en_modelo": bool(r["barrio_en_modelo"]),
        "barrio_key": str(r["barrio_key"]),
    }
