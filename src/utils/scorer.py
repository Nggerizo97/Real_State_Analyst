import pandas as pd
import numpy as np
import pickle
import io
import traceback
import xgboost as xgb

def score_dataframe(df: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Aplica el bundle al dataframe completo usando Safe-Load Hybrid y Market Features v8."""
    if bundle is None:
        df["precio_predicho"] = df["precio_num"]
        df["rentabilidad_potencial"] = 0.0
        df["estado_inversion"] = "Sin modelo"
        return df

    strategy = bundle.get("strategy", "absolute")
    city_stats = bundle.get("city_stats")
    comuna_stats = bundle.get("comuna_stats")
    segment_stats = bundle.get("segment_stats")
    micro_stats = bundle.get("micro_stats")
    sector_stats = bundle.get("sector_stats")
    fuente_ratio_stats = bundle.get("fuente_ratio_stats")
    fuente_segmento_ratio_stats = bundle.get("fuente_segmento_ratio_stats")
    hab_stats = bundle.get("hab_stats")
    market_meta = bundle.get("market_meta", {})

    try:
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
        df_pred["hab_bucket"] = df_pred["habitaciones"].fillna(-1).clip(-1, 6)
        df_pred["market_segment"] = df_pred["city_token"] + "__" + df_pred["tipo_inmueble"]
        df_pred["micro_market_segment"] = df_pred["market_segment"] + "__" + df_pred["comuna_mercado"]
        df_pred["sector_market_segment"] = df_pred["market_segment"] + "__" + df_pred["sector_mercado"]

        # 2. Merges de Estadísticas (Market Features)
        if city_stats is not None: df_pred = df_pred.merge(city_stats, on="city_token", how="left")
        if comuna_stats is not None: df_pred = df_pred.merge(comuna_stats, on=["city_token", "comuna_mercado"], how="left")
        if segment_stats is not None: df_pred = df_pred.merge(segment_stats, on="market_segment", how="left")
        if micro_stats is not None: df_pred = df_pred.merge(micro_stats, on="micro_market_segment", how="left")
        if sector_stats is not None: df_pred = df_pred.merge(sector_stats, on="sector_market_segment", how="left")
        if hab_stats is not None: df_pred = df_pred.merge(hab_stats, on=["city_token","hab_bucket"], how="left")
        if fuente_ratio_stats is not None: df_pred = df_pred.merge(fuente_ratio_stats, on="fuente", how="left")
        if fuente_segmento_ratio_stats is not None: df_pred = df_pred.merge(fuente_segmento_ratio_stats, on=["fuente","market_segment"], how="left")

        # 3. Cálculo de Features Estimadas (Lógica Paridad v8)
        gpm = market_meta.get("global_price_median", 0.0)
        gpm2 = market_meta.get("global_pm2_median", 0.0)
        gff = market_meta.get("global_fuente_factor", 1.0)

        # Fallbacks
        r = df_pred
        r["precio_mediano_ciudad"] = r.get("precio_mediano_ciudad", pd.Series(gpm, index=r.index)).fillna(gpm)
        r["precio_m2_mediano_ciudad"] = r.get("precio_m2_mediano_ciudad", pd.Series(gpm2, index=r.index)).fillna(gpm2)
        r["precio_m2_mediano_comuna"] = r.get("precio_m2_mediano_comuna", r["precio_m2_mediano_ciudad"]).fillna(r["precio_m2_mediano_ciudad"])
        r["precio_m2_mediano_segmento"] = r.get("precio_m2_mediano_segmento", r["precio_m2_mediano_comuna"]).fillna(r["precio_m2_mediano_comuna"])
        r["precio_m2_mediano_microsegmento"] = r.get("precio_m2_mediano_microsegmento", r["precio_m2_mediano_segmento"]).fillna(r["precio_m2_mediano_segmento"])
        r["precio_m2_mediano_sector"] = r.get("precio_m2_mediano_sector", r["precio_m2_mediano_comuna"]).fillna(r["precio_m2_mediano_comuna"])
        r["precio_m2_p25_sector"] = r.get("precio_m2_p25_sector", r["precio_m2_mediano_sector"]).fillna(r["precio_m2_mediano_sector"])
        r["precio_m2_p75_sector"] = r.get("precio_m2_p75_sector", r["precio_m2_mediano_sector"]).fillna(r["precio_m2_mediano_sector"])
        
        r["fuente_factor"] = r.get("fuente_factor", pd.Series(gff, index=r.index)).fillna(gff)
        r["fuente_segmento_factor"] = r.get("fuente_segmento_factor", r["fuente_factor"]).fillna(r["fuente_factor"])

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

        # 4. Preparación de Features para el Booster
        feature_cols = bundle.get("feature_cols", [])
        if not feature_cols: raise ValueError("Bundle no contiene feature_cols.")

        for col in feature_cols:
            if col not in r.columns:
                if col in ["tipo_inmueble", "estado_inmueble", "fuente", "city_token", "texto_completo"]:
                    r[col] = "desconocido" if col != "texto_completo" else ""
                else:
                    r[col] = 0.0

        X = r[feature_cols].copy()

        # 5. Predicción Safe-Load
        preprocessor = None
        preprocessor_blob = bundle.get("preprocessor_pickle")
        if preprocessor_blob:
            try:
                import base64
                # Manejar formato Base64 (común en bundles JSON) o raw bytes
                if isinstance(preprocessor_blob, str):
                    try:
                        preprocessor_blob = base64.b64decode(preprocessor_blob)
                    except:
                        preprocessor_blob = preprocessor_blob.encode('latin1')
                preprocessor = pickle.loads(preprocessor_blob)
            except Exception as e:
                unpickle_error = str(e)
                print(f"Error unpickling preprocessor: {e}")
                pass
        
        if preprocessor is None:
            old_pipe = bundle.get("model")
            if hasattr(old_pipe, 'named_steps'): preprocessor = old_pipe.named_steps.get('preprocessor')

        model_json = bundle.get("model_json")
        if model_json:
            bst = xgb.Booster()
            if isinstance(model_json, str):
                try:
                    import base64
                    model_json = base64.b64decode(model_json)
                except:
                    model_json = model_json.encode('utf-8')
            bst.load_model(bytearray(model_json))
            
            # Intento de recuperación final del preprocesador
            if not preprocessor:
                # Buscar en otras llaves posibles
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
            precio_pred = np.expm1(precio_pred) # Inverse Log
            
            if strategy == "residual":
                precio_pred = precio_pred * r["precio_estimado_segmento_area_ajustado"].fillna(gpm).to_numpy()
        else:
            pipeline = bundle.get("model")
            if not pipeline: raise ValueError("No hay modelo disponible.")
            precio_pred = pipeline.predict(X)
            if strategy == "residual" or "Residual" in str(type(pipeline)): # v8 unified fallback
                precio_pred = precio_pred * r["precio_estimado_segmento_area_ajustado"].fillna(gpm).to_numpy()

        df["precio_predicho"] = precio_pred
        df["rentabilidad_potencial"] = (
            (df["precio_predicho"] - df["precio_num"]) / df["precio_num"].replace(0, np.nan) * 100
        ).replace([np.inf, -np.inf], 0).fillna(0).round(1)

        mape_modelo = bundle.get("metrics", {}).get("mape", 20.0)
        signal_threshold = max(12.0, min(25.0, float(mape_modelo) * 0.75))
        df["estado_inversion"] = df["rentabilidad_potencial"].apply(
            lambda x: "Oportunidad" if x > signal_threshold else ("Sobrevalorado" if x < -signal_threshold else "En mercado")
        )

    except Exception as e:
        traceback.print_exc()
        df["precio_predicho"] = np.nan
        df["estado_inversion"] = f"Error Crítico: {str(e)[:50]}"

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
        "estado": r["estado_inversion"]
    }
