import pandas as pd
import numpy as np
import streamlit as st

def score_dataframe(df: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Aplica el bundle_v3 al dataframe completo."""
    if bundle is None:
        df["precio_predicho"] = df["precio_num"]
        df["rentabilidad_potencial"] = 0.0
        df["estado_inversion"] = "Sin modelo"
        return df

    pipeline = bundle.get("model")
    strategy = bundle.get("strategy", "absolute")
    city_stats = bundle.get("city_stats")
    segment_stats = bundle.get("segment_stats")
    fuente_ratio_stats = bundle.get("fuente_ratio_stats")
    fuente_segmento_ratio_stats = bundle.get("fuente_segmento_ratio_stats")
    market_meta = bundle.get("market_meta", {})

    try:
        df_pred = df.copy()

        for col in ["area_m2", "habitaciones", "banos", "garajes", "num_portales",
                    "dispersion_pct_grupo", "precio_desviacion_grupo_pct", "data_completeness"]:
            if col not in df_pred.columns:
                df_pred[col] = np.nan
            df_pred[col] = pd.to_numeric(df_pred[col], errors="coerce")

        # --- CAPPING INTELIGENTE (Guardrails de Inferencia) ---
        if "area_m2" in df_pred.columns:
            m_area = df_pred["area_m2"]
            if "banos" in df_pred.columns:
                # Si area > 120m2, se esperan al menos 2 baños. Si area > 180m2, al menos 3
                min_banos = np.where(m_area > 180, 3, np.where(m_area > 120, 2, 1))
                df_pred["banos"] = np.maximum(df_pred["banos"].fillna(1), min_banos)
            if "garajes" in df_pred.columns:
                # Si area > 150m2, se espera al menos 1 garaje. Si area > 200m2, al menos 2
                min_gar = np.where(m_area > 200, 2, np.where(m_area > 150, 1, 0))
                df_pred["garajes"] = np.maximum(df_pred["garajes"].fillna(0), min_gar)
        # ------------------------------------------------------

        for col, default in [
            ("tipo_inmueble", "otro"),
            ("estado_inmueble", "desconocido"),
            ("fuente", "desconocido"),
            ("city_token", "otra_ciudad"),
        ]:
            if col not in df_pred.columns:
                df_pred[col] = default
            df_pred[col] = df_pred[col].fillna(default).astype(str)

        text_cols = [c for c in ["ubicacion_norm", "ubicacion_clean", "titulo"] if c in df_pred.columns]
        if text_cols:
            df_pred["texto_completo"] = df_pred[text_cols].fillna("").agg(" ".join, axis=1)
        else:
            df_pred["texto_completo"] = ""

        df_pred["market_segment"] = (
            df_pred["city_token"].astype(str) + "__" + df_pred["tipo_inmueble"].astype(str)
        )
        df_pred["habitaciones_bucket"] = (
            df_pred["habitaciones"].fillna(-1).clip(-1, 6)
        )
        df_pred["log_area_m2"] = np.log1p(df_pred["area_m2"].clip(lower=0))
        df_pred["banos_por_habitacion"] = (
            df_pred["banos"] / df_pred["habitaciones"].replace(0, np.nan)
        ).replace([np.inf, -np.inf], np.nan)

        if city_stats is not None and not city_stats.empty and "city_token" in city_stats.columns:
            df_pred = df_pred.merge(city_stats, on="city_token", how="left")

        if segment_stats is not None and not segment_stats.empty and "market_segment" in segment_stats.columns:
            df_pred = df_pred.merge(segment_stats, on="market_segment", how="left")

        # hab_stats fallback
        hab_stats = bundle.get("hab_stats")
        if hab_stats is not None and not hab_stats.empty:
            df_pred = df_pred.merge(hab_stats, on=["city_token", "habitaciones_bucket"], how="left")

        if fuente_ratio_stats is not None and not fuente_ratio_stats.empty and "fuente" in fuente_ratio_stats.columns:
            df_pred = df_pred.merge(fuente_ratio_stats, on="fuente", how="left")

        if (
            fuente_segmento_ratio_stats is not None
            and not fuente_segmento_ratio_stats.empty
            and all(c in fuente_segmento_ratio_stats.columns for c in ["fuente", "market_segment"])
        ):
            df_pred = df_pred.merge(
                fuente_segmento_ratio_stats,
                on=["fuente", "market_segment"],
                how="left",
            )

        global_price_median = market_meta.get("global_price_median", float(df["precio_num"].median()))
        global_pm2_median = market_meta.get(
            "global_pm2_median",
            float((df["precio_num"] / df["area_m2"].replace(0, np.nan)).median())
        )
        global_area_median = market_meta.get("global_area_median", float(df["area_m2"].median()))
        global_fuente_factor = market_meta.get("global_fuente_factor", 1.0)

        for col in ["precio_mediano_ciudad", "precio_m2_mediano_ciudad", "area_mediana_ciudad",
                    "precio_m2_mediano_segmento", "precio_mediano_segmento", "precio_m2_mediano_habs",
                    "fuente_factor", "fuente_segmento_factor"]:
            if col not in df_pred.columns:
                df_pred[col] = np.nan

        df_pred["precio_mediano_ciudad"] = df_pred["precio_mediano_ciudad"].fillna(global_price_median)
        df_pred["precio_m2_mediano_ciudad"] = df_pred["precio_m2_mediano_ciudad"].fillna(global_pm2_median)
        df_pred["area_mediana_ciudad"] = df_pred["area_mediana_ciudad"].fillna(global_area_median)
        df_pred["precio_m2_mediano_segmento"] = df_pred["precio_m2_mediano_segmento"].fillna(
            df_pred["precio_m2_mediano_ciudad"]
        )
        df_pred["precio_mediano_segmento"] = df_pred["precio_mediano_segmento"].fillna(
            df_pred["precio_mediano_ciudad"]
        )
        df_pred["precio_m2_mediano_habs"] = df_pred["precio_m2_mediano_habs"].fillna(
            df_pred["precio_m2_mediano_segmento"]
        )
        df_pred["fuente_factor"] = df_pred["fuente_factor"].fillna(global_fuente_factor)
        df_pred["fuente_segmento_factor"] = df_pred["fuente_segmento_factor"].fillna(
            df_pred["fuente_factor"]
        )

        df_pred["precio_estimado_ciudad_area"] = df_pred["area_m2"] * df_pred["precio_m2_mediano_ciudad"]
        df_pred["precio_estimado_segmento_area"] = df_pred["area_m2"] * df_pred["precio_m2_mediano_segmento"]
        df_pred["precio_estimado_segmento_area_ajustado"] = (
            df_pred["precio_estimado_segmento_area"] * df_pred["fuente_segmento_factor"]
        ).fillna(df_pred["precio_estimado_segmento_area"]).fillna(df_pred["precio_estimado_ciudad_area"])

        df_pred["area_vs_ciudad_ratio"] = (
            df_pred["area_m2"] / df_pred["area_mediana_ciudad"].replace(0, np.nan)
        ).replace([np.inf, -np.inf], np.nan).fillna(1.0)

        df_pred["ajuste_fuente_pct"] = (df_pred["fuente_segmento_factor"] - 1.0) * 100.0

        feature_cols = bundle.get("feature_cols", [])
        if not feature_cols:
            feature_cols = [
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
            ]

        for col in feature_cols:
            if col not in df_pred.columns:
                if col in ["tipo_inmueble", "estado_inmueble", "fuente", "city_token", "texto_completo"]:
                    df_pred[col] = "desconocido" if col != "texto_completo" else ""
                else:
                    df_pred[col] = 0.0

        X = df_pred[feature_cols].copy()

        if strategy == "residual":
            pred_ratio = pipeline.predict(X)
            ratio_low = bundle.get("ratio_clip_low", 0.25)
            ratio_high = bundle.get("ratio_clip_high", 4.0)
            pred_ratio = np.clip(pred_ratio, ratio_low, ratio_high)
            baseline = df_pred["precio_estimado_segmento_area_ajustado"].fillna(global_price_median)
            precio_pred = pred_ratio * baseline.to_numpy()
        else:
            precio_pred = pipeline.predict(X)

        df["precio_predicho"] = precio_pred
        df["rentabilidad_potencial"] = (
            (df["precio_predicho"] - df["precio_num"]) / df["precio_num"] * 100
        ).replace([np.inf, -np.inf], 0).fillna(0).round(1)

        mape_modelo = bundle.get("metrics", {}).get("mape", 23.0)
        signal_threshold = max(12.0, min(20.0, float(mape_modelo) * 0.65))

        df["estado_inversion"] = df["rentabilidad_potencial"].apply(
            lambda x: "Oportunidad" if x > signal_threshold
            else ("Sobrevalorado" if x < -signal_threshold else "En mercado")
        )

    except Exception as e:
        df["precio_predicho"] = df["precio_num"]
        df["rentabilidad_potencial"] = 0.0
        df["estado_inversion"] = "Sin señal"

    return df

def score_single(row: dict, bundle: dict) -> dict:
    """Valora un único inmueble manualmente ingresado."""
    if bundle is None:
        return {"error": "Bundle no disponible"}
    
    df_temp = pd.DataFrame([row])
    # Asegurar columnas mínimas para evitar fallos en score_dataframe
    for col in ["precio_num", "num_portales", "dispersion_pct_grupo", 
                "precio_desviacion_grupo_pct", "data_completeness"]:
        if col not in df_temp.columns:
            df_temp[col] = 0.0
            
    scored = score_dataframe(df_temp, bundle)
    r = scored.iloc[0]
    
    mape_pct = bundle.get("metrics", {}).get("mape", 23.0)
    valor = float(r["precio_predicho"])
    
    return {
        "valor_predicho": valor,
        "precio_m2_pred": valor / max(1, row.get("area_m2", 1)),
        "rango_low": valor * (1 - mape_pct/100),
        "rango_high": valor * (1 + mape_pct/100),
        "mape_pct": mape_pct,
        "estado": r["estado_inversion"]
    }
