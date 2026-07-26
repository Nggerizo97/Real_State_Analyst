# === ARCHIVO GENERADO — NO EDITAR AQUI ===
# Origen: lakehouse-repo/src/quality/listing_lifecycle.py
# Regenerar: python tools/sync_geo_to_app.py
# -*- coding: utf-8 -*-
"""Ciclo de vida del aviso: distinguir lo que sigue publicado de lo que ya no.

Problema que resuelve
---------------------
El pipeline conserva todo el historico, lo cual es correcto para entrenar el
modelo, pero la app mostraba las 74.784 filas sin filtrar. Solo el 8,3% de
esas filas se habia visto en los ultimos 14 dias del crawl de su propio
portal; la mediana llevaba ~100 dias sin aparecer. De ahi los enlaces que
abren en "Este inmueble no esta disponible".

Por que no basta con el codigo HTTP
-----------------------------------
Se midio sobre la tabla real: los portales devuelven 200 OK para avisos ya
retirados y sirven una pagina de "no disponible". Sobre 250 URLs muestreadas,
el estado HTTP casi nunca fue 404; el marcador util es el cuerpo de la pagina.

Senal principal: recencia relativa al portal
--------------------------------------------
Se compara la fecha del aviso contra el ULTIMO CRAWL DE SU PROPIO PORTAL, no
contra hoy. Si el scraper de mercadolibre lleva 56 dias detenido, sus avisos
no deben marcarse como retirados: nadie ha ido a mirar. Lo que indica que un
aviso desaparecio es que el portal SI se recorrio despues y el aviso no
volvio a salir.

Tasa de avisos muertos medida por antiguedad (muestra de 50 por rango):
      0-7 dias   ->   4%
      8-30 dias  ->  18%
     31-60 dias  ->  30%
     61-90 dias  ->  24%
       91+ dias  ->  36%
"""

ACTIVE_WINDOW_DAYS = 30      # se muestra en la app
REVIEW_WINDOW_DAYS = 60      # zona gris, se verifica por URL
RETIRED_WINDOW_DAYS = 90     # se conserva solo para el modelo

STATUS_ACTIVA = "activa"
STATUS_EN_REVISION = "en_revision"
STATUS_PROBABLE_RETIRADA = "probable_retirada"
STATUS_RETIRADA = "retirada"

URL_VIVA = "viva"
URL_MUERTA = "muerta"
URL_SIN_VERIFICAR = "sin_verificar"

LIFECYCLE_COLUMNS = [
    "portal_last_crawl_at",
    "dias_sin_ver",
    "listing_status",
    "is_active",
]


def classify_status(dias_sin_ver, url_status=None):
    """Estado del aviso a partir de su antiguedad relativa y la verificacion.

    Una URL verificada como muerta manda sobre la antiguedad: es evidencia
    directa, no inferencia.
    """
    if url_status == URL_MUERTA:
        return STATUS_RETIRADA
    if dias_sin_ver is None:
        return STATUS_EN_REVISION
    if dias_sin_ver <= ACTIVE_WINDOW_DAYS:
        return STATUS_ACTIVA
    if dias_sin_ver <= REVIEW_WINDOW_DAYS:
        return STATUS_EN_REVISION
    if dias_sin_ver <= RETIRED_WINDOW_DAYS:
        return STATUS_PROBABLE_RETIRADA
    return STATUS_RETIRADA


# ══════════════════════════════════════════════════════════════════
# pandas — usado por la app y por el scoring local
# ══════════════════════════════════════════════════════════════════

def add_lifecycle_columns(df, fecha_col="fecha_extraccion", portal_col="fuente",
                          url_status_col=None):
    """Agrega las columnas de ciclo de vida a un DataFrame de pandas."""
    import numpy as np
    import pandas as pd

    result = df.copy()
    seen_at = pd.to_datetime(result[fecha_col], errors="coerce")
    result["last_seen_at"] = seen_at

    portal_last_crawl = seen_at.groupby(result[portal_col]).transform("max")
    result["portal_last_crawl_at"] = portal_last_crawl
    result["dias_sin_ver"] = (portal_last_crawl - seen_at).dt.days

    url_status = (
        result[url_status_col]
        if url_status_col and url_status_col in result.columns
        else pd.Series(URL_SIN_VERIFICAR, index=result.index)
    )
    result["listing_status"] = [
        classify_status(None if pd.isna(dias) else int(dias), status)
        for dias, status in zip(result["dias_sin_ver"], url_status)
    ]
    result["is_active"] = result["listing_status"] == STATUS_ACTIVA
    result["dias_sin_ver"] = result["dias_sin_ver"].astype("float").replace({np.nan: None})
    return result


# ══════════════════════════════════════════════════════════════════
# Spark — usado por los notebooks de Silver y Gold
# ══════════════════════════════════════════════════════════════════

def add_lifecycle_columns_spark(df, fecha_col="fecha_extraccion",
                                portal_col="fuente", url_status_col=None):
    """Version Spark, sin UDFs: todo se expresa como Window + when."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    portal_window = Window.partitionBy(portal_col)

    result = (
        df
        .withColumn("last_seen_at", F.col(fecha_col).cast("timestamp"))
        .withColumn("portal_last_crawl_at", F.max(F.col(fecha_col).cast("timestamp")).over(portal_window))
        .withColumn(
            "dias_sin_ver",
            F.datediff(F.col("portal_last_crawl_at"), F.col("last_seen_at")),
        )
    )

    url_status = (
        F.coalesce(F.col(url_status_col), F.lit(URL_SIN_VERIFICAR))
        if url_status_col and url_status_col in result.columns
        else F.lit(URL_SIN_VERIFICAR)
    )

    result = (
        result
        .withColumn(
            "listing_status",
            F.when(url_status == F.lit(URL_MUERTA), F.lit(STATUS_RETIRADA))
            .when(F.col("dias_sin_ver").isNull(), F.lit(STATUS_EN_REVISION))
            .when(F.col("dias_sin_ver") <= ACTIVE_WINDOW_DAYS, F.lit(STATUS_ACTIVA))
            .when(F.col("dias_sin_ver") <= REVIEW_WINDOW_DAYS, F.lit(STATUS_EN_REVISION))
            .when(F.col("dias_sin_ver") <= RETIRED_WINDOW_DAYS, F.lit(STATUS_PROBABLE_RETIRADA))
            .otherwise(F.lit(STATUS_RETIRADA)),
        )
        .withColumn("is_active", F.col("listing_status") == F.lit(STATUS_ACTIVA))
    )
    return result
