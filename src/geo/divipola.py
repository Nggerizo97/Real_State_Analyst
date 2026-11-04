# === ARCHIVO GENERADO — NO EDITAR AQUI ===
# Origen: lakehouse-repo/src/geo/divipola.py
# Regenerar: python tools/sync_geo_to_app.py
"""Construye la dimension geografica canonica a partir de DIVIPOLA (DANE).

Salida: tres tablas que se materializan en S3 y consume tanto el ETL como
la app de Streamlit.

    dim_departamento   33 filas    codigo, nombre oficial, region, lat/lon
    dim_municipio    1.122 filas   codigo DANE, nombre oficial, lat/lon,
                                   mercado comercial, prioridad de desempate
    dim_barrio     ~38.000 filas   cabeceras y centros poblados (DIVIPOLA),
                                   barrios y veredas oficiales de 4-72 con
                                   codigo postal, y la semilla manual de
                                   barrios; lat/lon y precision declarada

    dim_municipio_alias            todas las grafias -> cod_mpio (indice de
                                   busqueda que usa el resolver)

Frente al esquema anterior (52 city_token y 22 departamento_token derivados
de diccionarios escritos a mano) esto aporta cobertura nacional completa,
codigos DANE reales y coordenadas para el mapa.
"""

import os

import pandas as pd

from .aliases import (
    CODMPIO_TO_MARKET,
    DEFAULT_PRIORITY,
    DEPARTAMENTO_REGION,
    MARKET_MEMBER_PRIORITY,
    MUNICIPIO_ALIASES,
    MUNICIPIO_PRIORITY,
    ZONAS_ADMINISTRATIVAS,
)
from .normalize import normalize_text

DEFAULT_SOURCE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "reference",
    "mapeo_geografico_colombia.xlsx",
)

# Barrios y veredas oficiales de 4-72 (tools/build_barrios_postales.py).
CATALOGO_BARRIOS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "reference",
    "barrios_postales_4_72.csv",
)

# Prefijos honorificos que DIVIPOLA incluye y los portales omiten.
# 'San Jose de Cucuta' -> 'cucuta', 'Santiago de Cali' -> 'cali'.
# Se generan como alias ADICIONAL; el nombre completo siempre se conserva.
_HONORIFIC_PREFIXES = (
    "san jose de ",
    "santiago de ",
    "santa fe de ",
    "san juan de ",
    "san sebastian de ",
    "guadalajara de ",
    "villa de ",
    "puerto ",
    "ciudad ",
)

# Sufijos administrativos que DIVIPOLA anexa al nombre.
_ADMIN_SUFFIXES = (
    " d c",
    " distrito capital",
    " distrito especial",
    " distrito turistico y cultural",
    " distrito turistico cultural e historico",
)


def _strip_admin_suffix(name_norm: str) -> str:
    for suffix in _ADMIN_SUFFIXES:
        if name_norm.endswith(suffix):
            return name_norm[: -len(suffix)].strip()
    return name_norm


def _derive_aliases(official_name: str) -> set:
    """Alias que se pueden inferir mecanicamente del nombre oficial."""
    base = normalize_text(official_name)
    derived = {base}

    without_suffix = _strip_admin_suffix(base)
    if without_suffix and without_suffix != base:
        derived.add(without_suffix)

    for candidate in list(derived):
        for prefix in _HONORIFIC_PREFIXES:
            if candidate.startswith(prefix) and len(candidate) > len(prefix) + 3:
                derived.add(candidate[len(prefix):].strip())

    # 'Itagui' aparece con y sin dieresis; normalize_text ya lo resuelve.
    return {alias for alias in derived if len(alias) >= 3}


def load_source(path: str = None) -> dict:
    """Lee las hojas del libro DIVIPOLA."""
    path = path or DEFAULT_SOURCE
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"No se encontro la fuente DIVIPOLA en {path}. "
            "Copia reference/mapeo_geografico_colombia.xlsx al repo."
        )
    codes = {"cod_dpto": str, "cod_mpio": str, "cod_barrio": str}
    return {
        "mapeo": pd.read_excel(path, "mapeo", dtype=codes),
        "departamentos": pd.read_excel(path, "departamentos", dtype=codes),
        "municipios": pd.read_excel(path, "municipios", dtype=codes),
    }


def build_dim_departamento(sheets: dict) -> pd.DataFrame:
    df = sheets["departamentos"].copy()
    df["cod_dpto"] = df["cod_dpto"].str.zfill(2)
    df["departamento"] = df["departamento"].astype(str).str.strip()
    df["region"] = df["cod_dpto"].map(DEPARTAMENTO_REGION).fillna("otra")
    df["key_departamento"] = df["departamento"].map(normalize_text)
    df = df.rename(columns={"latitud": "dpto_lat", "longitud": "dpto_lon"})
    return df[
        ["cod_dpto", "departamento", "key_departamento", "region", "dpto_lat", "dpto_lon"]
    ].sort_values("cod_dpto").reset_index(drop=True)


def build_dim_municipio(sheets: dict) -> pd.DataFrame:
    df = sheets["municipios"].copy()
    df["cod_dpto"] = df["cod_dpto"].str.zfill(2)
    df["cod_mpio"] = df["cod_mpio"].str.zfill(5)
    df["departamento"] = df["departamento"].astype(str).str.strip()
    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["key_municipio"] = df["municipio"].map(normalize_text)
    df["region"] = df["cod_dpto"].map(DEPARTAMENTO_REGION).fillna("otra")
    df["market_token"] = df["cod_mpio"].map(CODMPIO_TO_MARKET).fillna("mercado_otro")

    # Prioridad: la explicita si existe; si no, un piso para los municipios
    # que pertenecen a un mercado comercial curado.
    #
    # Ese piso importa porque el resolver exige corroboracion por departamento
    # para alias cortos de un solo token, y sin el, municipios reales de 4
    # letras como Sopo, Nilo o Cota quedaban sin resolver cuando el aviso no
    # nombraba el departamento ("sopo incluye yerbabuena" -> otra_ciudad).
    # Al estar en MARKET_CATALOG son plazas revisadas a mano, asi que
    # aceptarlas sueltas no abre la puerta a falsos positivos.
    market_floor = df["cod_mpio"].map(
        lambda code: MARKET_MEMBER_PRIORITY if code in CODMPIO_TO_MARKET else DEFAULT_PRIORITY
    )
    explicit = df["cod_mpio"].map(MUNICIPIO_PRIORITY)
    df["priority"] = explicit.fillna(market_floor).astype(int)
    df["priority"] = df[["priority"]].join(market_floor.rename("floor")).max(axis=1).astype(int)
    # Capital departamental: en DIVIPOLA el codigo de capital termina en 001.
    df["es_capital"] = df["cod_mpio"].str.endswith("001")
    df = df.rename(columns={"latitud": "mpio_lat", "longitud": "mpio_lon"})
    return df[
        [
            "cod_dpto", "departamento", "cod_mpio", "municipio", "key_municipio",
            "region", "market_token", "priority", "es_capital", "mpio_lat", "mpio_lon",
        ]
    ].sort_values("cod_mpio").reset_index(drop=True)


def build_dim_municipio_alias(dim_municipio: pd.DataFrame) -> pd.DataFrame:
    """Indice alias normalizado -> cod_mpio, con su prioridad de desempate."""
    rows = []
    for record in dim_municipio.itertuples():
        aliases = _derive_aliases(record.municipio)
        aliases.update(
            normalize_text(alias) for alias in MUNICIPIO_ALIASES.get(record.cod_mpio, [])
        )
        for alias in aliases:
            if not alias:
                continue
            rows.append(
                {
                    "alias": alias,
                    "alias_tokens": len(alias.split()),
                    "cod_mpio": record.cod_mpio,
                    "cod_dpto": record.cod_dpto,
                    "priority": record.priority,
                }
            )
    df = pd.DataFrame(rows).drop_duplicates(subset=["alias", "cod_mpio"])
    return df.sort_values(
        ["alias_tokens", "priority"], ascending=[False, False]
    ).reset_index(drop=True)


def _nivel_mapeo(record) -> str:
    if record.tipo == "Centro poblado":
        return "centro_poblado"
    if record.tipo == "Cabecera municipal":
        return "cabecera"
    if record.key_barrio in ZONAS_ADMINISTRATIVAS.get(record.cod_mpio, ()):
        return "comuna"
    return "barrio"


def _filas_catalogo(catalogo: pd.DataFrame) -> pd.DataFrame:
    """Catalogo 4-72 (tools/build_barrios_postales.py) con el esquema de dim_barrio.

    barrio_zona queda en la localidad (Bogota) o en la zona postal: 4-72 no
    publica nombres de comuna y el codigo postal agrupa barrios vecinos en
    todo el pais.
    """
    df = catalogo.copy()
    df["cod_mpio"] = df["cod_mpio"].astype(str).str.zfill(5)
    df["cod_dpto"] = df["cod_mpio"].str[:2]
    zona_postal = "Zona postal " + df["codigo_postal"].fillna("")
    df["zona"] = df["localidad"].where(df["localidad"].notna(), zona_postal.where(df["codigo_postal"].notna()))
    df["barrio"] = df["nombre"].astype(str).str.strip()
    df["cod_barrio"] = None  # cod_barrio es solo para codigos DANE
    df["tipo"] = df["nivel"].map({"barrio": "Barrio urbano", "vereda": "Vereda"})
    df["precision"] = df["lat"].notna().map({True: "oficial_4_72", False: "lista_4_72"})
    df = df.rename(columns={"lat": "latitud", "lon": "longitud"})
    return df[["cod_dpto", "cod_mpio", "barrio", "latitud", "longitud", "zona", "cod_barrio",
               "tipo", "precision", "nivel", "codigo_postal"]]


# Cuando un nombre aparece en varias fuentes del mismo municipio se conserva
# una sola fila, en este orden: barrio oficial 4-72 (poligono y codigo postal),
# barrio de la semilla manual (nombres comerciales como Provenza), centro
# poblado o cabecera DIVIPOLA (codigo DANE), vereda 4-72, comuna de la semilla.
_PRIORIDAD_FUENTE = {
    ("barrio", "4-72"): 0, ("barrio", "semilla"): 1, ("centro_poblado", "divipola"): 2,
    ("cabecera", "divipola"): 2, ("vereda", "4-72"): 3, ("comuna", "semilla"): 4,
}


# Distancia maxima para heredar el codigo postal del barrio oficial vecino.
MAX_KM_CODIGO_POSTAL = 1.5


def _completar_codigo_postal(df: pd.DataFrame, max_km: float = MAX_KM_CODIGO_POSTAL) -> pd.DataFrame:
    """Codigo postal para los barrios de la semilla manual (Provenza, Rosales...).

    4-72 no los publica porque son nombres comerciales o subsectores. Se toma
    el del barrio oficial 4-72 mas cercano del mismo municipio, si esta a
    menos de max_km; las coordenadas de la semilla son aproximadas, asi que
    mas lejos no se asigna. Las comunas no reciben codigo: abarcan varios.
    """
    import numpy as np

    df = df.copy()
    lat = pd.to_numeric(df["latitud"], errors="coerce")
    lon = pd.to_numeric(df["longitud"], errors="coerce")
    referencia = (df["origen"] == "4-72") & (df["nivel"] == "barrio") & df["codigo_postal"].notna() & lat.notna()
    faltan = (df["origen"] == "semilla") & (df["nivel"] == "barrio") & df["codigo_postal"].isna() & lat.notna()
    for cod_mpio in df.loc[faltan, "cod_mpio"].unique():
        ref = referencia & (df["cod_mpio"] == cod_mpio)
        if not ref.any():
            continue
        objetivo = faltan & (df["cod_mpio"] == cod_mpio)
        coseno = np.cos(np.radians(lat[objetivo].to_numpy()))[:, None]
        dx = (lon[objetivo].to_numpy()[:, None] - lon[ref].to_numpy()[None, :]) * 111.32 * coseno
        dy = (lat[objetivo].to_numpy()[:, None] - lat[ref].to_numpy()[None, :]) * 110.57
        distancia = np.hypot(dx, dy)
        cercano = distancia.argmin(axis=1)
        codigos = df.loc[ref, "codigo_postal"].to_numpy()[cercano]
        df.loc[objetivo, "codigo_postal"] = np.where(distancia.min(axis=1) <= max_km, codigos, None)
    return df


def build_dim_barrio(sheets: dict, dim_municipio: pd.DataFrame, catalogo: pd.DataFrame = None) -> pd.DataFrame:
    df = sheets["mapeo"].copy()
    df["cod_dpto"] = df["cod_dpto"].astype(str).str.zfill(2)
    df["cod_mpio"] = df["cod_mpio"].astype(str).str.zfill(5)
    df["barrio"] = df["barrio"].astype(str).str.strip()
    df["key_barrio"] = df["barrio"].map(normalize_text)
    df["nivel"] = [_nivel_mapeo(r) for r in df.itertuples()]
    df["origen"] = df["tipo"].map({"Barrio urbano": "semilla"}).fillna("divipola")
    df["codigo_postal"] = None

    if catalogo is not None and not catalogo.empty:
        extra = _filas_catalogo(catalogo)
        extra["key_barrio"] = extra["barrio"].map(normalize_text)
        extra["origen"] = "4-72"
        df = pd.concat([df, extra], ignore_index=True)
        df["_prioridad"] = [_PRIORIDAD_FUENTE.get((n, o), 5) for n, o in zip(df["nivel"], df["origen"])]
        df = (df.sort_values(["cod_mpio", "key_barrio", "_prioridad"])
                .drop_duplicates(["cod_mpio", "key_barrio"]).drop(columns="_prioridad"))
        df = _completar_codigo_postal(df)

    df["barrio_tokens"] = df["key_barrio"].str.split().str.len().fillna(0).astype(int)
    df = df.rename(columns={"latitud": "barrio_lat", "longitud": "barrio_lon"})

    # Alinear nombres de municipio/departamento con dim_municipio (fuente unica).
    df = df.drop(columns=["departamento", "municipio"], errors="ignore").merge(
        dim_municipio[["cod_mpio", "cod_dpto", "departamento", "municipio", "market_token", "region"]],
        on=["cod_mpio", "cod_dpto"],
        how="inner",
    )

    df = df[df["key_barrio"].str.len() >= 3]
    return df[
        [
            "cod_dpto", "departamento", "cod_mpio", "municipio", "market_token", "region",
            "cod_barrio", "barrio", "key_barrio", "barrio_tokens", "zona", "tipo",
            "precision", "barrio_lat", "barrio_lon", "nivel", "codigo_postal",
        ]
    ].sort_values(["cod_mpio", "barrio"]).reset_index(drop=True)


DIM_NAMES = ("dim_departamento", "dim_municipio", "dim_municipio_alias", "dim_barrio")


def load_catalogo_barrios(path: str = CATALOGO_BARRIOS):
    """Catalogo 4-72 versionado en el repo, o None si no esta."""
    if not os.path.isfile(path):
        return None
    return pd.read_csv(path, dtype={"cod_mpio": str, "codigo_postal": str, "localidad": str})


def build_all(path: str = None) -> dict:
    """Construye la dimension desde el libro DIVIPOLA y el catalogo 4-72."""
    sheets = load_source(path)
    dim_departamento = build_dim_departamento(sheets)
    dim_municipio = build_dim_municipio(sheets)
    return {
        "dim_departamento": dim_departamento,
        "dim_municipio": dim_municipio,
        "dim_municipio_alias": build_dim_municipio_alias(dim_municipio),
        "dim_barrio": build_dim_barrio(sheets, dim_municipio, load_catalogo_barrios()),
    }


def load_from_parquet_dir(directory: str) -> dict:
    """Carga la dimension ya materializada (parquet).

    Es la via preferida en Databricks: evita depender de openpyxl y del
    libro de Excel dentro del cluster, y garantiza que ETL y app usen
    exactamente la misma version de la dimension.
    """
    dims = {}
    for name in DIM_NAMES:
        candidate = os.path.join(directory, f"{name}.parquet")
        if not os.path.isfile(candidate):
            raise FileNotFoundError(f"Falta {candidate}")
        dims[name] = pd.read_parquet(candidate)
    return dims


def load_dims(source: str = None) -> dict:
    """Carga la dimension desde un directorio de parquet o desde el Excel.

    Orden: directorio parquet indicado -> reference/geo/ local -> Excel.
    """
    if source and os.path.isdir(source):
        return load_from_parquet_dir(source)
    if source and source.endswith((".xlsx", ".xls")):
        return build_all(source)

    default_parquet_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "reference",
        "geo",
    )
    if os.path.isdir(default_parquet_dir):
        try:
            return load_from_parquet_dir(default_parquet_dir)
        except FileNotFoundError:
            pass
    return build_all(source)
