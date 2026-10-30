# === ARCHIVO GENERADO — NO EDITAR AQUI ===
# Origen: lakehouse-repo/src/geo/normalize.py
# Regenerar: python tools/sync_geo_to_app.py
"""Normalizacion de texto compartida por todo el stack geografico.

Un solo lugar define que significa "texto normalizado". Silver, Gold, el
resolver y la app de Streamlit importan de aqui, de modo que un cambio de
reglas no puede desincronizar las capas.
"""

import re
import unicodedata

_ACCENT_RE = re.compile(r"[̀-ͯ]")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]")
_SPACE_RE = re.compile(r"\s+")

# Ruido que los portales anteponen o intercalan en el campo de ubicacion.
#
# OJO: "colombia" NO se elimina aqui. Al quitarlo, "Puerto Colombia" (08573)
# quedaba reducido a "puerto" y dejaba de resolver: ~500 avisos del area
# metropolitana de Barranquilla caian en otra_ciudad ("villa campestre
# puerto", "puerto puerto", "punta roca sabanilla puerto").
# El municipio homonimo "Colombia" (Huila) ya esta neutralizado por
# ALIAS_BLOCKLIST en geo_resolver, y como palabra suelta la filtra
# GEO_GENERIC_STOPWORDS al construir el nombre de barrio.
_LOCATION_NOISE = (
    "distrito capital",
    "distrito especial",
    "area metropolitana",
    "departamento de",
    "departamento",
    "municipio de",
    "municipio",
)


def normalize_text(text) -> str:
    """minusculas, sin acentos, sin puntuacion, espacios colapsados.

    'Bogotá, D.C. - Chapinero' -> 'bogota d c chapinero'
    """
    if text is None:
        return ""
    text = str(text).lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = _ACCENT_RE.sub("", text)
    text = _NON_ALNUM_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def normalize_location(text) -> str:
    """normalize_text mas la limpieza de ruido propia de campos de ubicacion.

    Colapsa ademas 'd c' suelto, que es como queda 'D.C.' tras normalizar y
    que ensuciaba los nombres de barrio en la version anterior del pipeline.
    """
    normalized = normalize_text(text)
    if not normalized:
        return ""
    for noise in _LOCATION_NOISE:
        normalized = normalized.replace(noise, " ")
    normalized = re.sub(r"\bd\s+c\b", " ", normalized)
    normalized = re.sub(r"\bdc\b", " ", normalized)
    return _SPACE_RE.sub(" ", normalized).strip()


def tokens(text) -> list:
    """Lista de tokens normalizados."""
    normalized = normalize_text(text)
    return normalized.split() if normalized else []


def contains_phrase(haystack_norm: str, needle_norm: str) -> bool:
    """True si needle aparece en haystack respetando limites de palabra.

    Esta es la correccion central frente al pipeline anterior, que usaba
    `keyword in location` y por eso hacia match de 'en' dentro de 'medellin'
    o de 'cali' dentro de 'calima'.
    """
    if not haystack_norm or not needle_norm:
        return False
    return f" {needle_norm} " in f" {haystack_norm} "


# Marcadores de "sin dato" que llegan como texto y no como null: Gold
# escribe el barrio ausente como "None" y el Gold puntuado como "".
BLANK_MARKERS = frozenset({"", "nan", "none", "null", "<na>", "nat"})

# Categoria de las filas sin barrio o con un barrio sin masa suficiente.
BARRIO_OTRO = "barrio_otro"


def barrio_key(city_token, barrio) -> str:
    """Llave de barrio que ven el modelo y sus estadisticas.

    Es la UNICA definicion de la llave: el entrenamiento (04), la inferencia
    batch (05) y la app de Streamlit la importan de aqui. Antes cada capa
    armaba su propio f"{ciudad} | {barrio}" con el texto tal cual, y la app
    ademas capitalizaba los barrios de texto libre ("fonqueta" -> "Fonqueta").
    Una llave que no coincide con la del entrenamiento no da error: el
    OneHotEncoder(handle_unknown="ignore") la convierte en ceros en silencio.

    Ciudad y barrio se normalizan igual que `key_barrio` de dim_barrio:
    ('Medellín', 'La Candelaria (Centro)') -> 'medellin | la candelaria centro'.
    """
    if barrio is None or str(barrio).strip().lower() in BLANK_MARKERS:
        return BARRIO_OTRO
    barrio_norm = normalize_text(barrio)
    city_norm = normalize_text(city_token)
    if not barrio_norm or not city_norm or city_norm in BLANK_MARKERS:
        return BARRIO_OTRO
    return f"{city_norm} | {barrio_norm}"
