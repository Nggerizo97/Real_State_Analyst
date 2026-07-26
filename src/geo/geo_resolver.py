# === ARCHIVO GENERADO — NO EDITAR AQUI ===
# Origen: lakehouse-repo/src/geo/geo_resolver.py
# Regenerar: python tools/sync_geo_to_app.py
"""Resolver geografico unico para todo el stack.

Reemplaza cuatro catalogos que se habian desincronizado entre si:
    src/geo/geography_catalog.py            (ETL, 52 ciudades a mano)
    src/geo/sector_mapping.py               (ETL, keywords de barrio)
    GEOGRAPHY_CATALOG_FALLBACK              (copia embebida en 02_Silver)
    Real_State_Analyst/src/utils/geo_utils.py  (app, otro diccionario mas)

Ahora hay un solo resolver, respaldado por DIVIPOLA, que devuelve codigos
DANE, nombres oficiales y coordenadas.

Correcciones frente a la version anterior
-----------------------------------------
* Match por limite de palabra en vez de subcadena. Antes `"en" in location`
  hacia match dentro de 'medellin' y producia sector_mercado='en' en 10.972
  filas, 'd c en' en 3.899 y 'en en' en 1.221.
* El barrio solo se emite si corresponde a un registro real de DIVIPOLA.
  El texto libre sobrante viaja aparte en `barrio_texto`, ya filtrado con
  stopwords en espanol, de modo que nunca se muestra basura como barrio.
* Homonimos por prioridad y por departamento mencionado, en vez de las
  reglas rlike escritas a mano dentro del notebook de Gold.
* Salida con lat/lon, que habilita mapas reales en la app.

Uso en Python puro (app, scoring, tests):
    from src.geo.geo_resolver import get_resolver
    get_resolver().resolve("Apartamento en El Poblado, Medellin")

Uso en Spark (notebooks de Databricks):
    from src.geo.geo_resolver import build_spark_udf
    resolve_udf = build_spark_udf(spark)
    df.withColumn("geo", resolve_udf(F.col("ubicacion_raw"), F.col("titulo")))
"""

from functools import lru_cache

from .aliases import SECTOR_STOPWORDS
from .normalize import normalize_location, normalize_text

# Un alias de un solo token y muy corto ('toro', 'hato', 'rico', 'une')
# es casi siempre un falso positivo dentro de una descripcion inmobiliaria.
# Se acepta solo si el texto tambien nombra su departamento, o si el
# municipio es un mercado relevante ('cali', 'chia', 'cota').
MIN_STANDALONE_ALIAS_LEN = 5
STANDALONE_PRIORITY_FLOOR = 50

# Alias que jamas deben resolver municipio: aparecen en casi toda direccion.
ALIAS_BLOCKLIST = {"colombia"}

MATCH_BARRIO = "barrio"
MATCH_MUNICIPIO = "municipio"
MATCH_DEPARTAMENTO = "departamento"
MATCH_NONE = "sin_match"

EMPTY_RESULT = {
    "cod_dpto": None,
    "departamento": None,
    "cod_mpio": None,
    "municipio": None,
    "market_token": "mercado_otro",
    "region": None,
    "cod_barrio": None,
    "barrio": None,
    "barrio_zona": None,
    "barrio_precision": None,
    "barrio_texto": None,
    "lat": None,
    "lon": None,
    "geo_source": None,
    "geo_match_level": MATCH_NONE,
    "geo_confidence": 0,
}

RESULT_FIELDS = list(EMPTY_RESULT.keys())


def _phrase_positions(text_tokens, phrase_tokens):
    """Posiciones donde phrase_tokens aparece completo dentro de text_tokens."""
    span = len(phrase_tokens)
    if span == 0 or span > len(text_tokens):
        return []
    return [
        i
        for i in range(len(text_tokens) - span + 1)
        if text_tokens[i:i + span] == phrase_tokens
    ]


class GeoResolver:
    """Indexa la dimension DIVIPOLA y resuelve texto libre a geografia."""

    def __init__(self, dims: dict):
        self.dim_departamento = dims["dim_departamento"]
        self.dim_municipio = dims["dim_municipio"]
        self.dim_barrio = dims["dim_barrio"]
        self._build_indexes(dims["dim_municipio_alias"])

    # ── construccion de indices ───────────────────────────────────
    def _build_indexes(self, dim_alias):
        # Municipio por codigo
        self.municipio_by_code = {
            record.cod_mpio: {
                "cod_dpto": record.cod_dpto,
                "departamento": record.departamento,
                "cod_mpio": record.cod_mpio,
                "municipio": record.municipio,
                "market_token": record.market_token,
                "region": record.region,
                "priority": int(record.priority),
                "lat": float(record.mpio_lat) if record.mpio_lat == record.mpio_lat else None,
                "lon": float(record.mpio_lon) if record.mpio_lon == record.mpio_lon else None,
            }
            for record in self.dim_municipio.itertuples()
        }

        # Departamento indexado por su primer token, para match por frase.
        self.departamento_index = {}
        self.departamento_keys = set()
        for record in self.dim_departamento.itertuples():
            key = record.key_departamento
            # 'bogota d c' como departamento se maneja via municipio 11001.
            phrase = tuple(key.split())
            self.departamento_index.setdefault(phrase[0], []).append(
                (phrase, record.cod_dpto, record.departamento, record.region)
            )
            self.departamento_keys.add(key)
        for bucket in self.departamento_index.values():
            bucket.sort(key=lambda item: -len(item[0]))

        # Alias de municipio indexados por primer token.
        self.alias_index = {}
        self.aliases_by_code = {}
        for record in dim_alias.itertuples():
            alias = record.alias
            if alias in ALIAS_BLOCKLIST:
                continue
            phrase = tuple(alias.split())
            self.alias_index.setdefault(phrase[0], []).append(
                (phrase, record.cod_mpio, int(record.priority))
            )
            self.aliases_by_code.setdefault(record.cod_mpio, []).append(phrase)
        for bucket in self.alias_index.values():
            bucket.sort(key=lambda item: (-len(item[0]), -item[2]))

        # Barrios indexados por municipio y primer token.
        self.barrio_index = {}
        for record in self.dim_barrio.itertuples():
            phrase = tuple(record.key_barrio.split())
            if not phrase:
                continue
            bucket = self.barrio_index.setdefault(record.cod_mpio, {})
            bucket.setdefault(phrase[0], []).append(
                (
                    phrase,
                    record.barrio,
                    record.cod_barrio,
                    record.zona,
                    record.precision,
                    float(record.barrio_lat) if record.barrio_lat == record.barrio_lat else None,
                    float(record.barrio_lon) if record.barrio_lon == record.barrio_lon else None,
                )
            )
        for municipio_bucket in self.barrio_index.values():
            for token_bucket in municipio_bucket.values():
                token_bucket.sort(key=lambda item: -len(item[0]))

    # ── deteccion por capas ───────────────────────────────────────
    def _find_departamentos(self, text_tokens):
        found = {}
        for position, token in enumerate(text_tokens):
            for phrase, cod_dpto, nombre, region in self.departamento_index.get(token, []):
                if tuple(text_tokens[position:position + len(phrase)]) == phrase:
                    found.setdefault(
                        cod_dpto,
                        {
                            "cod_dpto": cod_dpto,
                            "departamento": nombre,
                            "region": region,
                            "span": (position, position + len(phrase)),
                        },
                    )
                    break
        return found

    def _find_municipios(self, text_tokens, dept_codes, ubicacion_len=None):
        if ubicacion_len is None:
            ubicacion_len = len(text_tokens)
        candidates = []
        for position, token in enumerate(text_tokens):
            # Varios municipios comparten la misma grafia ('Rionegro' existe en
            # Antioquia y en Santander). Hay que emitirlos TODOS como candidatos
            # y dejar que _pick_municipio desempate; si se corta en el primero,
            # gana siempre el de mayor prioridad y la corroboracion por
            # departamento nunca llega a aplicarse.
            best_span = 0
            matches_here = []
            for phrase, cod_mpio, priority in self.alias_index.get(token, []):
                if len(phrase) < best_span:
                    break  # el indice viene ordenado por longitud descendente
                if tuple(text_tokens[position:position + len(phrase)]) != phrase:
                    continue

                dept_of_municipio = cod_mpio[:2]
                corroborated = dept_of_municipio in dept_codes

                # Guarda contra falsos positivos de alias corto de un token
                # ('toro', 'hato', 'rico' aparecen en cualquier descripcion).
                standalone_ok = (
                    len(phrase) > 1
                    or len(" ".join(phrase)) >= MIN_STANDALONE_ALIAS_LEN
                    or priority >= STANDALONE_PRIORITY_FLOOR
                )
                if not standalone_ok and not corroborated:
                    continue

                best_span = len(phrase)
                matches_here.append(
                    {
                        "cod_mpio": cod_mpio,
                        "priority": priority,
                        "alias_tokens": len(phrase),
                        "corroborated": corroborated,
                        "in_ubicacion": position < ubicacion_len,
                        "span": (position, position + len(phrase)),
                    }
                )
            candidates.extend(
                match for match in matches_here if match["alias_tokens"] == best_span
            )
        return candidates

    @staticmethod
    def _pick_municipio(candidates):
        """Desempate entre municipios candidatos.

        El orden importa. La prioridad debe pesar mas que la longitud del
        alias: con el orden inverso, 'Bogota, Usaquen, San Antonio' resolvia
        a San Antonio (Tolima) porque 'san antonio' tiene dos tokens y
        'bogota' uno. Y el campo de ubicacion manda sobre el titulo, porque
        el titulo suele nombrar ciudades de referencia ('cerca a Bogota').
        """
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda item: (
                item["in_ubicacion"],
                item["corroborated"],
                item["priority"],
                item["alias_tokens"],
            ),
        )

    def _find_barrio(self, text_tokens, cod_mpio, consumed):
        municipio_bucket = self.barrio_index.get(cod_mpio)
        if not municipio_bucket:
            return None
        best = None
        for position, token in enumerate(text_tokens):
            if position in consumed:
                continue
            for entry in municipio_bucket.get(token, []):
                phrase = entry[0]
                end = position + len(phrase)
                if tuple(text_tokens[position:end]) != phrase:
                    continue
                if any(index in consumed for index in range(position, end)):
                    continue
                if best is None or len(phrase) > len(best[0][0]):
                    best = (entry, (position, end))
                break
        return best

    @staticmethod
    def _free_text_barrio(text_tokens, consumed):
        """Candidato de barrio en texto libre, ya depurado.

        Solo se usa cuando DIVIPOLA no tiene el barrio. Filtra stopwords en
        espanol, que es lo que faltaba y generaba 'en', 'd c en', 'no en'.
        Deduplica tokens repetidos: el texto de entrada concatena ubicacion y
        titulo, que suelen repetir el mismo nombre ('suramerica suramerica').
        """
        leftovers = []
        seen = set()
        for index, token in enumerate(text_tokens):
            if index in consumed or token in seen:
                continue
            if token in SECTOR_STOPWORDS or token.isdigit() or len(token) <= 2:
                continue
            seen.add(token)
            leftovers.append(token)
        if not leftovers:
            return None
        candidate = " ".join(leftovers[:3])
        return candidate if len(candidate) >= 3 else None

    # ── API publica ───────────────────────────────────────────────
    def resolve(self, ubicacion, titulo=None) -> dict:
        result = dict(EMPTY_RESULT)
        ubicacion_text = normalize_location(ubicacion)
        ubicacion_tokens = ubicacion_text.split()
        text_tokens = list(ubicacion_tokens)
        if titulo:
            text_tokens.extend(normalize_location(titulo).split())
        if not text_tokens:
            return result

        departamentos = self._find_departamentos(text_tokens)
        consumed = set()

        candidates = self._find_municipios(
            text_tokens, set(departamentos), len(ubicacion_tokens)
        )
        chosen = self._pick_municipio(candidates)

        # Todo tramo que parezca nombre de municipio es contexto geografico,
        # no nombre de barrio, aunque no sea el municipio elegido. Sin esto,
        # un aviso de Chia que menciona Bogota terminaba con barrio_texto
        # 'bogota bogota'. Los barrios que comparten nombre con un municipio
        # se siguen recuperando por la capa DIVIPOLA de barrios.
        context_spans = {
            index
            for candidate in candidates
            for index in range(*candidate["span"])
        }
        context_spans.update(
            index
            for info in departamentos.values()
            for index in range(*info["span"])
        )

        if chosen is None:
            # Sin municipio: al menos fijar el departamento si aparece.
            if departamentos:
                first = min(departamentos.values(), key=lambda item: item["span"][0])
                result.update(
                    cod_dpto=first["cod_dpto"],
                    departamento=first["departamento"],
                    region=first["region"],
                    geo_match_level=MATCH_DEPARTAMENTO,
                    geo_confidence=40,
                )
                for index in range(*first["span"]):
                    consumed.add(index)
                result["barrio_texto"] = self._free_text_barrio(text_tokens, consumed | context_spans)
            else:
                result["barrio_texto"] = self._free_text_barrio(text_tokens, consumed | context_spans)
            return result

        municipio = self.municipio_by_code[chosen["cod_mpio"]]

        # Consumir TODAS las apariciones del municipio y de su departamento,
        # no solo la primera. El texto de entrada concatena ubicacion y titulo,
        # asi que la ciudad suele repetirse y antes se colaba en barrio_texto
        # como 'bogota bogota' o 'chico bogota'.
        for phrase in self.aliases_by_code.get(municipio["cod_mpio"], []):
            for position in _phrase_positions(text_tokens, list(phrase)):
                consumed.update(range(position, position + len(phrase)))
        for info in departamentos.values():
            if info["cod_dpto"] == municipio["cod_dpto"]:
                consumed.update(range(*info["span"]))

        result.update(
            cod_dpto=municipio["cod_dpto"],
            departamento=municipio["departamento"],
            cod_mpio=municipio["cod_mpio"],
            municipio=municipio["municipio"],
            market_token=municipio["market_token"],
            region=municipio["region"],
            lat=municipio["lat"],
            lon=municipio["lon"],
            geo_source=MATCH_MUNICIPIO,
            geo_match_level=MATCH_MUNICIPIO,
            geo_confidence=75 if chosen["corroborated"] else 65,
        )

        barrio_hit = self._find_barrio(text_tokens, municipio["cod_mpio"], consumed)
        if barrio_hit is not None:
            entry, span = barrio_hit
            _, barrio, cod_barrio, zona, precision, lat, lon = entry
            consumed.update(range(*span))
            result.update(
                cod_barrio=cod_barrio or None,
                barrio=barrio,
                barrio_zona=zona if zona == zona else None,
                barrio_precision=precision,
                geo_match_level=MATCH_BARRIO,
                geo_confidence=95 if precision == "oficial_DANE" else 85,
            )
            if lat is not None and lon is not None:
                result.update(lat=lat, lon=lon, geo_source=MATCH_BARRIO)

        result["barrio_texto"] = self._free_text_barrio(text_tokens, consumed | context_spans)
        return result

    def resolve_many(self, pairs):
        return [self.resolve(ubicacion, titulo) for ubicacion, titulo in pairs]


@lru_cache(maxsize=2)
def get_resolver(source_path: str = None) -> GeoResolver:
    """Resolver compartido. Se construye una sola vez por proceso.

    `source_path` puede ser un directorio con los parquet de la dimension
    (lo normal en Databricks y en la app) o el libro DIVIPOLA. Si se omite,
    usa reference/geo/ y cae al Excel como ultimo recurso.
    """
    from .divipola import load_dims

    return GeoResolver(load_dims(source_path))


def resolve(ubicacion, titulo=None) -> dict:
    return get_resolver().resolve(ubicacion, titulo)


# ══════════════════════════════════════════════════════════════════
# Integracion con Spark
# ══════════════════════════════════════════════════════════════════

GEO_STRUCT_FIELDS = [
    ("cod_dpto", "string"),
    ("departamento", "string"),
    ("cod_mpio", "string"),
    ("municipio", "string"),
    ("market_token", "string"),
    ("region", "string"),
    ("cod_barrio", "string"),
    ("barrio", "string"),
    ("barrio_zona", "string"),
    ("barrio_precision", "string"),
    ("barrio_texto", "string"),
    ("lat", "double"),
    ("lon", "double"),
    ("geo_source", "string"),
    ("geo_match_level", "string"),
    ("geo_confidence", "int"),
]


def geo_struct_type():
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType,
    )

    spark_types = {"string": StringType(), "double": DoubleType(), "int": IntegerType()}
    return StructType(
        [StructField(name, spark_types[kind], True) for name, kind in GEO_STRUCT_FIELDS]
    )


def build_spark_udf(spark=None, source_path: str = None):
    """UDF que devuelve un struct con toda la geografia resuelta.

    La dimension se serializa una vez por executor via closure; no se lee
    el Excel en cada fila.
    """
    from pyspark.sql.functions import udf

    resolver = get_resolver(source_path)

    def _resolve(ubicacion, titulo=None):
        try:
            resolved = resolver.resolve(ubicacion, titulo)
        except Exception:
            resolved = dict(EMPTY_RESULT)
        return tuple(resolved[field] for field, _ in GEO_STRUCT_FIELDS)

    return udf(_resolve, geo_struct_type())
