# === ARCHIVO GENERADO — NO EDITAR AQUI ===
# Origen: lakehouse-repo/src/geo/aliases.py
# Regenerar: python tools/sync_geo_to_app.py
"""Capa de sobreescritura sobre DIVIPOLA: como escriben los portales.

DIVIPOLA es la verdad oficial de nombres y codigos, pero los portales no
escriben "Santiago de Cali" ni "Bogota, D.c.". Este modulo aporta las tres
piezas que DIVIPOLA no puede dar:

  1. ALIAS       -> las grafias que si aparecen en los avisos.
  2. PRIORIDAD   -> desempate cuando un nombre existe en varios departamentos
                    (67 nombres ambiguos / 153 municipios en DIVIPOLA).
  3. MERCADO     -> agrupacion comercial (area metropolitana), que es una
                    decision de negocio, no una division administrativa.

Todo se referencia por codigo DANE de municipio, nunca por nombre suelto.
"""

# ══════════════════════════════════════════════════════════════════
# 1. ALIAS DE MUNICIPIO — cod_mpio -> grafias usadas por los portales
#    El nombre oficial de DIVIPOLA se agrega automaticamente en divipola.py;
#    aqui solo van las variantes que NO se derivan del nombre oficial.
# ══════════════════════════════════════════════════════════════════

MUNICIPIO_ALIASES = {
    "11001": ["bogota", "bogota dc", "bogota distrito capital", "santafe de bogota"],
    "76001": ["cali"],
    "13001": ["cartagena"],
    "54001": ["cucuta"],
    "52001": ["pasto"],
    "05001": ["medellin", "medellin distrito especial"],
    "08001": ["barranquilla", "bquilla", "barranquilla distrito especial"],
    "47001": ["santa marta", "sta marta"],
    "05266": ["envigado"],
    "05360": ["itagui"],
    "05615": ["rionegro antioquia"],
    "05607": ["retiro", "el retiro antioquia"],
    "05376": ["la ceja del tambo"],
    "68276": ["floridablanca"],
    "76364": ["jamundi"],
    "76520": ["palmira"],
    "25754": ["soacha"],
    "25175": ["chia"],
    "25126": ["cajica"],
    "73001": ["ibague"],
    "63001": ["armenia quindio"],
    "05002": ["abejorral"],
    "76111": ["buga", "guadalajara de buga"],
    "76126": ["calima", "calima el darien", "el darien"],
    "05658": ["san jeronimo antioquia"],
    "05042": ["santafe de antioquia", "santa fe de antioquia"],
    "68547": ["piedecuesta"],
    "68307": ["giron", "giron santander"],
    "66170": ["dosquebradas"],
    "17873": ["villamaria"],
    "63130": ["calarca"],
    "63401": ["la tebaida"],
    "63594": ["quimbaya"],
    "73268": ["espinal", "el espinal"],
    "73449": ["melgar"],
    "73275": ["flandes"],
    "73443": ["mariquita", "san sebastian de mariquita"],
    "25290": ["fusagasuga"],
    "25307": ["girardot"],
    "25035": ["anapoima"],
    "15001": ["tunja"],
    "15407": ["villa de leyva", "villa de leiva"],
    "15238": ["duitama"],
    "15759": ["sogamoso"],
    "15516": ["paipa"],
    "68081": ["barrancabermeja"],
    "50606": ["restrepo meta"],
    "50226": ["cumaral"],
    "54405": ["los patios"],
    "54874": ["villa del rosario"],
    "13836": ["turbaco"],
    "13052": ["arjona"],
    "19698": ["santander de quilichao"],
    "41551": ["pitalito"],
    "41298": ["garzon"],
    "17380": ["la dorada"],
    "08758": ["soledad", "soledad atlantico"],
    "08573": ["puerto colombia", "pradomar", "salgar atlantico"],
    "68679": ["san gil"],
    "68418": ["santos", "mesa de los santos"],
    "05129": ["caldas antioquia"],
    "05079": ["barbosa antioquia"],
    "68077": ["barbosa santander"],
    # "san vicente" a secas es como lo escriben los portales; sin este alias
    # solo resolvia cuando el aviso nombraba el departamento.
    "05674": ["san vicente", "san vicente ferrer", "san vicente antioquia"],
    "05400": ["la union antioquia"],
    "76400": ["la union valle"],
    "05148": ["el carmen de viboral", "carmen de viboral"],
    "05541": ["penol", "el penol"],
    "05321": ["guatape"],
    "05318": ["guarne"],
    "05440": ["marinilla"],
    "05212": ["copacabana"],
    "05380": ["la estrella"],
    "05631": ["sabaneta"],
    "05088": ["bello"],
    "05030": ["amaga"],
    "05364": ["jardin"],
    "05282": ["fredonia"],
    "05664": ["san pedro de los milagros"],
    "05761": ["sopetran"],
    "25899": ["zipaquira"],
    "25214": ["cota"],
    "25286": ["funza"],
    "25473": ["mosquera cundinamarca"],
    "25430": ["madrid cundinamarca"],
    "25817": ["tocancipa"],
    "25377": ["la calera"],
    "25758": ["sopo"],
    "25799": ["tenjo"],
    "25785": ["tabio"],
    "25322": ["guasca"],
    "25769": ["subachoque"],
    "25736": ["sesquile"],
    "25183": ["choconta"],
    "25181": ["choachi"],
    "25386": ["la mesa"],
    "25875": ["villeta"],
    "25743": ["silvania"],
    "25488": ["nilo"],
    "25599": ["apulo", "rafael reyes"],
    "25612": ["ricaurte cundinamarca"],
    "25513": ["pacho"],
    "76892": ["yumbo"],
    "76130": ["candelaria valle"],
    "76233": ["dagua"],
    "76377": ["la cumbre"],
    "76147": ["cartago"],
    "76834": ["tulua"],
    "76020": ["alcala"],
    "76248": ["el cerrito"],
    "76563": ["pradera"],
    "76275": ["florida valle"],
    "76109": ["buenaventura"],
    "08372": ["juan de acosta"],
    "08832": ["tubara"],
    "08433": ["malambo"],
    "08296": ["galapa"],
    "66682": ["santa rosa de cabal"],
    "63190": ["circasia"],
    "63470": ["montenegro"],
    "63272": ["filandia"],
    "17524": ["palestina caldas"],
    "17877": ["viterbo"],
    "73148": ["carmen de apicala"],
    "73026": ["alvarado"],
    "73349": ["honda"],
    "73319": ["guamo"],
    "15176": ["chiquinquira"],
    "68406": ["lebrija"],
}

# ══════════════════════════════════════════════════════════════════
# 2. PRIORIDAD PARA HOMONIMOS
#    Gana el codigo con prioridad mas alta cuando el texto no menciona
#    departamento. Reemplaza las reglas anti-homonimo que estaban
#    incrustadas a mano en el notebook de Gold.
# ══════════════════════════════════════════════════════════════════

DEFAULT_PRIORITY = 10

# Piso de prioridad para municipios que pertenecen a un mercado comercial
# curado (MARKET_CATALOG). Debe quedar por encima de
# STANDALONE_PRIORITY_FLOOR del resolver para que alias cortos como "sopo",
# "nilo" o "cota" resuelvan sin necesidad de que el aviso nombre el
# departamento.
MARKET_MEMBER_PRIORITY = 55

MUNICIPIO_PRIORITY = {
    # Capitales y mercados dominantes
    "11001": 100,  # Bogota
    "05001": 99,   # Medellin
    "76001": 98,   # Cali
    "08001": 97,   # Barranquilla
    "13001": 96,   # Cartagena
    "68001": 95,   # Bucaramanga
    "66001": 94,   # Pereira
    "17001": 93,   # Manizales
    "63001": 92,   # Armenia (Quindio) gana sobre Armenia (Antioquia)
    "47001": 91,   # Santa Marta
    "54001": 90,   # Cucuta
    "73001": 89,   # Ibague
    "50001": 88,   # Villavicencio
    "52001": 87,   # Pasto
    "23001": 86,   # Monteria
    "41001": 85,   # Neiva
    "19001": 84,   # Popayan
    "20001": 83,   # Valledupar
    "70001": 82,   # Sincelejo
    "15001": 81,   # Tunja
    # Areas metropolitanas de alto volumen
    "05266": 70, "05631": 70, "05360": 70, "05088": 70, "05380": 70,
    "05212": 70, "05615": 70, "05376": 70, "05607": 70,
    "25754": 68, "25175": 68, "25126": 68, "25899": 68, "25214": 68,
    "68276": 66, "68547": 66, "68307": 66,
    "76364": 64, "76892": 64, "76520": 64,
    "08758": 62, "08573": 62,
    "66170": 60, "63130": 60,
    "25307": 58, "73449": 58,
    # Desempates explicitos frente a su homonimo
    "05129": 30,  # Caldas (Antioquia) - homonimo del departamento
    "05079": 30,  # Barbosa (Antioquia) vs Barbosa (Santander)
    "05674": 30,  # San Vicente Ferrer (Antioquia)
    "05400": 20,  # La Union (Antioquia)
}

# ══════════════════════════════════════════════════════════════════
# 3. MERCADOS COMERCIALES — agrupacion de negocio por codigo DANE
# ══════════════════════════════════════════════════════════════════

MARKET_CATALOG = {
    "bogota_metropolitana": [
        "11001", "25754", "25175", "25899", "25126", "25214", "25286",
        "25473", "25430", "25817", "25377", "25758", "25799", "25785",
        "25322", "25769", "25736", "25183", "25181",
    ],
    "turismo_cundinamarca": [
        "25307", "25290", "25035", "25386", "25875", "25743", "25488",
        "25599", "25612", "25513",
    ],
    "valle_aburra": [
        "05001", "05266", "05631", "05360", "05088", "05380", "05212",
        "05129", "05079", "05030", "05282", "05664",
    ],
    "oriente_antioqueno": [
        "05615", "05376", "05607", "05318", "05440", "05148", "05541",
        "05321", "05674",
    ],
    "occidente_antioqueno": ["05761", "05042", "05658", "05364"],
    "cali_metropolitana": [
        "76001", "76364", "76892", "76520", "76130", "76233", "76377",
        "76248", "76563", "76275", "76126",
    ],
    "norte_valle": ["76147", "76834", "76020", "76111", "76109"],
    "barranquilla_metropolitana": [
        "08001", "08758", "08573", "08372", "08832", "08433", "08296",
    ],
    "cartagena_metropolitana": ["13001", "13836", "13052"],
    "bucaramanga_metropolitana": [
        "68001", "68276", "68547", "68307", "68406", "68418", "68679",
    ],
    "eje_cafetero": [
        "66001", "17001", "63001", "66170", "66682", "63190", "63130",
        "63401", "63594", "63272", "63470", "17873", "17524", "17380",
        "17877",
    ],
    "turismo_tolima": [
        "73449", "73148", "73275", "73443", "73026", "73268", "73349",
        "73319",
    ],
    "cucuta_metropolitana": ["54001", "54405", "54874"],
    "santa_marta_metropolitana": ["47001"],
    "villavicencio_metropolitana": ["50001", "50606", "50226"],
    "pasto_metropolitana": ["52001"],
    "monteria_metropolitana": ["23001"],
    "neiva_metropolitana": ["41001", "41551", "41298"],
    "ibague_metropolitana": ["73001"],
    "popayan_metropolitana": ["19001", "19698"],
    "valledupar_metropolitana": ["20001"],
    "sincelejo_metropolitana": ["70001"],
    "tunja_metropolitana": ["15001", "15407", "15238", "15759", "15516", "15176"],
    "barrancabermeja_metropolitana": ["68081"],
}

CODMPIO_TO_MARKET = {
    code: market for market, codes in MARKET_CATALOG.items() for code in codes
}

# ══════════════════════════════════════════════════════════════════
# 4. REGION MACRO por codigo de departamento
# ══════════════════════════════════════════════════════════════════

DEPARTAMENTO_REGION = {
    "05": "andina", "08": "caribe", "11": "andina", "13": "caribe",
    "15": "andina", "17": "andina", "18": "amazonia", "19": "pacifica",
    "20": "caribe", "23": "caribe", "25": "andina", "27": "pacifica",
    "41": "andina", "44": "caribe", "47": "caribe", "50": "orinoquia",
    "52": "pacifica", "54": "andina", "63": "andina", "66": "andina",
    "68": "andina", "70": "caribe", "73": "andina", "76": "pacifica",
    "81": "orinoquia", "85": "orinoquia", "86": "amazonia",
    "88": "insular", "91": "amazonia", "94": "amazonia",
    "95": "amazonia", "97": "amazonia", "99": "orinoquia",
}

# ══════════════════════════════════════════════════════════════════
# 5. STOPWORDS PARA NOMBRES DE BARRIO
#    Esta lista es la correccion directa del bug que producia
#    sector_mercado = 'en' (10.972 filas), 'd c en' (3.899), 'en en',
#    'comuna en', 'no en', 'de indias en'. La version anterior solo
#    filtraba sustantivos inmobiliarios y dejaba pasar preposiciones,
#    articulos y conjunciones.
# ══════════════════════════════════════════════════════════════════

SPANISH_STOPWORDS = {
    "a", "al", "ante", "bajo", "cabe", "con", "contra", "de", "del",
    "desde", "durante", "e", "el", "en", "entre", "hacia", "hasta", "la",
    "las", "lo", "los", "mediante", "o", "para", "por", "según", "segun",
    "sin", "so", "sobre", "tras", "u", "un", "una", "unas", "unos", "y",
    "no", "si", "su", "sus", "que", "es", "mas", "muy", "d", "c",
}

PROPERTY_STOPWORDS = {
    "apartamento", "apartaestudio", "apto", "aptos", "casa", "casas",
    "lote", "lotes", "finca", "fincas", "oficina", "oficinas", "local",
    "locales", "bodega", "comercial", "venta", "vender", "arriendo",
    "arrendar", "alquiler", "inmueble", "inmuebles", "propiedad",
    "proyecto", "proyectos", "estrenar", "nuevo", "nueva", "usado",
    "usada", "sector", "sectores", "barrio", "barrios", "zona", "zonas",
    "urbanizacion", "unidad", "unidades", "conjunto", "cerrado",
    "edificio", "torre", "torres", "piso", "pisos", "etapa", "manzana",
    "habitaciones", "habitacion", "alcobas", "alcoba", "banos", "bano",
    "garaje", "garajes", "parqueadero", "parqueaderos", "metros", "m2",
    "area", "areas", "precio", "millones", "cop",
}

GEO_GENERIC_STOPWORDS = {
    "norte", "sur", "oriente", "occidente", "oriental", "occidental",
    "centro", "cerca", "frente", "calle", "carrera", "avenida", "av",
    "transversal", "diagonal", "circular", "via", "autopista", "km",
    "kilometro", "vereda", "corregimiento", "comuna", "localidad",
    "urbana", "rural", "colombia", "departamento", "municipio",
    "distrito", "capital", "especial", "metropolitana", "area",
    "otro", "otra", "otros", "otras", "lejos", "sobre", "planos",
}

# Adjetivos y ganchos de marketing que sobreviven al filtro anterior porque
# no son ni preposiciones ni sustantivos inmobiliarios, pero tampoco son
# nombres de barrio. Sin esta lista aparecian candidatos como
# "lejos nido" o "excelente ubicacion" en el selector de barrios.
MARKETING_STOPWORDS = {
    "excelente", "excelentes", "hermoso", "hermosa", "bonito", "bonita",
    "amplio", "amplia", "amplios", "amplias", "moderno", "moderna",
    "exclusivo", "exclusiva", "lujo", "lujoso", "lujosa", "acogedor",
    "acogedora", "espectacular", "increible", "unico", "unica",
    "espacioso", "espaciosa", "comodo", "comoda", "ideal", "perfecto",
    "perfecta", "mejor", "mejores", "gran", "grande", "grandes",
    "pequeno", "pequena", "nuevo", "nueva", "nuevos", "nuevas",
    "oportunidad", "ganga", "rebajado", "negociable", "urge", "urgente",
    "vista", "vistas", "panoramica", "iluminado", "iluminada",
    "remodelado", "remodelada", "terminado", "terminada", "entrega",
    "inmediata", "disponible", "ubicacion", "ubicado", "ubicada",
    "nido", "sueno", "hogar", "familia", "familiar", "inversion",
}

SECTOR_STOPWORDS = (
    SPANISH_STOPWORDS
    | PROPERTY_STOPWORDS
    | GEO_GENERIC_STOPWORDS
    | MARKETING_STOPWORDS
)
