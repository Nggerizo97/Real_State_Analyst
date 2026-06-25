import re
import pandas as pd

CIUDAD_A_DEPARTAMENTO = {
    "bogota": "Bogotá D.C.",
    "medellin": "Antioquia",
    "envigado": "Antioquia",
    "sabaneta": "Antioquia",
    "bello": "Antioquia",
    "itagui": "Antioquia",
    "rionegro": "Antioquia",
    "el retiro": "Antioquia",
    "la ceja": "Antioquia",
    "cali": "Valle del Cauca",
    "palmira": "Valle del Cauca",
    "jamundi": "Valle del Cauca",
    "barranquilla": "Atlántico",
    "soledad": "Atlántico",
    "puerto colombia": "Atlántico",
    "pereira": "Risaralda",
    "bucaramanga": "Santander",
    "floridablanca": "Santander",
    "cartagena": "Bolívar",
    "armenia": "Quindío",
    "manizales": "Caldas",
    "girardot": "Cundinamarca",
    "chia": "Cundinamarca",
    "cajica": "Cundinamarca",
    "zipaquira": "Cundinamarca",
    "ibague": "Tolima",
    "santa marta": "Magdalena",
    "villavicencio": "Meta",
    "tunja": "Boyacá",
    "villa de leyva": "Boyacá",
    "monteria": "Córdoba",
    "neiva": "Huila",
    "pasto": "Nariño",
    "sincelejo": "Sucre",
    "valledupar": "Cesar",
    "cucuta": "Norte de Santander",
    "popayan": "Cauca",
    "guamo": "Tolima",
    "honda": "Tolima",
    "mariquita": "Tolima",
    "alvarado": "Tolima",
    "viani": "Cundinamarca",
    "apulo": "Cundinamarca",
    "la mesa": "Cundinamarca",
    "silvania": "Cundinamarca",
    "villeta": "Cundinamarca",
    "chaguani": "Cundinamarca",
    "nilo": "Cundinamarca",
    "pacho": "Cundinamarca",
    "fredonia": "Antioquia",
    "amaga": "Antioquia",
    "sopetran": "Antioquia",
    "san jeronimo": "Antioquia",
    "santa fe de antioquia": "Antioquia",
    "barbosa antioquia": "Antioquia",
    "caldas antioquia": "Antioquia",
    "san pedro de los milagros": "Antioquia",
    "jardin": "Antioquia",
    "viterbo": "Caldas",
    "la dorada": "Caldas",
    "palestina": "Caldas",
    "tulua": "Valle del Cauca",
    "buga": "Valle del Cauca",
    "cartago": "Valle del Cauca",
    "alcala": "Valle del Cauca",
    "buenaventura": "Valle del Cauca",
    "barrancabermeja": "Santander",
    "otra ciudad": "Otros",
}

MUNICIPIOS_BY_DEP = {
    "Antioquia": {
        "envigado": "Envigado", 
        "sabaneta": "Sabaneta", 
        "itagui": "Itagüí", 
        "bello": "Bello", 
        "la estrella": "La Estrella", 
        "copacabana": "Copacabana", 
        "caldas": "Caldas", 
        "barbosa": "Barbosa", 
        "rionegro": "Rionegro", 
        "la ceja": "La Ceja", 
        "ceja": "La Ceja", 
        "el retiro": "El Retiro", 
        "retiro": "El Retiro", 
        "guarne": "Guarne", 
        "marinilla": "Marinilla", 
        "el carmen de viboral": "El Carmen de Viboral", 
        "penol": "Peñol", 
        "guatape": "Guatapé", 
        "sopetran": "Sopetrán", 
        "santa fe de antioquia": "Santa Fe de Antioquia", 
        "san jeronimo": "San Jerónimo", 
        "amaga": "Amagá", 
        "jardin": "Jardín", 
        "fredonia": "Fredonia", 
        "san pedro de los milagros": "San Pedro de los Milagros", 
        "la union": "La Unión", 
        "union": "La Unión"
    },
    "Cundinamarca": {
        "soacha": "Soacha", 
        "chia": "Chía", 
        "zipaquira": "Zipaquirá", 
        "cajica": "Cajicá", 
        "cota": "Cota", 
        "funza": "Funza", 
        "mosquera": "Mosquera", 
        "madrid": "Madrid", 
        "tocancipa": "Tocancipá", 
        "la calera": "La Calera", 
        "calera": "La Calera", 
        "sopo": "Sopó", 
        "tenjo": "Tenjo", 
        "tabio": "Tabio", 
        "guasca": "Guasca", 
        "subachoque": "Subachoque", 
        "sesquile": "Sesquilé", 
        "choconta": "Chocontá", 
        "choachi": "Choachí", 
        "girardot": "Girardot", 
        "fusagasuga": "Fusagasugá", 
        "anapoima": "Anapoima", 
        "la mesa": "La Mesa", 
        "villeta": "Villeta", 
        "silvania": "Silvania", 
        "nilo": "Nilo", 
        "apulo": "Apulo", 
        "ricaurte": "Ricaurte", 
        "pacho": "Pacho", 
        "chaguani": "Chaguaní", 
        "viani": "Vianí"
    },
    "Valle del Cauca": {
        "jamundi": "Jamundí", 
        "yumbo": "Yumbo", 
        "palmira": "Palmira", 
        "candelaria": "Candelaria", 
        "dagua": "Dagua", 
        "la cumbre": "La Cumbre", 
        "calima": "Calima", 
        "cartago": "Cartago", 
        "tulua": "Tuluá", 
        "alcala": "Alcalá", 
        "buga": "Buga", 
        "buenaventura": "Buenaventura"
    },
    "Atlántico": {
        "soledad": "Soledad", 
        "puerto colombia": "Puerto Colombia", 
        "puerto": "Puerto Colombia", 
        "juan de acosta": "Juan de Acosta", 
        "tubara": "Tubará", 
        "malambo": "Malambo", 
        "galapa": "Galapa"
    },
    "Santander": {
        "floridablanca": "Floridablanca", 
        "piedecuesta": "Piedecuesta", 
        "giron": "Girón", 
        "lebrija": "Lebrija", 
        "los santos": "Los Santos", 
        "barrancabermeja": "Barrancabermeja", 
        "san gil": "San Gil"
    },
    "Risaralda": {
        "dosquebradas": "Dosquebradas", 
        "santa rosa de cabal": "Santa Rosa de Cabal"
    },
    "Quindío": {
        "circasia": "Circasia", 
        "calarca": "Calarcá", 
        "la tebaida": "La Tebaida", 
        "quimbaya": "Quimbaya", 
        "filandia": "Filandia", 
        "montenegro": "Montenegro"
    },
    "Caldas": {
        "villamaria": "Villamaría", 
        "palestina": "Palestina", 
        "viterbo": "Viterbo", 
        "la dorada": "La Dorada"
    },
    "Tolima": {
        "melgar": "Melgar", 
        "carmen de apicala": "Carmen de Apicalá", 
        "flandes": "Flandes", 
        "mariquita": "Mariquita", 
        "alvarado": "Alvarado", 
        "espinal": "Espinal", 
        "honda": "Honda", 
        "guamo": "Guamo"
    },
    "Boyacá": {
        "tunja": "Tunja", 
        "villa de leyva": "Villa de Leyva", 
        "duitama": "Duitama", 
        "sogamoso": "Sogamoso", 
        "paipa": "Paipa", 
        "chiquinquira": "Chiquinquirá"
    },
    "Meta": {
        "villavicencio": "Villavicencio", 
        "restrepo": "Restrepo", 
        "cumaral": "Cumaral"
    },
    "Norte de Santander": {
        "cucuta": "Cúcuta", 
        "los patios": "Los Patios", 
        "villa del rosario": "Villa del Rosario"
    },
    "Bolívar": {
        "cartagena": "Cartagena", 
        "turbaco": "Turbaco", 
        "arjona": "Arjona"
    },
    "Cauca": {
        "popayan": "Popayán", 
        "santander de quilichao": "Santander de Quilichao"
    },
    "Huila": {
        "neiva": "Neiva", 
        "pitalito": "Pitalito", 
        "garzon": "Garzón"
    },
    "Nariño": {
        "pasto": "Pasto"
    },
    "Córdoba": {
        "monteria": "Montería"
    },
    "Magdalena": {
        "santa marta": "Santa Marta"
    },
    "Cesar": {
        "valledupar": "Valledupar"
    },
    "Sucre": {
        "sincelejo": "Sincelejo"
    }
}

def get_departamento(row):
    city = str(row["city_token"]).lower().strip()
    if city in CIUDAD_A_DEPARTAMENTO:
        return CIUDAD_A_DEPARTAMENTO[city]
    
    norm = str(row.get("ubicacion_norm", "")).lower()
    for c, dep in CIUDAD_A_DEPARTAMENTO.items():
        if c != "otra ciudad" and c in norm:
            return dep
            
    # Patrones comunes
    if "antioquia" in norm:
        return "Antioquia"
    if "cundinamarca" in norm or "bogota" in norm:
        return "Cundinamarca"
    if "valle del cauca" in norm:
        return "Valle del Cauca"
    if "atlantico" in norm:
        return "Atlántico"
    if "risaralda" in norm:
        return "Risaralda"
    if "tolima" in norm:
        return "Tolima"
    if "caldas" in norm:
        return "Caldas"
    if "quindio" in norm:
        return "Quindío"
    if "bolivar" in norm:
        return "Bolívar"
    if "santander" in norm:
        return "Santander"
    if "magdalena" in norm:
        return "Magdalena"
    if "meta" in norm:
        return "Meta"
    if "boyaca" in norm:
        return "Boyacá"
    if "cordoba" in norm:
        return "Córdoba"
    if "huila" in norm:
        return "Huila"
    if "narino" in norm or "nariño" in norm:
        return "Nariño"
    if "sucre" in norm:
        return "Sucre"
    if "cesar" in norm:
        return "Cesar"
    if "norte de santander" in norm:
        return "Norte de Santander"
    if "cauca" in norm:
        return "Cauca"
        
    return "Otros"

def clean_sector_name(sector: str, municipio: str) -> str:
    if not sector or pd.isna(sector):
        return "General"
    
    # Remove any coordinates or strange patterns like "4°43'19..."
    if any(char in str(sector) for char in ["°", "'", '"', "©", "®"]):
        return "General"
        
    # Standardize spaces
    sector = str(sector)
    sector = re.sub(r"\s+", " ", sector).strip()
    
    # Remove prefix "sector", "barrio", "sectores", "barrios" from the start
    sector = re.sub(r"^(sector|barrio|sectores|barrios)\b\s*", "", sector, flags=re.IGNORECASE).strip()
    
    # Remove accents/normalize for matching
    def normalize_str(s):
        import unicodedata
        return "".join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn').lower()
        
    norm_sector = normalize_str(sector)
    norm_mun = normalize_str(municipio)
    
    # Simple city names to strip
    cities_to_strip = [norm_mun, "bogota", "medellin", "cali", "barranquilla", "cartagena", "bucaramanga", "pereira"]
    
    # Strip from start/end
    words = sector.split()
    cleaned_words = []
    for w in words:
        norm_w = normalize_str(w)
        if norm_w in cities_to_strip or norm_w == "d.c" or norm_w == "dc":
            continue
        cleaned_words.append(w)
        
    sector = " ".join(cleaned_words).strip()
    
    # Remove double words like "Chia Chia", "Cajica Cajica", "La Calera La Calera"
    words = sector.split()
    if len(words) >= 2:
        unique_words = []
        for i, w in enumerate(words):
            if i > 0 and normalize_str(w) == normalize_str(words[i-1]):
                continue
            unique_words.append(w)
        sector = " ".join(unique_words).strip()
        
    # If the sector name becomes empty or matches the city name, fallback to "General"
    if not sector or normalize_str(sector) == norm_mun or sector.lower() in ["general", "sector", "otros", "desconocido", "desconocida", "centro"]:
        return "General"
        
    # Capitalize properly
    sector = sector.title()
    
    return sector


def resolve_municipio_and_sector(row):
    dep = row["departamento"]
    city_token = str(row["city_token"]).lower().strip()
    norm_text = str(row.get("ubicacion_norm", "")).lower()
    clean_text = str(row.get("ubicacion_clean", "")).lower()
    search_text = f" {clean_text} {norm_text} "

    # Específico para Bogotá/Cundinamarca:
    # Si es clasificado como bogota, pero el texto menciona un municipio de Cundinamarca, re-clasificar
    if city_token == "bogota" or dep == "Bogotá D.C.":
        for key, val in MUNICIPIOS_BY_DEP["Cundinamarca"].items():
            if f" {key} " in search_text:
                dep = "Cundinamarca"
                municipio = val
                # Resolver sector
                sector_clean = re.sub(rf"\b{key}\b", "", clean_text, flags=re.IGNORECASE).strip()
                sector_clean = re.sub(r"\s+", " ", sector_clean).strip().title()
                sector = sector_clean if sector_clean else "General"
                # Limpiar
                sector = clean_sector_name(sector, municipio)
                return pd.Series([dep, municipio, sector])

    # Resolucion normal
    municipio = city_token.replace("_", " ").title()
    if city_token == "bogota":
        municipio = "Bogotá D.C."
    elif city_token == "medellin":
        municipio = "Medellín"
    elif city_token == "cali":
        municipio = "Cali"
    elif city_token == "barranquilla":
        municipio = "Barranquilla"
    elif city_token == "pereira":
        municipio = "Pereira"
    elif city_token == "otra ciudad":
        municipio = "Otro"
        
    sector = str(row.get("ubicacion_clean", "General")).strip()
    
    # Resolver usando MUNICIPIOS_BY_DEP
    target_dep = dep
    if target_dep == "Bogotá D.C.":
        target_dep = "Cundinamarca"  # Buscar en Cundinamarca por si acaso
        
    if target_dep in MUNICIPIOS_BY_DEP:
        matched_key = None
        sorted_keys = sorted(MUNICIPIOS_BY_DEP[target_dep].keys(), key=len, reverse=True)
        for key in sorted_keys:
            if f" {key} " in search_text:
                matched_key = key
                break
                
        if matched_key:
            dep = target_dep
            municipio = MUNICIPIOS_BY_DEP[target_dep][matched_key]
            sector_clean = re.sub(rf"\b{matched_key}\b", "", clean_text, flags=re.IGNORECASE).strip()
            sector_clean = re.sub(r"\s+", " ", sector_clean).strip().title()
            sector = sector_clean if sector_clean else "General"
            
    # Limpiar sector
    sector = clean_sector_name(sector, municipio)
    
    return pd.Series([dep, municipio, sector])
