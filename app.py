"""
app.py — Real Estate Analyst Colombia (Versión Light)
======================================================
Esta es una versión ligera diseñada para correr gratuitamente en Streamlit Cloud.
Incluye:
  1. Simulador Financiero e Inmuebles (Happy Path) — presupuesto según ingresos y deuda.
  2. Inteligencia de Mercado — gráficos agregados macro del sector en Colombia.
  3. Tasación IA Teaser — demostración interactiva de la funcionalidad premium.
"""

import io
import json
import os
import re
import sys
import time
import warnings
import dotenv
dotenv.load_dotenv()
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Evitar advertencias molestas
warnings.filterwarnings("ignore")

# Configuración de página con look premium
st.set_page_config(
    page_title="Real Estate Analyst — Colombia",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Cargar CSS personalizado
if os.path.exists("style.css"):
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# DATA & AWS CONFIG FALLBACK
# ══════════════════════════════════════════════════════════════════

def _get_aws_config() -> dict:
    try:
        if "aws" in st.secrets:
            return dict(st.secrets["aws"])
    except Exception:
        pass
    return {
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
        "s3_bucket_name": os.getenv("S3_BUCKET_NAME", ""),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    }

aws_config = _get_aws_config()

# Intentar inicializar s3fs
s3_active = False
try:
    import s3fs
    import pyarrow.dataset as ds
    import pyarrow.compute as pc
    import pyarrow as pa
    
    bucket_name = aws_config.get("s3_bucket_name")
    if bucket_name:
        # Probar conexión básica
        fs = s3fs.S3FileSystem(
            key=aws_config.get("aws_access_key_id") or None,
            secret=aws_config.get("aws_secret_access_key") or None
        )
        if fs.exists(f"{bucket_name}/gold/"):
            s3_active = True
except Exception:
    s3_active = False

# Constantes de diseño para Plotly
_BG   = "#1e1e2a"
_PLOT = "#16161f"
_GRID = "#2a2a3a"
_TEXT = "#c8c4bc"
_MUTED_TEXT = "#7a7a8c"

def dark_layout(fig, height=380, **extra):
    """Aplica tema oscuro consistente a cualquier figura Plotly."""
    layout_params = dict(
        paper_bgcolor=_BG, plot_bgcolor=_PLOT,
        font=dict(family="DM Sans", size=11, color=_TEXT),
        height=height, margin=dict(l=40, r=20, t=30, b=40),
        xaxis=dict(gridcolor=_GRID, linecolor=_GRID, zerolinecolor=_GRID,
                   tickfont=dict(color=_TEXT), title_font=dict(color=_MUTED_TEXT)),
        yaxis=dict(gridcolor=_GRID, linecolor=_GRID, zerolinecolor=_GRID,
                   tickfont=dict(color=_TEXT), title_font=dict(color=_MUTED_TEXT)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=_TEXT)),
    )
    layout_params.update(extra)
    fig.update_layout(**layout_params)
    return fig

def fmt_cop(val):
    if pd.isna(val) or val is None:
        return "—"
    return f"${int(val):,}".replace(",", ".")

# ══════════════════════════════════════════════════════════════════
# DATASETS & FALLBACK MOCK DATABASE
# ══════════════════════════════════════════════════════════════════

def _generate_demo_dataset() -> pd.DataFrame:
    """Genera un dataset de demostración sumamente completo, ordenado y realista."""
    np.random.seed(42)
    n = 2500
    
    ciudades_info = [
        ("bogota", "Bogotá D.C.", ["Usaquén", "Chapinero", "Cedritos", "Rosales", "Chicó", "Teusaquillo", "Suba", "Engativá", "Modelia", "Salitre"]),
        ("medellin", "Antioquia", ["El Poblado", "Laureles", "Belén", "Robledo", "Aranjuez", "La América", "Conquistadores"]),
        ("envigado", "Antioquia", ["Las Antillas", "El Esmeraldal", "La Sebastiana", "Otraparte", "Loma de El Escobero"]),
        ("sabaneta", "Antioquia", ["Vegas del Sur", "Lomitas", "Aves María", "Pan de Azúcar"]),
        ("bello", "Antioquia", ["Niquía", "Cabañas", "Centro Bello"]),
        ("itagui", "Antioquia", ["Centro Itagüí", "Santa María", "Ditaires"]),
        ("rionegro", "Antioquia", ["San Antonio de Pereira", "El Porvenir", "Llanogrande"]),
        ("el retiro", "Antioquia", ["Fizebad", "Pantanillo", "Centro Retiro"]),
        ("la ceja", "Antioquia", ["Centro Ceja", "Tambora"]),
        ("jardin", "Antioquia", ["Centro Jardín"]),
        ("cali", "Valle del Cauca", ["Ciudad Jardín", "San Fernando", "Oeste de Cali", "El Limonar", "Chipichape", "Pance"]),
        ("palmira", "Valle del Cauca", ["Las Mercedes", "El Prado"]),
        ("jamundi", "Valle del Cauca", ["Alfaguara", "El Castillo"]),
        ("barranquilla", "Atlántico", ["Riomar", "Alto Prado", "El Prado", "Norte Centro Histórico"]),
        ("soledad", "Atlántico", ["Centro Soledad", "Los Almendros"]),
        ("puerto colombia", "Atlántico", ["Villa Campestre", "Sabanilla", "Pradomar"]),
        ("pereira", "Risaralda", ["Cerritos", "Pinares", "Dosquebradas", "Centro Pereira", "La Florida"]),
        ("armenia", "Quindío", ["Norte Armenia", "Centro Armenia", "La Castellana"]),
        ("manizales", "Caldas", ["El Cable", "Palermo", "Chipre", "La Florida"]),
        ("girardot", "Cundinamarca", ["El Peñón", "Condominio Oasis", "Centro Girardot"]),
        ("fusagasuga", "Cundinamarca", ["Comuna Norte", "Centro Fusagasugá"]),
        ("chia", "Cundinamarca", ["Fagua", "Guaymaral", "Centro Chía"]),
        ("cajica", "Cundinamarca", ["Capellanía", "Canelón"]),
        ("zipaquira", "Cundinamarca", ["San Carlos", "Algarrra"]),
        ("ibague", "Tolima", ["El Vergel", "Cádiz", "Centro Ibagué"]),
        ("melgar", "Tolima", ["La Herradura", "Centro Melgar"]),
        ("santa marta", "Magdalena", ["El Rodadero", "Bello Horizonte", "Centro Histórico", "Pozos Colorados"]),
        ("cartagena", "Bolívar", ["Bocagrande", "Manga", "Castillogrande", "El Cabrero", "Crespo", "Zona Norte"]),
        ("bucaramanga", "Santander", ["Cabecera del Llano", "Sotomayor", "Real de Minas", "Ruitoque"]),
        ("floridablanca", "Santander", ["Cañaveral", "El Bosque"]),
        ("cucuta", "Norte de Santander", ["Lleras Restrepo", "Caobos", "La Riviera"]),
        ("villavicencio", "Meta", ["El Caudal", "Trapiche", "Centro Villavicencio"]),
        ("pasto", "Nariño", ["Avenida Maridíaz", "Centro Pasto"]),
        ("monteria", "Córdoba", ["El Recreo", "Castellana", "Centro Montería"]),
        ("neiva", "Huila", ["La Toma", "Quirinal", "Ipanema"]),
        ("tunja", "Boyacá", ["Norte Tunja", "Centro Tunja"]),
        ("villa de leyva", "Boyacá", ["Centro Histórico", "La Villa"]),
        ("valledupar", "Cesar", ["Novales", "Las Flores"]),
        ("sincelejo", "Sucre", ["La Selva", "Venecia"]),
        ("popayan", "Cauca", ["Norte Popayán", "Centro Popayán"]),
    ]
    
    portales = ["fincaraiz", "metrocuadrado", "ciencuadras_usado", "properati", "bancolombia_tu360"]
    
    rows = []
    for i in range(n):
        city_data = ciudades_info[np.random.randint(len(ciudades_info))]
        city = city_data[0]
        dep_name = city_data[1]
        barrio = np.random.choice(city_data[2])
        
        tipo = np.random.choice(["apartamento", "casa"], p=[0.75, 0.25])
        estado = np.random.choice(["usado", "nuevo"], p=[0.8, 0.2])
        fuente = np.random.choice(portales)
        
        # Área y distribución realista
        if tipo == "apartamento":
            area = int(np.random.randint(45, 140))
            habs = int(np.random.choice([1, 2, 3], p=[0.15, 0.45, 0.40]))
        else:
            area = int(np.random.randint(90, 320))
            habs = int(np.random.choice([3, 4, 5], p=[0.50, 0.40, 0.10]))
            
        banos = int(max(1, habs + np.random.choice([-1, 0, 1], p=[0.3, 0.6, 0.1])))
        garajes = int(np.random.choice([0, 1, 2], p=[0.3, 0.5, 0.2]))
        
        # Precios basados en departamento y area
        precio_m2_base = 5.2e6
        if dep_name == "Bogotá D.C.":
            precio_m2_base = 6.2e6
        elif dep_name == "Antioquia":
            precio_m2_base = 5.8e6
        elif dep_name == "Valle del Cauca":
            precio_m2_base = 4.2e6
        elif dep_name == "Atlántico":
            precio_m2_base = 4.6e6
        elif dep_name == "Bolívar":
            precio_m2_base = 5.5e6
        elif dep_name == "Santander":
            precio_m2_base = 4.4e6
            
        barrio_multiplier = 1.0
        if barrio in ["Rosales", "Chicó", "El Poblado", "Ciudad Jardín", "Alto Prado", "Cerritos", "Bocagrande", "Zona Norte", "Ruitoque", "Cañaveral"]:
            barrio_multiplier = 1.35
            
        precio_m2 = precio_m2_base * barrio_multiplier * np.random.uniform(0.85, 1.15)
        precio = round(area * precio_m2, -6)
        
        url = f"https://www.{fuente}.com.co/inmueble/propiedad-colombia-id-{10000+i}"
        
        rentabilidad = np.random.uniform(0.045, 0.095)
        score = int(np.random.randint(60, 99))
        rows.append({
            "id_original": f"DEMO-{i:04d}",
            "city_token": city,
            "market_token": f"{city}_metropolitana",
            "ubicacion_clean": barrio,
            "precio_num": float(precio),
            "area_m2": float(area),
            "habitaciones": float(habs),
            "banos": float(banos),
            "garajes": float(garajes),
            "tipo_inmueble": tipo,
            "estado_inmueble": estado,
            "fuente": fuente,
            "url": url,
            "titulo": f"{tipo.capitalize()} en venta en {barrio}",
            "precio_m2": precio_m2,
            "rentabilidad_potencial": float(rentabilidad),
            "score_inversion": float(score),
        })
        
    return pd.DataFrame(rows)

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

def resolve_municipio_and_sector(row):
    dep = row["departamento"]
    city_token = str(row["city_token"]).lower().strip()
    
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
        
    sector = str(row.get("ubicacion_clean", "Sector")).strip()
    
    if dep in MUNICIPIOS_BY_DEP:
        norm_text = str(row.get("ubicacion_norm", "")).lower()
        clean_text = str(row.get("ubicacion_clean", "")).lower()
        search_text = f" {clean_text} {norm_text} "
        
        matched_key = None
        sorted_keys = sorted(MUNICIPIOS_BY_DEP[dep].keys(), key=len, reverse=True)
        for key in sorted_keys:
            if f" {key} " in search_text:
                matched_key = key
                break
                
        if matched_key:
            municipio = MUNICIPIOS_BY_DEP[dep][matched_key]
            sector_clean = re.sub(rf"\b{matched_key}\b", "", clean_text, flags=re.IGNORECASE).strip()
            sector_clean = re.sub(r"\s+", " ", sector_clean).strip().title()
            if sector_clean:
                sector = sector_clean
            else:
                sector = "Centro"
                
    sector = sector.title()
    if sector.lower() == municipio.lower():
        sector = "Centro"
        
    return pd.Series([municipio, sector])

@st.cache_resource(show_spinner=False)
def load_inmuebles_data():
    """Carga los datos desde S3 si está disponible, o cae al mock de demostración."""
    if s3_active:
        try:
            bucket = aws_config.get("s3_bucket_name")
            fs = s3fs.S3FileSystem(
                key=aws_config.get("aws_access_key_id") or None,
                secret=aws_config.get("aws_secret_access_key") or None
            )
            s3_path = f"{bucket}/gold/app_inmuebles_scored/"
            
            # Cargar un subset inicial para no agotar la memoria en la versión light
            dataset = ds.dataset(s3_path, filesystem=fs, format="parquet")
            table = dataset.to_table(
                columns=["city_token", "market_token", "ubicacion_norm", "precio_num", 
                         "area_m2", "habitaciones", "banos", "garajes", "tipo_inmueble", 
                         "estado_inmueble", "fuente", "url", "titulo", "rentabilidad_potencial", "score_inversion"]
            )
            df = table.to_pandas()
            
            # Limpiar columnas
            if "ubicacion_norm" in df.columns:
                df["ubicacion_clean"] = df["ubicacion_norm"].astype(str).str.replace(r"\|.*", "", regex=True).str.strip()
            else:
                df["ubicacion_clean"] = "Desconocida"
                
            df["precio_m2"] = df["precio_num"] / df["area_m2"]
            df["departamento"] = df.apply(get_departamento, axis=1)
            
            # Resolver municipio y sector
            resolved = df.apply(resolve_municipio_and_sector, axis=1)
            df["municipio"] = resolved[0]
            df["sector"] = resolved[1]
            return df, "S3 Live"
        except Exception:
            pass
            
    # Fallback
    df = _generate_demo_dataset()
    df["departamento"] = df.apply(get_departamento, axis=1)
    
    # Resolver municipio y sector para demo
    resolved = df.apply(resolve_municipio_and_sector, axis=1)
    df["municipio"] = resolved[0]
    df["sector"] = resolved[1]
    return df, "Local Demo"

# Carga de datos
df_master, source_name = load_inmuebles_data()

# ══════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(
        '<div style="font-family:Playfair Display,serif;font-size:1.3rem;'
        'font-weight:900;color:white;margin-bottom:.2rem">◈ REA Light</div>'
        '<div style="font-size:.62rem;letter-spacing:.15em;color:#b8935a;'
        'text-transform:uppercase;margin-bottom:1.2rem">Real Estate Analyst · Colombia</div>',
        unsafe_allow_html=True,
    )
    
    # Status de conexión
    status_color = "#2a9b6a" if source_name == "S3 Live" else "#b8935a"
    st.markdown(
        f'<div style="display:inline-block;background:{status_color}22;border:1px solid {status_color};'
        f'padding:.3rem .8rem;border-radius:2px;font-size:.7rem;color:{status_color};'
        f'font-weight:bold;letter-spacing:1px;text-transform:uppercase;margin-bottom:1.5rem">'
        f'🔌 Origen: {source_name}'
        f'</div>',
        unsafe_allow_html=True
    )
    
    # KPIs rápidos en el sidebar
    st.markdown('<div class="section-label">Estadísticas del Catálogo</div>', unsafe_allow_html=True)
    N_total = len(df_master)
    N_ciudades = df_master["city_token"].nunique()
    med_precio = df_master["precio_num"].median()
    
    for label, val in [
        ("Inmuebles Activos", f"{N_total:,}"),
        ("Ciudades Cubiertas", f"{N_ciudades}"),
        ("Precio Mediano", f"${med_precio/1e6:.0f}M COP"),
        ("Soporte Técnico", "Opcional / Libre")
    ]:
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;font-size:.78rem;'
            f'padding:.22rem 0;border-bottom:1px solid rgba(255,255,255,.07)">'
            f'<span style="color:rgba(255,255,255,.45)">{label}</span>'
            f'<span style="color:white;font-family:DM Mono,monospace">{val}</span></div>',
            unsafe_allow_html=True,
        )
        
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Limpiar Caché local", key="btn_clear_cache"):
        st.cache_data.clear()
        st.rerun()
        
    st.markdown(
        '<div style="font-size:.62rem;color:rgba(255,255,255,.22);margin-top:2.5rem;line-height:1.6">'
        'No constituye asesoría financiera certificada.<br>'
        'Propiedades extraídas de portales inmobiliarios de acceso público.</div>',
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════════
# HEADER & TICKER
# ══════════════════════════════════════════════════════════════════
ticker_txt = "  ·  ".join([
    f"◈ {N_total:,} INMUEBLES DISPONIBLES",
    f"PRECIO MEDIANO ${med_precio/1e6:.0f}M COP",
    f"{N_ciudades} CIUDADES COLOMBIANAS",
    "ANÁLISIS BAJO COSTO SIN INFRAESTRUCTURA ACTIVA",
    "MODELO ESTIMADOR: MODO DEMOSTRACIÓN",
])
st.markdown(f'<div class="ticker">{ticker_txt}</div>', unsafe_allow_html=True)

col_h, col_s = st.columns([3, 1])
with col_h:
    st.markdown('<div class="section-label">Plataforma Inmobiliaria — Versión Ultra-Light</div>', unsafe_allow_html=True)
    st.title("Real Estate Analyst Colombia")
    st.markdown(
        f'<p style="color:var(--muted);font-size:.88rem;margin-top:-.5rem">'
        f'Buscador optimizado y simulador financiero para corredores e inversionistas</p>',
        unsafe_allow_html=True,
    )

st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# TABS DEFINITION
# ══════════════════════════════════════════════════════════════════
tab_finance, tab_market, tab_val_teaser = st.tabs([
    "Simulador Financiero y Propiedades",
    "Análisis de Mercados",
    "Valoración IA (Premium Teaser)"
])

# ══════════════════════════════════════════════════════════════════
# TAB 1: SIMULADOR FINANCIERO Y PROPIEDADES (Happy Path)
# ══════════════════════════════════════════════════════════════════
with tab_finance:
    st.markdown("## Simulación Financiera del Cliente")
    st.markdown(
        '<div class="disclaimer">'
        'Ingresa las capacidades financieras del cliente comprador. '
        'El simulador calculará el endeudamiento máximo permitido (tasa de esfuerzo bancaria del 30%) '
        'y filtrará los inmuebles que se ajusten a su presupuesto real.'
        '</div>',
        unsafe_allow_html=True
    )
    
    # 1. Inputs Financieros
    with st.expander("▸ Perfil Financiero del Comprador", expanded=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            salario = st.number_input(
                "Ingresos mensuales netos (COP)", 
                min_value=1_000_000, 
                max_value=100_000_000, 
                value=4_500_000, 
                step=500_000,
                format="%d",
                key="in_salario"
            )
        with f2:
            ahorros = st.number_input(
                "Ahorros iniciales (Cuota inicial) (COP)", 
                min_value=0, 
                max_value=2_000_000_000, 
                value=50_000_000, 
                step=5_000_000,
                format="%d",
                key="in_ahorros"
            )
        with f3:
            patrimonio = st.number_input(
                "Otros activos o patrimonio disponible (COP)", 
                min_value=0, 
                max_value=2_000_000_000, 
                value=15_000_000, 
                step=2_000_000,
                format="%d",
                key="in_patrimonio"
            )
            
        f_cond1, f_cond2, f_cond3 = st.columns(3)
        with f_cond1:
            tasa_anual = st.slider(
                "Tasa de Interés del Crédito (E.A. %)", 
                min_value=6.0, 
                max_value=25.0, 
                value=12.0, 
                step=0.5,
                key="in_tasa"
            )
        with f_cond2:
            plazo_anos = st.selectbox(
                "Plazo de Pago (Años)", 
                options=[10, 15, 20, 30], 
                index=2,
                key="in_plazo"
            )
        with f_cond3:
            tasa_esfuerzo = st.slider(
                "Límite de cuota sobre salario (%)",
                min_value=20,
                max_value=45,
                value=30,
                step=5,
                help="Bancos colombianos permiten máximo el 30% del salario para cuota hipotecaria VIS y 40% para No-VIS.",
                key="in_esfuerzo"
            )

        # Validación de Aptitud para Crédito
        apto_credito = True
        motivos_no_apto = []
        
        if salario < 1_400_000:
            apto_credito = False
            motivos_no_apto.append("Ingresos mensuales menores al salario mínimo legal vigente (COP 1.4M).")
        if (ahorros + patrimonio) < 10_000_000:
            apto_credito = False
            motivos_no_apto.append("Aporte propio disponible (ahorros + patrimonio) inferior a COP 10M, insuficiente para cubrir una cuota inicial mínima.")

        # Cálculos de endeudamiento según estándar financiero Colombiano
        # Convertir tasa E.A. a mensual vencida
        tasa_mensual = (1 + tasa_anual / 100) ** (1 / 12) - 1
        plazo_meses = plazo_anos * 12
        
        # Cuota máxima mensual permitida por la tasa de esfuerzo
        cuota_maxima = salario * (tasa_esfuerzo / 100)
        
        # Monto del préstamo máximo que se puede amortizar con esa cuota
        if apto_credito:
            if tasa_mensual > 0:
                prestamo_maximo = cuota_maxima * ((1 - (1 + tasa_mensual) ** (-plazo_meses)) / tasa_mensual)
            else:
                prestamo_maximo = cuota_maxima * plazo_meses
            presupuesto_total = ahorros + patrimonio + prestamo_maximo
        else:
            prestamo_maximo = 0.0
            presupuesto_total = float(ahorros + patrimonio)
        
        # Visualización de resultados financieros
        st.markdown('<hr style="border-color:rgba(255,255,255,.05);margin:.8rem 0">', unsafe_allow_html=True)
        r1, r2, r3, r4 = st.columns(4)
        
        if apto_credito:
            r1.metric("Cuota Mensual Máx.", fmt_cop(cuota_maxima), help="Basado en el porcentaje de ingresos permitido.")
            r2.metric("Crédito Hipotecario Viable", fmt_cop(prestamo_maximo), help="Monto máximo que el banco prestaría.")
            r3.metric("Aporte Propio (Cuota Inicial)", fmt_cop(ahorros + patrimonio), help="Ahorros + Otros Activos.")
            r4.metric("Presupuesto de Compra Máximo", fmt_cop(presupuesto_total), help="Patrimonio propio + capacidad máxima de crédito.")
        else:
            r1.metric("Cuota Mensual Máx.", "N/A", help="No califica para financiación.")
            r2.metric("Crédito Hipotecario Viable", "$0", help="No apto para crédito bancario.")
            r3.metric("Aporte Propio (Cuota Inicial)", fmt_cop(ahorros + patrimonio), help="Ahorros + Otros Activos.")
            r4.metric("Presupuesto de Compra Máximo", fmt_cop(presupuesto_total), help="Presupuesto limitado únicamente a fondos propios.")

        if not apto_credito:
            st.markdown(
                f'<div style="background:rgba(217, 83, 79, 0.08);border:1px solid rgba(217, 83, 79, 0.4);'
                f'padding:1.2rem;border-radius:4px;margin-top:1.2rem">'
                f'  <div style="font-weight:bold;color:#f2dede;font-size:.92rem;margin-bottom:.5rem;display:flex;align-items:center">'
                f'    ⚠️ PERFIL FINANCIERO CON RESTRICCIONES (CLIENTE NO APTO PARA CRÉDITO DE VIVIENDA)'
                f'  </div>'
                f'  <p style="font-size:.78rem;color:rgba(255,255,255,.7);line-height:1.6;margin:0">'
                f'    El cliente no cumple con las condiciones básicas para acceder a una financiación hipotecaria o adquirir un inmueble en Colombia por las siguientes razones:<br>'
                f'    <ul style="margin-top:.4rem;margin-bottom:.4rem;padding-left:1.2rem">'
                f'      {"".join([f"<li>{m}</li>" for m in motivos_no_apto])}'
                f'    </ul>'
                f'    <strong>Acción comercial recomendada:</strong> Sugerir al cliente un plan de ahorro programado para la cuota inicial o consolidar ingresos familiares con un codeudor para incrementar los ingresos demostrables ante las entidades bancarias.'
                f'  </p>'
                f'</div>',
                unsafe_allow_html=True
            )

    # 2. Buscador e Inmuebles Viables
    st.markdown("### Inmuebles que se ajustan al Presupuesto")
    
    if not apto_credito:
        st.markdown(
            '<div style="background:rgba(255,255,255,.02);border:1px dashed rgba(255,255,255,.1);'
            'padding:2.5rem;text-align:center;color:var(--muted);font-size:.85rem;border-radius:4px;margin-top:1rem">'
            '❌ <strong>Búsqueda deshabilitada</strong>: El cliente no es apto para crédito ni adquisición de vivienda.<br>'
            '<span style="font-size:.75rem; color:var(--muted)">Ajusta los parámetros financieros a la izquierda (Ingresos mensuales o Cuota inicial) para desbloquear el buscador.</span>'
            '</div>',
            unsafe_allow_html=True
        )
    else:
        with st.expander("▸ Ajustar Filtros de Propiedad", expanded=True):
            r1c1, r1c2, r1c3, r1c4 = st.columns(4)
            with r1c1:
                deps_disp = sorted(list(df_master["departamento"].dropna().unique()))
                default_deps = [d for d in ["Bogotá D.C.", "Antioquia"] if d in deps_disp] or [deps_disp[0]]
                departamento_sel = st.multiselect(
                    "Filtrar por Departamento", 
                    options=deps_disp, 
                    default=default_deps,
                    key="filter_departamento"
                )
            with r1c2:
                # Filtrar ciudades por departamentos seleccionados
                if departamento_sel:
                    df_temp_mun = df_master[df_master["departamento"].isin(departamento_sel)]
                else:
                    df_temp_mun = df_master
                mun_disp = sorted(list(df_temp_mun["municipio"].dropna().unique()))
                municipio_sel = st.multiselect(
                    "Ciudad / Municipio", 
                    options=mun_disp, 
                    default=[],
                    help="Si se deja vacío, se simularán todas las ciudades del departamento.",
                    key="filter_municipio"
                )
            with r1c3:
                # Filtrar sectores por depto y ciudad seleccionados
                df_temp_sec = df_temp_mun.copy()
                if municipio_sel:
                    df_temp_sec = df_temp_sec[df_temp_sec["municipio"].isin(municipio_sel)]
                sec_disp = sorted(list(df_temp_sec["sector"].dropna().unique()))
                sector_sel = st.multiselect(
                    "Sector / Barrio", 
                    options=sec_disp, 
                    default=[],
                    help="Si se deja vacío, se simularán todos los sectores.",
                    key="filter_sector"
                )
            with r1c4:
                tipos_disp = list(df_master["tipo_inmueble"].unique())
                tipo_sel = st.multiselect(
                    "Tipo de Inmueble", 
                    options=tipos_disp, 
                    default=tipos_disp,
                    format_func=lambda x: str(x).title(),
                    key="filter_type"
                )
                
            r2c1, r2c2, r2c3 = st.columns([1, 1, 2])
            with r2c1:
                habs_min = st.selectbox(
                    "Habitaciones Mínimas", 
                    options=[1, 2, 3, 4], 
                    index=1,
                    key="filter_rooms"
                )
            with r2c2:
                order_by = st.selectbox(
                    "Ordenar resultados por",
                    options=[
                        "Mejor Inversión (Score / Rentabilidad)",
                        "Menor Precio",
                        "Mayor Precio",
                        "Mayor Área"
                    ],
                    index=0,
                    key="filter_order"
                )
            with r2c3:
                # Rango de precio interactivo en millones
                max_slider_val = max(50, int(presupuesto_total / 1e6))
                min_slider_val = min(30, max_slider_val - 10)
                default_slider_range = (min(80, max_slider_val - 20), max_slider_val)
                
                price_range_m = st.slider(
                    "Rango de Precio de Compra (Millones COP)",
                    min_value=int(min_slider_val),
                    max_value=int(max_slider_val),
                    value=(int(default_slider_range[0]), int(default_slider_range[1])),
                    step=5,
                    key="filter_price_range_m"
                )
                
        # Filtrar dataframe master
        df_filtered = df_master.copy()
        
        # Filtro de precio por slider en millones COP
        df_filtered = df_filtered[
            (df_filtered["precio_num"] >= price_range_m[0] * 1e6) & 
            (df_filtered["precio_num"] <= price_range_m[1] * 1e6)
        ]
        
        # Aplicar filtros geográficos y tipología
        if departamento_sel:
            df_filtered = df_filtered[df_filtered["departamento"].isin(departamento_sel)]
        if municipio_sel:
            df_filtered = df_filtered[df_filtered["municipio"].isin(municipio_sel)]
        if sector_sel:
            df_filtered = df_filtered[df_filtered["sector"].isin(sector_sel)]
        if tipo_sel:
            df_filtered = df_filtered[df_filtered["tipo_inmueble"].isin(tipo_sel)]
        if habs_min:
            df_filtered = df_filtered[df_filtered["habitaciones"] >= habs_min]
            
        # Ordenar resultados
        if order_by == "Mejor Inversión (Score / Rentabilidad)":
            sort_cols = []
            sort_ascending = []
            if "score_inversion" in df_filtered.columns:
                sort_cols.append("score_inversion")
                sort_ascending.append(False)
            if "rentabilidad_potencial" in df_filtered.columns:
                sort_cols.append("rentabilidad_potencial")
                sort_ascending.append(False)
            if sort_cols:
                df_filtered = df_filtered.sort_values(sort_cols, ascending=sort_ascending)
            else:
                df_filtered = df_filtered.sort_values("precio_num", ascending=True)
        elif order_by == "Menor Precio":
            df_filtered = df_filtered.sort_values("precio_num", ascending=True)
        elif order_by == "Mayor Precio":
            df_filtered = df_filtered.sort_values("precio_num", ascending=False)
        elif order_by == "Mayor Área":
            df_filtered = df_filtered.sort_values("area_m2", ascending=False)
        
        # Mostrar resultados
        st.markdown(
            f'<div class="section-label">{len(df_filtered)} Inmuebles Viables Encontrados</div>',
            unsafe_allow_html=True
        )
        
        if df_filtered.empty:
            st.warning(
                "⚠️ No se encontraron inmuebles en ese rango de precios para las ubicaciones seleccionadas. "
                "Prueba a aumentar los ingresos del cliente, ampliar el ahorro o seleccionar más regiones."
            )
        else:
            # Dividir resultados en tabla y tarjetas interactivas
            st.write("A continuación se presentan los inmuebles ordenados según el criterio seleccionado dentro del rango de presupuesto.")
            
            # Grid de tarjetas de inmuebles (Happy Path con enlace directo)
            top_properties = df_filtered.head(12)
            cols_cards = st.columns(3)
            
            for idx, (_, row) in enumerate(top_properties.iterrows()):
                col_target = cols_cards[idx % 3]
                
                titulo = str(row.get("titulo", "Propiedad Inmobiliaria"))
                titulo_short = titulo[:38] + "..." if len(titulo) > 38 else titulo
                precio_val = row["precio_num"]
                precio_str = fmt_cop(precio_val)
                ciudad_str = str(row.get("municipio", "Municipio"))
                barrio_str = str(row.get("sector", "Sector"))
                url_target = str(row.get("url", "#"))
                fuente_portal = str(row.get("fuente", "Portal")).replace("_", " ").title()
                
                area = row.get("area_m2", 0)
                habs = row.get("habitaciones", 0)
                banos = row.get("banos", 0)
                
                rent_val = row.get("rentabilidad_potencial")
                score_val = row.get("score_inversion")
                
                rent_txt = f"{rent_val * 100:.1f}% E.A." if pd.notna(rent_val) and rent_val > 0 else "N/D"
                score_txt = f"{int(score_val)} pts" if pd.notna(score_val) and score_val > 0 else "N/D"
                
                col_target.markdown(
                    f'<div style="background:var(--surface2);border:1px solid var(--border);'
                    f'padding:1.1rem;border-radius:4px;margin-bottom:1rem;min-height:245px;'
                    f'display:flex;flex-direction:column;justify-content:space-between">'
                    f'  <div>'
                    f'    <div style="font-size:.62rem;color:var(--gold);text-transform:uppercase;letter-spacing:1px;font-weight:bold;margin-bottom:.3rem">{ciudad_str} · {barrio_str}</div>'
                    f'    <div style="font-size:.82rem;font-weight:bold;color:white;margin-bottom:.5rem;line-height:1.3">{titulo_short}</div>'
                    f'    <div style="font-size:1.15rem;font-weight:800;color:var(--gold);margin-bottom:.6rem">{precio_str}</div>'
                    f'    <div style="font-size:.72rem;color:var(--muted);margin-bottom:.8rem">'
                    f'      📐 {area:.0f}m² &nbsp;&nbsp; 🛏️ {habs:.0f} Hab &nbsp;&nbsp; 🚿 {banos:.0f} Baños'
                    f'    </div>'
                    f'    <div style="font-size:.7rem;color:white;background:rgba(255,255,255,.03);padding:.35rem .5rem;border-radius:2px;margin-bottom:.5rem">'
                    f'      📈 Rentabilidad: <span style="color:var(--gold);font-weight:bold">{rent_txt}</span> &nbsp;&nbsp;&nbsp;&nbsp;'
                    f'      🤖 IA Score: <span style="color:#2a9b6a;font-weight:bold">{score_txt}</span>'
                    f'    </div>'
                    f'  </div>'
                    f'  <div style="border-top:1px solid rgba(255,255,255,.05);padding-top:.7rem;display:flex;justify-content:space-between;align-items:center">'
                    f'    <span style="font-size:.65rem;color:var(--muted)">Soporte: <span style="color:var(--gold)">✔ Incluido</span></span>'
                    f'    <a href="{url_target}" target="_blank" style="font-size:.72rem;color:white;background:var(--gold);'
                    f'    padding:.35rem .8rem;border-radius:2px;text-decoration:none;font-weight:bold;transition:all 0.2s">Ver en {fuente_portal} ↗</a>'
                    f'  </div>'
                    f'</div>',
                    unsafe_allow_html=True
                )
                
            st.markdown("<br>", unsafe_allow_html=True)
            # Mostrar tabla detallada completa abajo
            with st.expander("▸ Ver listado completo en formato tabla", expanded=False):
                df_table = df_filtered[["titulo", "municipio", "sector", "precio_num", "area_m2", "habitaciones", "fuente"]].copy()
                df_table.columns = ["Título", "Ciudad / Municipio", "Sector / Barrio", "Precio (COP)", "Área (m²)", "Hab.", "Portal Fuente"]
                
                st.dataframe(
                    df_table.style.format({
                        "Precio (COP)": lambda x: f"${int(x):,}".replace(",", "."),
                        "Área (m²)": "{:.0f}",
                        "Hab.": "{:.0f}"
                    }),
                    use_container_width=True,
                    hide_index=True
                )

# ══════════════════════════════════════════════════════════════════
# TAB 2: ANÁLISIS DE MERCADOS (Dashboard)
# ══════════════════════════════════════════════════════════════════
with tab_market:
    st.markdown("## Inteligencia de Mercado Inmobiliario")
    st.markdown(
        '<div class="disclaimer">'
        'Métricas macro agregadas de la oferta de inmuebles en Colombia. '
        'Estos gráficos ayudan a identificar los precios medianos por metro cuadrado en las distintas ciudades.'
        '</div>',
        unsafe_allow_html=True
    )
    
    # Agrupaciones macro para gráficos
    stats_ciudad = df_master.groupby("city_token").agg(
        ofertas=("precio_num", "count"),
        precio_mediano=("precio_num", "median"),
        precio_m2_mediano=("precio_m2", "median")
    ).reset_index().sort_values("precio_m2_mediano", ascending=True)
    
    m_col1, m_col2 = st.columns(2)
    
    with m_col1:
        st.markdown('<div class="section-label">Precio Mediano por m² por Ciudad</div>', unsafe_allow_html=True)
        fig_m2 = go.Figure(go.Bar(
            y=stats_ciudad["city_token"].str.replace("_", " ").str.title(),
            x=stats_ciudad["precio_m2_mediano"] / 1e6,
            orientation="h",
            marker=dict(color=stats_ciudad["precio_m2_mediano"], colorscale=[[0, "#dfc69f"], [1, "#b8935a"]], showscale=False),
            text=(stats_ciudad["precio_m2_mediano"] / 1e6).apply(lambda x: f"${x:.2f}M"),
            textposition="outside",
        ))
        dark_layout(fig_m2, height=400, xaxis=dict(title="Millones COP / m²"), yaxis=dict(showgrid=False))
        st.plotly_chart(fig_m2, width="stretch")
        
    with m_col2:
        st.markdown('<div class="section-label">Distribución de Ofertas por Ciudad</div>', unsafe_allow_html=True)
        fig_pie = go.Figure(go.Pie(
            labels=stats_ciudad["city_token"].str.replace("_", " ").str.title(),
            values=stats_ciudad["ofertas"],
            hole=0.45,
            marker=dict(colors=["#b8935a", "#d4aa72", "#1a6b4a", "#2a6ab8", "#8b2020", "#4b208b"]),
            textinfo="percent+label",
            textfont=dict(size=10)
        ))
        dark_layout(fig_pie, height=400)
        st.plotly_chart(fig_pie, width="stretch")
        
    st.markdown('<hr class="gold-rule">', unsafe_allow_html=True)
    
    # Análisis VIS vs No-VIS
    st.markdown('<div class="section-label">Segmentación VIS (Vivienda de Interés Social)</div>', unsafe_allow_html=True)
    
    vis_col1, vis_col2 = st.columns([1, 2])
    with vis_col1:
        # Clasificación simplificada colombiana: VIS <= 150 SMMLV (~$195M - $260M en Bogotá/zonas urbanas)
        # Usamos 200 Millones COP como límite promedio aproximado.
        df_master["segmento"] = df_master["precio_num"].apply(lambda x: "VIS (≤$200M)" if x <= 200_000_000 else "No VIS (>$200M)")
        seg_counts = df_master["segmento"].value_counts()
        
        fig_seg = go.Figure(go.Pie(
            labels=seg_counts.index,
            values=seg_counts.values,
            hole=0.5,
            marker=dict(colors=["#2a6ab8", "#b8935a"]),
            textinfo="percent+value"
        ))
        dark_layout(fig_seg, height=260)
        st.plotly_chart(fig_seg, width="stretch")
        
    with vis_col2:
        st.markdown(
            '<div style="background:var(--surface2);border:1px solid var(--border);padding:1.2rem;border-radius:4px;height:100%">'
            '  <div style="font-weight:bold;color:white;font-size:.9rem;margin-bottom:.5rem">Análisis de Oportunidades y VIS</div>'
            f'  <p style="font-size:.78rem;color:var(--ink);line-height:1.6">'
            f'    Actualmente, el segmento <strong>VIS representa el {(seg_counts.get("VIS (≤$200M)", 0)/N_total*100):.1f}%</strong> del catálogo total de inmuebles.<br>'
            f'    Para los corredores inmobiliarios, el segmento No-VIS sigue acumulando la mayor liquidez y variedad de portales '
            f'    de oferta de vivienda de segunda mano en ciudades capitales (Bogotá, Medellín y Cali).<br><br>'
            f'    <strong>Margen de negociación:</strong> La discrepancia de precio entre diferentes portales para una misma zona indica '
            f'    un margen promedio del 8% al 12% negociable directamente con los vendedores.'
            '  </p>'
            '</div>',
            unsafe_allow_html=True
        )

# ══════════════════════════════════════════════════════════════════
# TAB 3: VALORACIÓN PREMIUM TEASER
# ══════════════════════════════════════════════════════════════════
with tab_val_teaser:
    st.markdown("## Valoración Estimada por Inteligencia Artificial")
    st.markdown(
        '<div class="disclaimer">'
        'Nuestra inteligencia artificial entrena modelos predictivos XGBoost cruzando '
        'información geográfica detallada para calcular el precio justo de un inmueble.'
        '</div>',
        unsafe_allow_html=True
    )
    
    t_col1, t_col2 = st.columns([1, 1])
    
    with t_col1:
        st.markdown('<div class="section-label">Ficha del Inmueble a tasar</div>', unsafe_allow_html=True)
        t_area = st.number_input("Área en m²", min_value=20, max_value=800, value=85, key="teaser_area")
        t_habs = st.number_input("Habitaciones", min_value=1, max_value=8, value=3, key="teaser_habs")
        t_banos = st.number_input("Baños", min_value=1, max_value=6, value=2, key="teaser_banos")
        t_garajes = st.number_input("Garajes", min_value=0, max_value=4, value=1, key="teaser_garajes")
        t_ciudad = st.selectbox(
            "Ciudad", 
            options=sorted(list(df_master["city_token"].unique())),
            format_func=lambda x: str(x).replace("_", " ").title(),
            key="teaser_city"
        )
        t_tipo = st.selectbox("Tipo de Propiedad", options=["apartamento", "casa"], key="teaser_type")
        
        btn_teaser = st.button("Calcular Tasación Inteligente ◈", key="teaser_btn")
        
    with t_col2:
        st.markdown('<div class="section-label">Resultado de la Tasación</div>', unsafe_allow_html=True)
        
        if btn_teaser:
            # Mostrar teaser de licenciamiento
            st.markdown(
                f'<div style="background:var(--surface2);border:1px solid var(--border);'
                f'border-left:4px solid var(--gold);padding:1.5rem;border-radius:4px;text-align:center">'
                f'  <div style="font-size:2.5rem;margin-bottom:.8rem">🔒</div>'
                f'  <div style="font-weight:bold;color:white;font-size:1.1rem;margin-bottom:.5rem">'
                f'    Funcionalidad Premium Desactivada'
                f'  </div>'
                f'  <p style="font-size:.78rem;color:var(--muted);line-height:1.6;max-width:380px;margin:0 auto 1.2rem">'
                f'    La valoración automatizada con el modelo XGBoost (calibrado con +100.000 datos históricos) '
                f'    y el análisis de comparables con RAG LLM están bloqueados temporalmente en la versión libre para evitar costos de nube.'
                f'  </p>'
                f'  <div style="background:rgba(184,147,90,.1);border:1px solid var(--gold);'
                f'  padding:.6rem 1rem;border-radius:2px;font-size:.75rem;color:var(--gold);'
                f'  font-weight:bold;display:inline-block;cursor:pointer">'
                f'    📧 CONTACTAR A VENTAS PARA LICENCIA FULL'
                f'  </div>'
                f'</div>',
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                '<div style="background:var(--surface2);border:1px solid var(--border);'
                'padding:2.5rem;text-align:center;color:var(--muted);font-size:.82rem;'
                'border-radius:4px">'
                'Completa la ficha técnica a la izquierda y presiona<br>'
                '<strong style="color:white">Calcular Tasación Inteligente ◈</strong><br>para ver el prototipo.'
                '</div>',
                unsafe_allow_html=True,
            )