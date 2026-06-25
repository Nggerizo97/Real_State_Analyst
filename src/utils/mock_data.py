import numpy as np
import pandas as pd

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
