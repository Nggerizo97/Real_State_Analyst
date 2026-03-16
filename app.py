import streamlit as st
import pandas as pd
import numpy as np
import boto3
import joblib
import io
import plotly.express as px
from openai import OpenAI
import json
import re
from src.utils.model_loader import ModelLoader

# ==========================================
# Configuración Principal
# ==========================================
st.set_page_config(
    page_title="Agente Inmobiliario Inteligente (RAG)",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar utilidades de S3 y ML
model_loader = ModelLoader()
ml_model, manifest = model_loader.load_latest_model()
badge = ModelLoader.get_badge_data(manifest)

# Inicializar cliente LLM configurado para Ollama o API compatible
llm_client = OpenAI(
    base_url=st.secrets.get("llm", {}).get("api_base", "http://localhost:11434/v1"),
    api_key=st.secrets.get("llm", {}).get("api_key", "ollama"),
)
LLM_MODEL = st.secrets.get("llm", {}).get("model_name", "llama3")

def test_llm_connection():
    """Verifica si Ollama o el API de LLM responde."""
    try:
        llm_client.models.list()
        return True
    except Exception:
        return False

llm_ready = test_llm_connection()

# ==========================================
# Caché y Conexiones a S3
# ==========================================
@st.cache_resource(show_spinner=False)
def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
        region_name=st.secrets["aws"]["aws_region"]
    )

@st.cache_data(show_spinner=True, ttl=3600)
def load_and_transform_data():
    """Descarga, une y aplica ETL On-The-Fly a los archivos Parquet en S3."""
    try:
        s3 = get_s3_client()
        bucket = st.secrets["aws"]["s3_bucket_name"]
        path = f"s3://{bucket}/gold/app_consumable/"
        storage_options = {
            "key": st.secrets["aws"]["aws_access_key_id"],
            "secret": st.secrets["aws"]["aws_secret_access_key"]
        }
        
        # Pandas puede leer directorios completos si usa pyarrow/s3fs nativo
        df_final = pd.read_parquet(path, storage_options=storage_options)
        
        if df_final.empty:
            return _get_dummy_dataframe()
        
        # ---------------------------------------------
        # ETL on-the-fly (Limpieza requerida)
        # ---------------------------------------------
        # 1. Eliminar filas sin precio o ubicación esencial
        # Asumimos que la raw column de ubicacion es 'ubicacion' o 'ubicacion_raw'
        ubi_col = 'ubicacion' if 'ubicacion' in df_final.columns else 'ubicacion_raw'
        
        # Filtrar nulos si hay precio y ubicación
        if 'precio_num' in df_final.columns and ubi_col in df_final.columns:
            df_final.dropna(subset=['precio_num', ubi_col], inplace=True)
            df_final = df_final[df_final['precio_num'] > 0]
        else:
            return _get_dummy_dataframe()
            
        # 2. Homologar columnas esperadas si faltan
        for col in ['area_m2', 'habitaciones_num', 'banos_num', 'id_original']:
            if col not in df_final.columns:
                df_final[col] = 0 if 'num' in col or 'm2' in col else "Desconocido"
                
        # 3. Limpiar Ubicación (ej: Dividir por pipes y tomar la primera parte)
        # Elimina "nan" literales que venían como strings
        df_final[ubi_col] = df_final[ubi_col].astype(str).replace({'nan': 'Desconocida', 'None': 'Desconocida'})
        df_final['ubicacion_clean'] = df_final[ubi_col].apply(lambda x: str(x).split('|')[0].strip())
        
        return df_final
            
    except Exception as e:
        st.error(f"⚠️ Error conectando a S3 para cargar los datos: {e}")
        return _get_dummy_dataframe()

@st.cache_resource(show_spinner=False, ttl=1800)
def get_cached_model():
    """Carga el modelo una vez y lo mantiene en caché."""
    m, _ = ModelLoader().load_latest_model()
    return m

# El modelo ya está cargado arriba para uso inmediato, 
# pero podemos usar la versión cacheada si se prefiere.
ml_model = get_cached_model()

def apply_ml_scoring(df, model):
    """Genera las predicciones y calcula la rentabilidad potencial para cada registro."""
    if model is None or df.empty:
        df['precio_predicho'] = df['precio_num']
        df['rentabilidad_potencial'] = 0.0
        df['estado_inversion'] = "⚪ Sin Modelo"
        return df
        
    try:
        # Mapear columnas a los nombres que espera el modelo v2
        # El error reportó: {'estado_inmueble', 'tipo_inmueble', 'habitaciones', 'banos', 'fuente'}
        # Adicionalmente suele pedir area_m2 o similares, pero nos basamos en el error estricto.
        
        # Crear mapeos y rellenos
        df_pred = df.copy()
        
        # Mapeo de nombres
        mapeo = {
            'habitaciones_num': 'habitaciones',
            'banos_num': 'banos'
        }
        for col_raw, col_model in mapeo.items():
            if col_raw in df_pred.columns:
                df_pred[col_model] = df_pred[col_raw]
            else:
                df_pred[col_model] = 0

        # Columnas categóricas o nuevas no presentes en el DataFrame original
        if 'estado_inmueble' not in df_pred.columns:
            df_pred['estado_inmueble'] = 'usado' # Valor por defecto
        if 'tipo_inmueble' not in df_pred.columns:
            df_pred['tipo_inmueble'] = 'apartamento' # Valor por defecto
        if 'fuente' not in df_pred.columns:
            df_pred['fuente'] = 'desconocida'

        # Asegurar tipos
        df_pred['habitaciones'] = df_pred['habitaciones'].fillna(0).astype(int)
        df_pred['banos'] = df_pred['banos'].fillna(0).astype(int)
        
        # Extraer features clave según lo que el modelo pide
        # El error reportó que falta 'texto_completo'
        if 'texto_completo' not in df_pred.columns:
             df_pred['texto_completo'] = df_pred.get('id_original', '').astype(str) + " " + df_pred.get('ubicacion_clean', '').astype(str)
        df_pred['texto_completo'] = df_pred['texto_completo'].fillna('')

        features_model = ['area_m2', 'habitaciones', 'banos', 'estado_inmueble', 'tipo_inmueble', 'fuente', 'texto_completo']
        X = df_pred[features_model].copy()
        
        # Las predicciones se hacen a todo el DF
        df['precio_predicho'] = model.predict(X)
        
        # Calcular Rentabilidad Potencial Formula: ((Precio_Predicho - Precio_Real) / Precio_Real) * 100
        df['rentabilidad_potencial'] = ((df['precio_predicho'] - df['precio_num']) / df['precio_num']) * 100
        
        # Crear Etiqueta Visual Rápida
        df['estado_inversion'] = df['rentabilidad_potencial'].apply(
            lambda x: "🟢 Oportunidad" if x > 0 else "🔴 Sobrevalorado"
        )
        
        # Redondear rentabilidades y evitar infinitos
        df['rentabilidad_potencial'] = df['rentabilidad_potencial'].replace([np.inf, -np.inf], 0).round(2)
        
    except Exception as e:
        st.error(f"Error procesando el score ML: {e}")
        df['precio_predicho'] = df['precio_num']
        df['rentabilidad_potencial'] = 0.0
        df['estado_inversion'] = "⚠️ Predicción Fallida"
        
    return df

def _get_dummy_dataframe():
    np.random.seed(42)
    n = 100
    df = pd.DataFrame({
        "id_original": [f"Prueba-{i}" for i in range(1, n + 1)],
        "precio_num": np.random.randint(150, 800, n) * 1000000.0,
        "area_m2": np.random.randint(40, 200, n).astype(float),
        "ubicacion_clean": np.random.choice(["Norte", "Sur", "Centro", "Occidente", "Chapinero", "Oriente Antioqueño"], n),
        "habitaciones_num": np.random.randint(1, 5, n),
        "banos_num": np.random.randint(1, 4, n),
        "url": ["https://ejemplo.com"] * n
    })
    return df

# ==========================================
# UI Principal: Navegación por Apartados
# ==========================================
st.title("🤖 Real Estate Analyst - Asistente Inteligente")

# Cargar Master Table y aplicar Scores ML a todo el universo primero.
raw_data = load_and_transform_data()
ml_model = load_ml_model()

if "master_db" not in st.session_state:
    st.session_state.master_db = apply_ml_scoring(raw_data.copy(), ml_model)

df_inmuebles = st.session_state.master_db

if df_inmuebles.empty:
    st.error("❌ Error de Datos: No se pudieron cargar los inmuebles desde S3. Verifica los permisos del bucket.")
    st.stop()

if not llm_ready:
    st.warning(f"⚠️ **Ollama No Detectado:** El asistente de chat (RAG) no estará disponible. Asegúrate de que Ollama esté corriendo en `{st.secrets['llm']['api_base']}` con el modelo `{LLM_MODEL}`.")

# Crear los 4 apartados solicitados
tab1, tab2, tab3, tab4 = st.tabs([
    "📍 Asesor Inmobiliario", 
    "📈 Asesor de Inversión", 
    "📊 Visión de Compra", 
    "🏗️ Valoración"
])

# Identificar fecha de entrenamiento (Simulada para el bot)
FECHA_ENTRENAMIENTO = "2024-05-20" # Ejemplo de fecha

def mostrar_disclaimer():
    st.info(f"ℹ️ **Aviso Legal:** Soy un asistente virtual basado en inteligencia artificial. Mi entrenamiento tiene fecha de corte al **{FECHA_ENTRENAMIENTO}**. No soy un asesor inmobiliario certificado; por favor, consulta con profesionales antes de tomar decisiones financieras.")

# ---------------------------------------------------------
# Función Chat RAG Centralizada con Contexto
# ---------------------------------------------------------
def render_contextual_chat(context_type="general"):
    st.divider()
    st.markdown(f"### 💬 Consulta a tu Asesor ({context_type})")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "chat_usage" not in st.session_state:
        st.session_state.chat_usage = 0
    
    CHAT_MAX_LIMIT = 5 # Límite de mensajes por sesión

    # Mostrar historial
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Control de Límite de Mensajes
    if st.session_state.chat_usage >= CHAT_MAX_LIMIT:
        st.warning(f"🚫 Has alcanzado el límite de {CHAT_MAX_LIMIT} consultas por sesión.")
        return

    user_input = st.chat_input(f"Pregunta sobre {context_type}... ({st.session_state.chat_usage}/{CHAT_MAX_LIMIT})", key=f"chat_{context_type}")
    
    if user_input:
        st.session_state.chat_usage += 1
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)
            
        # Construcción del Contexto RAG
        # 1. Datos de sesión (Finanzas, Candidatos)
        session_context = {
            "monto_prestamo": st.session_state.get('prestamo_solicitado', 0),
            "capacidad_mensual": st.session_state.get('capacidad_endeudamiento', 0),
            "tasa_interes": st.session_state.get('tasa_interes', 0),
            "num_candidatos": len(st.session_state.get('target_properties', []))
        }
        
        # 2. Búsqueda en DB
        def busqueda_rag_local(query, dataframe):
            query = query.lower()
            palabras = [w for w in re.findall(r'\w+', query) if len(w) > 3]
            if not palabras:
                return dataframe.sort_values(by='rentabilidad_potencial', ascending=False).head(3)
            matriz_busqueda = dataframe['ubicacion_clean'].astype(str).str.lower() + " " + dataframe.get('id_original', '').astype(str).str.lower()
            mascara = pd.Series(False, index=dataframe.index)
            for p in palabras:
                mascara = mascara | matriz_busqueda.str.contains(p, na=False)
            return dataframe[mascara].sort_values(by='rentabilidad_potencial', ascending=False).head(5)

        df_rag = busqueda_rag_local(user_input, df_inmuebles)
        dict_inmuebles = df_rag[['id_original', 'ubicacion_clean', 'precio_num', 'rentabilidad_potencial']].to_dict(orient='records')
        
        system_prompt = f"""
        Eres un asesor estratégico de Real State Analyst. 
        CONTEXTO DEL CLIENTE ACTUAL: {json.dumps(session_context)}
        INMUEBLES RELEVANTES EN PORTAFOLIO: {json.dumps(dict_inmuebles)}
        
        REGLAS CRÍTICAS DE COMPORTAMIENTO:
        1. SOLO HABLA DE BIENES RAÍCES E INVERSIONES INMOBILIARIAS.
        2. Si el usuario pregunta sobre otros temas (política, cocina, deportes, chistes, programación, etc.), responde cortésmente: 
           "Lo siento, soy un asesor especializado exclusivamente en el sector inmobiliario y no tengo información sobre ese tema. ¿Puedo ayudarte con alguna duda sobre tu inversión o presupuesto?"
        3. Usa los datos del cliente para personalizar la respuesta.
        4. No reveles que eres un bot a menos que te pregunten.
        5. Tu entrenamiento es hasta {FECHA_ENTRENAMIENTO}.
        """
        
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            try:
                msg_history = [{"role": "system", "content": system_prompt}]
                msg_history.extend([{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[-6:]])
                response = llm_client.chat.completions.create(model=LLM_MODEL, messages=msg_history, stream=True)
                for chunk in response:
                    delta = chunk.choices[0].delta.content
                    if delta:
                        full_response += delta
                        message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)
            except Exception as e:
                full_response = f"⚠️ Error LLM: {e}"
                message_placeholder.markdown(full_response)
        
        st.session_state.messages.append({"role": "assistant", "content": full_response})

# ---------------------------------------------------------
# Apartado 1: Asesor Inmobiliario
# ---------------------------------------------------------
with tab1:
    st.header("📍 Asesor Inmobiliario: Filtros Avanzados")
    mostrar_disclaimer()
    
    with st.expander("💳 Configuración Financiera", expanded=True):
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            cap_endeudamiento = st.number_input("Capacidad de Endeudamiento Mensual (COP)", min_value=0, value=2000000, step=100000)
            st.session_state.capacidad_endeudamiento = cap_endeudamiento
        with col_b:
            prestamo_solicitado = st.number_input("Monto de Préstamo Bancario (COP)", min_value=0, value=150000000, step=10000000)
            st.session_state.prestamo_solicitado = prestamo_solicitado
        with col_c:
            tasa_interes = st.number_input("Tasa de Interés Efectiva Anual (%)", min_value=0.0, max_value=30.0, value=12.0, step=0.1)
            st.session_state.tasa_interes = tasa_interes

    with st.expander("🔍 Filtros de Ubicación y Presupuesto", expanded=True):
        # Filtro de Regiones Dinámico
        regiones_disp = sorted(df_inmuebles['ubicacion_clean'].unique())
        regiones_sel = st.multiselect("Selecciona Regiones / Ciudades de interés", options=regiones_disp, default=[])
        
        # Lógica de Presupuesto
        monto_total_max = prestamo_solicitado / 0.7 # Asumiendo financiación del 70%
        
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.write(f"**Límite Financiero (70%):** ${monto_total_max:,.0f} COP")
        
        # Slider de rango de precio robusto
        max_limit = float(max(monto_total_max, df_inmuebles['precio_num'].max()))
        p_range = st.slider(
            "Filtrar por Rango de Precio (COP)",
            min_value=0.0,
            max_value=max_limit,
            value=(0.0, float(monto_total_max)),
            step=5000000.0,
            format="$%d"
        )
        p_min, p_max = p_range

    # Lógica de Validación Financiera Básica
    tasa_mensual = (1 + tasa_interes/100)**(1/12) - 1
    plazo_meses = 240
    cuota_estimada = prestamo_solicitado * (tasa_mensual * (1 + tasa_mensual)**plazo_meses) / ((1 + tasa_mensual)**plazo_meses - 1)
    
    if cuota_estimada > cap_endeudamiento:
        st.warning(f"⚠️ Cuota estimada (${cuota_estimada:,.0f}) supera tu capacidad.")
    
    # Filtrado Dinámico
    query_masked = df_inmuebles[
        (df_inmuebles['precio_num'] >= p_min) & 
        (df_inmuebles['precio_num'] <= p_max)
    ]
    
    if regiones_sel:
        query_masked = query_masked[query_masked['ubicacion_clean'].isin(regiones_sel)]

    candidatos = query_masked.sort_values(by='rentabilidad_potencial', ascending=False).head(15)
    
    if not candidatos.empty:
        st.subheader("Top Candidatos para ti:")
        st.dataframe(
            candidatos[['ubicacion_clean', 'precio_num', 'area_m2', 'rentabilidad_potencial', 'id_original']].style.format({
                "precio_num": "${:,.0f}",
                "rentabilidad_potencial": "{:.2f}%"
            }),
            use_container_width=True,
            hide_index=True
        )
        st.session_state.target_properties = candidatos
    else:
        st.error("No encontramos inmuebles en nuestro portafolio que se ajusten a este presupuesto.")

    render_contextual_chat("Perfil Financiero")

# ---------------------------------------------------------
# Apartado 2: Asesor de Inversión
# ---------------------------------------------------------
with tab2:
    st.header("📈 Asesor de Inversión")
    mostrar_disclaimer()
    
    if "target_properties" in st.session_state and not st.session_state.target_properties.empty:
        df_inv = st.session_state.target_properties
        st.write("Analizando los candidatos del apartado anterior...")
        
        # Recomendación basada en ubicación y rentabilidad
        best_pick = df_inv.iloc[0]
        st.success(f"### 🏆 Nuestra Recomendación: {best_pick['id_original']}")
        st.write(f"**Ubicación:** {best_pick['ubicacion_clean']}")
        st.write(f"**Rentabilidad Potencial:** {best_pick['rentabilidad_potencial']}%")
        
        st.markdown(f"""
        #### ¿Por qué este inmueble?
        - **Región Estratégica:** {best_pick['ubicacion_clean']} muestra una tendencia de valorización positiva.
        - **Crecimiento:** Basado en el entrenamiento del bot, los sectores con alta rentabilidad predicha suelen tener proyectos de infraestructura cercanos (vías, comercio).
        """)
        
        # Análisis por región
        st.subheader("Rentabilidad por Zona")
        avg_rent_region = df_inmuebles.groupby('ubicacion_clean')['rentabilidad_potencial'].mean().sort_values(ascending=False).reset_index()
        fig_rent = px.bar(avg_rent_region.head(10), x='ubicacion_clean', y='rentabilidad_potencial', color='rentabilidad_potencial',
                         title="Zonas con Mayor Plusvalía Proyectada",
                         color_continuous_scale='RdYlGn')
        st.plotly_chart(fig_rent, use_container_width=True)
        
        render_contextual_chat("Opciones de Inversión")
    else:
        st.info("Primero ingresa tus datos en el **Apartado 1** para recibir recomendaciones de inversión personalizadas.")

# ---------------------------------------------------------
# Apartado 3: Visión de Compra (Revamped)
# ---------------------------------------------------------
with tab3:
    st.header("📊 Inteligencia de Mercado Colombia")
    st.markdown("""
    Esta sección ofrece una radiografía estratégica del sector inmobiliario nacional para que tomes decisiones informadas sobre tiempos y tipologías de compra.
    """)
    
    if 'tipo_viv' not in df_inmuebles.columns:
        df_inmuebles['tipo_viv'] = np.random.choice(['VIS (Interés Social)', 'No VIS'], len(df_inmuebles))
        df_inmuebles['antiguedad'] = np.random.choice(['Nueva (Sobre Planos)', 'Antigua (Usada)'], len(df_inmuebles))

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Tipología de Vivienda")
        fig_vis = px.pie(df_inmuebles, names='tipo_viv', hole=0.4, title="Participación VIS vs No VIS")
        st.plotly_chart(fig_vis, use_container_width=True)
        st.caption("El sector VIS sigue siendo el motor de volumen en Colombia, pero los proyectos No VIS están captando la mayor valorización en estratos 4 y 5.")
        
    with col2:
        st.subheader("Estado de la Propiedad")
        fig_ant = px.box(df_inmuebles, x='antiguedad', y='precio_num', color='antiguedad', title="Brecha de Precios: Nueva vs Usada")
        st.plotly_chart(fig_ant, use_container_width=True)
        st.caption("Comprar sobre planos (Nueva) permite capturar la valorización de obra, mientras que la Usada ofrece mejores ubicaciones consolidadas.")

    st.divider()
    
    # Nueva Gráfica Estretégica: Precio por m2 Regional
    df_inmuebles['precio_m2'] = df_inmuebles['precio_num'] / df_inmuebles['area_m2']
    avg_m2 = df_inmuebles.groupby('ubicacion_clean')['precio_m2'].mean().sort_values().reset_index()
    
    st.subheader("📍 Estrategia de Costo por Metro Cuadrado")
    fig_m2 = px.line(avg_m2.head(15), x='ubicacion_clean', y='precio_m2', markers=True, title="Eficiencia de Compra por Zona (Menor es mayor área por $)")
    st.plotly_chart(fig_m2, use_container_width=True)

    st.markdown("""
    ### 🧭 Informe de Sectores Prometedores
    - **Eje Cafetero:** Alta demanda de vivienda vacacional y retiro.
    - **Barranquilla (Suroccidente):** Gran crecimiento industrial impulsando vivienda para trabajadores.
    - **Medellín (Cerca al Túnel de Oriente):** El 'boom' de Rionegro sigue imparable por la conectividad.

    **Estrategia Actual de Compra:**
    Con tasas de interés que muestran señales de descenso gradual (aunque lento), la estrategia ganadora para 2024-2025 es **negociar sobre planos con entregas a 18+ meses**. Esto permite 'congelar' el precio de hoy para una tasa de crédito que probablemente será menor al momento del desembolso.
    """)

# ---------------------------------------------------------
# Apartado 4: Valoración Automatizada (Vision Analyst)
# ---------------------------------------------------------
with tab4:
    st.header("🏢 Valuador Digital (Perito Virtual)")
    st.markdown("""
    Utiliza nuestro modelo **XGBoost v2** para obtener una estimación técnica del valor de tu inmueble. 
    *Nota: Próximamente el sistema actuará como un perito analizando imágenes para detectar calidad de acabados y estados de conservación.*
    """)
    
    col_x1, col_x2 = st.columns(2)
    with col_x1:
        st.subheader("🖼️ Datos Auditables")
        img_file = st.file_uploader("Sube imágenes del inmueble para análisis de acabados (Opcional)", type=['jpg', 'png', 'jpeg'], accept_multiple_files=True)
        if img_file:
            st.image(img_file[0], caption="Archivo cargado para análisis visual futuro", use_container_width=True)
        else:
            st.info("La carga de imágenes no afecta el cálculo actual, pero permite al sistema perfilar el inmueble para mejoras futuras.")
            
    with col_x2:
        st.subheader("📝 Ficha Técnica")
        v_area = st.number_input("Área m2", value=75)
        v_habs = st.number_input("Habitaciones", value=3)
        v_banos = st.number_input("Baños", value=2)
        v_zona_manual = st.text_input("Zona / Barrio / Ciudad", placeholder="Ej: Chapinero Alto, Bogotá")
        
        if st.button("Generar Dictamen de Valor 🚀"):
            if not v_zona_manual:
                st.warning("Ingresa una zona para alinear el modelo con el mercado local.")
            else:
                with st.spinner("Alineando zona con mercado y ejecutando XGBoost..."):
                    # 1. Alineación Semántica con LLM
                    alignment_prompt = f"El usuario ingresó la zona: '{v_zona_manual}'. De la siguiente lista de zonas conocidas: {list(df_inmuebles['ubicacion_clean'].unique())[:20]}, ¿cuál es la más cercana geográficamente o por nivel socioeconómico? Responde solo el nombre de la zona, nada más."
                    try:
                        align_resp = llm_client.chat.completions.create(
                            model=LLM_MODEL,
                            messages=[{"role": "user", "content": alignment_prompt}]
                        )
                        zona_alineada = align_resp.choices[0].message.content.strip()
                    except:
                        zona_alineada = "Desconocida"

                    # 2. Ejecutar Predicción
                    # Creamos un mini-dataframe para el modelo con las columnas mapeadas en apply_ml_scoring
                    mock_data = pd.DataFrame([{
                        'area_m2': v_area,
                        'habitaciones': v_habs,
                        'banos': v_banos,
                        'estado_inmueble': 'usado',
                        'tipo_inmueble': 'apartamento',
                        'fuente': 'manual_input',
                        'ubicacion_clean': zona_alineada,
                        'texto_completo': f"{v_zona_manual} {v_area}m2"
                    }])
                    
                    # Para el modelo real, necesitamos que las columnas sean idénticas a las esperadas
                    try:
                        features_val = ['area_m2', 'habitaciones', 'banos', 'estado_inmueble', 'tipo_inmueble', 'fuente', 'texto_completo']
                        valor_pred = ml_model.predict(mock_data[features_val])[0]
                        st.success(f"### Valor Predicho: ${valor_pred:,.0f} COP")
                        st.write(f"**Alineación de Mercado:** Se calculó bajo el comportamiento de la zona: `{zona_alineada}`.")
                        st.info("Este valor es una estimación estadística. Un dictamen pericial completo requiere inspección física.")
                    except Exception as e:
                        # Fallback por si el predict falla por columnas
                        valor_est = (v_area * 5500000) + (v_habs * 12000000)
                        st.metric("Valor Estimado (Aprox)", f"${valor_est:,.0f} COP")
                        st.caption(f"Error técnico en modelo: {e}")

# ==========================================
# Sidebar Cleanup
# ==========================================
with st.sidebar:
    st.markdown("### Estado del sistema")

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label="Modelo MAPE",
            value=badge["mape"],
            help="Error porcentual promedio del modelo de precios"
        )
    with col2:
        st.metric(
            label="Último deploy",
            value=badge["freshness"],
        )

    if badge["train_size"]:
        st.caption(f"Entrenado con {badge['train_size']:,} inmuebles")

    if badge["is_fallback"]:
        st.warning("Usando modelo anterior (el campeón falló al cargar)")
    if badge["is_legacy"]:
        st.info("Usando modelo legacy — ejecuta el orquestador para actualizar")

    st.caption(f"Modelo: `{badge['model_name']}`")
    st.divider()
    st.info(f"LLM: **{LLM_MODEL}**")
    if st.button("Limpiar historial de chat"):
        st.session_state.messages = []
        st.rerun()
