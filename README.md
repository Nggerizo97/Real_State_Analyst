# Real State Analyst (AI & Data Platform)

Proyecto de Ingeniería de Datos y Analytics para el sector inmobiliario con enfoque **"Zero Cost"** (Maximizando Free Tiers) impulsado por modelos de Machine Learning y un Agente de Inteligencia Artificial RAG.

## 🚀 Arquitectura
1. **Scraping Multi-Portal**: Extracción tolerante a fallos y anti-baneos usando Playwright (Facebook, MercadoLibre, Finca Raiz, etc.).
2. **Almacenamiento (Capa Medallion)**: Amazon S3 (Bronze, Silver, Gold Data Layers usando `fastparquet`).
3. **Machine Learning (Predictive Scoring)**: Pipeline v2 en Scikit-learn para predecir precios basados en atributos y NLP (texto_completo). Calcula la *Rentabilidad Potencial*.
4. **Agente Conversacional (RAG)**: UI en Streamlit conectada a un LLM (Ollama/Llama3/Mistral) para realizar consultas inmobiliarias expertas y estratégicas sobre el portafolio en S3 sin alucinaciones.
5. **Infraestructura y Orquestación**: GitHub Actions (Cron Jobs) y Terraform.

## 📁 Estructura del Proyecto
```text
Real_State_Analyst/
├── app.py                  # Streamlit App: Dashboard Analítico & Chatbot RAG
├── src/
│   ├── scrapers/           # Lógica de Extracción de cada portal
│   └── utils/              # Conectores de S3, Logging, Configuración estática
├── config/                 # Configuraciones de portales (settings.py)
├── infrastructure/         # Terraform para recursos AWS (IAM, Least Privilege Policies)
├── .github/workflows/      # CI/CD y Cron Jobs Automáticos
└── README.md               # Esta documentación
```

## 💡 Mecanismos Anti-Costos e Inteligencia artificial
* **Sistema RAG Estricto**: El chatbot de Streamlit hace un filtro Pandas on-the-fly (`busqueda_rag_local`) para inyectar contexto duro al LLM, evitando invenciones de locaciones o precios irreales.
* **Procesamiento de Datos Eficiente**: Pandas y PyArrow leen nativamente la carpeta Gold en S3 sin descargar ficheros uno por uno, optimizando el uso de memoria RAM.
* **Deduplicación en Origen (AWS)**: Los scrapers evitan re-descargar o re-insertar inmuebles duplicados para mantener el bucket S3 en la capa gratuita.

## 🔐 Configuración de Secretos en GitHub (DevOps & DataOps)

Para ejecutar la recolección automática en la nube de GitHub Actions (`.github/workflows/scraper_cron.yml`) y encadenarlo (CI/CD) con el procesamiento de Databricks, necesitas configurar los siguientes secretos en tu repositorio:

Navega a la configuración de tu repositorio en GitHub:
`Settings > Secrets and variables > Actions > New repository secret` y agrega:

### AWS (Almacenamiento Bronze)
1. `AWS_ACCESS_KEY_ID`: Tu llave de acceso de IAM en AWS.
2. `AWS_SECRET_ACCESS_KEY`: Tu llave secreta de IAM.
3. `S3_BUCKET_NAME`: El nombre de tu bucket de S3 donde residirá la capa Bronze (ej. `mi-bucket-bronze-real-estate`).
4. `AWS_REGION` *(Opcional)*: Por default usa `us-east-1` (modificar en GitHub Action directamente si es necesario).

### Databricks (DataOps & ML Retrain)
1. `DATABRICKS_HOST`: La URL de tu workspace de Databricks (ej: `https://adb-123...azuredatabricks.net`).
2. `DATABRICKS_TOKEN`: Tu token de acceso personal (PAT) de la API de Databricks.
3. `DATABRICKS_JOB_ID`: El ID numérico entero de tu cluster/pipeline ETL (ej: `13516656597401`).

### 2. Secretos de la Aplicación Streamlit (RAG)
Debes crear `.streamlit/secrets.toml` para conectar la aplicación:
```toml
[aws]
aws_access_key_id = "TU_LLAVE"
aws_secret_access_key = "TU_SECRETO"
aws_region = "us-east-1"
s3_bucket_name = "tu-bucket"

[llm]
api_base = "http://localhost:11434/v1" # O Groq/OpenRouter
api_key = "ollama"
model_name = "llama3"
```

## 🛠️ Cómo ejecutar de forma local

**1. Instalar dependencias:**
```bash
pip install -r scrapers/requirements.txt
# Asegúrate de instalar dependencias de la UI si no están incluidas:
pip install streamlit pandas numpy boto3 joblib plotly openai s3fs
playwright install chromium
```

**2. Levantar el Agente Inteligente (UI):**
Asegúrate de tener corriendo tu motor LLM local (ej. `ollama run llama3`) u obtener llaves de nube, y ejecuta:
```bash
python -m streamlit run app.py
```

**3. Ejecutar los Web Scrapers individualmente (Testing):**
```bash
python main.py
```
