# api/Dockerfile
# Construcción desde la raíz del proyecto (docker build -f api/Dockerfile .)
FROM python:3.12-slim

WORKDIR /app

# Copiar dependencias primero para aprovechar la caché de capas
COPY api/requirements.txt ./api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt
RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL httpfs;')"

# Copiar el código fuente completo (se necesita src/ para scorer.py)
COPY src/ ./src/
COPY api/ ./api/
COPY config/ ./config/

# PYTHONPATH para que `from utils.scorer import ...` funcione dentro de api/
ENV PYTHONPATH="/app/src"

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
