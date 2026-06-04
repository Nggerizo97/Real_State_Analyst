# api/Dockerfile
# Construcci\u00f3n desde la ra\u00edz del proyecto: docker build -f api/Dockerfile .
FROM python:3.12-slim

WORKDIR /app

# 1. Crear usuario sin privilegios para el runtime
RUN useradd -m -u 1000 appuser

# 2. Instalar dependencias Python como root (site-packages del sistema)
COPY api/requirements.txt ./api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

# 3. Copiar c\u00f3digo y transferir ownership
COPY src/ ./src/
COPY api/ ./api/
COPY config/ ./config/
RUN chown -R appuser:appuser /app

# 4. Cambiar a usuario sin privilegios
USER appuser

# 5. Pre-instalar extensi\u00f3n httpfs de DuckDB en el home del usuario de runtime
RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL httpfs;')"

ENV PYTHONPATH="/app/src"

EXPOSE 8000

# Readiness probe: espera 60s al arranque (carga del modelo desde S3)
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Respetar API_WORKERS (default 1 para instancias peque\u00f1as)
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers ${API_WORKERS:-1}"]
