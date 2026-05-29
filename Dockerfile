FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# Entrypoint: genera secrets.toml desde env vars antes de arrancar Streamlit
RUN chmod +x /app/docker-entrypoint.sh

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["/app/docker-entrypoint.sh"]
