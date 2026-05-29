#!/bin/bash
# docker-entrypoint.sh
# Genera /app/.streamlit/secrets.toml a partir de variables de entorno
# antes de iniciar Streamlit.
#
# Prioridad:
#   1. STREAMLIT_SECRETS_JSON  → JSON inyectado por AWS Secrets Manager
#   2. Variables de entorno individuales (AWS_ACCESS_KEY_ID, etc.)
#   3. Valores vacíos → boto3 usa el IAM Task Role del ECS metadata endpoint
set -e

mkdir -p /app/.streamlit /root/.streamlit

if [ -n "$STREAMLIT_SECRETS_JSON" ]; then
    # Convertir JSON a TOML usando Python (disponible en la imagen base)
    python3 - <<'PY'
import json, os, sys

def to_toml(d):
    scalar_lines = []
    table_blocks = []
    for k, v in d.items():
        if isinstance(v, dict):
            lines = [f"[{k}]"]
            for kk, vv in v.items():
                if isinstance(vv, str):
                    lines.append(f'{kk} = "{vv}"')
                elif isinstance(vv, bool):
                    lines.append(f'{kk} = {str(vv).lower()}')
                else:
                    lines.append(f'{kk} = {vv}')
            table_blocks.append("\n".join(lines))
        elif isinstance(v, str):
            scalar_lines.append(f'{k} = "{v}"')
        elif isinstance(v, bool):
            scalar_lines.append(f'{k} = {str(v).lower()}')
        else:
            scalar_lines.append(f'{k} = {v}')
    return "\n".join(scalar_lines + [""] + table_blocks)

data = json.loads(os.environ["STREAMLIT_SECRETS_JSON"])
toml = to_toml(data)
with open("/app/.streamlit/secrets.toml", "w") as f:
    f.write(toml)
print("secrets.toml written from STREAMLIT_SECRETS_JSON", file=sys.stderr)
PY

else
    # Generar un secrets.toml mínimo desde env vars.
    # Credenciales vacías → boto3 usa ECS IAM Task Role (más seguro en ECS).
    cat > /app/.streamlit/secrets.toml <<TOML
[aws]
aws_access_key_id     = "${AWS_ACCESS_KEY_ID:-}"
aws_secret_access_key = "${AWS_SECRET_ACCESS_KEY:-}"
aws_region            = "${AWS_REGION:-us-east-1}"
s3_bucket_name        = "${S3_BUCKET:-}"
TOML
    echo "secrets.toml written from env vars" >&2
fi

# Symlink por si Streamlit busca también en /root/.streamlit/
ln -sf /app/.streamlit/secrets.toml /root/.streamlit/secrets.toml 2>/dev/null || true

exec streamlit run app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    "$@"
