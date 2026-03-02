import json
import boto3
import io
import pandas as pd
from botocore.exceptions import ClientError
from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME
from src.utils.logger import get_logger

logger = get_logger(__name__)

class S3Connector:
    """Clase wrapper para abstraer la comunicación con S3."""
    
    def __init__(self):
        # Conexión usando credenciales de .env local (cargadas por python-dotenv)
        # o, si no están, cae de vuelta a la configuración de sistema de la máquina o IAM Roles
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
        else:
            self.s3_client = boto3.client('s3', region_name=AWS_REGION)
            
        self.bucket = S3_BUCKET_NAME

    def item_exists(self, key: str) -> bool:
        """Verifica si un objeto (identificado por key) existe en el bucket."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error AWS al validar el objeto {key}: {e}")
                raise

    def upload_json(self, key: str, data: dict) -> bool:
        """Sube un diccionario Python formateado como JSON al S3."""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(data, ensure_ascii=False),
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            logger.error(f"Error AWS al subir o reemplazar {key}: {e}")
            return False

    def download_hash_index(self, key: str) -> set:
        """Descarga el índice histórico de hashes (SCD Type 2) desde S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return set(content.splitlines())
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Índice de hashes {key} no existe. Se creará uno nuevo.")
                return set()
            logger.error(f"Error descargando el índice de hashes {key}: {e}")
            return set()

    def upload_hash_index(self, key: str, hashes: set) -> bool:
        """Sube el índice histórico de hashes actualizado a S3."""
        try:
            content = '\n'.join(list(hashes))
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content.encode('utf-8'),
                ContentType='text/plain'
            )
            return True
        except ClientError as e:
            logger.error(f"Error subiendo el índice de hashes {key}: {e}")
            return False

    def upload_parquet(self, key: str, df: pd.DataFrame) -> bool:
        """Sube un Pandas DataFrame como Parquet a S3 usando memoria buffer."""
        try:
            # Escribir el DataFrame a un buffer de bytes
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            
            # Subir el buffer a S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=parquet_buffer.getvalue()
            )
            return True
        except Exception as e:
            logger.error(f"Error AWS al subir Parquet batch a {key}: {e}")
            return False
