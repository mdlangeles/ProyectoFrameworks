import dlt
import pyarrow.parquet as pq
import requests
from minio import Minio
from io import BytesIO

# URL parquet
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

# Descargar archivo
print("Descargando parquet...")
response = requests.get(parquet_url)
response.raise_for_status()

# Conectar a Minio
client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

bucket_name = "minio-bucket"
object_name = "yellow_tripdata_2025-01.parquet"

# Crear bucket si no existe
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

# Subir parquet a Minio
print("Guardando en Minio...")
client.put_object(
    bucket_name,
    object_name,
    data=BytesIO(response.content),
    length=len(response.content),
    content_type="application/octet-stream"
)

print("Archivo parquet cargado en Minio:", object_name)
