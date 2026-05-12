import mysql.connector
import pandas as pd
from google.cloud import bigquery
import pyarrow  
import os
from datetime import datetime

# Variables Conexión DB y BQ
MYSQL_HOST = ""
MYSQL_PORT = 
MYSQL_USER = ""
MYSQL_PASSWORD = ""
MYSQL_DB = ""

BQ_PROJECT = ""
BQ_DATASET = ""
BQ_TABLE = ""
GOOGLE_CREDENTIALS_PATH = ""

#Credenciales de GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS_PATH

# Consulta SQL
query = """
SELECT * 
FROM cat_calendario_psgroup
"""

# Conexión a MySQL
print("Conectando a MySQL...")
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB
)

# Ejecutar consulta
print("Ejecutando consulta...")
df = pd.read_sql(query, conn)
conn.close()

# Conexión a BigQuery
print("Conectando a BigQuery...")
client = bigquery.Client(project=BQ_PROJECT)
table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

# Cargar datos a BigQuery con PyArrow
print(f"Subiendo {len(df)} registros a BigQuery con Parquet/pyarrow...")
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    source_format=bigquery.SourceFormat.PARQUET
)
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

print(f"[{datetime.now()}] Carga exitosa a BigQuery.")