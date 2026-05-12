import mysql.connector
import pandas as pd
from google.cloud import bigquery
import pyarrow  # Asegúrate de que esté instalado
import os
from datetime import datetime

# CONFIGURA TUS VARIABLES
MYSQL_HOST = ""
MYSQL_PORT = 
MYSQL_USER = ""
MYSQL_PASSWORD = ""
MYSQL_DB = ""

BQ_PROJECT = ""
BQ_DATASET = ""
BQ_TABLE = ""
GOOGLE_CREDENTIALS_PATH = ""

# Configurar credenciales de GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS_PATH

# Consulta SQL
query = """
SELECT
    V.id as idVenta
    , V.Fecha_venta
    , V.idCliente_full as idcliente
    , V.Descuento
    , V.Total as total_precio_distribuidor
    , V.Adeudo
    ,V.Extraido
    , V.Empaquetado
    , V.Timbrado
    , V.Ventas_Directas
    ,v.status
    ,CASE
        WHEN D.Division IN ('Experiencias', 'Eventos') THEN 'Experiencias'
        ELSE 'Productos'
    END AS Tipo
    ,v.idsucursal,
    PR.Fecha_autorizacion -- <<<<<<< Campo nuevo

FROM ventas as v
    left JOIN ventas_menudeo VM ON vm.idVenta = V.id
    left join detalle_venta_menudeo DV on dv.idVenta_menudeo = vm.id
	LEFT JOIN catalogo C ON DV.idCatalogo = C.ID
	LEFT JOIN divisiones D ON C.Division = D.ID
	LEFT JOIN factura F ON V.ID = F.IDVenta

where year(v.fecha_venta) = 2025 OR year(v.fecha_venta) = 2026

GROUP BY V.id,vm.idventa
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