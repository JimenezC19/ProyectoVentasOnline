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
with c as 

(select 
c.id
,ccf.idsucursal
,concat(c.nombre," ",c.apellidos) as nombre_distribuidor 
from clientes_full as c 
join configuracion_clientes_full as ccf on c.id = ccf.idcliente 
group by 1,2,3) 
,

cm as 

(select 
c.id
,ccf.idsucursal
,concat(c.nombre," ",c.apellidos) as nombre_salon 
from clientes_full as c 
join configuracion_clientes_full as ccf on c.id = ccf.idcliente 
group by 1,2,3)

select 
date(v.fecha_venta) as fecha_venta
,idventa
,v.idSucursal
,c.nombre_distribuidor
,v.idcliente_full as id_distribuidor
,vm.idcliente_full as id_salon
,v.status as status_venta
,idventa_menudeo
,idCatalogo
,idPromocion
,idOferta
,ct.producto as nombre_producto
,cantidad
,Precio_unitario as precio_unitario_publico
,importe as importe_precio_publico
,d.division

from detalle_venta_menudeo_temp as dvm
left join ventas_menudeo as vm on vm.id  = dvm.idventa_menudeo
left join ventas as v on v.id = vm.idventa
left join catalogo as ct on ct.id = dvm.idcatalogo
left join c on c.id = v.idCliente_full and c.idsucursal = v.idsucursal
left join cm on cm.id = vm.idcliente_full
LEFT JOIN divisiones D ON Ct.Division = D.ID

where date(v.fecha_venta) > "2024-12-31"
order by v.Fecha_venta desc
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