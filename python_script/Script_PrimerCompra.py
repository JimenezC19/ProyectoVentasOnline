import mysql.connector
import pandas as pd
from google.cloud import bigquery
import pyarrow  
import os
from datetime import datetime

# CONFIGURA TUS VARIABLES
MYSQL_HOST = ""
MYSQL_PORT = ""
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

WITH a AS 
(
    SELECT 
        idventa,
        idcliente,
        primer_compra
    FROM (
        SELECT DISTINCT
            v2.id AS idventa,
            v.idcliente,
            DATE(v.primer_compra) AS primer_compra,
            ROW_NUMBER() OVER(PARTITION BY v.idcliente ORDER BY v2.id ASC) AS rn
        FROM 
            (SELECT idcliente_full AS idcliente, MIN(fecha_venta) AS primer_compra 
             FROM ventas 
             WHERE adeudo = 0 
             GROUP BY 1) AS v
        JOIN 
            (SELECT id, fecha_venta, idcliente_full AS idcliente 
             FROM ventas) AS v2 
            ON v2.idcliente = v.idcliente 
            AND v2.fecha_venta = v.primer_compra
    ) AS subquery
    WHERE rn = 1
)
,
clientes as 

(
select distinct
c.id as idcliente
,tp.tipo
,concat(Nombre, ' ',Apellidos) as nombre_cliente
,Telefono
,Email
,Pais
,Municipio
,Estado
,r.Region as regioncomercial
,Colonia
,Calle_numero
,CP
,idProsalon
,z.Zona as zona_ps
,c.Zona as zona_cliente
,c.Status as status_cliente
,d.director
,ej.ejecutivo
,coo.coordinador
,ccf.Cuota_inicial
,c.Fecha_ingreso
,c.Status
,ccf.Cuota
,ccf.descuento
,pc.primer_compra
,case when tp.tipo = 'salon' then cs.tipo_salon else null end as nivel
,ccf.idsucursal

 from clientes_full c
 
 left join municipios m on c.idMunicipio = m.ID
 left join estados e on c.idEstado = e.ID
 left join regiones r on c.idMunicipio = r.ID
 left join pais p on c.idPais = p.ID
 left join zonas_ps z on c.idZonaPs = z.ID
left join (select d.ID, concat(d.Nombre,' ',d.Apellidos) as director , cf.idCliente from directores d join configuracion_clientes_full cf on d.ID = cf.idDirector ) as d on c.ID = d.idCliente
left join (select e.ID, concat(e.Nombre,' ',e.Apellidos) as ejecutivo , cf.idCliente,cf.idSucursal from ejecutivos e join configuracion_clientes_full cf on e.ID = cf.idEjecutivo ) as ej on c.ID = ej.idCliente
left join (select coo.ID, concat(coo.Nombre,' ',coo.Apellidos) as coordinador , cf.idCliente from coordinadores coo join configuracion_clientes_full cf on coo.ID = cf.idCoordinador ) as coo on c.ID = coo.idCliente
left join (select idCliente,Cuota_inicial,`Descuento_%` as descuento,Cuota,idTipo,idSucursal,idNivel from configuracion_clientes_full ) as ccf on c.ID = ccf.idCliente
left join (select idcliente_full, date(min(fecha_venta)) as primer_compra from ventas_menudeo group by 1) as pc on c.id = pc.idcliente_full
left join tipos_cliente tp on ccf.idTipo = tp.id
left join configuracion_salones as cs on cs.id = ccf.idnivel 
where tp.tipo != 'salon'
group by c.id
)

select nombre_cliente,a.*,min(date(p.fecha_autorizacion)) as fecha_pago ,c.idsucursal

from a

left join pagos_referenciados as p on p.idventa = a.idventa
join ventas as vts on vts.id = a.idventa
left join ventas_menudeo as vm on vm.idventa = a.idventa
join clientes as c on c.idcliente = a.idcliente


group by 1,2,3,4

order by Fecha_autorizacion desc
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