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
select 
c.id as idcliente
,tp.tipo
,concat(Nombre, ' ',Apellidos) as nombre_cliente
,Telefono
,Email
,Pais
,Municipio
,Estado
,r.Region as regioncomercial_municipio
,re.region as regioncomercial
,Colonia
,Calle_numero
,CP
,z.Zona as zona_ps
,c.Zona as zona_cliente
,c.Status as status_cliente
,d.director
,ej.ejecutivo
,coo.coordinador
,ccf.Cuota_inicial
,c.Fecha_ingreso
,ccf.Cuota
,ccf.descuento
,pc.primer_compra as fecha_pago_primer_compra
,ccf.idsucursal
,case when ccf.idsucursal = 1 then 'AVN' when ccf.idsucursal = 2 then 'ANQ' end as empresa
,c.idprosalon
, case 
       when idprosalon in (2074, 2075, 2076, 2077, 2078, 2079, 2080, 
                        2081, 2082, 2083, 2084, 2085, 2086, 2087, 2088, 
                        2089, 2090, 2128,131) 
                        and ccf.idsucursal = 2 then 'pruebas'
                        
        when idprosalon in (131, 2081, 2089, 631, 951, 1317, 2203,234210) and ccf.idsucursal = 1 then 'pruebas'

        when idprosalon = 1967 and ccf.idsucursal = 1 then 'ecommerce'

        when idprosalon in (1,2172)  and ccf.idsucursal = 2 then 'ecommerce'

        when ccf.idsucursal = 1 and idprosalon in (151, 231, 241, 251, 261, 271, 281, 311, 1321, 1322, 1323, 1324, 1328, 
                                    1395, 1423, 1428, 1438, 1449, 1503, 1504, 1507, 1516, 1531, 1536, 
                                    1538, 1542, 1574, 1575, 1576, 1577, 1580, 1581, 1629, 1630, 1631, 
                                    1632, 1677, 1722, 1760, 1807, 1818, 1837, 1842, 1861, 1862, 1872, 
                                    1904, 1908, 2096, 2116, 2167, 2188, 2190, 2191, 2192, 2193, 2194, 
                                    2200,2139,2195,2187,221)
                        then 'Ventas_directas'

        when ccf.idsucursal = 2 and idprosalon in (2, 3, 2135, 2153,2097,2096,2095,209,2093,2092,2091,2094,2162,251) then 'ventas_directas'

        else 'distribuidor'
        end 
    as filtro_tipo_distribuidor

from clientes_full c
 
            join (select idCliente,Cuota_inicial,`Descuento_%` as descuento,Cuota,idTipo,idSucursal,idNivel from configuracion_clientes_full ) as ccf on c.ID = ccf.idCliente 
            left join municipios m on c.idMunicipio = m.ID
            left join estados e on c.idEstado = e.ID
            left join regiones r on c.idMunicipio = r.ID
            left join regiones re on c.idregion = re.ID
            left join pais p on c.idPais = p.ID
            left join zonas_ps z on c.idZonaPs = z.ID
            left join (select 
                v2.id as idventa,v.idcliente,date(v.primer_compra) as primer_compra,v.idsucursal
                from 
                (select idcliente_full as idcliente, min(fecha_venta) as primer_compra,idsucursal from ventas where adeudo =  0 group by 1,3) as v
                join (select id, fecha_venta, idcliente_full as idcliente, idsucursal from ventas) 
                as v2 on v2.idcliente = v.idcliente 
                and v2.fecha_venta = v.primer_compra 
                and v.idsucursal = v2.idsucursal) as pc on c.id = pc.idcliente and ccf.idsucursal = pc.idsucursal
            left join (select d.ID, concat(d.Nombre,' ',d.Apellidos) as director , cf.idCliente ,d.idsucursal from directores d join configuracion_clientes_full cf on d.ID = cf.idDirector ) as d on c.ID = d.idCliente and ccf.idsucursal = d.idsucursal
            left join (select e.ID, concat(e.Nombre,' ',e.Apellidos) as ejecutivo , cf.idCliente,cf.idSucursal from ejecutivos e join configuracion_clientes_full cf on e.ID = cf.idEjecutivo ) as ej on c.ID = ej.idCliente and ej.idsucursal = ccf.idsucursal
            left join (select coo.ID, concat(coo.Nombre,' ',coo.Apellidos) as coordinador , cf.idCliente, cf.idsucursal from coordinadores coo join configuracion_clientes_full cf on coo.ID = cf.idCoordinador ) as coo on c.ID = coo.idCliente and coo.idsucursal = ccf.idsucursal 
            left join tipos_cliente tp on ccf.idTipo = tp.id
            left join configuracion_salones as cs on cs.id = ccf.idnivel 

where tp.tipo != 'salon' 

group by c.id,ccf.idsucursal

order by idprosalon desc

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