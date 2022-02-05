# https://www.paradigmadigital.com/dev/snowflake-y-python/
# RECUPERACION DE DATOS

import snowflake.connector
import pandas as pd
import os


# Crear conexion
conn=snowflake.connector.connect(
    user='hsanmartin',
    password='Cobre.22',
    account='ob28420.east-us-2.azure',        
    warehouse='COMPUTE_WH',
    database='SNOWFLAKE_SAMPLE_DATA',
    schema='TPCH_SF1'  )


query =  'SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE '
query += 'FROM LINEITEM WHERE L_ORDERKEY = 2400001'

# Ejecutar query
data = conn.cursor().execute(query).fetchall()
# Crear DataFrame
df = pd.DataFrame(data, columns=['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY', 'EXTENDEDPRICE'])
# Agregar columna calculada
df.insert(6, "IVA",     df['EXTENDEDPRICE'] * 21/100      , allow_duplicates=False)

os.chdir('/0.PERSONAL/CAPACITACION/PROYECTOS PY/Snowflake & parquet')
# os.getcwd()

# Armar archivo parquet
df.to_parquet('ORDERKEY_2400001.gzip', compression='gzip')  



# pd.read_parquet('suppliers.gzip')  










