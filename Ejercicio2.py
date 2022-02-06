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


os.chdir('/0.PERSONAL/CAPACITACION/PROYECTOS PY/Snowflake & parquet')

# CREAR EL PARQUET CUSTOMER
query =  'SELECT TOP 100 C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY FROM CUSTOMER ORDER BY C_CUSTKEY'
data = conn.cursor().execute(query).fetchall()
df = pd.DataFrame(data, columns=['CUSTKEY', 'CUSNAME', 'ADDRESS', 'NATIONKEY'])
df.to_parquet('CUSTOMER.gzip', compression='gzip')  
# df.to_excel('CUSTOMER.xlsx')


# CREAR EL PARQUET NATION
query =  'SELECT N_NATIONKEY, N_NAME FROM NATION'
data = conn.cursor().execute(query).fetchall()
df = pd.DataFrame(data, columns=['NATIONKEY', 'NATION'])
df.to_parquet('NATION.gzip', compression='gzip')  
# df.to_excel('NATION.xlsx')


# LEVANTAR LOS 2 ARCHIVOS PARQUET 
df1 = pd.read_parquet('CUSTOMER.gzip')  
df2 = pd.read_parquet('NATION.gzip')  

# HACER JOIN EN MEMORIA
final = pd.merge(df1, df2, how='inner', on='NATIONKEY')
# ACA SACO CUANTOS CLIENTES TENGO X CADA PAIS
final.groupby(['NATION']).sum()


