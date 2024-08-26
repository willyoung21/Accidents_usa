import pandas as pd

from db_conexion import establecer_conexion

conn, cursor = establecer_conexion()

# Consulta SQL para seleccionar los datos
query = "SELECT * FROM us_accidents"

# Leer los datos en un DataFrame de pandas
df = pd.read_sql_query(query, conn)

# Primeros datos
print(df.head())

# Tipos de datos
print(df.dtypes)

# Informacion de los datos
print(df.info())

# Descripción de los datos
print(df.describe())

# Presencia de datos nulos en la columna descripción
print(df['description'].isnull().sum())

# Obtener la fila donde esta el dato nulo de la columna descripcion 
print(df[df['description'].isnull()])

# Mostrar solo la informacion de descripcion de la fila 386964
print(df.loc[386964, 'description'])

