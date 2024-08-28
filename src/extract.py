import pandas as pd

# Configura la conexión a PostgreSQL
from db_conexion import establecer_conexion

conn, cursor = establecer_conexion()

# Consulta SQL para extraer datos
query = "SELECT * FROM us_accidents"

# Carga los datos en un DataFrame de pandas
df = pd.read_sql(query, conn)

# Cierra la conexión
conn.close()

# Guarda los datos en un archivo CSV para la transformación
df.to_csv('data/us_accidents_raw.csv', index=False)

