import pandas as pd
from db_conexion import establecer_conexion, cerrar_conexion
from dotenv import load_dotenv
import os

def upload_to_postgres(csv_file, table_name):
    # Cargar las variables de entorno desde el archivo .env
    load_dotenv()

    # Paso 1: Leer el archivo CSV
    df = pd.read_csv(csv_file)

    # Paso 2: Establecer la conexión a PostgreSQL usando el engine
    engine, session = establecer_conexion()

    try:
        # Paso 3: Subir los datos del CSV a una tabla de PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Datos subidos a la tabla {table_name} exitosamente')

    except Exception as e:
        print(f'Error al subir los datos: {e}')

    finally:
        # Cerrar la conexión
        cerrar_conexion(session)

# Puedes llamar a la función `upload_to_postgres()` si deseas ejecutar el script directamente
if __name__ == "__main__":
    csv_file = 'data/merged_data_cleaned.csv'  # Reemplaza con la ruta correcta a tu archivo CSV
    table_name = 'merged'  # Reemplaza con el nombre de tu tabla
    upload_to_postgres(csv_file, table_name)

