import pandas as pd
from db_conexion import establecer_conexion, cerrar_conexion

def extract_data():
    # Establece la conexión usando SQLAlchemy
    engine, session = establecer_conexion()  # Ahora engine es el primero que se recibe

    # Consulta SQL para extraer datos
    query = "SELECT * FROM us_accidents"

    # Carga los datos en un DataFrame de pandas usando el engine de SQLAlchemy
    df = pd.read_sql(query, con=engine)

    # Cierra la sesión y la conexión
    cerrar_conexion(session)
    print("Datos cargados con éxito")

    # Guarda los datos en un archivo CSV para la transformación
    df.to_csv('data/us_accidents_raw.csv', index=False)
    print("Datos guardados en 'data/us_accidents_raw.csv'")

# Puedes llamar a la función `extract_data()` si deseas ejecutar el script directamente
if __name__ == "__main__":
    extract_data()




