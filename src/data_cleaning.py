import pandas as pd
from db_conexion import establecer_conexion, cerrar_conexion

def clean_data():
    # Establece la conexión usando SQLAlchemy
    engine, session = establecer_conexion()

    # SQL query para seleccionar todos los datos de la tabla 'us_accidents'
    query = "SELECT * FROM us_accidents"

    # Lee los datos en un DataFrame de pandas usando el engine de SQLAlchemy
    df = pd.read_sql(query, con=engine)

    # Configura pandas para mostrar más filas y columnas
    pd.set_option('display.max_rows', 100)  # Mostrar hasta 100 filas
    pd.set_option('display.max_columns', None)  # Mostrar todas las columnas sin truncar
    pd.set_option('display.width', None)  # Ajustar automáticamente el ancho de visualización

    # Mostrar las primeras 20 filas en formato tabular
    print(df.head(20))

    # Columnas a eliminar
    columns_to_drop = ['id', 'source', 'country', 'description', 'end_lat', 'end_lng', 
                       'civil_twilight', 'nautical_twilight', 'astronomical_twilight']

    # Eliminar las columnas especificadas
    df_cleaned = df.drop(columns=columns_to_drop)

    # Imputar valores faltantes en columnas numéricas con la media
    df_cleaned['temperature_f'].fillna(df_cleaned['temperature_f'].mean(), inplace=True)

    # Imputar valores faltantes en columnas categóricas con la moda (valor más frecuente)
    df_cleaned['weather_condition'].fillna(df_cleaned['weather_condition'].mode()[0], inplace=True)

    # Imputar valores faltantes en múltiples columnas numéricas con la media
    num_cols = ['wind_chill_f', 'humidity_percent', 'pressure_in', 'visibility_mi', 'wind_speed_mph', 'precipitation_in']
    df_cleaned[num_cols] = df_cleaned[num_cols].apply(lambda col: col.fillna(col.mean()))

    # Imputar la columna 'wind_direction' con la moda
    df_cleaned['wind_direction'] = df_cleaned['wind_direction'].fillna(df_cleaned['wind_direction'].mode()[0])

    # Imputar 'weather_timestamp' con el valor anterior (forward fill) para las marcas de tiempo faltantes
    df_cleaned['weather_timestamp'] = df_cleaned['weather_timestamp'].fillna(method='ffill')

    # Eliminar las filas que contengan cualquier valor faltante restante
    df_cleaned.dropna(inplace=True)

    # Contar valores faltantes (NaN) en cada columna
    nan_counts = df_cleaned.isna().sum()

    # Contar cadenas vacías ('') en cada columna
    empty_counts = (df_cleaned == '').sum()

    # Combinar los conteos en un solo DataFrame para mejor visualización
    null_summary = pd.DataFrame({
        'NaN Count': nan_counts,
        'Empty String Count': empty_counts,
        'Total Missing': nan_counts + empty_counts
    })

    # Mostrar el resumen de valores faltantes
    print(null_summary)

    # Mostrar las primeras 100 filas del DataFrame limpio en formato tabular
    print(df_cleaned.head(100))

    # Guardar los datos limpios en un archivo CSV
    df_cleaned.to_csv('data/us_accidents_cleaned.csv', index=False)
    print("Cleaned data saved to data/us_accidents_cleaned.csv")

    # Cerrar la sesión y la conexión
    cerrar_conexion(session)
