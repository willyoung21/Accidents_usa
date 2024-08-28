import pandas as pd

from db_conexion import establecer_conexion

conn, cursor = establecer_conexion()

# Consulta SQL para seleccionar los datos
query = "SELECT * FROM us_accidents"

# Leer los datos en un DataFrame de pandas
df = pd.read_sql_query(query, conn)

# Primeros datos
print(df.head())

# Leer los datos en un DataFrame de pandas
df = pd.read_sql_query(query, conn)

# Columnas a eliminar
columns_to_drop = ['id', 'source', 'country', 'description', 'end_lat', 'end_lng', 
                'civil_twilight', 'nautical_twilight', 'astronomical_twilight']

# Eliminación de las columnas
df_cleaned = df.drop(columns=columns_to_drop)

# Imputar valores nulos en columnas numéricas con la media
df_cleaned['temperature_f'].fillna(df_cleaned['temperature_f'].mean(), inplace=True)

# Imputar valores nulos en columnas categóricas con la moda
df_cleaned['weather_condition'].fillna(df_cleaned['weather_condition'].mode()[0], inplace=True)

# Imputar columnas numéricas con la media
num_cols = ['wind_chill_f', 'humidity_percent', 'pressure_in', 'visibility_mi', 'wind_speed_mph', 'precipitation_in']
df_cleaned[num_cols] = df_cleaned[num_cols].apply(lambda col: col.fillna(col.mean()))

# Imputar columna categórica con el valor más frecuente
df_cleaned['wind_direction'] = df_cleaned['wind_direction'].fillna(df_cleaned['wind_direction'].mode()[0])

# Imputar weather_timestamp con el valor más cercano (por tiempo)
df_cleaned['weather_timestamp'] = df_cleaned['weather_timestamp'].fillna(method='ffill')

# Eliminar filas que contienen valores nulos
df_cleaned.dropna(inplace=True)

# Contar valores nulos (NaN) en cada columna
nan_counts = df_cleaned.isna().sum()

# Contar valores vacíos ('') en cada columna
empty_counts = (df_cleaned == '').sum()

# Combinar los conteos en un solo DataFrame para visualizar mejor
null_summary = pd.DataFrame({
    'NaN Count': nan_counts,
    'Empty String Count': empty_counts,       
    'Total Missing': nan_counts + empty_counts
})

# Mostrar el resumen
print(null_summary)

# Configuración para mostrar más filas y columnas si es necesario
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', None)

# Mostrar el DataFrame en formato tabular
print(df_cleaned.head(100))

datos_limpios = df_cleaned

# Guardar los datos limpios en un archivo CSV
df_cleaned.to_csv('data/us_accidents_cleaned.csv', index=False)