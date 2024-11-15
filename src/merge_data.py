import sqlite3
import pandas as pd

# Función para el proceso de merge de datos
def merge_data():
    # Conexión a la base de datos SQLite (o cambiar para tu base de datos)
    conn = sqlite3.connect('ETL_Proyecto2')
    cursor = conn.cursor()

    # Leer el archivo CSV
    df = pd.read_csv('data/merged_data_cleaned.csv')

    # Función para insertar en dim_date
    def insert_dim_date(df, cursor):
        # Convertir crash_date a formato de fecha y extraer año, mes y día
        df['year'] = pd.to_datetime(df['crash_date']).dt.year
        df['month'] = pd.to_datetime(df['crash_date']).dt.month
        df['day'] = pd.to_datetime(df['crash_date']).dt.day
        
        # Crear DataFrame con los campos correctos para dim_date
        date_df = df[['year', 'month', 'day']].drop_duplicates().reset_index(drop=True)
        
        # Añadir una columna ID (clave primaria para la tabla dim_date)
        date_df['id'] = date_df.index + 1
        
        # Insertar solo los campos year, month, day (no crash_date)
        date_df[['id', 'year', 'month', 'day']].to_sql('dim_date', conn, if_exists='append', index=False)
    
    # Función para insertar en dim_location
    def insert_dim_location(df, cursor):
        location_df = df[['city', 'borough']].drop_duplicates().reset_index(drop=True)
        location_df['id'] = location_df.index + 1
        location_df.to_sql('dim_location', conn, if_exists='append', index=False)

    # Función para insertar en dim_time
    def insert_dim_time(df, cursor):
        df['hour'] = pd.to_datetime(df['crash_time'], format='%H:%M').dt.hour
        df['minute'] = pd.to_datetime(df['crash_time'], format='%H:%M').dt.minute
        time_df = df[['hour', 'minute']].drop_duplicates().reset_index(drop=True)
        time_df['id'] = time_df.index + 1
        time_df.to_sql('dim_time', conn, if_exists='append', index=False)

    # Función para insertar en dim_weather
    def insert_dim_weather(df, cursor):
        weather_df = df[['weather_condition']].drop_duplicates().reset_index(drop=True)
        weather_df['id'] = weather_df.index + 1
        weather_df.to_sql('dim_weather', conn, if_exists='append', index=False)

    # Función para insertar en dim_vehicle
    def insert_dim_vehicle(df, cursor):
        vehicle_df = df[['vehicle_type_code1']].drop_duplicates().reset_index(drop=True)
        vehicle_df['id'] = vehicle_df.index + 1
        vehicle_df.to_sql('dim_vehicle', conn, if_exists='append', index=False)

    # Función para insertar en fact_accident
    def insert_fact_accident(df, cursor):
        fact_df = df[['number_of_persons_injured']].copy()
        fact_df['id'] = df.index + 1
        # Aquí deberías buscar los ids de las dimensiones correspondientes
        fact_df['date_id'] = fact_df.index + 1  # Esto debe reemplazarse con las claves correctas
        fact_df['location_id'] = fact_df.index + 1
        fact_df['time_id'] = fact_df.index + 1
        fact_df['weather_id'] = fact_df.index + 1
        fact_df['vehicle_id'] = fact_df.index + 1
        fact_df.to_sql('fact_accident', conn, if_exists='append', index=False)

    # Insertar los datos en las tablas de dimensiones
    insert_dim_date(df, cursor)
    insert_dim_location(df, cursor)
    insert_dim_time(df, cursor)
    insert_dim_weather(df, cursor)
    insert_dim_vehicle(df, cursor)

    # Insertar los datos en la tabla de hechos
    insert_fact_accident(df, cursor)

    # Confirmar y cerrar la conexión
    conn.commit()
    conn.close()

# Esta función se puede llamar en el DAG para ejecutar el proceso

