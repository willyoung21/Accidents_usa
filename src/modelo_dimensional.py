import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Cargar las variables de entorno del archivo .env
load_dotenv()

# Obtener las variables de entorno
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')

# Configurar la conexión a la base de datos
DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URL)

# Cargar el archivo CSV de accidentes
merged_df = pd.read_csv('data/merged_data_cleaned.csv')

# Convertir crash_date y crash_time a datetime
merged_df['crash_datetime'] = pd.to_datetime(merged_df['crash_date'] + ' ' + merged_df['crash_time'], errors='coerce')

# Verificar si hay errores de conversión de fecha
if merged_df['crash_datetime'].isnull().any():
    print("Advertencia: Algunas fechas no se pudieron convertir.")

# Extraer información de tiempo
merged_df['dim_tiempo_id'] = range(1, len(merged_df) + 1)
merged_df['fecha'] = merged_df['crash_datetime'].dt.strftime('%Y-%m')  # Convertir a formato de cadena 'YYYY-MM'
merged_df['hora'] = merged_df['crash_datetime'].dt.strftime('%H:%M')  # Formato HH:MM
merged_df['dia'] = merged_df['crash_datetime'].dt.day_name()
merged_df['mes'] = merged_df['crash_datetime'].dt.month_name()
merged_df['año'] = merged_df['crash_datetime'].dt.year

# Insertar datos en la tabla dim_tiempo
dim_tiempo = merged_df[['dim_tiempo_id', 'fecha', 'dia', 'mes', 'año', 'hora']].drop_duplicates()
dim_tiempo.to_sql('dim_tiempo', engine, if_exists='append', index=False)

# Crear ID para dim_clima y filtrar las columnas
dim_clima = merged_df[['temperature_f', 'wind_chill_f', 'humidity_percent', 'pressure_in', 
                        'visibility_mi', 'wind_direction', 'wind_speed_mph', 'precipitation_in']].drop_duplicates()
dim_clima['dim_clima_id'] = range(1, len(dim_clima) + 1)

# Insertar datos en la tabla dim_clima
dim_clima.to_sql('dim_clima', engine, if_exists='append', index=False)

# Crear ID para dim_vehiculo y filtrar las columnas
dim_vehiculo = merged_df[['vehicle_type_code1', 'contributing_factor_vehicle_1']].drop_duplicates()
dim_vehiculo['dim_vehiculo_id'] = range(1, len(dim_vehiculo) + 1)

# Insertar datos en la tabla dim_vehiculo
dim_vehiculo.to_sql('dim_vehiculo', engine, if_exists='append', index=False)

# Crear ID para dim_ubicacion y filtrar las columnas
dim_ubicacion = merged_df[['city', 'borough', 'location']].drop_duplicates()
dim_ubicacion['dim_ubicacion_id'] = range(1, len(dim_ubicacion) + 1)

# Insertar datos en la tabla dim_ubicacion
dim_ubicacion.to_sql('dim_ubicacion', engine, if_exists='append', index=False)

# Mapea y asigna los IDs de las tablas de dimensiones a fact_accidentes
fact_accidentes = merged_df[['crash_date', 'crash_time', 'number_of_persons_injured', 'number_of_persons_killed', 
                                'number_of_pedestrians_injured', 'number_of_pedestrians_killed', 
                                'number_of_cyclist_injured', 'number_of_cyclist_killed', 
                                'number_of_motorist_injured', 'number_of_motorist_killed', 
                                'severity', 'weather_condition']].copy()

# Para dim_tiempo
fact_accidentes = pd.merge(fact_accidentes, dim_tiempo[['dim_tiempo_id']], how='left', left_index=True, right_index=True)

# Para dim_clima
fact_accidentes = pd.merge(fact_accidentes, dim_clima[['dim_clima_id']], how='left', left_index=True, right_index=True)

# Para dim_vehiculo
fact_accidentes = pd.merge(fact_accidentes, dim_vehiculo[['dim_vehiculo_id']], how='left', left_index=True, right_index=True)

# Para dim_ubicacion
fact_accidentes = pd.merge(fact_accidentes, dim_ubicacion[['dim_ubicacion_id']], how='left', left_index=True, right_index=True)

# Rellenar los valores nulos con ceros
fact_accidentes[['dim_tiempo_id', 'dim_clima_id', 'dim_vehiculo_id', 'dim_ubicacion_id']] = fact_accidentes[['dim_tiempo_id', 'dim_clima_id', 'dim_vehiculo_id', 'dim_ubicacion_id']].fillna(0)

# Convertir los IDs a enteros
fact_accidentes[['dim_tiempo_id', 'dim_clima_id', 'dim_vehiculo_id', 'dim_ubicacion_id']] = fact_accidentes[['dim_tiempo_id', 'dim_clima_id', 'dim_vehiculo_id', 'dim_ubicacion_id']].astype(int)

# Filtrar filas donde dim_vehiculo_id es cero antes de la inserción
fact_accidentes = fact_accidentes[fact_accidentes['dim_vehiculo_id'] != 0]

# Verificar si existen IDs en la tabla dim_vehiculo
existing_vehiculo_ids = dim_vehiculo['dim_vehiculo_id'].unique()
fact_accidentes = fact_accidentes[fact_accidentes['dim_vehiculo_id'].isin(existing_vehiculo_ids)]

# Insertar en la base de datos
fact_accidentes.to_sql('fact_accidentes', engine, if_exists='append', index=False)

print("Datos insertados exitosamente en las tablas.")