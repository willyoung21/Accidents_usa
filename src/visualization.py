import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#llamamos a la base de datos
from data_cleaning import df_cleaned


def cargar_datos_limpios():
    df = df_cleaned
    return df



def plot_severity_distribution(df_cleaned):
    plt.figure(figsize=(8, 6))
    sns.countplot(data=df_cleaned, x='severity', palette='viridis')
    plt.title('Distribución de la Severidad')
    plt.xlabel('Severidad')
    plt.ylabel('Cantidad')
    plt.show()

def plot_accidents_by_hour(df_cleaned):
    df_cleaned['start_time'] = pd.to_datetime(df_cleaned['start_time'])
    df_cleaned['hour'] = df_cleaned['start_time'].dt.hour

    plt.figure(figsize=(10, 6))
    sns.histplot(df_cleaned['hour'], bins=24, kde=False, color='blue')
    plt.title('Distribución de Accidentes por Hora del Día')
    plt.xlabel('Hora del Día')
    plt.ylabel('Cantidad de Accidentes')
    plt.show()

def plot_weather_distribution(df_cleaned):
    plt.figure(figsize=(12, 6))
    weather_counts = df_cleaned['weather_condition'].value_counts().nlargest(10)
    sns.barplot(x=weather_counts, y=weather_counts.index, palette='Spectral')
    plt.title('Distribución de Accidentes por Condiciones Climáticas (Top 10)')
    plt.xlabel('Cantidad de Accidentes')
    plt.ylabel('Condición Climática')
    plt.show()

def plot_geographic_distribution(df_cleaned):
    plt.figure(figsize=(10, 8))
    sns.scatterplot(data=df_cleaned, x='start_lng', y='start_lat', hue='severity', palette='viridis', alpha=0.6)
    plt.title('Distribución Geográfica de Accidentes por Severidad')
    plt.xlabel('Longitud')
    plt.ylabel('Latitud')
    plt.grid(True)
    plt.show()

def plot_accidents_by_sunrise_sunset(df_cleaned):
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df_cleaned, x='sunrise_sunset', palette='viridis')
    plt.title('Distribución de Accidentes por Momento del Día')
    plt.xlabel('Momento del Día')
    plt.ylabel('Número de Accidentes')
    plt.grid(True)
    plt.show()

def plot_top_streets(df_cleaned):
    top_streets = df_cleaned['street'].value_counts().head(10)

    plt.figure(figsize=(12, 6))
    sns.barplot(x=top_streets.index, y=top_streets.values, palette='viridis')
    plt.title('Top 10 Calles con Más Accidentes')
    plt.xlabel('Calle')
    plt.ylabel('Número de Accidentes')
    plt.xticks(rotation=90)
    plt.grid(True)
    plt.show()

def plot_humidity_distribution(df_cleaned):
    humidity_bins = [0, 20, 40, 60, 80, 100]  # Puedes ajustar estos rangos según los datos
    humidity_labels = ['0-20%', '21-40%', '41-60%', '61-80%', '81-100%']

    df_cleaned['humidity_range'] = pd.cut(df_cleaned['humidity_percent'], bins=humidity_bins, labels=humidity_labels, include_lowest=True)
    top_humidity_ranges = df_cleaned['humidity_range'].value_counts()

    plt.figure(figsize=(10, 8))
    plt.pie(top_humidity_ranges, labels=top_humidity_ranges.index, autopct='%1.1f%%', colors=plt.cm.viridis(range(len(top_humidity_ranges))))
    plt.title('Porcentaje de Accidentes en los Rangos de Humedad Más Frecuentes')
    plt.show()
    
    
# Ejecución de las visualizaciones
df = cargar_datos_limpios()
plot_severity_distribution(df_cleaned)
plot_accidents_by_hour(df_cleaned)
plot_weather_distribution(df_cleaned)
plot_geographic_distribution(df_cleaned)
plot_accidents_by_sunrise_sunset(df_cleaned)
plot_top_streets(df_cleaned)
plot_humidity_distribution(df_cleaned)