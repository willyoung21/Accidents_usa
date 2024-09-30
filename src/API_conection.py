import requests
import pandas as pd

# URL del dataset
url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

# Parámetros para limitar a 500,000 registros
params = {
    "$limit": 200000
}

# Hacer la solicitud GET al API
response = requests.get(url, params=params)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    data = response.json()  # Convertir la respuesta a JSON
    df = pd.DataFrame(data)  # Crear un DataFrame de pandas
    print(df.head())  # Mostrar los primeros registros
    df.to_csv('data/API_data.csv', index=False, encoding='utf-8')
else:
    print(f"Error en la solicitud: {response.status_code}")