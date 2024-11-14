import json
import time
from kafka import KafkaConsumer
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

def kafka_consumer_to_google_sheets():
    # Configuración del consumidor de Kafka
    consumer = KafkaConsumer(
        'injuries_by_day',  # Nombre del topic de Kafka
        bootstrap_servers='localhost:9092',  # Servidor de Kafka
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialización de mensajes
    )

    # Ruta al archivo de credenciales JSON de la cuenta de servicio de Google
    creds = Credentials.from_service_account_file(
        'accidents_usa.json',  # Ruta del archivo de credenciales JSON
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    # ID de la hoja de cálculo de Google Sheets y el rango donde escribir los datos
    SPREADSHEET_ID = '1Dflw-mewAhm6Nqa60dxc9BgLTklrMrbnyWf485XWOFE'  # ID de la hoja de cálculo
    RANGE_NAME = 'Hoja1!A:A'  # Columna A para referencia

    # Crear el servicio de Google Sheets
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Función para obtener la última fila de la hoja de cálculo
    def get_last_row():
        try:
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME
            ).execute()
            values = result.get('values', [])
            return len(values) + 1  # La próxima fila disponible
        except HttpError as err:
            print(f'Error al obtener la última fila: {err}')
            return 2  # Retorna la fila 2 en caso de error

    # Función para actualizar Google Sheets con los datos de Kafka
    def update_google_sheet(data):
        last_row = get_last_row()  # Obtener la última fila disponible
        range_to_update = f'Hoja1!A{last_row}:B{last_row}'  # Escribir en las columnas A y B

        values = [[data['date'], data['total_injured']]]  # Formato para Google Sheets
        body = {
            'values': values
        }

        try:
            response = sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_to_update,
                valueInputOption="RAW",
                body=body
            ).execute()
            print(f'Datos actualizados en Google Sheets: {response}')
        except HttpError as err:
            print(f'Error al actualizar la hoja de cálculo: {err}')

    # Consumir mensajes y enviar los datos a Google Sheets
    for message in consumer:
        data = message.value  # Extraer los datos recibidos de Kafka
        print(f'Mensaje recibido: {data}')
        
        # Actualizar Google Sheets con los datos recibidos
        update_google_sheet(data)
        
        # Espera de 3 segundo entre mensajes
        time.sleep(3)

    consumer.close()

if __name__ == "__main__":
    kafka_consumer_to_google_sheets()
