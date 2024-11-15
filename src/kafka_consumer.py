import json
import time
from kafka import KafkaConsumer
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

def kafka_consumer_to_google_sheets():
    # Kafka consumer configuration
    consumer = KafkaConsumer(
        'injuries_by_day',  # Kafka topic name
        bootstrap_servers='localhost:9092',  # Kafka server
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Message deserialization
    )

    # Path to the Google service account JSON credentials file
    creds = Credentials.from_service_account_file(
        'accidents_usa.json',  # Path to the credentials JSON file
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    # Google Sheets spreadsheet ID and the range to write data
    SPREADSHEET_ID = '1Dflw-mewAhm6Nqa60dxc9BgLTklrMrbnyWf485XWOFE'  # Spreadsheet ID
    RANGE_NAME = 'Sheet1!A:A'  # Column A as a reference

    # Create the Google Sheets service
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Function to get the last row of the spreadsheet
    def get_last_row():
        try:
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME
            ).execute()
            values = result.get('values', [])
            return len(values) + 1  # The next available row
        except HttpError as err:
            print(f'Error getting the last row: {err}')
            return 2  # Return row 2 in case of error

    # Function to update Google Sheets with Kafka data
    def update_google_sheet(data):
        last_row = get_last_row()  # Get the last available row
        range_to_update = f'Sheet1!A{last_row}:B{last_row}'  # Write to columns A and B

        values = [[data['date'], data['total_injured']]]  # Format for Google Sheets
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
            print(f'Data updated in Google Sheets: {response}')
        except HttpError as err:
            print(f'Error updating the spreadsheet: {err}')

    # Consume messages and send data to Google Sheets
    for message in consumer:
        data = message.value  # Extract the data received from Kafka
        print(f'Received message: {data}')
        
        # Update Google Sheets with the received data
        update_google_sheet(data)
        
        # Wait for 3 seconds between messages
        time.sleep(3)

    consumer.close()

if __name__ == "__main__":
    kafka_consumer_to_google_sheets()
