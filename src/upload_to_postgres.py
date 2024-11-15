import pandas as pd
from db_conexion import establish_connection, close_connection
from dotenv import load_dotenv
import os

def upload_to_postgres(csv_file, table_name):
    # Load environment variables from the .env file
    load_dotenv()

    # Step 1: Read the CSV file
    df = pd.read_csv(csv_file)

    # Step 2: Establish the connection to PostgreSQL using the engine
    engine, session = establish_connection()

    try:
        # Step 3: Upload the CSV data to a PostgreSQL table
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Data successfully uploaded to the {table_name} table')

    except Exception as e:
        print(f'Error uploading data: {e}')

    finally:
        # Close the connection
        close_connection(session)

# You can call the `upload_to_postgres()` function if you want to run the script directly
if __name__ == "__main__":
    csv_file = 'data/merged_data_cleaned.csv'  # Replace with the correct path to your CSV file
    table_name = 'merged'  # Replace with the name of your table
    upload_to_postgres(csv_file, table_name)


