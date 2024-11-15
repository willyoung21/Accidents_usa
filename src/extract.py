import pandas as pd
from db_conexion import establish_connection, close_connection

def extract_data():
    # Establish connection using SQLAlchemy
    engine, session = establish_connection()  # Now engine is the first one returned

    # SQL query to extract data
    query = "SELECT * FROM us_accidents"

    # Load the data into a pandas DataFrame using SQLAlchemy engine
    df = pd.read_sql(query, con=engine)

    # Close the session and connection
    close_connection(session)
    print("Data loaded successfully")

    # Save the data to a CSV file for transformation
    df.to_csv('data/us_accidents_raw.csv', index=False)
    print("Data saved to 'data/us_accidents_raw.csv'")

# You can call the `extract_data()` function if you want to run the script directly
if __name__ == "__main__":
    extract_data()





