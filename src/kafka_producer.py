import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import json
import time  # Import the time module to use sleep
from kafka import KafkaProducer  

def kafka_producer():
    # Load environment variables from the .env file
    load_dotenv()

    # Get environment variables
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_NAME = os.getenv('DB_NAME')

    # Configure the database connection
    DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(DATABASE_URL)

    # PostgreSQL connection to get the total number of injured people
    try:
        # SQL query to get the total number of injured people by day
        query = """
        SELECT 
            crash_date AS date, 
            SUM(number_of_persons_injured + 
                number_of_pedestrians_injured + 
                number_of_cyclist_injured + 
                number_of_motorist_injured) AS total_injured
        FROM 
            fact_accidents
        GROUP BY 
            crash_date
        ORDER BY 
            crash_date;
        """
        
        # Execute the query using pandas to get the result in a DataFrame
        df = pd.read_sql(query, engine)
        
        # Check if the DataFrame contains data
        if df.empty:
            print("No data found in the query.")
            return

        # Configure the Kafka producer
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Send each row of data to the Kafka topic
        for index, row in df.iterrows():
            # Create the message with the date and total injured
            message = {
                "date": row['date'],  # The accident date
                "total_injured": row['total_injured']  # Total number of injured people
            }
            # Send the message to Kafka in the 'injuries_by_day' topic
            producer.send('injuries_by_day', value=message)
            print(f"Sent to Kafka: {message}")
            
            # Pause for 3 seconds before sending the next message
            time.sleep(3)

        # Ensure all messages are sent before closing the producer
        producer.flush()

    except Exception as e:
        print(f"Error connecting to the database or sending data to Kafka: {e}")

if __name__ == "__main__":
    kafka_producer()
