import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import json
import time  # Importa el módulo time para usar sleep
from kafka import KafkaProducer  

def kafka_producer():
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

    # Conexión a PostgreSQL para obtener el número total de personas heridas
    try:
        # Consulta SQL para obtener el total de personas heridas por día
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
        
        # Ejecutar la consulta usando pandas para obtener el resultado en un DataFrame
        df = pd.read_sql(query, engine)
        
        # Verificar si el DataFrame contiene datos
        if df.empty:
            print("No se encontraron datos en la consulta.")
            return

        # Configurar el productor de Kafka
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Enviar cada fila de datos al tema de Kafka
        for index, row in df.iterrows():
            # Crear el mensaje con la fecha y el total de personas heridas
            message = {
                "date": row['date'],  # La fecha del accidente
                "total_injured": row['total_injured']  # Total de personas heridas
            }
            # Enviar el mensaje a Kafka en el tema 'injuries_by_day'
            producer.send('injuries_by_day', value=message)
            print(f"Enviado a Kafka: {message}")
            
            # Pausar 3 segundos antes de enviar el siguiente mensaje
            time.sleep(3)

        # Asegurarse de que todos los mensajes se envíen antes de cerrar el productor
        producer.flush()

    except Exception as e:
        print(f"Error al conectar a la base de datos o enviar datos a Kafka: {e}")

if __name__ == "__main__":
    kafka_producer()
