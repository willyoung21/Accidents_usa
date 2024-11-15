from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get the credentials from environment variables
db = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')

def establish_connection():
    # Create the connection string for SQLAlchemy
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(connection_string)
    
    # Create a session (optional, if you need to work with ORM later)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("Successful database connection")
    return engine, session  # Return both engine and session if needed

def close_connection(session):
    session.close()
    print("Database connection closed")

# Establish the connection
engine, session = establish_connection()

# Now you can use `engine` with pandas without any issues
