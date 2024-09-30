import pandas as pd
from sqlalchemy import create_engine
import sys
import os
from Credentials import usuario,password,host,puerto,db

# Step 1: Read the CSV file
csv_file = '../data/merged_data.csv'  # Replace with the path to your CSV file
df = pd.read_csv(csv_file)

# Step 2: Configure the PostgreSQL connection
user = usuario  # Replace with your PostgreSQL user
password = password  # Replace with your password
host = host  # Replace if your database is on another server
port = puerto  # Default PostgreSQL port
db = db  # Replace with your database name

# Create the connection string
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

# Step 3: Upload the CSV data to a PostgreSQL table
# You can specify the table name and whether to replace or append the data
table_name = "merged"   # Replace with the name of your table
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f'Data uploaded to the table {table_name} successfully')