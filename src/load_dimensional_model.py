import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def dimensional_model():

    # Load environment variables from the .env file
    load_dotenv()

    # Get the environment variables
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_NAME = os.getenv('DB_NAME')

    # Configure the database connection
    DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(DATABASE_URL)

    # Load the CSV file of accidents
    merged_df = pd.read_csv('data/merged_data_cleaned.csv', encoding='utf-8')

    # Convert crash_date and crash_time to datetime
    merged_df['crash_datetime'] = pd.to_datetime(merged_df['crash_date'] + ' ' + merged_df['crash_time'], errors='coerce')

    # Check for date conversion errors
    if merged_df['crash_datetime'].isnull().any():
        print("Warning: Some dates could not be converted.")

    # Extract time information
    merged_df['dim_time_id'] = range(1, len(merged_df) + 1)
    merged_df['date'] = merged_df['crash_datetime'].dt.strftime('%Y-%m')  # Convert to 'YYYY-MM' format
    merged_df['hour'] = merged_df['crash_datetime'].dt.strftime('%H:%M')  # HH:MM format
    merged_df['day'] = merged_df['crash_datetime'].dt.day_name()
    merged_df['month'] = merged_df['crash_datetime'].dt.month_name()
    merged_df['year'] = merged_df['crash_datetime'].dt.year

    # Insert data into the dim_time table
    dim_time = merged_df[['dim_time_id', 'date', 'day', 'month', 'year', 'hour']].drop_duplicates()
    dim_time.to_sql('dim_time', engine, if_exists='append', index=False)

    # Create ID for dim_weather and filter the columns
    dim_weather = merged_df[['temperature_f', 'wind_chill_f', 'humidity_percent', 'pressure_in', 
                            'visibility_mi', 'wind_direction', 'wind_speed_mph', 'precipitation_in']]
    dim_weather['dim_weather_id'] = range(1, len(dim_weather) + 1)

    # Insert data into the dim_weather table
    dim_weather.to_sql('dim_weather', engine, if_exists='append', index=False)

    # Create ID for dim_vehicle and filter the columns
    dim_vehicle = merged_df[['vehicle_type_code1', 'contributing_factor_vehicle_1']]
    dim_vehicle['dim_vehicle_id'] = range(1, len(dim_vehicle) + 1)

    # Insert data into the dim_vehicle table
    dim_vehicle.to_sql('dim_vehicle', engine, if_exists='append', index=False)

    # Create ID for dim_location and filter the columns
    dim_location = merged_df[['city', 'borough', 'location']]
    dim_location['dim_location_id'] = range(1, len(dim_location) + 1)

    # Insert data into the dim_location table
    dim_location.to_sql('dim_location', engine, if_exists='append', index=False)

    # Map and assign dimension table IDs to fact_accidents
    fact_accidents = merged_df[['crash_date', 'crash_time', 'number_of_persons_injured', 'number_of_persons_killed', 
                                'number_of_pedestrians_injured', 'number_of_pedestrians_killed', 
                                'number_of_cyclist_injured', 'number_of_cyclist_killed', 
                                'number_of_motorist_injured', 'number_of_motorist_killed', 
                                'severity', 'weather_condition']].copy()

    # For dim_time
    fact_accidents = pd.merge(fact_accidents, dim_time[['dim_time_id']], how='left', left_index=True, right_index=True)

    # For dim_weather
    fact_accidents = pd.merge(fact_accidents, dim_weather[['dim_weather_id']], how='left', left_index=True, right_index=True)

    # For dim_vehicle
    fact_accidents = pd.merge(fact_accidents, dim_vehicle[['dim_vehicle_id']], how='left', left_index=True, right_index=True)

    # For dim_location
    fact_accidents = pd.merge(fact_accidents, dim_location[['dim_location_id']], how='left', left_index=True, right_index=True)

    # Fill null values with np.nan
    fact_accidents[['dim_time_id', 'dim_weather_id', 'dim_vehicle_id', 'dim_location_id']] = fact_accidents[['dim_time_id', 'dim_weather_id', 'dim_vehicle_id', 'dim_location_id']].fillna(np.nan)

    # Convert IDs to integers, ensuring that null values remain as np.nan
    fact_accidents[['dim_time_id', 'dim_weather_id', 'dim_vehicle_id', 'dim_location_id']] = fact_accidents[['dim_time_id', 'dim_weather_id', 'dim_vehicle_id', 'dim_location_id']].astype('Int64')

    # Filter rows where dim_vehicle_id is np.nan before insertion
    fact_accidents = fact_accidents[fact_accidents['dim_vehicle_id'].notna()]

    # Check for valid IDs in the dim_weather table
    valid_weather_ids = dim_weather['dim_weather_id'].unique()
    fact_accidents = fact_accidents[fact_accidents['dim_weather_id'].isin(valid_weather_ids)]

    # Also filter by other valid IDs to ensure they are not np.nan
    valid_time_ids = dim_time['dim_time_id'].unique()
    fact_accidents = fact_accidents[fact_accidents['dim_time_id'].isin(valid_time_ids)]

    valid_location_ids = dim_location['dim_location_id'].unique()
    fact_accidents = fact_accidents[fact_accidents['dim_location_id'].isin(valid_location_ids)]

    # Insert data into the fact_accidents table
    fact_accidents.to_sql('fact_accidents', engine, if_exists='append', index=False)
    print("Data successfully inserted into the tables.")
