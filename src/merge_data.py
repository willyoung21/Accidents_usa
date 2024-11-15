import pandas as pd

def merge_data():
    # Load the CSV files
    API_merge = pd.read_csv('data/API_data_Cleaned.csv')
    db_merge = pd.read_csv('data/us_accidents_cleaned.csv')

    # Convert date columns to datetime format and use only the date
    API_merge['crash_date'] = pd.to_datetime(API_merge['crash_date']).dt.date  # Use only the date
    db_merge['start_time'] = pd.to_datetime(db_merge['start_time']).dt.date  # Use only the date

    # Filter both datasets for rows where the city is 'New York'
    api_data_ny = API_merge[API_merge['city'] == 'New York']
    us_accidents_ny = db_merge[db_merge['city'] == 'New York']

    # Merge the two datasets based on the date (inner join)
    merged_df = pd.merge(api_data_ny, us_accidents_ny, left_on='crash_date', right_on='start_time', how='inner')

    # Drop duplicate city columns ('city_x' and 'city_y')
    merged_df = merged_df.drop(columns=['city_x', 'city_y'])

    # 1. Ensure that the date column is in datetime format
    merged_df['crash_date'] = pd.to_datetime(merged_df['crash_date'], errors='coerce')

    # 2. Create a new column that contains the month and year
    # Here it's formatted as "YYYY-MM"
    merged_df['crash_date'] = merged_df['crash_date'].dt.to_period('M')

    # Add a new column 'city' with the value "New York"
    merged_df['city'] = "New York"

    merged_df = merged_df.sort_values(by='crash_date', ascending=True)

    # Move the 'city' column to the beginning of the DataFrame
    cols = ['city'] + [col for col in merged_df.columns if col != 'city']
    merged_df = merged_df[cols]

    # Convert the 'crash_time' column to datetime if it's not already
    merged_df['crash_time'] = pd.to_datetime(merged_df['crash_time'], errors='coerce')

    # Extract only the hour
    merged_df['crash_time'] = merged_df['crash_time'].dt.strftime('%H:%M')

    # Convert 'number_of_persons_injured' to integer
    merged_df['number_of_persons_injured'] = pd.to_numeric(merged_df['number_of_persons_injured'], errors='coerce').fillna(0).astype(int)

    # 3. Drop 'collision_id'
    merged_df.drop(columns=['collision_id'], inplace=True)

    # 4. Drop 'contributing_factor_vehicle_2'
    merged_df.drop(columns=['contributing_factor_vehicle_2'], inplace=True)

    # 5. Drop 'vehicle_type_code2'
    merged_df.drop(columns=['vehicle_type_code2'], inplace=True)

    # 6. Combine 'borough' with 'zip_code'
    # Assuming 'borough' and 'zip_code' are the columns in merged_clean
    merged_df['borough'] = merged_df['borough'] + ' - ' + merged_df['zip_code'].astype(str)

    # 7. Drop latitude and longitude
    merged_df.drop(columns=['latitude', 'longitude'], inplace=True)

    # 8. Drop 'start time' and 'end time'
    merged_df.drop(columns=['start_time', 'end_time'], inplace=True)

    # 9. Drop 'start latitude' and 'end latitude'
    merged_df.drop(columns=['start_lat', 'start_lng'], inplace=True)

    # 10. Drop 'distance in miles'
    merged_df.drop(columns=['distance_mi'], inplace=True)

    # 11. Drop 'county'
    merged_df.drop(columns=['county'], inplace=True)

    # 12. Combine 'state' with 'city'
    merged_df['city'] = merged_df['city'] + ', ' + merged_df['state']

    # 13. Drop 'zipcode'
    merged_df.drop(columns=['zipcode'], inplace=True)

    # 14. Drop unnecessary columns
    columns_to_drop = [
        'airport_code', 'amenity', 'bump', 'crossing', 'give_way', 'junction', 
        'no_exit', 'railway', 'roundabout', 'station', 'stop', 'traffic_calming', 
        'traffic_signal', 'turning_loop'
    ]
    merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # 15. Drop additional columns
    merged_df.drop(columns=['zip_code', 'state', 'weather_timestamp'], inplace=True)

    # Assign an ID to each row
    merged_df['id'] = range(1, len(merged_df) + 1)

    # Move the 'id' column to the beginning
    cols = ['id'] + [col for col in merged_df.columns if col != 'id']
    merged_df = merged_df[cols]

    # Check the number of rows after the merge
    merged_count = merged_df.shape[0]
    print(f"Number of rows after the merge: {merged_count}")

    # Check for null values in the merged DataFrame
    print(f"Null values: \n{merged_df.isnull().sum()}\n")

    # Save the merged result to a CSV file
    merged_df.to_csv('../data/merged_data.csv', index=False, encoding='utf-8')
