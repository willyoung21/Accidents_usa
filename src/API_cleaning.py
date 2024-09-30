import pandas as pd
import os

# Get the absolute path of the file
file_path = os.path.abspath(os.path.join('../data/API_data.csv'))

# Load the CSV file using pandas
data = pd.read_csv(file_path)

# Set pandas to display all columns
pd.set_option('display.max_columns', None)

# 2. Convert `crash_date` and `crash_time` to datetime format
# Handle errors by setting invalid parsing as NaT (Not a Time)
data['crash_date'] = pd.to_datetime(data['crash_date'], errors='coerce')
data['crash_time'] = pd.to_datetime(data['crash_time'], format='%H:%M', errors='coerce')

# 3. Fix inconsistent values (e.g., remove whitespace or correct capitalization in the `borough` column)
data['borough'] = data['borough'].str.strip().str.title()

# Filter data to keep only rows with valid crash dates and from the year 2021 or later
data = data[data['crash_date'].notna() & (data['crash_date'].dt.year >= 2021)]

# 4. Remove duplicates based on the `collision_id` column (assuming it's unique for each accident)
data = data.drop_duplicates(subset='collision_id')

# Convert `crash_date` to just the date (drop the time part)
data['crash_date'] = data['crash_date'].dt.date

# Drop unnecessary columns
data = data.drop(['vehicle_type_code_5', 'contributing_factor_vehicle_5',
                  'vehicle_type_code_4', 'contributing_factor_vehicle_4',
                  'vehicle_type_code_3', 'contributing_factor_vehicle_3',
                  'cross_street_name'], axis=1)

print("FILTERED AND CLEANED DATA: \n")

# Drop rows with any missing values
data = data.dropna()

# Add a `city` column with the value "New York"
data['city'] = "New York"

# Print summary information about null values and duplicates
print(f"The total of Null data is: \n{data.isnull().sum()}\n")
print(f"The total of duplicated data is: {data.duplicated().sum()}\n")
print(f"Data: {data.shape[0]} rows\n")

# Save the cleaned data to a new CSV file
data.to_csv('../data/API_data_Cleaned.csv', index=False, encoding='utf-8')
print("File Cleaned Successfully")