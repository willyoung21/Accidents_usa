import pandas as pd
from db_conexion import establish_connection, close_connection

def clean_data():
    # Establish the connection using SQLAlchemy
    engine, session = establish_connection()

    # SQL query to select all data from the 'us_accidents' table
    query = "SELECT * FROM us_accidents"

    # Read the data into a pandas DataFrame using the SQLAlchemy engine
    df = pd.read_sql(query, con=engine)

    # Configure pandas to display more rows and columns
    pd.set_option('display.max_rows', 100)  # Show up to 100 rows
    pd.set_option('display.max_columns', None)  # Show all columns without truncation
    pd.set_option('display.width', None)  # Automatically adjust display width

    # Display the first 20 rows in tabular format
    print(df.head(20))

    # Columns to drop
    columns_to_drop = ['id', 'source', 'country', 'description', 'end_lat', 'end_lng', 
                       'civil_twilight', 'nautical_twilight', 'astronomical_twilight']

    # Drop the specified columns
    df_cleaned = df.drop(columns=columns_to_drop)

    # Impute missing values in numerical columns with the mean
    df_cleaned['temperature_f'].fillna(df_cleaned['temperature_f'].mean(), inplace=True)

    # Impute missing values in categorical columns with the mode (most frequent value)
    df_cleaned['weather_condition'].fillna(df_cleaned['weather_condition'].mode()[0], inplace=True)

    # Impute missing values in multiple numerical columns with the mean
    num_cols = ['wind_chill_f', 'humidity_percent', 'pressure_in', 'visibility_mi', 'wind_speed_mph', 'precipitation_in']
    df_cleaned[num_cols] = df_cleaned[num_cols].apply(lambda col: col.fillna(col.mean()))

    # Impute 'wind_direction' column with the mode
    df_cleaned['wind_direction'] = df_cleaned['wind_direction'].fillna(df_cleaned['wind_direction'].mode()[0])

    # Impute 'weather_timestamp' with the previous value (forward fill) for missing timestamps
    df_cleaned['weather_timestamp'] = df_cleaned['weather_timestamp'].fillna(method='ffill')

    # Drop rows that contain any remaining missing values
    df_cleaned.dropna(inplace=True)

    # Count missing (NaN) values in each column
    nan_counts = df_cleaned.isna().sum()

    # Count empty strings ('') in each column
    empty_counts = (df_cleaned == '').sum()

    # Combine the counts into a single DataFrame for better visualization
    null_summary = pd.DataFrame({
        'NaN Count': nan_counts,
        'Empty String Count': empty_counts,
        'Total Missing': nan_counts + empty_counts
    })

    # Display the missing values summary
    print(null_summary)

    # Display the first 100 rows of the cleaned DataFrame in tabular format
    print(df_cleaned.head(100))

    # Save the cleaned data to a CSV file
    df_cleaned.to_csv('data/us_accidents_cleaned.csv', index=False)
    print("Cleaned data saved to data/us_accidents_cleaned.csv")

    # Close the session and connection
    close_connection(session)

