import requests
import pandas as pd

# URL of the dataset (API endpoint)
url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

# Parameters to limit the response to 200,000 records
params = {
    "$limit": 200000
}

# Send a GET request to the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()  # Convert the response to JSON format
    df = pd.DataFrame(data)  # Create a pandas DataFrame from the data
    print(df.head())  # Display the first few records
    # Save the DataFrame to a CSV file
    df.to_csv('../data/API_data.csv', index=False, encoding='utf-8')
else:
    # If the request fails, print the error code
    print(f"Error in the request: {response.status_code}")