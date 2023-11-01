import pandas as pd
from datetime import datetime
from database import load_earthquake_data_local


def validate_data(data):
    """
    Validates the earthquake data.

    :data: (pd.Dataframe) -> the dataframe

    :return: (bool) -> validation status
    """

    print('Validating data...')

    is_valid = True

	# Check for missing values in the 'Latitude' and 'Longitude' columns
    if data['Latitude'].isnull().any() or data['Longitude'].isnull().any():
        print("Missing values found in 'Latitude' or 'Longitude' columns:")
        print(data[data['Latitude'].isnull() | data['Longitude'].isnull()])
        is_valid = False

    # Check for out-of-range latitude and longitude values
    invalid_latitude = data[(data['Latitude'] < -90) | (data['Latitude'] > 90)]
    invalid_longitude = data[(data['Longitude'] < -180) | (data['Longitude'] > 180)]

    if not invalid_latitude.empty:
        print("Out-of-range latitude values found:")
        print(invalid_latitude)
        is_valid = False

    if not invalid_longitude.empty:
        print("Out-of-range longitude values found:")
        print(invalid_longitude)
        is_valid = False

	# Check for missing values in the 'Magnitude' column
    if data['Magnitude'].isnull().any():
        print("Missing values found in 'Magnitude' column:")
        print(data[data['Magnitude'].isnull()])
        is_valid = False

	# Check for illogical 'Magnitude' values
    invalid_magnitude = data[(data['Magnitude'] < -1) | (data['Magnitude'] > 10)]  # adjust range if necessary
    if not invalid_magnitude.empty:
        print("Illogical magnitude values found:")
        print(invalid_magnitude)
        is_valid = False

	# Validate 'Date' and 'Time' format
    for index, row in data.iterrows():
        try:
            # Adjust the format if your 'Date' or 'Time' is in a different format
            datetime.strptime(row['Date'], '%m/%d/%Y')  
            datetime.strptime(row['Time'], '%H:%M:%S')
        except ValueError:
            print(f"Invalid date/time format at index {index}: Date: {row['Date']}, Time: {row['Time']}")
            is_valid = False

    # If no issues are found
    if is_valid:
        print("Validation successful: No missing or out-of-range values found.")


    return is_valid



fpath = '../data/earthquake_data.csv'
data = load_earthquake_data_local(fpath)

validate_data(data)
	














