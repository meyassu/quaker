import pandas as pd



def validate_data(data):
    """
    Validates the user data

    :data: (pd.Dataframe) -> the data

    :return: (bool, str) -> validation status and message regarding error, if any
    """

    print('Validating data...')

	# Check for missing values in the 'Latitude' and 'Longitude' columns
    if data['Latitude'].isnull().any() or data['Longitude'].isnull().any():
        return False, 'Data must include "Latitude" and "Longitude" fields'

    # Check for out-of-range latitude and longitude values
    invalid_latitude = data[(data['Latitude'] < -90) | (data['Latitude'] > 90)]
    invalid_longitude = data[(data['Longitude'] < -180) | (data['Longitude'] > 180)]

    if not invalid_latitude.empty or not invalid_longitude.empty:
        return False, 'Invalid data found in "Latitude" and/or "Longitude" fields'

    return True, None

	














