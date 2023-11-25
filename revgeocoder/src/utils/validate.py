import pandas as pd
from src import LOGGER


def validate_data(data):
    """
    Validates the user data

    :data: (pd.Dataframe) -> the data

    :return: (bool, str) -> validation status and message regarding error, if any
    """

    LOGGER.info('Validating data...')
    print('Validating data...', flush=True)

    is_valid = True
    error_message = ''

    # Check for missing values in the 'Latitude', 'Longitude', and 'Magnitude' columns
    if data[['Latitude', 'Longitude', 'Magnitude']].isnull().any().any():
        is_valid = False
        error_message += 'Data must include "Latitude", "Longitude", and "Magnitude" fields' + '\n'

    # Check for out-of-range latitude and longitude values
    invalid_latitude = data[(data['Latitude'] < -90) | (data['Latitude'] > 90)]
    invalid_longitude = data[(data['Longitude'] < -180) | (data['Longitude'] > 180)]

    if not invalid_latitude.empty or not invalid_longitude.empty:
        is_valid = False
        error_message += 'Invalid data found in "Latitude" and/or "Longitude" fields' + '\n' 

    # Check for valid Magnitude values (adjust range as per your data specifics)
    invalid_magnitude = data[(data['Magnitude'] < 0) | (data['Magnitude'] > 10)]
    if not invalid_magnitude.empty:
        is_valid = False
        error_message += 'Invalid data found in "Magnitude" field' + '\n'

    # Check for unique records
    if not data.drop_duplicates().shape[0] == data.shape[0]:
        is_valid = False
        error_message += 'Data contains duplicate records' + '\n'

    # Check for empty records
    if data.empty:
        is_valid = False
        error_message += 'Data should not be empty' + '\n'

    return is_valid, error_message


