import pandas as pd
from src import LOGGER

def validate_data(data):
    
    LOGGER.info('Validating data...')

    is_valid = True
    error_message = ""

    # Check for missing values
    if data.isnull().values.any():
        is_valid = False
        error_message += 'Validation failed: Missing values detected' + '\n' 

    # Check for duplicates
    if data.duplicated().any():
        is_valid = False
        error_message += 'Validation failed: Duplicate records found' + '\n' 

    # Check for empty records
    if (data.applymap(lambda x: x == '')).any(axis=None):
        is_valid = False
        error_message += 'Validation failed: Empty records found' + '\n'

    # Check for valid range of values
    # Assuming the year should be between 1800 and 2023 and rGDP should be positive
    if not data['Year'].between(1800, 2023).all():
        is_valid = False
        error_message += 'Validation failed: Year out of valid range' + '\n'
    
    if (data['rGDP'] < 0).any():
        is_valid = False
        error_message += 'Validation failed: rGDP contains negative values' + '\n'

    return is_valid, error_message
