import pandas as pd
import geopandas as gpd
import shutil
import os

from src.utils.database import get_db_engine, init_database, get_data, merge_tables
from src.utils.validate import validate_data
from src.core.qindex import build_rtree
from src.core.rgc import reverse_geocode

from src import configure_py, configure_dotenv
from src import USER_DATA_DIR, INPUT_DIR, OUTPUT_DIR, INTERNAL_DATA_DIR, LOGS_DIR
from src import LOGGER

import time


"""
Init. Configuration
"""
print(f'Configuring environment...')
LOGGER.info(f'Configuring environment...')
configure_py()
configure_dotenv()


"""
Local Constants
"""
DATA_TABLE_NAME = os.getenv('DATA_TABLE_NAME')
LOCATION_TABLE_NAME = os.getenv('LOCATION_TABLE_NAME')


if __name__ == "__main__":

    start_time = time.time()

    # Load user data
    data = pd.read_csv(os.path.join(INPUT_DIR, 'data.csv'))
    
    # Validate user data
    is_valid_data, error_message = validate_data(data)
    if not is_valid_data:
        LOGGER.error(f'{error_message}')
        raise Exception(f'{error_message}')

    # Get database engine
    engine = get_db_engine()

    # Initialize database
    init_database(data, data_table_name=DATA_TABLE_NAME, location_table_name=LOCATION_TABLE_NAME)

    # Load boundaries data
    boundaries_gdf = gpd.read_file(os.path.join(INTERNAL_DATA_DIR, 'boundaries.geojson'))

    # Load MBR data
    mbrs_gdf = gpd.read_file(os.path.join(INTERNAL_DATA_DIR, 'mbrs.geojson'))

    # Create R*-tree
    rtree_obj = build_rtree(mbrs_gdf)

    # Run reverse geocoding algorithm
    reverse_geocode(rtree_obj, boundaries_gdf, data_table_name=DATA_TABLE_NAME, location_table_name=LOCATION_TABLE_NAME, engine=engine)

    # Merge locations table into data table
    merge_tables(static_table_name=DATA_TABLE_NAME, merging_table_name=LOCATION_TABLE_NAME, fields=['province', 'country'], engine=engine)

    # Write output to file
    get_output_query = f'''
             SELECT * FROM {DATA_TABLE_NAME};
             '''
    output = get_data(get_output_query, engine)
    output_fpath = os.path.join(OUTPUT_DIR, 'data_out.csv')
    output.to_csv(output_fpath, index=False)

    
    # Copy log file to Docker volume
    log_fpath = os.path.join(LOGS_DIR, 'log.txt')
    try:
        # Check if source file exists
        if not os.path.exists(log_fpath):
            LOGGER.error(f'Error {log_fpath} does not exist')
            raise Exception(f'Error {log_fpath} does not exist')
        else:
            # Copy file over
            shutil.copy(log_fpath, USER_DATA_DIR)
            print(f'{log_fpath} copied successfully to {USER_DATA_DIR}')
    except IOError as e:
        LOGGER.error(f'Error occured while copying {log_fpath} to {USER_DATA_DIR}')
        raise e

    end_time = time.time()
    
    print(f'revgeocoder took {end_time - start_time} seconds to run')
    LOGGER.info(f'revgeocoder took {end_time - start_time} seconds to run')


