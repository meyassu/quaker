import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import os

from ..utils.database import get_engine_rds, init_database_rds
from ..utils.validate import validate_data
from qindex import build_rtree
from rgc import reverse_geocode

"""
Constants
"""
DATA_DIR = '../../data/'
DATA_TABLE_NAME = os.get_env('DATA_TABLE_NAME')
LOCATION_TABLE_NAME = os.get_env('LOCATION_TABLE_NAME')
BATCH_SIZE = 1000


dotenv_path = os.path.join(os.path.dirname(__file__), os.path.join(DATA_DIR, 'config/.env'))
load_dotenv(dotenv_path)

if __name__ == "__main__":
    
    # Load user data
    data = pd.read_csv(os.path.join(DATA_DIR), 'data.csv')
    
    # Validate user data
    is_valid_data, message = validate_data(data)
    if not is_valid_data:
        raise Exception(f'{message}: {data}')

    # Get RDS engine
    rds_engine = get_engine_rds()

    # Initialize database
    init_database_rds(data, data_table_name=DATA_TABLE_NAME, location_table_name=LOCATION_TABLE_NAME)

    # Load boundaries data
    boundaries_gdf = gpd.read_file('../data/internal/boundaries.geojson')

    # Load MBR data
    mbrs_gdf = gpd.read_file('../data/internal/mbrs.geojson')

    # Create R*-tree
    rtree_obj = build_rtree(mbrs_gdf)

    # Run reverse geocoding algorithm
    reverse_geocode(rtree_obj, boundaries_gdf, table_name=DATA_TABLE_NAME, batch_size=BATCH_SIZE, location_table_name=LOCATION_TABLE_NAME, engine=rds_engine)





