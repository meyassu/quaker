import geopandas as gpd
from dotenv import load_dotenv
import os

from ..utils.database import get_engine_rds
from qindex import build_rtree
from rgc import reverse_geocode

"""
Constants
"""
DATA_DIR = '../../data/'
BATCH_SIZE = 1000


dotenv_path = os.path.join(os.path.dirname(__file__), os.path.join(DATA_DIR, 'config/.env'))
load_dotenv(dotenv_path)

if __name__ == "__main__":
    
    # Validate user data


    # Get RDS engine
    rds_engine = get_engine_rds()

    # Load boundaries data
    boundaries_gdf = gpd.read_file('../data/internal/boundaries.geojson')

    # Load MBR data
    mbrs_gdf = gpd.read_file('../data/internal/mbrs.geojson')

    # Create R*-tree
    rtree_obj = build_rtree(mbrs_gdf)

    # Run reverse geocoding algorithm
    reverse_geocode(rtree_obj, boundaries_gdf, table_name=os.get_env('DATA_TABLE_NAME'), batch_size=BATCH_SIZE, location_table_name=os.get_env('LOCATION_TABLE_NAME'), engine=rds_engine)





