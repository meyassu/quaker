import geopandas as gpd

import database
from database import get_engine_rds, load_data_s3

import qindex
from qindex import build_rtree

import revgeocode
from revgeocode import reverse_geocode



if __name__ == "__main__":
    # Get RDS engine
    rds_engine = get_engine_rds()

    # Load boundaries data
    load_data_s3(bucket_name='quakerbucket', file_key='boundaries.geojson', local_fpath='../data/boundaries.geojson')
    boundaries_gdf = gpd.read_file('../data/boundaries.geojson')


    # Load MBR data
    load_data_s3(bucket_name='quakerbucket', file_key='mbrs.geojson', local_fpath='../data/mbrs.geojson')
    mbrs_gdf = gpd.read_file('../data/mbrs.geojson')


    # Create R*-tree
    rtree_obj = build_rtree(mbrs_gdf)


    # Run reverse geocoding algorithm
    reverse_geocode(rtree_obj, boundaries_gdf, table_name='earthquakes', batch_size=1000, location_table_name='locations', engine=rds_engine)





