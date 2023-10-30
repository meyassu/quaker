import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from shapely.geometry import mapping, shape
from utils import load_earthquake_data_local, load_earthquake_data_aws, get_psql_engine, push_psql, filter_table, add_fields, view_database
import os
from dotenv import load_dotenv

def reverse_geocode(boundaries_shp_fpath):
	"""
	Reverse geocodes coordinates in PostGreSQL database.
	Writes results to database.

	:param boundaries_shp_fpath: (str) -> raw .shp boundary polygon filepath
	"""

	pass


def create_database_neon():

	# Load earthquake data
	earthquake_data_fpath = '../data/earthquake_data_og.csv'
	earthquake_data = load_earthquake_data(earthquake_data_fpath)


	# Get PSQL engine
	psql_engine = create_engine(get_rds_connection_str())
	push_psql(data=earthquake_data, table_name="earthquakes_extra", engine=psql_engine)

	# Filter original table
	filter_table(new_table_name="earthquakes", og_table_name="earthquakes_extra", fields=['Date', 'Time', 'Latitude', 'Longitude', 'Magnitude'], engine=psql_engine)

	# Add geographic fields to filtered table
	add_fields(table_name="earthquakes", fields={'Region': 'TEXT', 'Subregion': 'TEXT', 'Country': 'TEXT'}, engine=psql_engine)



def init_database_aws(bucket_name, data_file_key, data_local_fpath):
    
    print('Initializing database...')
    # Load earthquake data
    earthquake_data = load_earthquake_data(bucket_name, data_file_key, data_local_fpath)


    # Get PSQL engine
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    username = os.getenv('PG_USERNAME')
    dbname = os.getenv('PG_DBNAME')
    region = os.getenv('REGION')

    psql_engine = get_psql_engine(host, port, username, dbname, region)
	

    # Create table in database
    push_psql(data=earthquake_data, table_name="earthquakes_extra", engine=psql_engine)


    # Filter original table
    filter_table(new_table_name='earthquakes', og_table_name='earthquakes_extra', fields=['Date', 'Time', 'Latitude', 'Longitude', 'Magnitude'], engine=psql_engine)

    # Add geographic fields to filtered table
    add_fields(table_name="earthquakes", fields={'Region': 'TEXT', 'Subregion': 'TEXT', 'Country': 'TEXT'}, engine=psql_engine)
    
    return psql_engine




bucket_name = 'quakerbucket'
data_file_key= 'earthquake_data.csv'
data_local_fpath = '../data/earthquake_data.csv'

psql_engine = init_database(bucket_name, data_file_key, data_local_fpath)

view_database(psql_engine)

