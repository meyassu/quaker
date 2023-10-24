import pandas as pd
import geopandas as gpd
from shapely.geometry import mapping, shape
from datetime import datetime

from utils import get_neon_connection_str, get_psql_engine, push_psql, filter_table, add_fields, load_earthquake_data





def _shapefile_to_geojson(boundaries_shp_fpath, output_fpath):
	"""
	Translates Shapefile into GeoJSON.

	:param boundaries_shp_fpath: (str) -> boundary polygons Shapefile path
	:param output_fpath: (str) -> output GeoJSON filepath

	:return: None
	"""

	boundaries_gdf = gpd.read_file(boundaries_shp_fpath)
	boundaries_gdf.to_file(output_fpath, driver='GeoJSON')


def rectangularize_boundaries(boundaries_gdf, output_fpath):
	"""
	Simplifies complex boundary polygons by replacing them with
	minimum bounding rectangle (MBR).

	:param boundaries_gdf: (GeoDataFrame) -> boundaries GeoDataFrame
	"""

	# Create new Geo dataframe for MBRs
	mbrs = gpd.GeoDataFrame(columns=['country', 'region', 'subregion', 'geometry'])

	# Compute bounding box for each boundary and store in new Geo dataframe
	for _, row in boundaries_gdf.iterrows():
		# Get geographic data
		country = row['SOVEREIGNT']
		region = row['REGION_UN']
		subregion = row['SUBREGION']
		mbr = row['geometry'].envelope

		mbrs = mbrs.append({'country': country, 'region': region, 'subregion': subregion, 'geometry': mbr}, ignore_index=True)

	# Save MBR data in new file
	mbrs.to_file(output_fpath, driver='GeoJSON')


def reverse_geocode(boundaries_shp_fpath):
	"""
	Reverse geocodes coordinates in PostGreSQL database.
	Writes results to database.

	:param boundaries_shp_fpath: (str) -> raw .shp boundary polygon filepath
	"""

	pass






"""
Push earthquake dataset to remote PSQL database
"""
# Load earthquake data
earthquake_data_fpath = '../data/earthquake_data_og.csv'
earthquake_data = load_earthquake_data(earthquake_data_fpath)


# Get PSQL engine
psql_engine = get_psql_engine(get_neon_connection_str())
push_psql(data=earthquake_data, table_name="earthquakes_extra", engine=psql_engine)

# Filter original table
filter_table(new_table_name="earthquakes", og_table_name="earthquakes_extra", fields=['Date', 'Time', 'Latitude', 'Longitude', 'Magnitude'], engine=psql_engine)

# Add geographic fields to filtered table
add_fields(table_name="earthquakes", fields={'Region': 'TEXT', 'Subregion': 'TEXT', 'Country': 'TEXT'}, engine=psql_engine)










