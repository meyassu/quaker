import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from shapely.geometry import Point
from shapely.ops import unary_union
from rtree import index
from collections import defaultdict


"""
Preprocess earthquakes CSV
"""
def _format_date(date):
	"""
	Helper function to func:format_dates.
	Formats given date to be MM/DD/YYYY.

	:param date: (str) -> the date

	:return: (str) -> the reformatted date
	"""

    
	parts = date.split('/')
	if len(parts) == 3:
		month, day, year = parts

		# Ensure month and day are two digits
		parts[0] = month.zfill(2)
		parts[1] = day.zfill(2)

		print(month)

		year = int(year)

		# Handle 20th/21st century years separately
		if 0 <= year <= 99:
			if 65 <= year <= 99:
				parts[2] = '19' + parts[2]
			else:
				parts[2] = '20' + parts[2].zfill(2)  # Add leading zeros if needed
		else:
			return date
	else:
		return date

	# Reconstruct date string and return
	return '/'.join(parts)


def format_dates(earthquake_df, date_column='Date'):
    """
    Formats dates to be MM/DD/YYYY.

    :param earthquake_df: (pd.DataFrame) The DataFrame containing your data
    :param date_column: (str) The name of the column containing the dates

    :return: (pd.Dataframe) -> the reformatted dataframe
    """

    print("Formatting dates...")

    # Apply the conversion function to the date column
    earthquake_df[date_column] = earthquake_df[date_column].apply(_format_date)

    return earthquake_df


"""
Preprocess Natural Earth boundary polygons
"""
def _shapefile_to_geojson(boundaries_shp_fpath, output_fpath):
	"""
	Translates Shapefile into GeoJSON.

	:param boundaries_shp_fpath: (str) -> boundary polygons Shapefile path
	:param output_fpath: (str) -> output GeoJSON filepath

	:return: None
	"""

	boundaries_gdf = gpd.read_file(boundaries_shp_fpath)
	boundaries_gdf.to_file(output_fpath, driver='GeoJSON')


def compute_country_mbrs(country_boundaries_gdf, output_fpath):
	"""
	Simplifies complex country boundary polygons in NaturalEarth type
	GeoJSON file by replacing them with minimum bounding rectangle (MBR).

	:param country_boundaries_gdf: (GeoDataFrame) -> country boundaries GeoDataFrame
	:param output_fpath: (str) -> the output filepath for country MBRs
	"""

	print('Computing country MBRs...')

	country_mbr_list = []
	# Compute bounding box for each boundary and store in new Geo dataframe
	for index, row in country_boundaries_gdf.iterrows():
		print(f'Processing {row['COUNTRY']}...')
		# Get geographic data
		country = row['FORMAL_EN']
		region = row['REGION_UN']
		subregion = row['SUBREGION']
		mbr = row['geometry'].envelope

		country_mbr_list.append({'COUNTRY': country, 'REGION': region, 'SUBREGION': subregion, 'geometry': mbr})


	country_mbrs_gdf = gpd.GeoDataFrame(country_mbr_list, crs=country_boundaries_gdf.crs)

	# Save MBR data in new file
	country_mbrs_gdf.to_file(output_fpath, driver='GeoJSON')

	return country_mbrs_gdf



def compute_subregion_boundaries(country_boundaries_gdf, output_fpath):
	"""
	Computes subregion boundaries by dissolving respective constituent country boundaries.

	:param country_boundaries_gdf: (GeoDataFrame) -> country boundaries GeoDataFrame 
	:param output_fpath: (str) -> the output filepath for region boundaries
	"""

	print('Computing subregion boundaries...')

	subregion_boundaries_gdf = country_boundaries_gdf.dissolve(by='SUBREGION')
	subregion_boundaries_gdf.to_file(output_fpath, driver='GeoJSON')

	return subregion_boundaries_gdf



def compute_subregion_mbrs(subregion_boundaries_gdf, output_fpath):
	"""
	Computes subregion MBRs.

	:param subregion_boundaries_gdf: (GeoDataFrame) -> the precise subregion boundaries
	:param output_fpath: (GeoDataFrame) -> the output filepath for subregion MBRs
	"""

	print("Computing subregion MBRs...")

	# Compute bounding boc for each boundary and store in new GeoDataFrame
	subregion_mbr_list = []
	for index, row in subregion_boundaries_gdf.iterrows():
		print(f'Processing {row['SUBREGION']}...')
		# Get geographic data
		subregion = row['SUBREGION']
		mbr = row['geometry'].envelope

		subregion_mbr_list.append({'SUBREGION': subregion, 'geometry': mbr})

	subregion_mbr_gdf = gpd.GeoDataFrame(subregion_mbr_list, crs=subregion_boundaries_gdf.crs)
	subregion_mbr_gdf['id'] = range(len(subregion_mbr_gdf))

	# Save MBR data in new file
	subregion_mbr_gdf.to_file(output_fpath, driver='GeoJSON')

	return subregion_mbr_gdf



# def compute_subregion_mbrs(country_mbrs_gdf, output_fpath):
# 	"""
# 	DEPRECATED
# 	Builds subregion MBRs by aggregating country MBRs.

# 	:param country_mbrs_gdf: (GeoDataFrame) -> contains country MBRs
# 	:param output_fpath: (str) -> the output filepath for subregion MBRs
# 	"""

# 	print('Computing subregion MBRs from country MBRs...')

# 	# Group countries by subregion
# 	# <K: (region<str>, sub-region<str>), V: list(<geometry>)>
# 	region_to_mbr = defaultdict(list)
	
# 	# populate region_to_mbr
# 	for index, row in country_mbrs_gdf.iterrows():
# 		print(f'Grouping country {index}...')
# 		region = row['region']
# 		subregion = row['subregion']
# 		geometry = row['geometry']
# 		region_to_mbr[(region, subregion)].append(geometry)


# 	# Create gdf
# 	data = []
# 	for (region, subregion), geometries in region_to_mbr.items():
# 		print(f'Computing {subregion} MBR...')
# 		# Aggregate geometries and compute MBR
# 		aggregated_geom = unary_union(geometries)
# 		mbr = box(*aggregated_geom.bounds)
# 		data.append({'region': region, 'subregion': subregion, 'geometry': mbr})

# 	# Create new GeoDataFrame with sub-region MBRs
# 	subregion_mbrs_gdf = gpd.GeoDataFrame(data, columns=['region', 'subregion', 'geometry'])
# 	subregion_mbrs_gdf.set_geometry('geometry', inplace=True)

# 	# Export to GeoJSON file
# 	subregion_mbrs_gdf.to_file(output_fpath, driver='GeoJSON')

# 	return subregion_mbrs_gdf


def build_rtree(subregion_mbrs_gdf):
	"""
	Spatially indexes subregion MBRs by building an R*-tree.

	:param subregion_mbrs_gdf: (GeoDataFrame) -> contains subregion MBRs

	:return: (rtree.Index) -> the R*-tree
	"""

	print('Building R*-tree...')
	# Populate R*-tree with MBRs
	idx = index.Index()
	for ind, row in subregion_mbrs_gdf.iterrows():
		print(f'Inserting {ind} subregion into R*-tree...')
		idx.insert(ind, row['geometry'].bounds)

	return idx


def pip(query_point, possible_subregions, subregions_boundaries_gdf):
	"""
	Determines which subregion the passed point is in.

	:param query_point: (Shapely.Point) -> the given point
	:param possible_subregions: (list(int)) -> the indices of the candidate subregions
	:param subregions_boundaries_gdf: (GeoDataFrame) -> the precise boundaries of all the subregions
	
	:return: (str) -> the subregion of the given point
	"""


	for subregion_id in possible_subregions:
		subregion_row = subregion_boundaries_gdf.loc[subregion_id]
		if query_point.within(subregion_row['geometry']):
			return subregion_row['SUBREGION']

	return None


country_boundaries_gdf = gpd.read_file('../data/country_boundaries.geojson')
subregion_boundaries_gdf = gpd.read_file('../data/subregion_boundaries.geojson')
subregion_mbrs_gdf = compute_subregion_mbrs(subregion_boundaries_gdf, output_fpath='../data/subregion_mbrs.geojson')



rtree_index = build_rtree(subregion_mbrs_gdf)
query_point = Point(145.616, 19.246)
# point = Point(127.352, 1.863)

possible_subregions = list(rtree_index.intersection(query_point.bounds))
print(subregion_mbrs_gdf.loc[possible_subregions])
print(subregion_boundaries_gdf.loc[possible_subregions])

quit()
region = pip(query_point, possible_subregions, subregion_boundaries_gdf)

possibles = subregion_mbrs_gdf.loc[possible_subregions]

print(region)
print(possibles)


















