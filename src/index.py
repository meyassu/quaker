import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from shapely.geometry import Point
from shapely.ops import unary_union, nearest_points
from rtree import index
from collections import defaultdict

from pyproj import CRS


GLOBAL_CRS = CRS("EPSG:4326")  
     

def shapefile_to_geojson(boundaries_shp_fpath, output_fpath):
    """
    Translates Shapefile into GeoJSON.

    :param boundaries_shp_fpath: (str) -> boundary polygons Shapefile path
    :param output_fpath: (str) -> output GeoJSON filepath

    :return: None
    """

    boundaries_gdf = gpd.read_file(boundaries_shp_fpath)
    boundaries_gdf.to_file(output_fpath, driver='GeoJSON')


"""
Subregion-level geospatial data
"""
def compute_subregion_boundaries(country_boundaries_gdf, output_fpath):
    """
    Computes subregion boundaries by dissolving respective constituent country boundaries.

    :param country_boundaries_gdf: (gpd.GeoDataFrame) -> country boundaries GeoDataFrame 
    :param output_fpath: (str) -> the output filepath for region boundaries

    :return: (gpd.GeoDataFrame) -> the precise subregion boundaries
    """

    print('Computing subregion boundaries...')

    subregion_boundaries_gdf = country_boundaries_gdf.dissolve(by='SUBREGION')
    subregion_boundaries_gdf.to_file(output_fpath, driver='GeoJSON')

    return subregion_boundaries_gdf

def compute_subregion_mbrs(subregion_boundaries_gdf, output_fpath):
    """
    Computes subregion MBRs.

    :param subregion_boundaries_gdf: (gpd.GeoDataFrame) -> the precise subregion boundaries
    :param output_fpath: (gpd.GeoDataFrame) -> the output filepath for subregion MBRs

    :return: (gpd.GeoDataFrame) -> the subregion MBRs
    """

    print("Computing subregion MBRs...")

    # Compute bounding box for each subregion boundary and store in new GeoDataFrame
    subregion_mbr_list = []
    for index, row in subregion_boundaries_gdf.iterrows():
        print(f'Processing {row["SUBREGION"]}...')
        # Get geographic data
        subregion = row['SUBREGION']
        mbr = row['geometry'].envelope

        subregion_mbr_list.append({'NAME': subregion, 'geometry': mbr})

    subregion_mbrs_gdf = gpd.GeoDataFrame(subregion_mbr_list, crs=GLOBAL_CRS)

	# Save MBR data in new file
    subregion_mbrs_gdf.to_file(output_fpath, driver='GeoJSON')

    return subregion_mbrs_gdf



"""
Country-level geospatial data
"""
def compute_country_mbrs(country_boundaries_gdf, output_fpath):
    """
    Simplifies complex country boundary polygons in NaturalEarth type
    GeoJSON file by replacing them with minimum bounding rectangle (MBR).

    :param country_boundaries_gdf: (gpd.GeoDataFrame) -> country boundaries GeoDataFrame
    :param output_fpath: (str) -> the output filepath for country MBRs

    :return: (gpd.GeoDataFrame) -> the country MBRs
    """

    print('Computing country MBRs...')

    country_mbr_list = []
    # Compute bounding box for each boundary and store in new Geo dataframe
    for index, row in country_boundaries_gdf.iterrows():
        print(f'Processing {row["NAME_LONG"]}...')
        # Get geographic data
        country = row['NAME_LONG']
        mbr = row['geometry'].envelope

        country_mbr_list.append({'NAME': country, 'geometry': mbr})

    country_mbrs_gdf = gpd.GeoDataFrame(country_mbr_list, crs=GLOBAL_CRS)
    country_mbrs_gdf['TERRAIN'] = 'LAND'

    # Save MBR data in new file
    country_mbrs_gdf.to_file(output_fpath, driver='GeoJSON')

    return country_mbrs_gdf


"""
Marine-level geospatial data
"""
def compute_marine_mbrs(marine_boundaries_gdf, output_fpath):
    """
    Computes marine MBRs.

    :param marine_bounaries_gdf: (gpd.GeoDataFrame) -> the precise marine boundaries
    :param output_fpath: (str) -> the output filepath

    :return: (gpd.GeoDataFrame) -> the marine MBRs
    """

    print('Computing marine MBRs')

    # Compute bounding box for each marine boundary and store in new GeoDataFrame
    marine_mbr_list = []
    for index, row in marine_boundaries_gdf.iterrows():
        print(f'Processing {row["name"]}...')
        # Get geographic data
        marine_region = row['name']
        mbr = row['geometry'].envelope
        marine_mbr_list.append({'NAME': marine_region, 'geometry': mbr})


    marine_mbrs_gdf = gpd.GeoDataFrame(marine_mbr_list, crs=GLOBAL_CRS)
    marine_mbrs_gdf['TERRAIN'] = 'WATER'

    # Save MBR data in new file
    marine_mbrs_gdf.to_file(output_fpath, driver='GeoJSON')

    return marine_mbrs_gdf


"""
Spatial indexing computations
"""
def build_rtree(mbrs_gdf):
	"""
	Spatially indexes MBRs by building an R*-tree.

	:param mbrs_gdf: (gpd.GeoDataFrame) -> contains all MBRs in geographical scope

	:return: (rtree.Index) -> the R*-tree
	"""

	print('Building R*-tree...')
	# Populate R*-tree with MBRs
	rtree = index.Index()
	for i, row in mbrs_gdf.iterrows():
		rtree.insert(i, row['geometry'].bounds)

	return rtree


def save_rtree(rtree, output_fpath):
    """
    Seralizes the rtree to a file.

    :param rtree: (rtree.index) -> the rtree
    :param output_fpath: (str) -> the output filepath

    :return: None
    """

    # Serialize the R-tree to a file
    with open(output_fpath, 'wb') as f:
        rtree.serialize(f)

def load_rtree(fpath):
    """
    Loads the rtree from file.
	
	:param fpath: (str) -> the rtree filepath
     
     :return (rtree.Index) -> the rtree
    """

    # Load the serialized R-tree from a file
    with open(fpath, 'rb') as f:
        rtree = rtree.index.Index()
        rtree.deserialize(f)

    return rtree

"""
Reverse geocoding algorithm
"""
def pip(query_point, possible_regions):
    """
    Determines which region the passed point is in.

    :param query_point: (Shapely.Point) -> the given point
    :param possible_regions_idx: (list(int)) -> the indices of the candidate regions
    :param boundaries_gdf: (GeoDataFrame) -> the precise boundaries of all the regions

    :return: (str) -> the region of the given point
    """

    for i, region in possible_regions.iterrows():
        if query_point.within(region['geometry']):
            return region['NAME']

    return None


def find_closest_region(query_point, possible_regions_idx, boundaries_gdf):
    """
    Finds the closest region to the passed point if PiP fails.

    :param query_point: (Shapely.Point) -> the given point
    :param possible_regions_idx: (list(int)) -> the indices of the candidate regions
    :param boundaries_gdf: (gpd.GeoDataFrame) -> the precise boundaries of all the regions

    :return: (str) -> the closest region to the given point
    """

    print(f'Point-in-Polygon failed, finding closest candidate region to {query_point}...')

    closest_region = None
    min_distance = float('inf')

    for region_id in possible_regions_idx:
        boundary_row = boundaries_gdf.loc[region_id]
        distance = query_point.distance(boundary_row['geometry'])
        # print(f'Distance to {boundary_row['NAME']}: {distance}')
        if distance < min_distance:
            min_distance = distance
            closest_region = boundary_row['NAME']
        # print(f'Current closest region: {closest_region}')
        # print('------------------------------------------------------------------------')

    return closest_region

def reverse_geocode(query_points, rtree, boundaries_gdf):
	for data in query_points:
		query_point = data[0]
		region_truth = data[1]
		print(f'Processing {query_point} in {region_truth}...')
		possible_regions_idx = list(rtree.intersection(query_point.bounds))
		possible_regions = boundaries_gdf.loc[possible_regions_idx]
		possible_regions = possible_regions.sort_values(by='TERRAIN', ascending=True)
		print(f'Possible regions:\n{possible_regions}')
		region = pip(query_point, possible_regions)
		if pd.isna(region):
			region = find_closest_region(query_point, possible_regions_idx, boundaries_gdf)
		print(f'Region: {region}')
		print('---------------------------------------------------------------------')



# Get country data
# country_boundaries_gdf = gpd.read_file('../data/country_boundaries/country_boundaries.geojson')
# country_mbrs_gdf = gpd.read_file('../data/country_boundaries/country_mbrs.geojson')


# # Get marine data
# marine_boundaries_gdf = gpd.read_file('../data/marine_boundaries/marine_boundaries.geojson')
# marine_mbrs_gdf = gpd.read_file('../data/marine_boundaries/marine_mbrs.geojson')

# Parallel GeoDataFrames containing both country/ocean data
boundaries_gdf = gpd.read_file('../data/boundaries.geojson')
mbrs_gdf = gpd.read_file('../data/mbrs.geojson')


rtree = build_rtree(mbrs_gdf)
save_rtree(rtree, output_fpath='../data/rtree.dat')

quit()


land = [  (Point(-77.197457, 38.816880), 'Virginia'), (Point(-84.292047, 12.343192), 'Nicaragua'), 
				(Point(-66.292447, -37.826000), 'Argentina'),(Point(-55.930024, -6.661990), 'Brazil'), 
				(Point(24.747279, -28.601562), 'South Africa'), (Point(47.368783, 9.064390), 'Somalia'),
                (Point(-5.265410, 8.067759), 'Ivory Coast'), (Point(26.315105, 30.023420), 'Egypt'), 
                (Point(-38.749716, 69.472438), 'Greenland'), (Point(-1.345844, 53.544831), 'UK'), 
                (Point(-4.369510, 39.652232), 'Spain'), (Point(9.516957, 49.507218), 'Germany'),
                (Point(27.211004, 49.797231), 'Ukraine'), (Point(18.475970, 65.863165), 'Sweden'), 
                (Point(43.001264, 56.496391), 'Russia'), (Point(44.793067, 31.943442), 'Iraq'), 
                (Point(64.502893, 31.371502), 'Afghanistan'), (Point(78.277374, 18.195612), 'India'),
                (Point(78.837312, 38.346827), 'China'), (Point(102.468798, 46.158452), 'Mongolia'),  
                (Point(98.915906, 65.545611),'Russia'), (Point(126.251947, 39.119298), 'North Korea'), 
                (Point(138.169224, 35.846773), 'Japan'), (Point(125.286249, 8.100553), 'Phillipines'),
                (Point(114.892655, 0.146878), 'Indonesia'), (Point(128.272641, 0.865758), 'Malaku Indonesia'),  
                (Point(141.919226, -5.979751), 'Papau New Guinea'), (Point(134.049964, -22.603699), 'Australia'), 
                (Point(169.981310, -44.814218), 'New Zealand'), (Point(21.321515, 8.397737), 'Central African Republic'),
                (Point(39.102201, -78.213568), 'Antarctica'),
                (Point(-77.616760, 23.976376), 'Bahamas'), (Point(-64.740367, 32.306103), 'Bermuda'),
                (Point(-108.790561, 71.285275), 'Victoria Island'), (Point(144.885639, 13.520001), 'Guam'),  
                (Point(177.650508, -17.966855), 'Fiji'), (Point(-155.200068, 19.559862), 'Hawaii'), 
                (Point(-169.530569, 16.729344), 'Johnston Island'), (Point(158.246271, 6.9186070), 'Micronesia'),
                (Point(159.765740, 6.687987), 'Mokil Indonesia')]

oceans = [
		   (Point(-19.410786, -25.399372), 'South Atlantic Ocean'), (Point(-38.681675, 36.970335), 'North Atlantic Ocean'),
		   (Point(-139.580115, -51.982655), 'South Pacific Ocean'), (Point(-163.373354, 34.648612), 'North Pacific Ocean'),
		   (Point(83.662074, -37.628773), 'South Indian Ocean'), (Point(78.289961, -0.547376), "North Indian Ocean"),
		  (Point(-165.104174, -73.822360), 'Southern Ocean')]
		   

seas = [
          (Point(124.064183, 31.397935), 'East China Sea'), (Point(133.284169, 40.052335), 'Japan Sea'), 
          (Point(113.741815, 16.406326), 'South China Sea'), (Point(89.307854, 16.406326), 'Bay of Bengal'),
          (Point(102.896795, 7.726632), 'Gulf of Thailand'), (Point(111.190989, -4.105610), 'Java Sea'),
          (Point(122.582401, 4.393135), 'Celebes Sea'), (Point(128.204137, -12.875791), 'Timor Sea (Australia)'),
          (Point(125.467240, -4.695625), 'Banda Sea (Indonesia)'), (Point(153.280038, -21.418900), 'Coral Sea (Australia)'),
          (Point(34.612570, 43.216938), 'Black Sea'), (Point(18.439507, 35.739415), 'Mediterranean Sea'), (Point(14.334037, 43.758497), 'Adriatic Sea'),
          (Point(38.517827, 18.808599), 'Red Sea'), (Point(49.092521, 13.251091), 'Gulf of Aden'), (Point(49.590154, 28.479664), 'Persian Gulf'), 
          (Point(-75.270514, 14.767139), 'Caribbean Sea')]
          



reverse_geocode(seas, rtree, mbrs_gdf, boundaries_gdf)

















