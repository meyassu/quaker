import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from qindex import build_rtree, save_rtree, load_rtree


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
          

# Parallel GeoDataFrames containing both country/ocean data
boundaries_gdf = gpd.read_file('../data/boundaries.geojson')
mbrs_gdf = gpd.read_file('../data/mbrs.geojson')

rtree = build_rtree(mbrs_gdf)

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
          



reverse_geocode(seas, rtree, boundaries_gdf)