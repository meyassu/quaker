import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from qindex import build_rtree, _set_rtree_properties


"""
Reverse geocoding algorithm
"""
def pip(query_point, possible_region_boundaries, name_field='name', admin_field='admin'):
    """
    Determines which region the passed point is in.

    :param query_point: (Shapely.Point) -> the given point
    :param possible_regions_idx: (list(int)) -> the indices of the candidate regions
    :param boundaries_gdf: (GeoDataFrame) -> the precise boundaries of all the regions

    :return: (str) -> the region of the given point
    """

    for i, region_boundary in possible_region_boundaries.iterrows():
        if query_point.within(region_boundary['geometry']):
            region = region_boundary[name_field]
            admin = region_boundary[admin_field]
            return region, admin

    return None


def find_closest_region(query_point, possible_region_boundaries_idx, boundaries_gdf, name_field='name', admin_field='admin'):
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

    for region_id in possible_region_boundaries_idx:
        boundary_row = boundaries_gdf.loc[region_id]
        distance = query_point.distance(boundary_row['geometry'])
        # print(f'Distance to {boundary_row[name_field]}: {distance}')
        if distance < min_distance:
            min_distance = distance
            closest_region = boundary_row[name_field]
            admin = boundary_row[admin_field]
        # print(f'Current closest region: {closest_region}')
        # print('------------------------------------------------------------------------')

    return closest_region, admin

def reverse_geocode(query_points, rtree_obj, boundaries_gdf):
    for data in query_points:
        query_point = data[0]
        loc_truth = data[1]
        print(f'Processing {query_point} in {loc_truth}...')
        possible_region_boundaries_idx = list(rtree_obj.intersection(query_point.bounds))
        possible_region_boundaries = boundaries_gdf.loc[possible_region_boundaries_idx]
        possible_region_boundaries = possible_region_boundaries.sort_values(by='TERRAIN', ascending=True)
        print(f'Possible regions:\n{possible_region_boundaries}')
        result = pip(query_point, possible_region_boundaries)
        region = None
        admin = None
        if pd.isna(result):
            region, admin = find_closest_region(query_point, possible_region_boundaries_idx, boundaries_gdf)
        else:
            region = result[0]
            admin = result[1]

        print(f'({region}, {admin})')
        print('---------------------------------------------------------------------')
          

# Parallel GeoDataFrames containing both country/ocean data
boundaries_gdf = gpd.read_file('../data/boundaries.geojson')
mbrs_gdf = gpd.read_file('../data/mbrs.geojson')

rtree_properties = _set_rtree_properties()
rtree_obj = build_rtree(mbrs_gdf, rtree_properties)

land = [ (Point(-77.197457, 38.816880), ('Virginia', 'United States of America')), (Point(-85.984452, 12.280283), ('Managua', 'Nicaragua')), 
         (Point(-67.948812, -33.966726), ('Mendoza', 'Argentina')),(Point(-62.154918, -12.090219), ('Rondonia', 'Brazil')), 
         (Point(28.140690, -26.417593), ('Pretoria', 'South Africa')), (Point(41.749049, 2.894312), ('Gedo', 'Somalia')),
         (Point(-6.500760, 8.240911), ('Woroba, Ivory Coast')), (Point(31.835150, 30.688430), ('Al-Sharqia', 'Egypt')), 
         (Point(-52.750548, 76.622056), ('Avannaata', 'Greenland')), (Point(-0.990407, 51.274151), ('South East', 'United Kingdom')), 
         (Point(-5.843060, 40.877832), ('Salamanca', 'Spain')), (Point(11.574789, 48.080932), ('Bavaria', 'Germany')),
         (Point(36.300909, 49.992746), ('Kharkiv', 'Ukraine')), (Point(15.161976, 56.365633), ('Blekinge', 'Sweden')), 
         (Point(128.264536, 53.661227), ('Amur', 'Russia')), (Point(42.363424, 35.915911), ('Ninawa', 'Iraq')), 
         (Point(65.785174, 31.639532), ('Kandahar', 'Afghanistan')), (Point(75.187279, 31.263367), ('Punjab', 'India')),
         (Point(112.117219, 28.465483), ('Gaungdong', 'China')), (Point(103.443507, 49.027224), ('Bulgan', 'Mongolia')),  
         (Point(47.089535, 42.802956),('Dagestan', 'Russia')), (Point(125.256221, 38.280782), ('South Hwanghae', 'North Korea')), 
         (Point(143.136155, 43.379200), ('Hokkaido', 'Japan')), (Point(121.279155, 14.648338), ('Rizal', 'Phillipines')),
         (Point(115.446232, -8.324705), ('Bali', 'Indonesia')), (Point(129.758810, -3.112694), ('Malaku', 'Indonesia')),  
         (Point(143.781299, -5.327070), ('Enga', 'Papau New Guinea')), (Point(148.456263, -24.999857), ('Queensland', 'Australia')), 
         (Point(172.505144, -43.002014), ('Canterbury', 'New Zealand')), (Point(21.791340, 5.357683), ('Kotto', 'Central African Republic')),
         (Point(-64.480459, -68.753132), ('East Antarctica', 'Antarctica')),
         (Point(-75.960226, 23.608383), ('Exuma', 'Bahamas')), (Point(-64.733968, 32.317806), ('Smiths', 'Bermuda')),
         (Point(-105.068909, 69.118733), ('Cambridge Bay', 'Victoria Island')), (Point(144.730715, 13.417842), ('Yona', 'Guam')),  
         (Point(179.195970, -16.559115), ('Macuata', 'Fiji')), (Point(-157.932348, 21.430016), ('Honolulu', 'Hawaii')), 
         (Point(-169.526758, 16.732058), ('Johnston Island', 'United States Minor Outlying Islands')), (Point(138.131944, 9.567096), ('Yap', 'Micronesia')),
         (Point(159.766396, 6.692348), ('Mokil', 'Indonesia'))
        ]

oceans = [
		   (Point(-19.410786, -25.399372), ('South Atlantic Ocean', None)), (Point(-38.681675, 36.970335), ('North Atlantic Ocean', None)),
		   (Point(-139.580115, -51.982655), ('South Pacific Ocean', None)), (Point(-163.373354, 34.648612), ('North Pacific Ocean', None)),
		   (Point(83.662074, -37.628773), ('South Indian Ocean', None)), (Point(78.289961, -0.547376), ('North Indian Ocean', None)),
		   (Point(-165.104174, -73.822360), ('Southern Ocean', None))
         ]
		   

seas =  [
          (Point(124.064183, 31.397935), ('East China Sea', None)), (Point(133.284169, 40.052335), ('Japan Sea', None)), 
          (Point(113.741815, 16.406326), ('South China Sea', None)), (Point(89.307854, 16.406326), ('Bay of Bengal', None)),
          (Point(102.896795, 7.726632), ('Gulf of Thailand', None)), (Point(111.190989, -4.105610), ('Java Sea', None)),
          (Point(122.582401, 4.393135), ('Celebes Sea', None)), (Point(128.204137, -12.875791), ('Timor Sea', None)),
          (Point(125.467240, -4.695625), ('Banda Sea', None)), (Point(153.280038, -21.418900), ('Coral Sea', None)),
          (Point(34.612570, 43.216938), ('Black Sea', None)), (Point(18.439507, 35.739415), ('Mediterranean Sea', None)), (Point(14.334037, 43.758497), ('Adriatic Sea', None)),
          (Point(38.517827, 18.808599), ('Red Sea', None)), (Point(49.092521, 13.251091), ('Gulf of Aden', None)), (Point(49.590154, 28.479664), ('Persian Gulf', None)), 
          (Point(-75.270514, 14.767139), ('Caribbean Sea', None))
        ]
          

reverse_geocode(land, rtree_obj, boundaries_gdf)