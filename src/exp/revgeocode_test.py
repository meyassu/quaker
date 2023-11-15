
from shapely.geometry import Point, MultiPolygon
import geopandas as gpd
import pandas as pd
import rtree
from rtree import index
from geopy.distance import geodesic
from pyproj import CRS
import math

from shapely.ops import nearest_points


"""
Constants
"""
# Territorial Zone threshold according to UN (km)
TERRITORIAL_THRESHOLD = 22.2

# Contiguous Zone threshold according to UN (km)
CONTIGUOUS_THRESHOLD = 44.4

# Exclusive Economic Zone (EEZ) threshold according to UN (km)
EEZ_THRESHOLD = 370.4

GLOBAL_CRS = CRS("EPSG:4326") 


def get_test_data(data):
    
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

    ocean = [
            (Point(-19.410786, -25.399372), ('South Atlantic Ocean', None)), (Point(-38.681675, 36.970335), ('North Atlantic Ocean', None)),
            (Point(-139.580115, -51.982655), ('South Pacific Ocean', None)), (Point(-163.373354, 34.648612), ('North Pacific Ocean', None)),
            (Point(83.662074, -37.628773), ('South Indian Ocean', None)), (Point(78.289961, -0.547376), ('North Indian Ocean', None)),
            (Point(-165.104174, -73.822360), ('Southern Ocean', None))
            ]
            

    sea =  [
            (Point(124.064183, 31.397935), ('East China Sea', None)), (Point(133.284169, 40.052335), ('Japan Sea', None)), 
            (Point(113.741815, 16.406326), ('South China Sea', None)), (Point(89.307854, 16.406326), ('Bay of Bengal', None)),
            (Point(102.896795, 7.726632), ('Gulf of Thailand', None)), (Point(111.190989, -4.105610), ('Java Sea', None)),
            (Point(122.582401, 4.393135), ('Celebes Sea', None)), (Point(128.204137, -12.875791), ('Timor Sea', None)),
            (Point(125.467240, -4.695625), ('Banda Sea', None)), (Point(153.280038, -21.418900), ('Coral Sea', None)),
            (Point(34.612570, 43.216938), ('Black Sea', None)), (Point(18.439507, 35.739415), ('Mediterranean Sea', None)), (Point(14.334037, 43.758497), ('Adriatic Sea', None)),
            (Point(38.517827, 18.808599), ('Red Sea', None)), (Point(49.092521, 13.251091), ('Gulf of Aden', None)), (Point(49.590154, 28.479664), ('Persian Gulf', None)), 
            (Point(-75.270514, 14.767139), ('Caribbean Sea', None))
            ]
    

    if data == 'land':
        return land
    elif data == 'ocean':
        return ocean
    elif data == 'sea':
        return sea


def build_rtree(mbrs_gdf):
    """
    Spatially indexes MBRs by building an R*-tree.

    :param mbrs_gdf: (gpd.GeoDataFrame) -> contains all MBRs in geographical scope

    :return: (rtree.Index) -> the R*-tree
    """

    print('Building R*-tree...')

	# Populate R*-tree with MBRs
    rtree_obj = index.Index()
    for i, row in mbrs_gdf.iterrows():
        rtree_obj.insert(i, row['geometry'].bounds)

    return rtree_obj



def pip(query_point, possible_region_boundaries, name_field='name', admin_field='admin'):
    """
    Determines which region the passed point is in.

    :param query_point: (Shapely.Point) -> the given point
    :param possible_region_boundaries: (gpd.GeoDataFrame) -> the boundaries of the candidate regions

    :return: (str, str) -> the province of the given point, the country of the given point
    """
    
    enclosing_boundary = gpd.GeoDataFrame(columns=['name', 'admin', 'TERRAIN', 'geometry'], geometry='geometry')
    
    closest_boundary = gpd.GeoDataFrame(columns=['name', 'admin', 'TERRAIN', 'geometry'], geometry='geometry')
    min_distance = float('inf')

    for i, region_boundary in possible_region_boundaries.iterrows():

        # Keep track of closest boundary in case query_point misses all boundaries
        distance = query_point.distance(region_boundary['geometry'])
        if distance < min_distance:
            closest_boundary = region_boundary
            min_distance = distance

        # Short-cicruit once enclosing boundary is found
        if query_point.within(region_boundary['geometry']):
            enclosing_boundary = region_boundary
            break

    
    # Map query_point to closest boundary if it misses all existing boundaries
    if enclosing_boundary.empty:
        enclosing_boundary = closest_boundary

    # Map ocean query points to nearby land masses, if any
    if enclosing_boundary['TERRAIN'] == 'WATER':
        coastline_boundaries = possible_region_boundaries[possible_region_boundaries['TERRAIN'] == 'LAND']
        nearest_coastline_boundary, dist = nearest_coastline(query_point, coastline_boundaries)
        if dist < EEZ_THRESHOLD:
            enclosing_boundary = nearest_coastline_boundary

    # Get province, country information and return
    province = enclosing_boundary[name_field]
    country = enclosing_boundary[admin_field]

    return province, country




def nearest_coastline(query_point, coastline_boundaries):
    """
    Finds the nearest coastline to the passed point.
    
    :param query_point: (Shapely.Point) -> the query point
    :param coastline_boundaries: (gpd.GeoDataFrame) -> boundaries of nearby coastlines

    :return: (gpd.GeoDataFrame, float) -> the nearest coastline, the distance to the nearest coastline (km)
    """

    min_dist = float('inf')
    nearest_coastline_boundary = None

    # Search for nearest coastline
    for i, coastline in coastline_boundaries.iterrows():
        geom = coastline['geometry']
        
        # Go through all constituent polygons if geo is MultiPolygon
        if isinstance(geom, MultiPolygon):
            for polygon in geom.geoms:
                # Compute distance to nearest point on coastline
                nearest_point = nearest_points(query_point, polygon)[1]
                dist = _distance_km(query_point, nearest_point)
                if dist < min_dist:
                    min_dist = dist
                    nearest_coastline_boundary = coastline
        else:
            # Compute distance to nearest point on coastline
            nearest_point = nearest_points(query_point, geom)[1]
            dist = _distance_km(query_point, nearest_point)
            if dist < min_dist:
                min_dist = dist
                nearest_coastline_boundary = coastline
    
    return nearest_coastline_boundary, min_dist


def _distance_km(point_a, point_b):
    """
    Compute the Haversine distance between points given in terms of latitude and longitude.
    
    :param point_a: (shapely.Point) -> the first point
    :param point_b: (shapely.Point) -> the second point
    
    :return: (float) -> distance in km
    """

    # Radius of the Earth in kilometers
    earth_radius = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat_a = math.radians(point_a.y)
    lon_a = math.radians(point_a.x)
    
    lat_b = math.radians(point_b.y)
    lon_b = math.radians(point_b.x)

    # Difference in coordinates
    dlat = lat_b - lat_a
    dlon = lon_b - lon_a

    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat_a) * math.cos(lat_b) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = earth_radius * c

    return distance





def reverse_geocode_test(test_data, rtree_obj, boundaries_gdf):
    """
    Reverse geocode points.

    :param query_points: ()
    """

    for data in test_data:
        query_point = data[0]
        loc_truth = data[1]
        print(f'Processing {query_point} in {loc_truth}...')
        possible_region_boundaries_idx = list(rtree_obj.intersection(query_point.bounds))
        possible_region_boundaries = boundaries_gdf.loc[possible_region_boundaries_idx]
        possible_region_boundaries = possible_region_boundaries.sort_values(by='TERRAIN', ascending=True)
        print(f'Possible regions:\n{possible_region_boundaries}')
        province, country = pip(query_point, possible_region_boundaries)
        print(f'({province}, {country})')
        print('---------------------------------------------------------------------')
          

# Parallel GeoDataFrames containing both country/ocean data
boundaries_gdf = gpd.read_file('../../data/boundaries.geojson')
mbrs_gdf = gpd.read_file('../../data/mbrs.geojson')

rtree_obj = build_rtree(mbrs_gdf)


reverse_geocode_test(test_data=[(Point(109.359039, -6.670817), ('Central Java', 'Indonesia'))], rtree_obj=rtree_obj, boundaries_gdf=boundaries_gdf)
