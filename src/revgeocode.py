import pandas as pd
import geopandas as gpd

from shapely.geometry import Point, MultiPolygon
from shapely.ops import nearest_points
import math


from qindex import build_rtree
from database import get_neon_engine, get_data, write_table, transfer_data

"""
Constants
"""
# Territorial Zone threshold according to UN (km)
TERRITORIAL_THRESHOLD = 22.2

# Contiguous Zone threshold according to UN (km)
CONTIGUOUS_THRESHOLD = 44.4

# Exclusive Economic Zone (EEZ) threshold according to UN (km)
EEZ_THRESHOLD = 370.4



"""
Reverse geocoding algorithm
"""
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
    for coastline in coastline_boundaries:
        geom = coastline['geometry']
        
        # Go through all constituent polygons if geo is MultiPolygon
        if isinstance(geom, MultiPolygon):
            for polygon in geom:
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

def reverse_geocode(rtree_obj, boundaries_gdf, table_name, batch_size, staging_table_name, engine):
    """
    Reverse geocode points and write results back to database.

    :param rtee_obj: (rtree.index.Index) -> the R*-tree
    :param boundaries_gdf: (gpd.GeoDataFrame) -> the boundary data
    :param table_name: (str) -> the target table name
    :param batch_size: (int) -> the batch size
    :param engine: (SQLAlchemy.engine) -> the engine used to interface with database

    :return: None
    """

    print('Reverse geocoding coordinates...')

    offset = 0
    while True:
        print(f'Processing batch {offset / batch_size}...')
        # Get batch
        query = f'SELECT "Latitude", "Longitude" FROM {table_name} LIMIT {batch_size} OFFSET {offset};'
        batch = get_data(query, engine)
        if batch.empty: # TEST THIS
            return
        batch_results = []
        # Reverse geocode points in batch
        for index, row in batch.iterrows():
            # Get coordinate point
            longitude = row['Longitude']
            latitude = row['Latitude']
            coordinates = Point(longitude, latitude)
            # print(f'Reverse geocoding {coordinates}...')
            # Narrow down options with R*-tree
            possible_region_boundaries_idx = list(rtree_obj.intersection(coordinates.bounds))
            possible_region_boundaries = boundaries_gdf.loc[possible_region_boundaries_idx]
            possible_region_boundaries = possible_region_boundaries.sort_values(by='TERRAIN', ascending=True)
            # print(f'Possible regions:\n{possible_region_boundaries}')
            # Run Point-in-Polygon on coordinate
            province, country = pip(coordinates, possible_region_boundaries)
            # print(f'Result: ({province}, {country})')
            # print('\n-----------------------------------------------------------------------------------------------------\n')
            # Add results to batch_results
            batch_results.append({'Province': province, 'Country': country})
        
        # Package batch_results into DataFrame
        batch_results = pd.DataFrame(batch_results, columns=['Province', 'Country'])

        # Write batch_results to staging table
        write_table(batch_results, table_name=staging_table_name, if_exists='append', engine=engine)
        
        # Increment offset
        offset += batch_size






# Parallel GeoDataFrames containing both country/ocean data
boundaries_gdf = gpd.read_file('../data/boundaries.geojson')
mbrs_gdf = gpd.read_file('../data/mbrs.geojson')

rtree_obj = build_rtree(mbrs_gdf)

engine = get_neon_engine()

reverse_geocode(rtree_obj=rtree_obj, boundaries_gdf=boundaries_gdf, table_name='earthquakes', batch_size=1000, staging_table_name='staging', engine=engine)

