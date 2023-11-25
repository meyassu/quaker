import pandas as pd
import geopandas as gpd

import rtree

from shapely.geometry import Point, MultiPolygon
from shapely.ops import nearest_points

import math
import os

from src.utils.database import write_table, get_data
from src import LOGGER


"""
Local Constants
"""
# Territorial Zone threshold according to UN (km)
TERRITORIAL_THRESHOLD = 22.2

# Contiguous Zone threshold according to UN (km)
CONTIGUOUS_THRESHOLD = 44.4

# Exclusive Economic Zone (EEZ) threshold according to UN (km)
EEZ_THRESHOLD = 370.4

# Batch size
BATCH_SIZE = int(os.getenv('BATCH_SIZE'))


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
    
    # Basic validation
    if not isinstance(query_point, Point):
        LOGGER.error('query_point must be a Shapely Point')
        raise TypeError('query_point must be a Shapely Point')
    if not isinstance(possible_region_boundaries, gpd.GeoDataFrame):
        LOGGER.error('possible_region_boundaries must be a GeoDataFrame')
        raise TypeError('possible_region_boundaries must be a GeoDataFrame')

    # Required fields check
    required_fields = [name_field, admin_field, 'TERRAIN', 'geometry']
    if not all(field in possible_region_boundaries.columns for field in required_fields):
        missing_fields = [field for field in required_fields if field not in possible_region_boundaries.columns]
        LOGGER.error(f'possible_region_boundaries is missing required fields: {missing_fields}')
        raise ValueError(f'possible_region_boundaries is missing required fields: {missing_fields}')
    
    try:
        enclosing_boundary = gpd.GeoDataFrame(columns=['name', 'admin', 'TERRAIN', 'geometry'], geometry='geometry')
        closest_boundary = gpd.GeoDataFrame(columns=['name', 'admin', 'TERRAIN', 'geometry'], geometry='geometry')
        min_distance = float('inf')

        for i, region_boundary in possible_region_boundaries.iterrows():
            try:
                # Keep track of closest boundary in case query_point misses all boundaries
                distance = query_point.distance(region_boundary['geometry'])
                if distance < min_distance:
                    closest_boundary = region_boundary
                    min_distance = distance

                # Short-cicruit once enclosing boundary is found
                if query_point.within(region_boundary['geometry']):
                    enclosing_boundary = region_boundary
                    break
            except Exception as e:
                LOGGER.error(f'Error processing region boundary at index {i}: {e}')
                raise

        
        # Map query_point to closest boundary if it misses all existing boundaries
        if enclosing_boundary.empty:
            enclosing_boundary = closest_boundary

        # Map ocean query points to nearby land masses, if any
        if enclosing_boundary['TERRAIN'] == 'WATER':
            coastline_boundaries = possible_region_boundaries[possible_region_boundaries['TERRAIN'] == 'LAND']
            if not coastline_boundaries.empty:
                nearest_coastline_boundary, dist = nearest_coastline(query_point, coastline_boundaries)
                if dist < EEZ_THRESHOLD:
                    enclosing_boundary = nearest_coastline_boundary

        # Get province, country information and return
        province = enclosing_boundary[name_field] 
        country = enclosing_boundary[admin_field]
    except Exception as e:
        LOGGER.error(f'Failed in point-in-polygon processing: {e}')
        raise
    
    return province, country


def nearest_coastline(query_point, coastline_boundaries):
    """
    Finds the nearest coastline to the passed point.
    
    :param query_point: (Shapely.Point) -> the query point
    :param coastline_boundaries: (gpd.GeoDataFrame) -> boundaries of nearby coastlines

    :return: (gpd.GeoDataFrame, float) -> the nearest coastline, the distance to the nearest coastline (km)
    """

    # Validate input types
    if not isinstance(query_point, Point):
        LOGGER.error('query_point must be a Shapely Point')
        raise TypeError('query_point must be a Shapely Point')
    if not isinstance(coastline_boundaries, gpd.GeoDataFrame):
        LOGGER.error('coastline_boundaries must be a GeoDataFrame')
        raise TypeError('coastline_boundaries must be a GeoDataFrame')

    # Check for empty GeoDataFrame
    if coastline_boundaries.empty:
        LOGGER.error('No coastline boundaries provided')
        raise ValueError('coastline_boundaries is empty')

    try:
        min_dist = float('inf')
        nearest_coastline_boundary = None

        # Search for nearest coastline
        for i, coastline in coastline_boundaries.iterrows():
            try:
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
            except Exception as e:
                LOGGER.error(f'Error processing coastline at index {i}: {e}')
                raise
    except Exception as e:
        LOGGER.error(f'Failed in searching nearest coastline: {e}')
        raise
    
    return nearest_coastline_boundary, min_dist

def _distance_km(point_a, point_b):
    """
    Compute the Haversine distance between points given in terms of latitude and longitude.
    
    :param point_a: (shapely.Point) -> the first point
    :param point_b: (shapely.Point) -> the second point
    
    :return: (float) -> distance in km
    """

    # Validate input types
    if not isinstance(point_a, Point) or not isinstance(point_b, Point):
        LOGGER.error('point_a and point_b must be a Shapely Point')
        raise TypeError('point_a and point_b must be a Shapely Point')

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

def reverse_geocode(rtree_obj, boundaries_gdf, data_table_name, location_table_name, engine):
    """
    Reverse geocode points and write results back to database.

    :param rtee_obj: (rtree.index.Index) -> the R*-tree
    :param boundaries_gdf: (gpd.GeoDataFrame) -> the boundary data
    :param table_name: (str) -> the target table name
    :param engine: (SQLAlchemy.engine) -> the engine used to interface with database

    :return: None
    """

    # Validate input types
    if not isinstance(rtree_obj, rtree.index.Index):
        LOGGER.error('rtree_obj must be an rtree.index.Index')
        raise TypeError('rtree_obj must be an rtree.index.Index')
    if not isinstance(boundaries_gdf, gpd.GeoDataFrame):
        LOGGER.error('boundaries_gdf must be a GeoDataFrame')
        raise TypeError('boundaries_gdf must be a GeoDataFrame')

    LOGGER.info('Reverse geocoding coordinates...')
    print('Reverse geocoding coordinates...', flush=True)
    offset = 0
    try:
        while True:
            LOGGER.info(f'Processing batch {offset / BATCH_SIZE}...')
            print(f'Processing batch {offset / BATCH_SIZE}...', flush=True)
            # Get batch
            query = f'SELECT "latitude", "longitude" FROM {data_table_name} LIMIT {BATCH_SIZE} OFFSET {offset};'
            
            batch = get_data(query, engine)
            
            if batch.empty:
                return
            batch_results = []
            # Reverse geocode points in batch
            for index, row in batch.iterrows():
                try:
                    # Get coordinate point
                    longitude = row['longitude']
                    latitude = row['latitude']
                    coordinates = Point(longitude, latitude)
                    LOGGER.debug(f'Reverse geocoding {coordinates}...')
                    # Narrow down options with R*-tree
                    possible_region_boundaries_idx = list(rtree_obj.intersection(coordinates.bounds))
                    possible_region_boundaries = boundaries_gdf.loc[possible_region_boundaries_idx]
                    possible_region_boundaries = possible_region_boundaries.sort_values(by='TERRAIN', ascending=True)
                    LOGGER.debug(f'Possible regions:\n{possible_region_boundaries}')
                    # Run Point-in-Polygon on coordinate
                    province, country = pip(coordinates, possible_region_boundaries)
                    LOGGER.debug(f'Result: ({province}, {country})')
                    LOGGER.debug('\n-----------------------------------------------------------------------------------------------------\n')
                    # Add results to batch_results
                    batch_results.append({'province': province, 'country': country})
                except Exception as e:
                    LOGGER.error(f"Error in reverse geocoding for batch index {index}: {e}")
            # Package batch_results into DataFrame
            batch_results = pd.DataFrame(batch_results, columns=['province', 'country'])

            # Write batch_results to staging table
            write_table(batch_results, table_name=location_table_name, if_exists='append', engine=engine)
            
            # Increment offset
            offset += BATCH_SIZE
    except Exception as e:
        LOGGER.error(f'Failed in reverse geocoding process: {e}')
        raise


