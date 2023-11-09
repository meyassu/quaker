import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


from qindex import build_rtree
from database import get_neon_engine, get_data, write_table, transfer_data



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
    
    enclosing_boundary = None

    closest_boundary = None
    min_distance = float('inf')

    for i, region_boundary in possible_region_boundaries.iterrows():

        # Keep track of closest boundary in case query_point misses all boundaries
        distance = query_point.distance(region_boundary['geometry'])
        if distance < min_distance:
            closest_boundary = region_boundary
            min_distance = distance

        if query_point.within(region_boundary['geometry']):
            enclosing_boundary = region_boundary
            break

    
    # Map query_point to closest boundary if it misses all existing boundaries
    if enclosing_boundary == None:
        enclosing_boundary = closest_boundary

        

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
            result = pip(coordinates, possible_region_boundaries)
            province = None
            country = None
            # Find closest region if PiP returned null 
            if pd.isna(result):
                province, country = find_closest_region(coordinates, possible_region_boundaries_idx, boundaries_gdf)
            else:
                province = result[0]
                country = result[1]
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

