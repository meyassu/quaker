import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from qindex import build_rtree, _set_rtree_properties
from database import get_neon_engine, get_data


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



def reverse_geocode(rtree_obj, boundaries_gdf, table_name, batch_size, engine):
    """
    Reverse geocode points and write results back to database.

    :param rtee_obj: (rtree.index.Index) -> the R*-tree
    :param boundaries_gdf: (gpd.GeoDataFrame) -> the boundary data
    :param table_name: (str) -> the target table name
    :param batch_size: (int) -> the batch size
    :param engine: (SQLAlchemy.engine) -> the engine used to interface with database

    :return: None
    """

    offset = 0
    while True:
        # Get batch
        query = f'SELECT "Latitude", "Longitude" FROM {table_name} LIMIT {batch_size} OFFSET {offset};'
        batch = get_data(query, engine)
        if batch.empty: # TEST THIS
            return
        batch_results = pd.DataFrame(columns=['Province', 'Country'])
        # Reverse geocode points in batch
        for index, row in batch.iterrows():
            # Get coordinate point
            longitude = row['Longitude']
            latitude = row['Latitude']
            coordinates = Point(longitude, latitude)
            # Narrow down options with R*-tree
            possible_region_boundaries_idx = list(rtree_obj.intersection(coordinates.bounds))
            possible_region_boundaries = boundaries_gdf.loc[possible_region_boundaries_idx]
            possible_region_boundaries = possible_region_boundaries.sort_values(by='TERRAIN', ascending=True)
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
            
            # Add results to batch_results
            batch_results = batch_results.append({'Province': province, 'Country': country}, ignore_index=True)

            offset += batch_size

            print(batch_results)
            print(type(batch_results))
            quit()

        quit()













    # for data in test_data:
    #     query_point = data[0]
    #     loc_truth = data[1]
    #     print(f'Processing {query_point} in {loc_truth}...')
    #     possible_region_boundaries_idx = list(rtree_obj.intersection(query_point.bounds))
    #     possible_region_boundaries = boundaries_gdf.loc[possible_region_boundaries_idx]
    #     possible_region_boundaries = possible_region_boundaries.sort_values(by='TERRAIN', ascending=True)
    #     print(f'Possible regions:\n{possible_region_boundaries}')
    #     result = pip(query_point, possible_region_boundaries)
    #     rregion = None
    #     admin = None
    #     if pd.isna(result):
    #         rregion, admin = find_closest_region(query_point, possible_region_boundaries_idx, boundaries_gdf)
    #     else:
    #         rregion = result[0]
    #         admin = result[1]

    #     print(f'({region}, {admin})')
    #     print('---------------------------------------------------------------------')



# Parallel GeoDataFrames containing both country/ocean data
# boundaries_gdf = gpd.read_file('../data/boundaries.geojson')
# mbrs_gdf = gpd.read_file('../data/mbrs.geojson')

# rtree_properties = _set_rtree_properties()
# rtree_obj = build_rtree(mbrs_gdf, rtree_properties)

engine = get_neon_engine()

reverse_geocode(None, None, table_name='earthquakes', batch_size=10, engine=engine)

