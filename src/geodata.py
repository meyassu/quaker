import geopandas as gpd
from pyproj import CRS

GLOBAL_CRS = CRS("EPSG:4326") 

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
Generalized dataframes
"""
def combine_boundaries(country_boundaries_gdf, marine_boundaries_gdf):

    pass

def combine_mbrs(country_mbrs_gdf, marine_mbrs_gdf):

    pass