from rtree import index
import geopandas as gpd
import os

from src import LOGGER


"""
Spatial indexing computations
"""
def build_rtree(mbrs_gdf):
    """
    Spatially indexes MBRs by building an R*-tree.

    :param mbrs_gdf: (gpd.GeoDataFrame) -> contains all MBRs in geographical scope

    :return: (rtree.Index) -> the R*-tree
    """

    # Basic validation
    if not isinstance(mbrs_gdf, gpd.GeoDataFrame):
        LOGGER.error('Input must be a GeoDataFrame')
        raise TypeError('Input must be a GeoDataFrame')

    # Check if 'geometry' column exists
    if 'geometry' not in mbrs_gdf.columns:
        LOGGER.error('GeoDataFrame must have a "geometry" column')
        raise ValueError('GeoDataFrame must have a "geometry" column')

	# Populate R*-tree with MBRs
    try:
        LOGGER.info('Building R*-tree...')
        print('Building R*-tree...', flush=True)
        rtree_obj = index.Index()
        for i, row in mbrs_gdf.iterrows():
            try:
                rtree_obj.insert(i, row['geometry'].bounds)
            except Exception as e:
                LOGGER.error(f'Error inserting MBR to R*-tree at index {i}: {e}') 
                raise
    except Exception as e:
        LOGGER.error(f'Failed to build R*-tree: {e}')
        raise
    
    return rtree_obj



