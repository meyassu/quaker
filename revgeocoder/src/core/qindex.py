from rtree import index
import geopandas as gpd
import logging

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
        raise TypeError('Input must be a GeoDataFrame')

    # Check if 'geometry' column exists
    if 'geometry' not in mbrs_gdf.columns:
        raise ValueError('GeoDataFrame must have a "geometry" column')

	# Populate R*-tree with MBRs
    try:
        logging.log('Building R*-tree...')
        rtree_obj = index.Index()
        for i, row in mbrs_gdf.iterrows():
            try:
                rtree_obj.insert(i, row['geometry'].bounds)
            except Exception as e:
                logging.error(f'Error inserting MBR to R*-tree at index {i}: {e}')
                raise
    except Exception as e:
        logging.error(f'Failed to build R*-tree: {e}')
        raise
    
    return rtree_obj



