from rtree import index
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

	# Populate R*-tree with MBRs
    try:
        print('Building R*-tree...')
        rtree_obj = index.Index()
        for i, row in mbrs_gdf.iterrows():
            rtree_obj.insert(i, row['geometry'].bounds)
    except Exception as e:
        logging.error(f"Failed to build R*-tree: {e}")
        raise

    return rtree_obj

