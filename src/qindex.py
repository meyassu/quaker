import rtree
from rtree import index
import geopandas as gpd

"""
Spatial indexing computations
"""
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