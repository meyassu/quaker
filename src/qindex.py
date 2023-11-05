import rtree
from rtree import index
import geopandas as gpd

"""
Spatial indexing computations
"""
def _set_rtree_properties(fpath='../data/rtree', overwrite=True, dat_extension=None, idx_extension=None):
    """
    Sets rtree properties mostly relevant to serialization / deserialization.

    :param fpath: (str) -> the rtree filepath
    :param overwrite: (bool) -> whether to overwrite existing rtree files during serialization
    :param dat_extension: (str) -> file extension for data file
    :param idx_extension: (str) -> file extension for index file

    :return: (rtree.index.Property) -> object storing all property preferences
    """

    properties = rtree.index.Property()

    properties.filename = fpath

    properties.overwrite = overwrite

    if dat_extension != None:
        properties.dat_extension = dat_extension
    if idx_extension != None:
        properties.idx_extension = idx_extension

    return properties


def build_rtree(mbrs_gdf, properties):
    """
    Spatially indexes MBRs by building an R*-tree.

    :param mbrs_gdf: (gpd.GeoDataFrame) -> contains all MBRs in geographical scope

    :return: (rtree.Index) -> the R*-tree
    """

    print('Building R*-tree...')

	# Populate R*-tree with MBRs
    rtree_obj = index.Index(properties.filename, properties=properties)
    for i, row in mbrs_gdf.iterrows():
        rtree_obj.insert(i, row['geometry'].bounds)

    return rtree_obj