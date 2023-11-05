import rtree
from rtree import index
import geopandas as gpd

"""
Spatial indexing computations
"""

def _set_properties(fname='../data/rtree', overwrite=True, dat_extension='data', idx_extension='index'):

    properties = rtree.index.Property()

    properties.filename = fname

    properties.overwrite = overwrite

    properties.dat_extension = dat_extension
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
    rtree = index.Index(properties.filename, properties=properties)
    for i, row in mbrs_gdf.iterrows():
        rtree.insert(i, row['geometry'].bounds)

    return rtree

def load_rtree(properties):
    """
    Loads the rtree from file.
	
	:param fpath: (str) -> the rtree filepath
     
    :return (rtree.Index) -> the rtree
    """

    dat_file = properties.fname + properties.dat_extension
    idx_file = properties.fname + properties.idx_extension
    rtree = rtree.index.Rtree(dat_file, idx_file, properties=properties)
    
    return rtree


mbrs_gdf = gpd.read_file('../data/mbrs.geojson')
rtree = build_rtree(mbrs_gdf, _set_properties())