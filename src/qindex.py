from rtree import index

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
	rtree = index.Index()
	for i, row in mbrs_gdf.iterrows():
		rtree.insert(i, row['geometry'].bounds)

	return rtree


def save_rtree(rtree, output_fpath):
    """
    Seralizes the rtree to a file.

    :param rtree: (rtree.index) -> the rtree
    :param output_fpath: (str) -> the output filepath

    :return: None
    """

    # Serialize the R-tree to a file
    with open(output_fpath, 'wb') as f:
        rtree.serialize(f)

def load_rtree(fpath):
    """
    Loads the rtree from file.
	
	:param fpath: (str) -> the rtree filepath
     
     :return (rtree.Index) -> the rtree
    """

    # Load the serialized R-tree from a file
    with open(fpath, 'rb') as f:
        rtree = rtree.index.Index()
        rtree.deserialize(f)

    return rtree





#