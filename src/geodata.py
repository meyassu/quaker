import pandas as pd
import geopandas as gpd
from pyproj import CRS

GLOBAL_CRS = CRS("EPSG:4326") 

def compute_mbrs(boundaries_gdf, output_fpath):
    """

    """
    print(f'Computing MBRs...')

    mbr_list = []
    # Compute bounding box for each boundary and store in new Geo dataframe
    for index, row in boundaries_gdf.iterrows():
        print(f'Processing {row['name']}...')
        # Get geographic data
        geodata_name = row['name']
        mbr = row['geometry'].envelope

        mbr_list.append({'NAME': geodata_name, 'geometry': mbr})

    mbrs_gdf = gpd.GeoDataFrame(mbr_list, crs=GLOBAL_CRS)

    # Save MBR data in new file
    mbrs_gdf.to_file(output_fpath, driver='GeoJSON')

    return mbrs_gdf


def _shapefile_to_geojson(boundaries_shp_fpath, output_fpath):
    """
    Translates Shapefile into GeoJSON.
    :param boundaries_shp_fpath: (str) -> boundary polygons Shapefile path
    :param output_fpath: (str) -> output GeoJSON filepath
    :return: None
    """

    print(f'Writing {boundaries_shp_fpath} to {output_fpath} as GeoJSON file...')

    boundaries_gdf = gpd.read_file(boundaries_shp_fpath)
    boundaries_gdf.to_file(output_fpath, driver='GeoJSON')










