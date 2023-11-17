import pandas as pd
import geopandas as gpd
from pyproj import CRS
import logging

GLOBAL_CRS = CRS("EPSG:4326")

"""
TODO
- model maritime territorial control w/ Shapely.Polygon.buffer
"""

def compute_mbrs(boundaries_gdf, output_fpath, name_field='name'):
    """
    Computes MBRs from boundary data.

    :param boundaries_gdf: (gpd.GeoDataFrame) -> the boundary data
    :param output_fpath: (str) -> the output filepath
    """
    
    
    logging.log(f'Computing MBRs...')
    
    try:
        # Compute bounding box for each boundary and store in new Geo dataframe
        mbr_list = []
        for index, row in boundaries_gdf.iterrows():
            logging.log(f'Processing {row[name_field]}...')
            try:
                # Get geographic data
                geodata_name = row[name_field]
                mbr = row['geometry'].envelope
                mbr_list.append({'NAME': geodata_name, 'geometry': mbr})
            except Exception as e:
                logging.error(f'Error computing MBR for {row} in boundaries_gdf @ index {index}: {e}')
                raise
        mbrs_gdf = gpd.GeoDataFrame(mbr_list, crs=GLOBAL_CRS)
        # Save MBR data in new file
        mbrs_gdf.to_file(output_fpath, driver='GeoJSON')
    except Exception as e:
        logging.error(f'Error computing MBRs: {e}')
        raise

    return mbrs_gdf


def _shapefile_to_geojson(boundaries_shp_fpath, output_fpath):
    """
    Translates Shapefile into GeoJSON.
    :param boundaries_shp_fpath: (str) -> boundary polygons Shapefile path
    :param output_fpath: (str) -> output GeoJSON filepath
    :return: None
    """

    logging.log(f'Writing {boundaries_shp_fpath} to {output_fpath} as GeoJSON file...')
    
    # Translate Shapefile into GeoJSON
    try:
        boundaries_gdf = gpd.read_file(boundaries_shp_fpath)
        boundaries_gdf.to_file(output_fpath, driver='GeoJSON')
    except Exception as e:
        logging.error(f'Error translating Shapefile @ {boundaries_shp_fpath} to GeoJSON file @ {output_fpath}: {e}')
        raise










