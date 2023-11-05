import pandas as pd
import geopandas as gpd
from pyproj import CRS

GLOBAL_CRS = CRS("EPSG:4326") 

def compute_mbrs(boundaries_gdf, output_fpath, name_field='name'):
    """
    Computes MBRs from boundary data.

    :param boundaries_gdf: (gpd.GeoDataFrame) -> the boundary data
    :param output_fpath: (str) -> the output filepath
    """
    print(f'Computing MBRs...')

    mbr_list = []
    # Compute bounding box for each boundary and store in new Geo dataframe
    for index, row in boundaries_gdf.iterrows():
        print(f'Processing {row[name_field]}...')
        # Get geographic data
        geodata_name = row[name_field]
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


region_boundaries_gdf = gpd.read_file('../data/region_boundaries/region_boundaries.geojson')
region_boundaries_gdf['TERRAIN'] = 'LAND'
region_boundaries_gdf.to_file('../data/region_boundaries/region_boundaries.geojson', driver='GeoJSON')

marine_boundaries_gdf = gpd.read_file('../data/marine_boundaries/marine_boundaries.geojson')
marine_boundaries_gdf['TERRAIN'] = 'WATER'
marine_boundaries_gdf.to_file('../data/marine_boundaries/marine_boundaries.geojson', driver='GeoJSON')


boundaries_gdf = pd.concat([region_boundaries_gdf, marine_boundaries_gdf], ignore_index=True)
boundaries_gdf = boundaries_gdf[['name', 'admin', 'TERRAIN', 'geometry']]
boundaries_gdf.to_file('../data/boundaries.geojson')













