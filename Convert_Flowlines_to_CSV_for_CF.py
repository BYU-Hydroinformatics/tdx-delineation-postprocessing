"""
Convert Flowlines to CSV for CF
Tool for RAPID Preprocessing
Louis Rosas, BYU Hydroinformatics Lab 
2023

To run, following the docstring of the function defined below
"""
import pandas as pd
import geopandas as gpd
import os

def CreateComidLatLonZ(network: gpd.GeoDataframe, out_dir: str, id_field: str) -> None:
    """
    Create a comid_lat_lon_z csv in output directory, based on an input drainage line network that has been read in as a Geopandas Geodataframe.
    id_field refers to the ID field that represents the unique id for each stream segment in your drainage line
    
    Note: We assume that the value "z", or the elevation, is not present in the drainage line, and so it is set to 0.
    """
    # Ensure that the drainage line is sorted
    network.sort_values(id_field, inplace=True)
    gdf = gpd.GeoDataFrame.copy(network)
    gdf['lat'] = network.geometry.apply(lambda geom: geom.xy[1][0])
    gdf['lon'] = network.geometry.apply(lambda geom: geom.xy[0][0])
    data = {id_field: network[id_field].values,
            "lat": gdf['lat'].values,
            "lon": gdf['lon'].values,
            "z": 0}
    
    pd.DataFrame(data).to_csv(os.path.join(out_dir, "comid_lat_lon_z.csv"), index=False, header=True)
