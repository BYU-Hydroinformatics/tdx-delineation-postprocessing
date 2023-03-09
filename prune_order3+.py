import json
import geopandas as gpd
import numpy as np
import pandas as pd


def prune_stream_network(streams_gdf, polygons_gdf):
    """
    Chat GPT-generated function
    Args:
        streams_gdf:
        polygons_gdf:

    Returns:

    """
    # Create subsets of order 1 and higher-order segments
    order_1 = streams_gdf.loc[streams_gdf[order_col] == 1]
    higher_order = streams_gdf.loc[streams_gdf[order_col] >= 3]

    # Add the next down ids field onto the polygons gdf
    polygons_gdf_new = polygons_gdf.merge(streams_gdf[[riv_id_col, ds_id_col, order_col]],
                                      left_on=chmt_riv_id_col,
                                      right_on=riv_id_col)
    print(polygons_gdf_new)
    # Create list of downstream IDs for higher-order segments
    # downstream_ids = list(higher_order[ds_id_col])
    # Create list of upstream IDs for higher-order segments
    upstream_ids = list(higher_order[ds_id_col])

    # Find order 1 segments with higher order streams as their direct children
    pruned_order_1 = order_1.loc[order_1[ds_id_col].isin(higher_order[riv_id_col].tolist())]
    pruned_order_1_ids = pruned_order_1[riv_id_col].to_list()
    higher_order_sibling_ids = higher_order.loc[
        higher_order[ds_id_col].isin(pruned_order_1[ds_id_col]), riv_id_col].tolist()

    # Get the polygons that need to be pruned and their siblings and dissolve them together by their downstream id to
    # find siblings
    polygons_to_merge = polygons_gdf_new.loc[
        polygons_gdf_new[riv_id_col].isin(higher_order_sibling_ids + pruned_order_1_ids)].sort_values(by=order_col)

    dissolved_polygons = polygons_to_merge.dissolve(by=ds_id_col, aggfunc='last')
    merged_polygons = gpd.GeoDataFrame(pd.concat(
        [polygons_gdf.loc[~polygons_gdf[chmt_riv_id_col].isin(higher_order_sibling_ids + pruned_order_1_ids)],
         dissolved_polygons[polygons_gdf.columns]]))

    print(dissolved_polygons)

    # Prune the unneccesary order_ones
    pruned_network = streams_gdf[~streams_gdf[riv_id_col].isin(pruned_order_1_ids)]

    return pruned_network, merged_polygons

pd.options.display.max_columns = 20
pd.options.display.width = 400

region = 'japan'
all_order_json = f'output_jsons/{region}_allorders.json'
network_gpkg = f'Japan/TDX_streamnet_4020034510_01.shp'
chmt_gpkg = f'Japan/TDX_streamreach_basins_4020034510_01.gpkg'
output_gpkg_name = f'output_gpkgs/pruned_network_{region}.gpkg'
output_gpkg_chmt_name = f'output_gpkgs/dissolved_catchments_{region}.gpkg'
output_json_name = f'output_jsons/{region}_streams_to_prune.json'

max_order = 2
riv_id_col = 'LINKNO'
ds_id_col = 'DSLINKNO'
order_col = 'strmOrder'
chmt_riv_id_col = 'streamID'

if __name__ == '__main__':
    pd.options.display.max_columns = 20
    pd.options.display.width = 400

    streams_gdf = gpd.read_file(network_gpkg)
    chmt_gdf = gpd.read_file(chmt_gpkg)

    pruned_network, dissolved_polygons = prune_stream_network(streams_gdf, chmt_gdf)

    dissolved_polygons.to_file(output_gpkg_chmt_name, driver="GPKG")
    pruned_network.to_file(output_gpkg_name, driver="GPKG")
