#%%
import numpy as np
import pandas as pd
import requests

import json

import geopandas as gpd
from shapely.geometry import shape

#%%
# get HUC shapes
print("Reading the HUC areas of interest")
wkaois = pd.read_feather(
    "C:\\Users\\sdamiano\\Desktop\\MusconectongWA\\SRGD_ModelMyWatershed\\mmw_results\\wkaois.feather"
)[["HUC Name", "HUC Code", "wkaoi_code"]]
wkaois["HUC Name"] = wkaois["HUC Name"].str.replace("-Delaware River", "")
wkaois["basin_size"] = wkaois["wkaoi_code"].str.upper()
wkaois["HUC10"] = (
    wkaois["HUC Code"]
    .str.slice(0, 10)
    .where(wkaois["basin_size"].isin(["HUC10", "HUC12"]), np.nan)
)
wkaois["HUC12"] = wkaois["HUC Code"].where(wkaois["basin_size"].isin(["HUC12"]), np.nan)

#%%
# get catchment list from sub-basin results
catchments = (
    pd.read_feather(
        "C:\\Users\\sdamiano\\Desktop\\MusconectongWA\\SRGD_ModelMyWatershed\\mmw_results\\catchment_nutrient_results.feather"
    )[["parent_huc", "ComID"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
catchments["HUC10"] = catchments["parent_huc"].str.slice(0, 10)
catchments["HUC12"] = catchments["parent_huc"]
catchments["is_in_focus_huc"] = (
    catchments["HUC10"].isin(
        [
            "0204010501",  # Paulins Kill
            "0204010502",  # Pequest River
            "0204010504",  # Pohatcong Creek
            "0204010505",  # Musconetcong River
            "0204010506",  # Upper Delaware River (Lopatcong)
            "0204010410",  # Flat Brook
        ]
    )
) | (
    catchments["HUC12"].isin(
        [
            "020401040308",  # Lower Neversink
            "020401040704",  # Shimers Brook
            "020401040705",  # Hornbecks Creek
        ]
    )
)
catchments["grouper"] = np.floor(catchments.index / 100)

#%%
# get details about each catchment
all_catchment_details = []
for name, chunk in catchments.groupby(by=["grouper"]):
    print(name)
    comid_list = chunk["ComID"].unique().tolist()
    comid_details = requests.get(
        "https://staging.modelmywatershed.org/mmw/modeling/subbasins/catchments",
        params={"catchment_comids": json.dumps(comid_list)},
    )
    print(comid_details)
    # all_catchment_details.extend(comid_details.json())
    for catchment in comid_details.json():
        all_catchment_details.append(catchment)

# %%
# save the catchment details
with open(
    "C:\\Users\\sdamiano\\Desktop\\MusconectongWA\\SRGD_ModelMyWatershed\\mmw_results\\nhd_catchment_details.json",
    "w",
) as fp:
    json.dump(all_catchment_details, fp, indent=2)

#%%
# turn catchment details into a dataframe
catchment_detail_df = pd.DataFrame(all_catchment_details)
catchment_detail_df["ComID"] = catchment_detail_df["id"].apply(str)
catchment_detail_df["nhd_catchment_geometry"] = catchment_detail_df.apply(
    lambda row: shape(row["shape"]), axis=1
)
catchment_detail_df["stream_geometry"] = catchment_detail_df.apply(
    lambda row: shape(row["stream"]), axis=1
)
catchments2 = catchments.merge(catchment_detail_df, on="ComID")[
    [
        "ComID",
        "HUC10",
        "HUC12",
        "is_in_focus_huc",
        "area",
        "nhd_catchment_geometry",
        "stream_geometry",
    ]
]
catchments3 = catchments2.merge(
    wkaois[["HUC Name", "HUC10", "HUC12"]], on=["HUC10", "HUC12"], how="left"
)


gdf_shapes = gpd.GeoDataFrame(
    catchments3.drop(columns=["stream_geometry"]),
    geometry="nhd_catchment_geometry",
).set_crs("EPSG:4326")
gdf_shapes.to_file(
    "C:\\Users\\sdamiano\\Desktop\\MusconectongWA\\SRGD_ModelMyWatershed\\mmw_results\\nhd_catchment_shapes.geojson",
    driver="GeoJSON",
)

gdf_lines = gpd.GeoDataFrame(
    catchments3.drop(columns=["area", "nhd_catchment_geometry"]),
    geometry="stream_geometry",
).set_crs("EPSG:4326")
gdf_lines.to_file(
    "C:\\Users\\sdamiano\\Desktop\\MusconectongWA\\SRGD_ModelMyWatershed\\mmw_results\\nhd_catchment_blue_lines.geojson",
    driver="GeoJSON",
)
