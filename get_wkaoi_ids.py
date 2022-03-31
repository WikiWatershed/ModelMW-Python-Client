"""
Created by Sara Geleskie Damiano

Get HUC codes by searching the national map viewer:  https://apps.nationalmap.gov/viewer/

Select the layer list and turn on the watershed boundary dataset

Click the three dots next to the Watershed Boundary Dataset's layer and select "view in attribute table"

Search in the attribute tables for HUC's within the super-HUC that are in the right state or boundary

Put that into a csv with name and HUC.

"""
#%%
from os import times
import sys
import time
import copy

import pandas as pd
import requests

import json


#%%  Read location data
sys.path.append("C:\\Users\\sdamiano\\Desktop\\SRGD_ModelMyWatershed")
from mmw_secrets import (
    srgd_mmw_api_key,
    srgd_staging_api_key,
    srgd_mmw_user,
    srgd_mmw_pass,
    save_path,
    feather_path,
    json_dump_path,
    json_extension,
    feather_extension,
)
from model_functions import ModelMyWatershedAPI

# Create an API user
mmw_run = ModelMyWatershedAPI(save_path, srgd_staging_api_key, True)
# Authenticate with MMW
mmw_run.login(mmw_user=srgd_mmw_user, mmw_pass=srgd_mmw_pass)

#%%
print("Reading the areas of interest")
wkaois = pd.read_csv(save_path + "ModelMW_wkaoi.csv")
wkaois["_huc_code_"] = wkaois["HUC Code"]
wkaois["HUC Code"] = wkaois["HUC Code"].str.replace("_", "")

#%%
# Searching for boundary ID's
for idx, huc in wkaois.iterrows():
    search_results = requests.get(
        "{}/{}/{}".format(
            "https://modelmywatershed.org", "mmw", "modeling/boundary-layers-search"
        ),
        params={"text": huc["HUC Name"]},
        headers={
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://staging.modelmywatershed.org/draw",
        },
    ).json()["suggestions"]
    got_result = False
    for result in search_results:
        if result["code"] == huc["wkaoi_code"] and result["text"] == huc["HUC Name"]:
            got_result = True
            if pd.isna(huc["wkaoi_id"]):
                print()
                print(huc["HUC Name"])
                print(json.dumps(result, indent=2))
            if pd.notna(huc["wkaoi_id"]) and result["id"] != huc["wkaoi_id"]:
                print()
                print("#########DISCREPANCY!!!#########")
                print(huc["HUC Name"])
                print(json.dumps(result, indent=2))
            else:
                wkaois.loc[idx, "wkaoi_id"] = str(result["id"])
                wkaois.loc[idx, "x"] = str(result["x"])
                wkaois.loc[idx, "y"] = str(result["y"])
    if not got_result:
        print()
        print(huc["HUC Name"])
        print("\t No matching result")
        print(json.dumps(search_results, indent=2))

#%%
wkaois["wkaoi_id"] = wkaois["wkaoi_id"].astype(int)
wkaois["x"] = wkaois["x"].astype(float)
wkaois["y"] = wkaois["y"].astype(float)
wkaois["wkaoi"] = wkaois["wkaoi_code"] + "__" + wkaois["wkaoi_id"].apply(str)


#%%
print("Saving the Area of Interest data...")
wkaois.sort_index().to_csv(feather_path + "wkaois.csv")
wkaois.sort_index().to_feather(feather_path + "wkaois" + feather_extension)

print("DONE!")

# %%
