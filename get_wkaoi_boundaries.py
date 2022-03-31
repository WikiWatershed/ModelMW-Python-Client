"""
Created by Sara Geleskie Damiano
"""
#%%
from os import times
import sys
import copy
import time
from pathlib import Path

import pandas as pd

import json

import requests


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
mmw_run = ModelMyWatershedAPI(save_path, srgd_mmw_api_key, False)
# Authenticate with MMW
mmw_run.login(mmw_user=srgd_mmw_user, mmw_pass=srgd_mmw_pass)

count_string = "{} of {} =>"

#%%
print("Reading the areas of interest")
wkaois = pd.read_feather(feather_path + "wkaois" + feather_extension)
huc_name_str = "HUC Name"
huc_code_str = "HUC Code"
keep_cols = [
    huc_name_str,
    huc_code_str,
    "State",
    "wkaoi_code",
    "wkaoi_id",
    "Other Names",
    "_huc_code_",
]

#%%
wkaoi_geoms = {"type": "FeatureCollection", "features": []}
for _, wkaoi in wkaois.iterrows():
    job_label = "{}".format(wkaoi[huc_name_str])
    resp = requests.get(
        "https://modelmywatershed.org/mmw/modeling/boundary-layers/{}/{}/".format(
            wkaoi["wkaoi_code"], wkaoi["wkaoi_id"]
        )
    )
    feature = resp.json()
    wkaoi_geoms["features"].append(copy.deepcopy(feature))
print("Saving the boundary data...")
with open(
    feather_path + "All_WKAoI_Geometries.geojson",
    "w",
) as fp:
    json.dump(wkaoi_geoms, fp)

# %%
