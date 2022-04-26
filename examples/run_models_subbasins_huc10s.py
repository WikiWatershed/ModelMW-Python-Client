"""
Created by Sara Geleskie Damiano
"""
#%%
from os import times
import sys
import time
import copy
from pathlib import Path
from dateutil import parser
import pytz
from datetime import datetime, timedelta

import pandas as pd

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

# https://www.njstormwater.org/pdf/SMDR_Stormwater_Calculations_Slides.pdf
tr5_rain_inches = 3.33

huc_name_str = "HUC Name"
huc_code_str = "HUC Code"
sort_cols = [huc_code_str, "Land_Use_Source"]
base_nlcd_for_modifications = "2019_2019"

#%%
print("Reading the areas of interest")
wkaois = pd.read_feather(feather_path + "wkaois" + feather_extension)
wkaois = wkaois.loc[wkaois["wkaoi_code"] == "huc10"]

#%% empty lists to hold results
mapshed_z_files = []
finished_sites = []

catchment_nutrients = []

#%%
keep_cols = [
    huc_name_str,
    huc_code_str,
    "State",
    "wkaoi_code",
    "wkaoi_id",
    "Other Names",
    "_huc_code_",
]
run_num = 0
total_runs = len(wkaois.index) * len(mmw_run.land_use_layers.keys())
for idx, wkaoi in wkaois.iterrows():
    for land_use_layer in mmw_run.land_use_layers.keys():
        run_num += 1
        print(
            "{} of {} =>".format(run_num, total_runs),
            wkaoi["wkaoi_code"],
            wkaoi[huc_name_str],
            land_use_layer,
        )

        if mmw_run.land_use_layers[land_use_layer] == "nlcd_2019_2019":
            land_use_modifications = ["unmodified", "centers", "corridors"]
        else:
            land_use_modifications = ["unmodified"]  # {"unmodified": "[{}]"}

        for lu_mod in land_use_modifications:

            if wkaoi["wkaoi_code"] != "huc10":
                continue

            mapshed_job_label = "{}_{}_subbasin".format(
                wkaoi[huc_name_str], mmw_run.land_use_layers[land_use_layer]
            )
            gwlfe_job_label = "{}_{}_{}_subbasin".format(
                wkaoi[huc_name_str], mmw_run.land_use_layers[land_use_layer], lu_mod
            )

            _, gwlfe_result = mmw_run.read_dumped_result(
                mmw_run.gwlfe_run_endpoint,
                gwlfe_job_label,
                "",
                "SummaryLoads",
            )

            if gwlfe_result is None:
                # continue

                # if we couldn't find the GWLF-E file, we need to rerun both MapShed and
                # GWLF-E becaues the cache of the MapShed job will probably have expired

                mapshed_job_id = None
                mapshed_req_dump, mapshed_result = mmw_run.read_dumped_result(
                    mmw_run.gwlfe_prepare_endpoint,
                    mapshed_job_label,
                    "",
                    "Area",
                )
                mapshed_job_still_valid = False

                if (
                    mapshed_req_dump is not None
                    and "result_response" in mapshed_req_dump.keys()
                ):
                    mapshed_job_id = mapshed_req_dump["result_response"]["job_uuid"]
                    finished_time = parser.parse(
                        mapshed_req_dump["result_response"]["finished"]
                    )
                    time_since_finished = (
                        datetime.utcnow().replace(tzinfo=pytz.utc) - finished_time
                    )
                    mapshed_job_still_valid = (
                        time_since_finished.total_seconds() / 3600 < 1
                    )

                if mapshed_job_still_valid is True:
                    print(
                        "\tRead recent {} mapshed job from JSON".format(land_use_layer)
                    )

                if mapshed_job_still_valid is False:

                    ## run MapShed once for each land use layer
                    pre_mapshed_input = '{{"layer_overrides":{{"__LAND__": "{}"}},"wkaoi":"{}"}}'.format(
                        land_use_layer, wkaoi["wkaoi"]
                    ).replace(
                        " ", ""
                    )
                    mapshed_input = pre_mapshed_input.replace("'", '"')
                    mapshed_payload = {"mapshed_input": mapshed_input}

                    mapshed_job_dict = mmw_run.run_mmw_job(
                        request_endpoint=mmw_run.gwlfe_prepare_endpoint,
                        job_label=mapshed_job_label,
                        params={"subbasin": "true"},
                        payload=mapshed_payload,
                    )
                    if "result_response" in mapshed_job_dict.keys():
                        mapshed_job_id = mapshed_job_dict["start_job_response"]["job"]
                        mapshed_result = mapshed_job_dict["result_response"]["result"]

                        mapshed_result["wkaoi"] = wkaoi["wkaoi"]
                        mapshed_z_files.append(mapshed_result)

                ## Run GWLF-E once for each layer, and then two more times for the
                # centers and coridors modifications of the 2011 data

                ## NOTE:  Don't run GWLF-E if we don't get MapShed results
                if mapshed_job_id is not None and mapshed_result is not None:

                    if lu_mod == "unmodified":
                        land_use_modification_set = "[{}]"
                    else:
                        land_use_modification_set = mmw_run.dump_land_use_modifications(
                            wkaoi[huc_name_str], lu_mod, base_nlcd_for_modifications
                        )

                    gwlfe_payload = {
                        # NOTE:  The value of the inputmod_hash doesn't really matter here
                        # Internally, the ModelMW site uses the inputmod_hash in scenerios to
                        # determine whether it can use cached results or if it needs to
                        # re-run the job
                        "inputmod_hash": mmw_run.inputmod_hash,
                        "modifications": land_use_modification_set,
                        "mapshed_job_uuid": mapshed_job_id,
                    }
                    gwlfe_job_dict = mmw_run.run_mmw_job(
                        request_endpoint=mmw_run.gwlfe_run_endpoint,
                        job_label=gwlfe_job_label,
                        params={"subbasin": "true"},
                        payload=gwlfe_payload,
                    )
                    if "result_response" in gwlfe_job_dict.keys():
                        gwlfe_result_raw = gwlfe_job_dict["result_response"]
                        gwlfe_result = copy.deepcopy(gwlfe_result_raw)["result"]

            if gwlfe_result is not None:
                huc12s = gwlfe_result["HUC12s"]
                catchment_frame = pd.DataFrame.from_dict(
                    {
                        (i, j, k): huc12s[i]["Catchments"][j][k]
                        for i in huc12s.keys()
                        for j in huc12s[i]["Catchments"].keys()
                        for k in huc12s[i]["Catchments"][j].keys()
                    },
                    orient="index",
                )
                catchment_frame.index.rename(
                    ["parent_huc", "ComID", "SRAT_Source"], inplace=True
                )
                catchment_frame.reset_index(inplace=True)
                catchment_frame["Land_Use_Source"] = lu_mod
                catchment_nutrients.append(catchment_frame.copy())

#%% join various result
catchment_nutrient_results = (
    pd.concat(catchment_nutrients, ignore_index=True)
    .drop_duplicates()
    .sort_values(by=["parent_huc", "ComID", "Land_Use_Source", "SRAT_Source"])
    .reset_index(drop=True)
)
catchment_nutrient_results.to_feather(
    feather_path + "catchment_nutrient_results" + feather_extension
)


#%%
print("DONE!")

# %%
