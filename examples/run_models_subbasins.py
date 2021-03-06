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

from modelmw_client import *

import logging

logging.basicConfig()
logging.getLogger("modelmw_client").setLevel(logging.INFO)


#%%  Read location data
from mmw_secrets import (
    srgd_staging_api_key,
    srgd_mmw_user,
    srgd_mmw_pass,
    save_path,
    csv_path,
    json_dump_path,
    csv_extension,
)

# Create an API user
mmw_run = ModelMyWatershedAPI(srgd_staging_api_key, save_path, True)
# Authenticate with MMW
mmw_run.login(mmw_user=srgd_mmw_user, mmw_pass=srgd_mmw_pass)

base_nlcd_for_modifications = "2019_2019"

# This is the HUC-10 for White Clay Creek
huc_aois = [
    "0204020503",
]

#%% empty lists to hold results
mapshed_z_files = []
finished_sites = []

catchment_nutrients = []

#%%
run_num = 0
total_runs = len(huc_aois) * len(mmw_run.land_use_layers.keys())
for idx, huc_aoi in enumerate(huc_aois):
    for land_use_layer in mmw_run.land_use_layers.keys():
        run_num += 1
        print(
            "{} of {} =>".format(run_num, total_runs),
            huc_aoi,
            land_use_layer,
        )

        if land_use_layer == "2019_2019":
            land_use_modifications = ["unmodified", "centers", "corridors"]
        else:
            land_use_modifications = ["unmodified"]  # {"unmodified": "[{}]"}

        for lu_mod in land_use_modifications:

            mapshed_job_label = "{}_{}_subbasin".format(huc_aoi, land_use_layer)
            gwlfe_job_label = "{}_{}_{}_subbasin".format(
                huc_aoi, land_use_layer, lu_mod
            )

            gwlfe_result = None
            _, gwlfe_result = mmw_run.read_dumped_result(
                mmw_run.subbasin_run_endpoint,
                gwlfe_job_label,
                "",
                "SummaryLoads",
            )

            if gwlfe_result is None:
                # continue

                # if we couldn't find the GWLF-E file, we need to rerun both MapShed and
                # GWLF-E becaues the cache of the MapShed job will probably have expired

                mapshed_job_id = None
                mapshed_req_dump = None
                mapshed_result = None
                mapshed_req_dump, mapshed_result = mmw_run.read_dumped_result(
                    mmw_run.subbasin_prepare_endpoint,
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

                    # run MapShed once for each land use layer
                    # NOTE:  when using the layer overrides, we need the full layer
                    # title, ie, "nlcd-2019-30m-epsg5070-512-byte".  We can get this
                    # from the land use dictionary in the modelmw_client.
                    mapshed_payload = {
                        "huc": huc_aoi,
                        "layer_overrides": {
                            "__LAND__": mmw_run.land_use_layers[land_use_layer]
                        },
                    }

                    mapshed_job_dict = mmw_run.run_mmw_job(
                        request_endpoint=mmw_run.subbasin_prepare_endpoint,
                        job_label=mapshed_job_label,
                        payload=mapshed_payload,
                    )
                    if "result_response" in mapshed_job_dict.keys():
                        mapshed_job_id = mapshed_job_dict["start_job_response"]["job"]
                        mapshed_result = mapshed_job_dict["result_response"]["result"]

                        mapshed_result["huc_aoi"] = huc_aoi
                        mapshed_z_files.append(mapshed_result)

                ## Run GWLF-E once for each layer, and then two more times for the
                # centers and coridors modifications of the 2011 data

                ## NOTE:  Don't run GWLF-E if we don't get MapShed results
                if mapshed_job_id is not None and mapshed_result is not None:

                    if lu_mod == "unmodified":
                        land_use_modification_set = "[{}]"
                    else:
                        land_use_modification_set = mmw_run.convert_predictions_to_modifications(
                            huc_aoi, lu_mod, base_nlcd_for_modifications
                        )

                    gwlfe_payload = {
                        # NOTE:  The value of the inputmod_hash doesn't really matter here
                        # Internally, the ModelMW site uses the inputmod_hash in scenerios to
                        # determine whether it can use cached results or if it needs to
                        # re-run the job
                        "inputmod_hash": mmw_run.inputmod_hash,
                        "modifications": land_use_modification_set,
                        "job_uuid": mapshed_job_id,
                    }
                    gwlfe_job_dict = mmw_run.run_mmw_job(
                        request_endpoint=mmw_run.subbasin_run_endpoint,
                        job_label=gwlfe_job_label,
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
catchment_nutrient_results.to_csv(
    csv_path + "catchment_nutrient_results" + csv_extension
)


#%%
print("DONE!")

# %%
