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
tr55_rain_inches = 3.33

huc_name_str = "HUC Name"
huc_code_str = "HUC Code"
sort_cols = [huc_code_str, "Land_Use_Source"]
base_nlcd_for_modifications = "2019_2019"

#%%
print("Reading the areas of interest")
wkaois = pd.read_feather(feather_path + "wkaois" + feather_extension)

#%%
result_filenames = [
    "gwlfe_monthly_hucs",
    "gwlfe_load_summaries_hucs",
    "gwlfe_lu_loads_hucs",
    "gwlfe_metadata_hucs",
    "gwlfe_summaries_hucs",
    #
    "tr55_censuses_hucs",
    "tr55_runoff_distributions_hucs",
    "tr55_runoff_totals_hucs",
    "step_l_qualities_hucs",
]

#%%
completed_models = {}
for filename in result_filenames:
    try:
        completed_models[filename] = pd.read_feather(
            feather_path + filename + feather_extension
        )
    except Exception as ex:
        print(ex)
        completed_models[filename] = pd.DataFrame(
            columns=["wkaoi", "Land_Use_Source"],
            index=pd.RangeIndex(1),
        )


#%% empty lists to hold results
mapshed_z_files = []
finished_sites = []

gwlfe_monthlies = []
gwlfe_load_summaries = []
gwlfe_lu_loads = []
gwlfe_metas = []
gwlfe_summaries = []

tr55_censuses = []
tr55_runoff_distributions = []
tr55_runoff_totals = []
step_l_qualities = []

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
    if (
        len(
            completed_models["gwlfe_summaries_hucs"]
            .loc[completed_models["gwlfe_summaries_hucs"]["wkaoi"] == wkaoi["wkaoi"]]
            .index
        )
        == 8
        and len(
            completed_models["tr55_runoff_totals_hucs"]
            .loc[completed_models["tr55_runoff_totals_hucs"]["wkaoi"] == wkaoi["wkaoi"]]
            .index
        )
        == 6
    ) or (
        wkaoi["wkaoi_code"] == "huc8"
        and len(
            completed_models["tr55_runoff_totals_hucs"]
            .loc[completed_models["tr55_runoff_totals_hucs"]["wkaoi"] == wkaoi["wkaoi"]]
            .index
        )
        == 6
    ):
        # print("\tAlready got all model results")
        run_num += len(mmw_run.land_use_layers.keys())

        for filename, result_list in zip(
            result_filenames,
            [
                gwlfe_monthlies,
                gwlfe_load_summaries,
                gwlfe_lu_loads,
                gwlfe_metas,
                gwlfe_summaries,
                tr55_censuses,
                tr55_runoff_distributions,
                tr55_runoff_totals,
                step_l_qualities,
            ],
        ):
            try:
                result_list.append(
                    completed_models[filename]
                    .loc[completed_models[filename]["wkaoi"] == wkaoi["wkaoi"]]
                    .drop(columns=keep_cols)
                    .copy()
                )
            except Exception as ex:
                print(ex)
        continue

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

            if wkaoi["wkaoi_code"] == "huc8":
                print("Skipping MapShed and GWLF-E on HUC8")
                continue

            mapshed_job_label = "{}_{}".format(
                wkaoi[huc_name_str], mmw_run.land_use_layers[land_use_layer]
            )
            gwlfe_job_label = "{}_{}_{}".format(
                wkaoi[huc_name_str], mmw_run.land_use_layers[land_use_layer], lu_mod
            )

            gwlfe_result = None
            _, gwlfe_result = mmw_run.read_dumped_result(
                mmw_run.gwlfe_run_endpoint,
                gwlfe_job_label,
                json_dump_path + "{}_gwlfe.json".format(gwlfe_job_label),
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
                    mmw_run.gwlfe_prepare_endpoint,
                    mapshed_job_label,
                    json_dump_path + "{}_mapshed.json".format(mapshed_job_label),
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
                        params=None,
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
                        params=None,
                        payload=gwlfe_payload,
                    )
                    if "result_response" in gwlfe_job_dict.keys():
                        gwlfe_result_raw = gwlfe_job_dict["result_response"]
                        gwlfe_result = copy.deepcopy(gwlfe_result_raw)["result"]

            if gwlfe_result is not None:
                gwlfe_monthly = pd.DataFrame(gwlfe_result.pop("monthly"))
                gwlfe_monthly["month"] = gwlfe_monthly.index + 1
                gwlfe_load_summary = pd.DataFrame(gwlfe_result.pop("SummaryLoads"))
                gwlfe_lu_load = pd.DataFrame(gwlfe_result.pop("Loads"))
                gwlfe_meta = pd.DataFrame(gwlfe_result.pop("meta"), index=[1])
                gwlfe_summary = pd.DataFrame(gwlfe_result, index=[1])

                for frame in [
                    gwlfe_monthly,
                    gwlfe_load_summary,
                    gwlfe_lu_load,
                    gwlfe_meta,
                    gwlfe_summary,
                ]:
                    frame["wkaoi"] = wkaoi["wkaoi"]
                    if lu_mod == "unmodified":
                        frame["Land_Use_Source"] = mmw_run.land_use_layers[
                            land_use_layer
                        ]
                    else:
                        frame["Land_Use_Source"] = lu_mod
                gwlfe_monthlies.append(gwlfe_monthly)
                gwlfe_load_summaries.append(gwlfe_load_summary)
                gwlfe_lu_loads.append(gwlfe_lu_load)
                gwlfe_metas.append(gwlfe_meta)
                gwlfe_summaries.append(gwlfe_summary)

        ## Run TR-55 once for each land use layer

        ## NOTE:  TR-55 is independent of success/failure of MapShed and GWLF-E

        pre_tr55_input = {
            # NOTE:  The value of the inputmod_hash and modification_hash don't really matter here.
            # Internally, the ModelMW site uses the inputmod_hash in scenerios to
            # determine whether it can use cached results or if it needs to
            # re-run the job
            "inputs": [
                {
                    "name": "precipitation",
                    "value": tr55_rain_inches,
                    "type": "",
                    "effectiveArea": None,
                    "effectiveUnits": None,
                    "effectiveShape": None,
                    "shape": None,
                    "area": "0",
                    "units": "m²",
                    "isValidForAnalysis": False,
                }
            ],
            "modification_pieces": [],
            "aoi_census": None,
            "modification_censuses": None,
            "inputmod_hash": "c41c79294c722aac7febf21a5bfc95e7d751713988987e9331980363e24189ce",
            "modification_hash": "d751713988987e9331980363e24189ce",
            "layer_overrides": {"__LAND__": land_use_layer},
            "wkaoi": wkaoi["wkaoi"],
        }

        tr55_input = json.dumps(pre_tr55_input).replace("'", '"')
        tr55_payload = {"model_input": tr55_input}
        tr55_job_label = "{}_{}".format(
            wkaoi[huc_name_str], mmw_run.land_use_layers[land_use_layer]
        )

        _, tr55_result = mmw_run.read_dumped_result(
            mmw_run.tr55_endpoint,
            tr55_job_label,
            json_dump_path + "{}_tr55.json".format(tr55_job_label),
            "aoi_census",
        )

        if tr55_result is None:
            # continue
            tr55_job_dict = mmw_run.run_mmw_job(
                request_endpoint=mmw_run.tr55_endpoint,
                job_label=tr55_job_label,
                params=None,
                payload=tr55_payload,
            )
            if "result_response" in tr55_job_dict.keys():
                tr55_result_raw = tr55_job_dict["result_response"]
                tr55_result = copy.deepcopy(tr55_result_raw)["result"]

        if tr55_result is not None:
            tr55_census = pd.DataFrame(tr55_result["aoi_census"]["distribution"])
            tr55_runoff_distribution = pd.DataFrame(
                tr55_result["runoff"]["unmodified"]["distribution"]
            ).reset_index()
            tr55_runoff_total = pd.DataFrame(
                {
                    key: val
                    for key, val in tr55_result["runoff"]["unmodified"].items()
                    if key not in ["BMPs", "cell_count", "distribution"]
                },
                index=pd.RangeIndex(1),
            )
            step_l_quality = pd.DataFrame(tr55_result["quality"]["unmodified"])
            for frame in [
                tr55_census,
                tr55_runoff_distribution,
                tr55_runoff_total,
                step_l_quality,
            ]:
                frame["wkaoi"] = wkaoi["wkaoi"]
                frame["Land_Use_Source"] = mmw_run.land_use_layers[land_use_layer]
            tr55_censuses.append(tr55_census)
            tr55_runoff_distributions.append(tr55_runoff_distribution)
            tr55_runoff_totals.append(tr55_runoff_total)
            step_l_qualities.append(step_l_quality)

#%% join various result
gwlfe_monthly_results = pd.concat(gwlfe_monthlies, ignore_index=True)
gwlfe_load_sum_results = pd.concat(gwlfe_load_summaries, ignore_index=True)
gwlfe_lu_load_results = pd.concat(gwlfe_lu_loads, ignore_index=True)
gwlfe_metas_results = pd.concat(gwlfe_metas, ignore_index=True)
gwlfe_sum_results = pd.concat(gwlfe_summaries, ignore_index=True)

tr55_census_results = pd.concat(tr55_censuses, ignore_index=True)
tr55_runoff_distribution_results = pd.concat(
    tr55_runoff_distributions, ignore_index=True
)
tr55_runoff_total_results = pd.concat(tr55_runoff_totals, ignore_index=True)
step_l_quality_results = pd.concat(step_l_qualities, ignore_index=True)
merges = {}
for frame, filename in zip(
    [
        gwlfe_monthly_results,
        gwlfe_load_sum_results,
        gwlfe_lu_load_results,
        gwlfe_metas_results,
        gwlfe_sum_results,
        #
        tr55_census_results,
        tr55_runoff_distribution_results,
        tr55_runoff_total_results,
        step_l_quality_results,
    ],
    [
        "gwlfe_monthly",
        "gwlfe_load_summaries",
        "gwlfe_lu_loads",
        "gwlfe_metadata",
        "gwlfe_summaries",
        #
        "tr55_censuses",
        "tr55_runoff_distributions",
        "tr55_runoff_totals",
        "step_l_qualities",
    ],
):
    merges[filename] = (
        pd.merge(
            wkaois[
                [
                    huc_name_str,
                    huc_code_str,
                    "State",
                    "wkaoi_code",
                    "wkaoi_id",
                    "Other Names",
                    "wkaoi",
                    "_huc_code_",
                ]
            ],
            frame,
            on=["wkaoi"],
        )
        .sort_values(by=sort_cols)
        .reset_index(drop=True)
        .copy()
    )

#%% Merge with location metadata and save feathers
print("Saving the GWLF-E data ...")
merge_gwlfe_monthly = (
    merges["gwlfe_monthly"].sort_values(by=sort_cols + ["month"]).reset_index(drop=True)
)
merge_gwlfe_monthly.to_feather(feather_path + "gwlfe_monthly_hucs" + feather_extension)

gwlfe_load_sum = (
    merges["gwlfe_load_summaries"]
    .sort_values(by=sort_cols + ["Source"])
    .reset_index(drop=True)
)
gwlfe_load_sum.to_feather(
    feather_path + "gwlfe_load_summaries_hucs" + feather_extension
)

gwlfe_lu_load = (
    merges["gwlfe_lu_loads"]
    .sort_values(by=sort_cols + ["Source"])
    .reset_index(drop=True)
)
gwlfe_lu_load.to_feather(feather_path + "gwlfe_lu_loads_hucs" + feather_extension)

gwlfe_metadata = (
    merges["gwlfe_metadata"].sort_values(by=sort_cols).reset_index(drop=True)
)
gwlfe_metadata.to_feather(feather_path + "gwlfe_metadata_hucs" + feather_extension)

gwlfe_summaries = (
    merges["gwlfe_summaries"].sort_values(by=sort_cols).reset_index(drop=True)
)
gwlfe_summaries.to_feather(feather_path + "gwlfe_summaries_hucs" + feather_extension)


print("Saving the TR-55 data ...")
merges["tr55_censuses"].to_feather(
    feather_path + "tr55_censuses_hucs" + feather_extension
)
merges["tr55_runoff_distributions"].to_feather(
    feather_path + "tr55_runoff_distributions_hucs" + feather_extension
)
merges["tr55_runoff_totals"].to_feather(
    feather_path + "tr55_runoff_totals_hucs" + feather_extension
)
merges["step_l_qualities"].to_feather(
    feather_path + "step_l_qualities_hucs" + feather_extension
)

#%%
print("DONE!")

# %%
