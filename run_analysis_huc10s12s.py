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
try:
    completed_lu = pd.read_feather(feather_path + "land_use_hucs" + feather_extension)
except Exception as ex:
    print(ex)
    completed_lu = pd.DataFrame(
        columns=["wkaoi", "Land_Use_Source"],
        index=pd.RangeIndex(1),
    )

try:
    completed_soil = pd.read_feather(feather_path + "soil_hucs" + feather_extension)
except Exception as ex:
    print(ex)
    completed_soil = pd.DataFrame(
        columns=["wkaoi"],
        index=pd.RangeIndex(1),
    )

try:
    completed_srat = pd.read_feather(
        feather_path + "srat_precalclated_nutrients_hucs" + feather_extension
    )
except Exception as ex:
    print(ex)
    completed_srat = pd.DataFrame(
        columns=["wkaoi"],
        index=pd.RangeIndex(1),
    )

try:
    completed_point_source = pd.read_feather(
        feather_path + "point_sources_hucs" + feather_extension
    )
except Exception as ex:
    print(ex)
    completed_point_source = pd.DataFrame(
        columns=["wkaoi"],
        index=pd.RangeIndex(1),
    )


#%% get land use data
lu_frames = []
run_num = 0
for _, wkaoi in wkaois.iterrows():
    run_num += 1
    print(
        count_string.format(run_num, len(wkaois.index) * 4),
        wkaoi[huc_name_str],
        len(completed_lu.loc[completed_lu["wkaoi"] == wkaoi["wkaoi"]].index),
    )

    if len(completed_lu.loc[completed_lu["wkaoi"] == wkaoi["wkaoi"]].index) == 8 * 16:
        print("\tAlready got land use")
        lu_frames.append(
            completed_lu.loc[completed_lu["wkaoi"] == wkaoi["wkaoi"]]
            .drop(columns=keep_cols)
            .copy()
        )
        continue

    for land_use_source in [
        "2011_2011",
        "2019_2001",
        "2019_2006",
        "2019_2011",
        "2019_2016",
        "2019_2019",
        "centers",
        "corridors",
    ]:
        lu_job_label = wkaoi[huc_name_str]
        if land_use_source == "centers" or land_use_source == "corridors":
            lu_endpoint = mmw_run.lu_endpoint_2100.format(land_use_source)
        else:
            lu_endpoint = mmw_run.lu_endpoint_nlcd.format(land_use_source)

        lu_properties = None
        _, lu_properties = mmw_run.read_dumped_result(
            lu_endpoint,
            lu_job_label,
            mmw_run.json_dump_path
            + "{}_{}_landuse.json".format(lu_job_label, land_use_source),
            "survey",
        )
        if (
            lu_properties is not None
            and "survey" in lu_properties.keys()
            and "categories" in lu_properties["survey"].keys()
        ):
            lu_frame = pd.DataFrame(
                copy.deepcopy(lu_properties["survey"]["categories"])
            )

        else:
            try:
                req_dump = mmw_run.run_mmw_job(
                    request_endpoint=lu_endpoint,
                    job_label=lu_job_label,
                    params={"wkaoi": wkaoi["wkaoi"]},
                    payload=None,
                )
                lu_frame = pd.DataFrame(
                    copy.deepcopy(
                        req_dump["result_response"]["result"]["survey"]["categories"]
                    )
                )
            except Exception as ex:
                print("\tUnexpected exception:\n\t{}".format(ex))
                continue
        lu_frame["wkaoi"] = wkaoi["wkaoi"]
        lu_frame["Land_Use_Source"] = land_use_source
        lu_frames.append(lu_frame)

#%% join land use frames
lu_results = pd.concat(lu_frames, ignore_index=True)
merge_lu = pd.merge(
    wkaois[keep_cols + ["wkaoi"]],
    lu_results,
    on=["wkaoi"],
)
print("Saving the Land Use Summary...")
merge_lu.to_feather(feather_path + "land_use_hucs" + feather_extension)

#%% get soil data
soil_frames = []
for _, wkaoi in wkaois.iterrows():
    run_num += 1
    print(
        count_string.format(run_num, len(wkaois.index) * 4),
        wkaoi[huc_name_str],
        len(completed_soil.loc[completed_soil["wkaoi"] == wkaoi["wkaoi"]].index),
    )

    if len(completed_soil.loc[completed_soil["wkaoi"] == wkaoi["wkaoi"]].index) == 7:
        print("\tAlready got soil data")
        soil_frames.append(
            completed_soil.loc[completed_soil["wkaoi"] == wkaoi["wkaoi"]]
            .drop(columns=keep_cols)
            .copy()
        )
        continue

    soil_job_label = "{}".format(wkaoi[huc_name_str])

    soil_properties = None
    _, soil_properties = mmw_run.read_dumped_result(
        mmw_run.soil_endpoint,
        soil_job_label,
        mmw_run.json_dump_path + "{}_soil.json".format(soil_job_label),
        "survey",
    )
    if (
        soil_properties is not None
        and "survey" in soil_properties.keys()
        and "categories" in soil_properties["survey"].keys()
    ):
        soil_frame = pd.DataFrame(
            copy.deepcopy(soil_properties["survey"]["categories"])
        )

    else:
        try:
            req_dump = mmw_run.run_mmw_job(
                request_endpoint=mmw_run.soil_endpoint,
                job_label=soil_job_label,
                params={"wkaoi": wkaoi["wkaoi"]},
                payload=None,
            )
            print("\tGot soil data")
            soil_frame = pd.DataFrame(
                copy.deepcopy(
                    req_dump["result_response"]["result"]["survey"]["categories"]
                )
            )
        except Exception as ex:
            print("\tUnexpected exception:\n\t{}".format(ex))
            continue
    soil_frame["wkaoi"] = wkaoi["wkaoi"]
    soil_frames.append(soil_frame.copy())

# join soil frames
soil_results = pd.concat(soil_frames, ignore_index=True)
merge_soil = pd.merge(
    wkaois[keep_cols + ["wkaoi"]],
    soil_results,
    on=["wkaoi"],
)
print("Saving the soil data...")
merge_soil.to_feather(feather_path + "soil_hucs" + feather_extension)


#%% get SRAT data
srat_frames = []
srat_geoms_geojson = {"type": "FeatureCollection", "features": []}
srat_geom_dict = {}
for _, wkaoi in wkaois.iterrows():
    run_num += 1
    print(
        count_string.format(run_num, len(wkaois.index) * 4),
        wkaoi[huc_name_str],
        len(completed_srat.loc[completed_srat["wkaoi"] == wkaoi["wkaoi"]].index),
    )

    # if wkaoi["wkaoi_code"] != "huc12":
    #     print("\tOnly using SRAT chunks within HUC-12's")
    #     continue

    srat_job_label = "{}".format(wkaoi[huc_name_str])

    srat_properties = None
    _, srat_properties = mmw_run.read_dumped_result(
        mmw_run.srat_endpoint,
        srat_job_label,
        mmw_run.json_dump_path + "{}_srat.json".format(srat_job_label),
        "survey",
    )
    if (
        srat_properties is not None
        and "survey" in srat_properties.keys()
        and "categories" in srat_properties["survey"].keys()
    ):
        srat_frame = pd.DataFrame(
            copy.deepcopy(srat_properties["survey"]["categories"])
        )

    else:
        try:
            req_dump = mmw_run.run_mmw_job(
                request_endpoint=mmw_run.srat_endpoint,
                job_label=srat_job_label,
                params={"wkaoi": wkaoi["wkaoi"]},
                payload=None,
            )
            print("\tGot SRAT data")
            srat_frame = pd.DataFrame(
                copy.deepcopy(
                    req_dump["result_response"]["result"]["survey"]["categories"]
                )
            )
        except Exception as ex:
            print("\tUnexpected exception:\n\t{}".format(ex))
            continue

    srat_frame["wkaoi"] = wkaoi["wkaoi"]
    # split out the geometry data
    for idx, srat_ws in srat_frame.iterrows():
        geo_feat = {
            "type": "Feature",
            "geometry": srat_ws["geom"],
            "properties": {
                "nord": srat_ws["nord"],
                "parent_wkaoi": wkaoi["wkaoi"],
                "parent_huc": wkaoi["HUC Code"],
            },
        }
        srat_geoms_geojson["features"].append(copy.deepcopy(geo_feat))
        srat_geom_dict[srat_ws["nord"]] = copy.deepcopy(srat_ws["geom"])
    srat_frames.append(srat_frame.copy().drop(columns=["geom"]))

# join SRAT frames
srat_results = pd.concat(srat_frames, ignore_index=True).drop_duplicates(
    subset=["nord"]
)
merge_srat = pd.merge(
    wkaois[keep_cols + ["wkaoi"]],
    srat_results,
    on=["wkaoi"],
)
print("Saving the SRAT data...")
merge_srat.to_feather(
    feather_path + "srat_precalclated_nutrients_hucs" + feather_extension
)
with open(
    feather_path + "All_SRAT_Geometries.geojson",
    "w",
) as fp:
    json.dump(srat_geoms_geojson, fp)
with open(
    feather_path + "SRAT_Geometry_Dict.json",
    "w",
) as fp:
    json.dump(srat_geom_dict, fp)

print("DONE!")

#%% get point source data
point_source_frames = []
for _, wkaoi in wkaois.iterrows():
    run_num += 1
    print(
        count_string.format(run_num, len(wkaois.index) * 4),
        wkaoi[huc_name_str],
        len(
            completed_point_source.loc[
                completed_point_source["wkaoi"] == wkaoi["wkaoi"]
            ].index
        ),
    )

    point_source_job_label = "{}".format(wkaoi[huc_name_str])

    point_source_properties = None
    _, point_source_properties = mmw_run.read_dumped_result(
        mmw_run.point_source_endpoint,
        point_source_job_label,
        mmw_run.json_dump_path + "{}_point_source.json".format(point_source_job_label),
        "survey",
    )
    if (
        point_source_properties is not None
        and "survey" in point_source_properties.keys()
        and "categories" in point_source_properties["survey"].keys()
    ):
        point_source_frame = pd.DataFrame(
            copy.deepcopy(point_source_properties["survey"]["categories"])
        )

    else:
        try:
            req_dump = mmw_run.run_mmw_job(
                request_endpoint=mmw_run.point_source_endpoint,
                job_label=point_source_job_label,
                params={"wkaoi": wkaoi["wkaoi"]},
                payload=None,
            )
            print("\tGot point source data")
            point_source_frame = pd.DataFrame(
                copy.deepcopy(
                    req_dump["result_response"]["result"]["survey"]["categories"]
                )
            )
        except Exception as ex:
            print("\tUnexpected exception:\n\t{}".format(ex))
            continue
    point_source_frame["wkaoi"] = wkaoi["wkaoi"]
    point_source_frames.append(point_source_frame.copy())

# join point_source frames
point_source_results = pd.concat(point_source_frames, ignore_index=True)
merge_point_source = pd.merge(
    wkaois[keep_cols + ["wkaoi"]],
    point_source_results,
    on=["wkaoi"],
)
print("Saving the point source data...")
merge_point_source.to_feather(feather_path + "point_sources_hucs" + feather_extension)


# %%
print("DONE!")
# %%
