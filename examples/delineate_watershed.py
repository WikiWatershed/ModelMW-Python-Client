
from modelmw_client import * 
from mmw_secrets import * 
import json
import copy


# initialize run 
mmw_run = ModelMyWatershedAPI(api_key=staging_api_key, use_staging=True, save_path = json_dump_path)

# define lat and long to delineate (example = Raccoon Creek)
lat = 40.052716203284696
long =  -82.41179466247559

# define input 
DATA = {"location": [lat, long],
        "snappingOn": True,
        "simplify": 0,
        "dataSource":"nhd"}

# initialize info for model run 
input = json.dumps(DATA).replace("'", '"')
payload = DATA
job_label = "TEST"

_, result = mmw_run.read_dumped_result(
    mmw_run.watershed_endpoint,
    job_label,
    json_dump_path + "{}_delineation.json".format(job_label),
    "aoi_census",
)

if result is None:
    # continue
    job_dict = mmw_run.run_mmw_job(
        request_endpoint=mmw_run.watershed_endpoint,
        job_label=job_label,
        payload=payload,
    )

    if "result_response" in job_dict.keys():
        result_raw = job_dict["result_response"]
        final_result = copy.deepcopy(result_raw)["result"]
