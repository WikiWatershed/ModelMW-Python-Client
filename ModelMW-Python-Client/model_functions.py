"""
Created by Sara Geleskie Damiano
"""
#%%
import sys
import time
import copy
import re
from pathlib import Path
from collections import OrderedDict

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd

import json

from typing import Dict, TypedDict, Union, Any

#%%
# import logging
# # These two lines enable debugging at httplib level (requests->urllib3->http.client)
# # You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# # The only thing missing will be the response.body which is not logged.
# try:
#     import http.client as http_client
# http_client.HTTPConnection.debuglevel = 1

# # You must initialize logging, otherwise you'll not see debug output.
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True


#%%
class ModelMyWatershedJob(TypedDict):
    job_label: str
    request_endpoint: str
    payload: Union[str, Dict]
    start_job_response: Any
    result_response: Any


class ModelMyWatershedAPI:
    # the ModelMyWatershed page
    staging_mmw_host = "https://staging.modelmywatershed.org"
    production_mmw_host = "https://modelmywatershed.org"

    api_endpoint = "api/"
    analyze_endpoint = api_endpoint + "analyze/"
    modeling_endpoint = "mmw/modeling/"

    streams_endpoint = analyze_endpoint + "streams/"
    protected_lands_endpoint = analyze_endpoint + "protected-lands/"
    soil_endpoint = analyze_endpoint + "soil/"
    terrain_endpoint = analyze_endpoint + "terrain/"
    climate_endpoint = analyze_endpoint + "climate/"
    point_source_endpoint = analyze_endpoint + "pointsource/"
    animal_endpoint = analyze_endpoint + "animals/"
    srat_endpoint = analyze_endpoint + "catchment-water-quality/"
    catchment_water_quality_endpoint = srat_endpoint

    lu_endpoint_nlcd = analyze_endpoint + "land/{}/"
    lu_endpoint_2100 = analyze_endpoint + "drb-2100-land/{}/"

    mapshed_endpoint = modeling_endpoint + "mapshed/"
    gwlfe_endpoint = modeling_endpoint + "gwlfe/"
    tr55_endpoint = modeling_endpoint + "tr55/"

    # NOTE:  These are NLCD layers ONLY!  The Shippensburg 2100 predictions are called
    # from the Drexel-provided API, and are not available as a geoprocessing layer
    # from https://github.com/WikiWatershed/model-my-watershed/blob/develop/src/mmw/js/src/modeling/utils.js
    land_use_layers = {
        "nlcd-2019-30m-epsg5070-512-byte": "nlcd_2019_2019",
        "nlcd-2016-30m-epsg5070-512-byte": "nlcd_2019_2016",
        "nlcd-2011-30m-epsg5070-512-byte": "nlcd_2019_2011",
        "nlcd-2006-30m-epsg5070-512-byte": "nlcd_2019_2006",
        "nlcd-2001-30m-epsg5070-512-byte": "nlcd_2019_2001",
        "nlcd-2011-30m-epsg5070-512-int8": "nlcd_2011_2011",
    }

    # conversion dictionaries
    # dictionary for converting NLCD types to those used by MapShed
    # we need the land use modifications for the Shippensburg 2100 predictions
    nlcd_to_mapshed = {
        "Pasture/Hay": "01-Hay/Past",
        "Cultivated Crops": "02-Cropland",
        "Deciduous Forest": "03-Forest",
        "Evergreen Forest": "03-Forest",
        "Mixed Forest": "03-Forest",
        "Shrub/Scrub": "03-Forest",
        "Woody Wetlands": "04-Wetland",
        "Emergent Herbaceous Wetlands": "04-Wetland",
        "Grassland/Herbaceous": "05-Open_Land",
        "Perennial Ice/Snow": "06-Bare_Rock",
        "Barren Land (Rock/Sand/Clay)": "06-Bare_Rock",
        "Developed, Low Intensity": "11-Ld_Mixed",
        "Developed, Medium Intensity": "12-Md_Mixed",
        "Developed, High Intensity": "13-Hd_Mixed",
        "Developed, Open Space": "14-Ld_Open_Space",
        "Open Water": None,
    }
    # the actual modification area value is a little different
    mapshed_to_area_id = {
        "01-Hay/Past": "Area__0",
        "02-Cropland": "Area__1",
        "03-Forest": "Area__2",
        "04-Wetland": "Area__3",
        "05-Open_Land": "Area__6",
        "06-Bare_Rock": "Area__7",
        "11-Ld_Mixed": "Area__10",
        "12-Md_Mixed": "Area__11",
        "13-Hd_Mixed": "Area__12",
        "14-Ld_Open_Space": "Area__13",
    }

    # The hash for no-modifications
    inputmod_hash = "d751713988987e9331980363e24189ced751713988987e9331980363e24189ce"

    def __init__(
        self,
        save_path: str,
        api_key: str,
        use_staging: bool = False,
    ):
        """Create a new class for accessing ModelMyWatershed's API's

        Args:
            save_path (str): The path you want any feathers and json objects to be saved to
            api_key (str): Your API key (needed for analysis requests)
            use_staging (bool, optional): Use the staging version of ModelMyWatershed rather than the
                production website. Defaults to False.
        """
        # set up instance variables
        self.mmw_host = (
            self.staging_mmw_host if use_staging else self.production_mmw_host
        )

        self.api_key = api_key
        self.save_path = save_path

        self.json_dump_path = self.save_path + "mmw_results\\json_results\\"

        # create a request session

        retry_strategy = Retry(
            total=50,
            backoff_factor=1,
            status_forcelist=[413, 429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.mmw_session = Session()
        self.mmw_session.verify = True
        self.mmw_session.mount("https://", adapter)
        self.mmw_session.mount("http://", adapter)

        self.mmw_session.headers.update(
            {
                "Host": "staging.modelmywatershed.org",
                "Authorization": "Token " + self.api_key,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0",
                "Accept": "*/*",
                "Connection": "keep-alive",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.5",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
                "Origin": "https://staging.modelmywatershed.org",
                "Referer": "{}/".format(self.mmw_host),
            }
        )

    def print_headers(self, headers: Dict) -> str:
        """Helper function for tracing errors in requests - prints out the header dictionary

        Args:
            headers (Dict): The headers to print

        Returns:
            str: A string version of the header dictionary
        """
        out_str = ""
        for header in headers:
            out_str += "\t{}: {}\n".format(header, headers[header])
        return out_str

    def print_req(self, the_request: requests.Response) -> None:
        """Helper function for tracing errors in requests - Prints out the request input

        Args:
            the_request (requests.Response): The response object from the request
        """
        print_format = "\nRequest:\nmethod: {}\nurl: {}\nheaders:\n{}\nbody: {}\n\nResponse:\nstatus code: {}\nurl: {}\nheaders: {}\ncookies: {}"
        print(
            print_format.format(
                the_request.request.method,
                the_request.request.url,
                self.print_headers(the_request.request.headers),
                the_request.request.body,
                the_request.status_code,
                the_request.url,
                self.print_headers(the_request.headers),
                the_request.cookies,
            )
        )

    def print_req_trace(self, the_request: requests.Response) -> None:
        """Helper function for tracing errors in requests - Prints out the request input and response

        Args:
            the_request (requests.Response): The response object from the request
        """
        if the_request.history:
            print("\nRequest was redirected")
            for resp in the_request.history:
                self.print_req(resp)
            print("\n\nFinal destination:")
            self.print_req(the_request)
        else:
            self.print_req(the_request)

    def login(self, mmw_user: str, mmw_pass: str) -> bool:
        """Log in to the ModelMyWatershed API

        Args:
            mmw_user (str): Your username
            mmw_pass (str): Your password

        Returns:
            bool: True if the login is successful
        """
        # construct the auth payload
        auth_payload = {
            "username": mmw_user,
            "password": mmw_pass,
        }
        # The log-in page
        login_page = "{}/user/login".format(self.mmw_host)

        try:
            # log in
            self.mmw_session.post(
                login_page,
                data=auth_payload,
                headers={
                    "Referer": self.mmw_host,
                    "Pragma": "no-cache",
                    "Cache-Control": "no-cache",
                },
            )
            self.mmw_session.headers.update(
                {"X-CSRFToken": self.mmw_session.cookies["csrftoken"]}
            )
        except Exception as ex:
            print("Failed to log in: {}".format(ex))
            return False

        print("\nSession cookies: {}".format(self.mmw_session.cookies))
        return True

    def set_request_headers(self, request_endpoint: str) -> None:
        """Adds the right referer and datatype headers to a request

        Args:
            request_endpoint (str): The endpoint for the request
        """

        if self.api_endpoint in request_endpoint:
            headers = {
                "Content-Type": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://staging.modelmywatershed.org/analyze",
            }
        elif self.modeling_endpoint in request_endpoint:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": "https://staging.modelmywatershed.org/project/",
                "X-Requested-With": "XMLHttpRequest",
            }

        self.mmw_session.headers.update(headers)

    def pprint_endpoint(self, request_endpoint):
        return (
            request_endpoint.replace(self.analyze_endpoint, "")
            .replace(self.modeling_endpoint, "")
            .replace("/", "_")
            .strip(" _")
        )

    # Analysis helper functions
    def start_job(
        self,
        request_endpoint: str,
        job_label: str,
        params: Dict = None,
        payload: Dict = None,
    ) -> ModelMyWatershedJob:
        """Starts an analysis or modeling job

        Args:
            request_endpoint (str): The endpoint for the request
            params (Dict): The parameters of the request (the part of the url after the ?)
            payload (Dict): The payload going to the request.
                Either a JSON serializable dictionary or pre-formatted form data.
            job_label (str): A label to use to save the output files

        Returns:
            ModelMyWatershedJob: A typed dictionary with the job inputs and output
        """
        job_dict: ModelMyWatershedJob = {
            "job_label": job_label,
            "request_host": self.mmw_host,
            "request_endpoint": request_endpoint,
            "payload": payload,
            "params": params,
        }

        self.set_request_headers(request_endpoint)
        if self.api_endpoint in request_endpoint:
            # the api endpoint expects json, expected to be dumped from a dictionary
            payload_compressed = json.dumps(payload, separators=(",", ":"))
        elif self.modeling_endpoint in request_endpoint:
            # the modeling endpoint expects form data, that should be pre-prepared by the user
            payload_compressed = payload

        attempts = 0
        while attempts < 5:
            start_job = self.mmw_session.post(
                "{}/{}".format(self.mmw_host, request_endpoint),
                data=payload_compressed,
                params=params,
            )
            # self.print_req_trace(start_job)

            throttle_time = 30.0
            if start_job.status_code != 200:
                print("\t***ERROR STARTING JOB***\n\t{}".format(start_job.content))
                self.print_req_trace(start_job)
                try:
                    detail = json.loads(start_job.content)["detail"]
                    if "throttled" in detail:
                        search_pat = (
                            "Expected available in (?P<throttle_time>[\d\.]+) seconds."
                        )
                        throttle_match = re.search(search_pat, detail)
                        if throttle_match is not None:
                            throttle_time = float(throttle_match.group("throttle_time"))
                        else:
                            return job_dict
                    else:
                        return job_dict
                except Exception as ex:
                    print("\tUnexpected exception:", ex)
                    return job_dict

            resp_json = None
            try:
                resp_json = start_job.json(object_pairs_hook=OrderedDict)
            except Exception as ex:
                print("\t***Proper JSON not returned for start job request!***")
                print("\t***Got {} with text {}!***".format(start_job, start_job.text))

            if (
                resp_json is not None
                and "job" in resp_json.keys()
                and resp_json["job"] is not None
            ):
                print(
                    "\t{} job started for {}".format(
                        self.pprint_endpoint(request_endpoint), job_label
                    )
                )
                attempts = 5

            # print("Start job result: {}".format(start_job.text))
            elif resp_json is not None and "job" not in start_job.json().keys():
                print("\t***ERROR STARTING JOB***\n\t{}".format(start_job.json()))
                return job_dict

            elif throttle_time > 60.0 * 30.0:
                print(
                    "\twait time of {}s is too long, will not retry".format(
                        throttle_time
                    )
                )
                attempts = 5

            elif throttle_time < 60.0 * 30.0 and attempts < 4:
                print("\tretrying in {}s...".format(throttle_time))
                time.sleep(throttle_time)
                attempts += 1

            elif attempts < 4:
                print("\tretrying in 30s...")
                time.sleep(30)
                attempts += 1

            else:
                print("\t{} job FAILED".format(request_endpoint))

        job_dict["start_job_response"] = resp_json
        return job_dict

    def get_job_result(
        self, start_job_dict: ModelMyWatershedJob
    ) -> ModelMyWatershedJob:
        """Given a job input, waits for and retrievs the job results

        Args:
            start_job_dict (ModelMyWatershedJob): The dictionary with the job input information

        Returns:
            ModelMyWatershedJob: A copy of the input dictionary with the job output appended.
        """
        if (
            "start_job_response" not in start_job_dict.keys()
            or "job" not in start_job_dict["start_job_response"].keys()
        ):
            return start_job_dict

        job_id = start_job_dict["start_job_response"]["job"]
        is_finished = False
        is_error = False

        self.set_request_headers(start_job_dict["request_endpoint"])
        if self.api_endpoint in start_job_dict["request_endpoint"]:
            job_endpoint = "{}/{}jobs/{}/".format(
                self.mmw_host, self.api_endpoint, job_id
            )
        elif self.modeling_endpoint in start_job_dict["request_endpoint"]:
            job_endpoint = "{}/{}jobs/{}/".format(
                self.mmw_host, self.modeling_endpoint, job_id
            )

        finished_job_dict = copy.deepcopy(start_job_dict)
        while is_finished == False and is_error == False:
            job_results_req = self.mmw_session.get(job_endpoint)
            if job_results_req.status_code != 200:
                print(
                    "\t***ERROR GETTING JOB RESULT***\n\t{}".format(
                        job_results_req.content
                    )
                )
                self.print_req_trace(job_results_req)
                return finished_job_dict
            try:
                job_results_req.json()
            except Exception as ex:
                is_error = True
                print("\t***Proper JSON not returned to job result request!***")
                print(
                    "\t***Got {} with text {}!***".format(
                        job_results_req,
                        job_results_req.text,
                    )
                )
                continue
            if (
                "error" in job_results_req.json().keys()
                and job_results_req.json()["error"] != ""
            ):
                print(
                    "\t***ERROR GETTING JOB RESULTS***\n\t{}".format(
                        job_results_req.json()["error"]
                    )
                )
                finished_job_dict["error_response"] = job_results_req.json(
                    object_pairs_hook=OrderedDict
                )
                is_error = True
            is_finished = job_results_req.json()["status"] == "complete"
            time.sleep(0.5)
        if (
            "result" in job_results_req.json().keys()
            and job_results_req.json()["result"] != ""
        ):
            finished_job_dict["result_response"] = job_results_req.json(
                object_pairs_hook=OrderedDict
            )
            print(
                "\tGot {} results for {}".format(
                    self.pprint_endpoint(start_job_dict["request_endpoint"]),
                    start_job_dict["job_label"],
                )
            )
        # dump out the whole job for posterity
        with open(
            self.get_dump_filename(
                start_job_dict["request_endpoint"], start_job_dict["job_label"]
            ),
            "w",
        ) as fp:
            json.dump(finished_job_dict, fp, indent=2)

        return finished_job_dict

    def run_mmw_job(
        self,
        request_endpoint: str,
        job_label: str,
        params: Dict = None,
        payload: Dict = None,
    ) -> ModelMyWatershedJob:
        """Starts a ModelMyWatershed job and waits for and returns the results

        Args:
            request_endpoint (str): The endpoint for the request
            params (Dict): The parameters of the request (the part of the url after the ?)
            payload (Dict): The payload going to the request.
                Either a JSON serializable dictionary or pre-formatted form data.
            job_label (str): A label to use to save the output files

        Returns:
            ModelMyWatershedJob: The job request and result
        """
        start_job_dict = self.start_job(
            request_endpoint=request_endpoint,
            params=params,
            payload=payload,
            job_label=job_label,
        )

        if (
            "start_job_response" in start_job_dict.keys()
            and "job" in start_job_dict["start_job_response"].keys()
            and start_job_dict["start_job_response"]["job"] is not None
        ):
            time.sleep(3.5)  # max of 20 requests per minute!
        else:
            print(
                "\t{} job FAILED for {}".format(
                    self.pprint_endpoint(start_job_dict["request_endpoint"]),
                    job_label,
                )
            )

        finished_job_dict = copy.deepcopy(self.get_job_result(start_job_dict))

        return finished_job_dict

    def get_dump_filename(self, request_endpoint: str, job_label: str) -> str:
        return (
            self.json_dump_path
            + job_label.replace("/", "_").strip(" _")
            + "_"
            + self.pprint_endpoint(request_endpoint)
            + ".json"
        )

    def read_dumped_result(
        self,
        request_endpoint: str,
        job_label: str,
        alt_filename: str = "",
        needed_result_key: str = "",
    ) -> ModelMyWatershedJob:

        saved_result = None
        req_dump = None
        dump_filename = self.get_dump_filename(request_endpoint, job_label)
        if Path(dump_filename).is_file() or (
            alt_filename != "" and Path(alt_filename).is_file()
        ):
            f = (
                open(dump_filename)
                if Path(dump_filename).is_file()
                else open(alt_filename)
            )
            req_dump = json.load(f)
            f.close()

            if needed_result_key != "":
                if (
                    "result_response" in req_dump.keys()
                    and "result" in req_dump["result_response"].keys()
                    and needed_result_key
                    in req_dump["result_response"]["result"].keys()
                ):
                    result_raw = req_dump["result_response"]
                    saved_result = copy.deepcopy(result_raw)["result"]

                elif (
                    "result" in req_dump.keys()
                    and needed_result_key in req_dump["result"].keys()
                ):
                    result_raw = req_dump
                    saved_result = copy.deepcopy(result_raw)["result"]

                elif needed_result_key in req_dump.keys():
                    saved_result = copy.deepcopy(req_dump)

            else:
                if (
                    "result_response" in req_dump.keys()
                    and "result" in req_dump["result_response"].keys()
                ):
                    result_raw = req_dump["result_response"]
                    saved_result = copy.deepcopy(result_raw)["result"]

                elif "result" in req_dump.keys():
                    result_raw = req_dump
                    saved_result = copy.deepcopy(result_raw)["result"]

            print(
                "\tRead saved {} results for {} from JSON".format(
                    self.pprint_endpoint(request_endpoint),
                    job_label,
                )
            )
        return (req_dump, saved_result)

    # note:  In the ModelMyWatershed javascript, the mapshed total area value is called the "autoTotal" and the analysis-derived total area is called the "presetTotal".  I do not understand at all why they aren't the same.

    # var totalArea = _.sum(attrs.dataModel['Area']);
    # this.set({
    #     autoTotal: totalArea,
    #     userTotal: totalArea,
    # });
    # // ^^^ attrs.dataModel['Area'] = mapshed_result['Area']

    # var m2ToHa = function(m2) { return m2 / coreUnits.METRIC.AREA_L.factor; },
    # // ^^^ coreUnits.METRIC.AREA_L.factor = 0.001

    # // Convert list of NLCD results to dictionary mapping
    # // NLCD to Hectares
    # nlcd = categories.reduce(function(acc, category) {
    #         acc[category.nlcd] = category.area;
    #         return acc;
    #     }, {}),
    # // ^^ This jusr reduces the complex object of the categoies to a simple array of only the category numbers and values

    # landcoverRaw = modelingUtils.nlcdToMapshedLandCover(nlcd).map(m2ToHa),
    # // ^^^ convert m2 to ha, and re-assign to MapShed cover types

    # // Proportion land cover with base NLCD 2011 total
    # // so that the user doesn't have to manually update it
    # autoTotal = self.model.get('autoTotal'),
    # presetTotal = _.sum(landcoverRaw),
    # landcover = landcoverRaw.map(function(l) {
    #     return l * autoTotal / presetTotal;
    # });

    def dump_land_use_modifications(
        self,
        landuse_job_label: str,
        modified_land_use_source: str,
        base_land_use_source: str,
    ):
        baselayer_ms_job_label = "{}_nlcd_{}".format(
            landuse_job_label, base_land_use_source
        )

        if (
            modified_land_use_source == "centers"
            or modified_land_use_source == "corridors"
        ):
            lu_endpoint = self.lu_endpoint_2100.format(modified_land_use_source)
        else:
            lu_endpoint = self.lu_endpoint_nlcd.format(modified_land_use_source)

        _, lu_modifications = self.read_dumped_result(
            lu_endpoint,
            landuse_job_label,
            self.json_dump_path
            + "{}_{}_landuse.json".format(landuse_job_label, modified_land_use_source),
            "survey",
        )
        _, mapshed_base = self.read_dumped_result(
            self.mapshed_endpoint,
            baselayer_ms_job_label,
            self.json_dump_path + "{}_mapshed.json".format(baselayer_ms_job_label),
            "Area",
        )

        if lu_modifications is not None and mapshed_base is not None:
            lu_modified = pd.DataFrame(lu_modifications["survey"]["categories"]).rename(
                columns={"area": "area_m2"}
            )
            mapshed_auto_total = sum(mapshed_base["Area"])
        else:
            return None

        # use the NLCD to MapShed dictionary to convert land uses
        lu_modified["area_ha"] = lu_modified["area_m2"] / 10000
        lu_modified["mapshed_lu"] = lu_modified["type"].replace(self.nlcd_to_mapshed)
        lu_modified["area_id"] = lu_modified["mapshed_lu"].replace(
            self.mapshed_to_area_id
        )

        # sum up the area for each mapshed type
        lu_conv_modified = (
            lu_modified.dropna(subset=["mapshed_lu"])
            .groupby(["mapshed_lu", "area_id"])["area_ha"]
            .sum()
            .reset_index()
        )
        lu_modified_preset_total = lu_conv_modified["area_ha"].sum()

        lu_conv_modified["factored_area"] = (
            lu_conv_modified["area_ha"] * mapshed_auto_total / lu_modified_preset_total
        )

        mod_dict = (
            lu_conv_modified[["area_id", "factored_area"]]
            .set_index("area_id")
            .to_dict(orient="dict")["factored_area"]
        )
        mod_dict_preset = {
            "entry_landcover_preset": "drb_2100_land_{}".format(
                modified_land_use_source
            )
        }
        mod_dict_2 = dict(mod_dict_preset, **mod_dict)
        mod_dict_dump = "[{}]".format(json.dumps(mod_dict_2).replace(" ", ""))

        return mod_dict_dump
