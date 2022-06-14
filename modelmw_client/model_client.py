"""
Created by Sara Geleskie Damiano
"""
#%%
import time
import copy
import re
from pathlib import Path

from typing import Dict, List, TypedDict, Union, Any
from typing_extensions import NotRequired
import collections
from collections import OrderedDict

import requests
from requests import Request, Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd

import json
import logging

module_logger = logging.getLogger(__name__)


#%%
class ModelMyWatershedJob(TypedDict):
    job_label: str
    request_host: str
    request_endpoint: str
    payload: NotRequired[Union[str, Dict]]
    start_job_status: str
    start_job_response: NotRequired[Any]
    job_result_status: str
    result_response: NotRequired[Any]
    error_response: NotRequired[Dict]


class ModemMyWatershedLayerOverride(TypedDict):
    __LAND__: NotRequired[str]
    __STREAMS__: NotRequired[str]


class ModelMyWatershedAPI:
    api_logger = module_logger.getChild(__qualname__)

    # the ModelMyWatershed page
    staging_mmw_host: str = "https://staging.modelmywatershed.org"
    production_mmw_host: str = "https://modelmywatershed.org"

    # low level endpoints
    api_endpoint: str = "api/"
    analyze_endpoint: str = api_endpoint + "analyze/"
    modeling_endpoint: str = api_endpoint + "modeling/"
    old_modeling_endpoint: str = "mmw/modeling/"

    # simple analysis endpoints
    protected_lands_endpoint: str = analyze_endpoint + "protected-lands/"
    soil_endpoint: str = analyze_endpoint + "soil/"
    terrain_endpoint: str = analyze_endpoint + "terrain/"
    climate_endpoint: str = analyze_endpoint + "climate/"
    point_source_endpoint: str = analyze_endpoint + "pointsource/"
    animal_endpoint: str = analyze_endpoint + "animals/"
    srat_endpoint: str = analyze_endpoint + "catchment-water-quality/"
    catchment_water_quality_endpoint: str = srat_endpoint

    # more detailed analysis endpoints
    land_endpoint: str = analyze_endpoint + "land/{}/"
    forcast_endpoint: str = analyze_endpoint + "drb-2100-land/{}/"
    streams_endpoint: str = analyze_endpoint + "streams/{}/"

    # GWLF-E endpoints
    gwlfe_prepare_endpoint: str = modeling_endpoint + "gwlf-e/prepare/"
    mapshed_endpoint: str = gwlfe_prepare_endpoint
    gwlfe_run_endpoint: str = modeling_endpoint + "gwlf-e/run/"
    gwlfe_endpoint: str = gwlfe_run_endpoint

    # Subbasin (GWLF-E) endpoints
    subbasin_prepare_endpoint: str = modeling_endpoint + "subbasin/prepare/"
    subbasin_run_endpoint: str = modeling_endpoint + "subbasin/run/"

    # TR-55 (Site Storm Model) endpoint
    tr55_endpoint: str = old_modeling_endpoint + "tr55/"

    # project endpoint
    project_endpoint: str = old_modeling_endpoint + "projects/"

    # NOTE:  These are NLCD layers ONLY!  The Shippensburg 2100 predictions are called
    # from the Drexel-provided API, and are not available as a geoprocessing layer
    # from https://github.com/WikiWatershed/model-my-watershed/blob/develop/src/mmw/js/src/modeling/utils.js
    land_use_layers: Dict[str, str] = {
        "2019_2019": "nlcd-2019-30m-epsg5070-512-byte",
        "2019_2016": "nlcd-2016-30m-epsg5070-512-byte",
        "2019_2011": "nlcd-2011-30m-epsg5070-512-byte",
        "2019_2006": "nlcd-2006-30m-epsg5070-512-byte",
        "2019_2001": "nlcd-2001-30m-epsg5070-512-byte",
        "2011_2011": "nlcd-2011-30m-epsg5070-512-int8",
    }

    drb_2011_keys = [
        "centers",
        "centers_np",
        "centers_osi",
        "corridors",
        "corridors_np",
        "corridors_osi",
    ]
    streams_datasources = ["nhd", "nhdhr", "drb"]
    weather_layers = ["NASA_NLDAS_2000_2019", "RCP45_2080_2099", "RCP85_2080_2099"]

    # conversion dictionaries
    # dictionary for converting NLCD types to those used by MapShed
    # we need the land use modifications for the Shippensburg 2100 predictions
    nlcd_to_mapshed: Dict[str, str] = {
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
    mapshed_to_area_id: Dict[str, str] = {
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
    inputmod_hash: str = (
        "d751713988987e9331980363e24189ced751713988987e9331980363e24189ce"
    )

    def __init__(
        self,
        api_key: str,
        save_path: str = None,
        use_staging: bool = False,
    ):
        """Create a new class for accessing ModelMyWatershed's API's

        Args:
            save_path (str): The path you want any json objects to be saved to
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

        if self.save_path is not None:
            self.json_dump_path = self.save_path + "mmw_results\\json_results\\"

        # TODO(SRGDamia1): Find out the max response time from Terence
        DEFAULT_TIMEOUT = 30  # seconds

        # create a TimeoutHTTPAdapter to enforce a default timeout on the session
        # from https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
        class TimeoutHTTPAdapter(HTTPAdapter):
            def __init__(self, *args, **kwargs):
                self.timeout = DEFAULT_TIMEOUT
                if "timeout" in kwargs:
                    self.timeout = kwargs["timeout"]
                    del kwargs["timeout"]
                super().__init__(*args, **kwargs)

            def send(self, request, **kwargs):
                timeout = kwargs.get("timeout")
                if timeout is None:
                    kwargs["timeout"] = self.timeout
                return super().send(request, **kwargs)

        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[413, 429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"],
        )
        adapter = TimeoutHTTPAdapter(max_retries=retry_strategy)
        # create a request session
        self.mmw_session = Session()
        self.mmw_session.verify = True
        # mount the session for all requests, attaching the timeout/retry adapter
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

    def _print_headers(self, headers: Dict) -> str:
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

    def _print_req(
        self,
        the_request: requests.Response,
        logging_level: int = logging.DEBUG,
    ) -> None:
        """Helper function for tracing errors in requests - Prints out the request input

        Args:
            the_request (requests.Response): The response object from the request
            logging_level (logging._Level): The logging level to use for the request
        """
        print_format = "\nRequest:\nmethod: {}\nurl: {}\nheaders:\n{}\nbody: {}\n\nResponse:\nstatus code: {}\nurl: {}\nheaders: {}\ncookies: {}"
        self.api_logger.log(
            logging_level,
            print_format.format(
                the_request.request.method,
                the_request.request.url,
                self._print_headers(the_request.request.headers),
                the_request.request.body
                if the_request.request.body is None
                or (
                    the_request.request.body is not None
                    and len(the_request.request.body) < 1000
                )
                else "{} ...".format(the_request.request.body[0:250]),
                the_request.status_code,
                the_request.url,
                self._print_headers(the_request.headers),
                the_request.cookies,
            ),
        )

    def _print_req_trace(
        self,
        the_request: requests.Response,
        logging_level: int = logging.DEBUG,
    ) -> None:
        """Helper function for tracing errors in requests - Prints out the request input and response

        Args:
            the_request (requests.Response): The response object from the request
            logging_level (logging._Level): The logging level to use for the request
        """
        if the_request.history:
            self.api_logger.debug("\nRequest was redirected")
            for resp in the_request.history:
                self._print_req(resp, logging.DEBUG)
            self.api_logger.debug("\n\nFinal destination:")
            self._print_req(the_request, logging_level)
        else:
            self._print_req(the_request, logging_level)

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
            self.api_logger.warn("Failed to log in: {}".format(ex))
            return False

        # self.api_logger.debug("\nSession cookies: {}".format(self.mmw_session.cookies))
        return True

    def _set_request_headers(self, request_endpoint: str) -> None:
        """Adds the right referer and datatype headers to a request

        Args:
            request_endpoint (str): The endpoint for the request
        """

        if self.api_endpoint in request_endpoint:
            headers = {
                "Content-Type": "application/json",
                "Referer": "https://staging.modelmywatershed.org/analyze",
                "X-Requested-With": "XMLHttpRequest",
            }
        elif self.project_endpoint in request_endpoint:
            headers = {
                "Content-Type": "application/json",
                "Referer": "https://staging.modelmywatershed.org/project/",
                "X-Requested-With": "XMLHttpRequest",
            }
        elif self.old_modeling_endpoint in request_endpoint:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": "https://staging.modelmywatershed.org/project/",
                "X-Requested-With": "XMLHttpRequest",
            }

        self.mmw_session.headers.update(headers)

    def _pprint_endpoint(self, request_endpoint: str) -> None:
        """Prints out the request endpoint in a format usable for a Windows endpoint

        Args:
            request_endpoint (string): the request endpoint
        """
        return (
            request_endpoint.replace(self.analyze_endpoint, "")
            .replace(self.modeling_endpoint, "")
            .replace("/", "_")
            .strip(" _")
        )

    def _make_mmw_request(
        self, req: Request, required_json_fields: Union[List[str], None] = None
    ) -> Dict:
        """Make a request to ModelMW with retries including handeling for throttling.

        Args:
            req (Request): A requests "Request" object
            required_json_fields (List[str]): A list of fields, at least one of which
                must be present in the response json.  If none of these fields are
                present, the request will be retried.

        Returns:
            Dict: The response json and details about the response
        """

        # "prepare" the request, in the session
        prepped = self.mmw_session.prepare_request(req)
        throttle_time = 30.0

        attempts = 0
        req_resp = None
        req_resp_json = None

        while attempts < 5:
            # use the session to send the request
            # NOTE:  The http method is already part of the prepared request, so here we just "send"
            try:
                req_resp = self.mmw_session.send(prepped)
                self._print_req_trace(req_resp, logging.DEBUG)
            except requests.exceptions.Timeout:
                self.api_logger.warn("\t***Request timed out!***")
                attempts += 1
                continue
            except requests.exceptions.RetryError:
                self.api_logger.warn("\tMaximum retries exceeded")
                attempts = 5
                break

            # make sure we got valid json - all responses from ModelMW - except for DELETE's - should be json, even errors
            try:
                if prepped.method != "DELETE":
                    req_resp_json = req_resp.json(object_pairs_hook=OrderedDict)
            except (requests.exceptions.JSONDecodeError, json.JSONDecodeError):
                self.api_logger.warn(
                    "\t***Proper JSON not returned for ModelMW request!***"
                )
                self.api_logger.debug(
                    "\t***Got {} with text {}!***".format(req_resp, req_resp.text)
                )

            # if we got a positive response code, we have proper json, and it has the required fields, return it
            if (
                req_resp.status_code in [200, 201]
                and (
                    (
                        req_resp_json is not None
                        and required_json_fields is not None
                        and required_json_fields != []
                        and (
                            (type(req_resp_json) in [dict, OrderedDict])
                            and any(
                                (
                                    (req_key in req_resp_json.keys())
                                    and (req_resp_json[req_key] is not None)
                                    for req_key in required_json_fields
                                )
                            )
                        )
                    )
                    or (
                        req_resp_json is not None
                        and (required_json_fields is None or required_json_fields == [])
                    )
                )
                or (req_resp.status_code in [204, 404] and prepped.method == "DELETE")
            ):
                return {
                    "succeeded": True,
                    "json_response": copy.deepcopy(req_resp_json),
                    "error_response": None,
                }

            # If we didn't get a positive response code, or we didn't get proper json,
            # or the expected fields aren't in it
            self.api_logger.warn(
                "\tModelMW {} request to {} FAILED on attempt {} with status code {}".format(
                    req_resp.request.method,
                    req_resp.request.url,
                    attempts,
                    req_resp.status_code,
                )
            )

            # status codes not to retry
            if req_resp.status_code in [400,404]:
                self.api_logger.warn(
                    "\tGot status code {}; will not retry".format(req_resp.status_code)
                )
                attempts = 5
                break

            # try to read the error details to see if we've been throttled
            req_resp_details = ""
            if (
                req_resp_json is not None
                and (
                    type(req_resp_details) is dict
                    or type(req_resp_details) is OrderedDict
                )
                and "detail" in req_resp_details.keys()
            ):
                req_resp_details = req_resp_details["detail"]
            if "throttled" in req_resp_details:
                search_pat = "Expected available in (?P<throttle_time>[\d\.]+) seconds."
                throttle_match = re.search(search_pat, req_resp_details)
                if throttle_match is not None:
                    throttle_time = float(throttle_match.group("throttle_time"))

            if throttle_time > 60.0 * 30.0:
                self.api_logger.warn(
                    "\twait time of {}s is too long, will not retry".format(
                        throttle_time
                    )
                )
                attempts = 5
                break

            if attempts < 4:
                self.api_logger.debug("\tretrying in {}s...".format(throttle_time))
                time.sleep(throttle_time)

            attempts += 1

        # if we get all the way here, just return whatever we got
        self.api_logger.error("\t***ERROR IN ModelMW REQUEST***")
        if req_resp is not None:
            self._print_req_trace(req_resp, logging.ERROR)
        return {
            "succeeded": True,
            "json_response": None,
            "error_response": req_resp_json if req_resp_json is not None else req_resp,
        }

    def start_job(
        self,
        request_endpoint: str,
        job_label: str,
        payload: Dict = None,
    ) -> ModelMyWatershedJob:
        """Starts an analysis or modeling job

        Args:
            request_endpoint (str): The endpoint for the request
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
            "start_job_status": "Not Started",
            "job_result_status": "Not Started",
        }

        self._set_request_headers(request_endpoint)
        if self.api_endpoint in request_endpoint:
            # the api endpoint expects json, expected to be dumped from a dictionary
            json_data = payload
            payload = None
        elif self.old_modeling_endpoint in request_endpoint:
            # the older modeling endpoint expected form data, that should be pre-prepared by the user
            json_data = None

        outgoing_request: Request = Request(
            "POST",
            "{}/{}".format(self.mmw_host, request_endpoint),
            data=payload,
            json=json_data,
        )
        start_job_req: Dict = self._make_mmw_request(
            outgoing_request, ["job", "job_uuid"]
        )

        if start_job_req["succeeded"] == True:
            job_dict["start_job_status"] = "succeeded"
            job_dict["start_job_response"] = start_job_req["json_response"]
        else:
            job_dict["start_job_status"] = "failed"
            job_dict["start_job_response"] = start_job_req["error_response"]
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

        if start_job_dict["start_job_status"] != "succeeded":
            self.api_logger.warn(
                "Job was not successfully started, cannot get results."
            )
            return start_job_dict

        job_id = None
        if (
            (
                type(start_job_dict["start_job_response"]) is dict
                or type(start_job_dict["start_job_response"]) is collections.OrderedDict
            )
            and "job_uuid" in start_job_dict["start_job_response"].keys()
            and start_job_dict["start_job_response"]["job_uuid"] is not None
        ):
            job_id = start_job_dict["start_job_response"]["job_uuid"]
        elif (
            (
                type(start_job_dict["start_job_response"]) is dict
                or type(start_job_dict["start_job_response"]) is collections.OrderedDict
            )
            and "job" in start_job_dict["start_job_response"].keys()
            and start_job_dict["start_job_response"]["job"] is not None
        ):
            job_id = start_job_dict["start_job_response"]["job"]

        if job_id is None:
            self.api_logger.warn(
                "Not enough information about start of job to retreive results."
            )
            return start_job_dict

        self.api_logger.debug("Job ID to retreive: {}".format(job_id))

        finished_job_dict = copy.deepcopy(start_job_dict)

        self._set_request_headers(start_job_dict["request_endpoint"])
        if self.api_endpoint in start_job_dict["request_endpoint"]:
            job_endpoint = "{}/{}jobs/{}/".format(
                self.mmw_host, self.api_endpoint, job_id
            )
        elif self.old_modeling_endpoint in start_job_dict["request_endpoint"]:
            job_endpoint = "{}/{}jobs/{}/".format(
                self.mmw_host, self.old_modeling_endpoint, job_id
            )

        job_results_req = Request("GET", job_endpoint)
        job_results_json = {}

        is_finished = False
        while is_finished == False:
            job_results_resp = self._make_mmw_request(job_results_req, ["status"])
            job_results_json = job_results_resp["json_response"]

            if job_results_resp["succeeded"] == False:
                finished_job_dict["error_response"] = job_results_resp["error_response"]
                finished_job_dict["job_result_status"] = "failed"
                return finished_job_dict

            elif (
                "error" in job_results_resp["json_response"].keys()
                and job_results_resp["json_response"]["error"] != ""
            ):
                self.api_logger.error(
                    "\t***ERROR GETTING JOB RESULTS***\n\t{}".format(
                        job_results_json["error"]
                    )
                )
                finished_job_dict["error_response"] = job_results_json
                finished_job_dict["job_result_status"] = "failed"
                return finished_job_dict

            is_finished = job_results_json["status"] == "complete"
            if not is_finished:
                self.api_logger.debug("ModelMW job has not yet finished.")
                time.sleep(0.5)

        if "result" in job_results_json.keys() and job_results_json["result"] != "":
            finished_job_dict["result_response"] = job_results_json
            finished_job_dict["job_result_status"] = "succeeded"
            self.api_logger.info(
                "\tGot {} results for {}".format(
                    self._pprint_endpoint(start_job_dict["request_endpoint"]),
                    start_job_dict["job_label"],
                )
            )
        else:
            self.api_logger.warn("\t***Did not get a results key in the job results***")
            finished_job_dict["error_response"] = job_results_json
            finished_job_dict["job_result_status"] = "failed"

        # dump out the whole job for posterity
        if self.save_path is not None:
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
        payload: Union[Dict, None] = None,
    ) -> ModelMyWatershedJob:
        """Starts a ModelMyWatershed job and waits for and returns the results

        Args:
            request_endpoint (str): The endpoint for the request
            payload (Dict): The payload going to the request.
                Either a JSON serializable dictionary or pre-formatted form data.
            job_label (str): A label to use to save the output files

        Returns:
            ModelMyWatershedJob: The job request and result
        """
        start_job_dict = self.start_job(
            request_endpoint=request_endpoint,
            payload=payload,
            job_label=job_label,
        )

        if start_job_dict["start_job_status"] != "succeeded":
            self.api_logger.warn(
                "\t{} job FAILED for {}".format(
                    self._pprint_endpoint(start_job_dict["request_endpoint"]),
                    job_label,
                )
            )
            return copy.deepcopy(start_job_dict)

        time.sleep(3.5)  # max of 20 requests per minute!

        finished_job_dict = copy.deepcopy(self.get_job_result(start_job_dict))

        return finished_job_dict

    def create_project(
        self,
        model_package: str,
        name: str = "Untitled Project",
        area_of_interest: Union[Dict, None] = None,
        wkaoi: Union[str, None] = None,
        huc: Union[str, None] = None,
        mapshed_job_uuid: Union[str, None] = None,
        subbasin_mapshed_job_uuid: Union[str, None] = None,
        layer_overrides: Union[ModemMyWatershedLayerOverride, None] = None,
    ) -> Dict:
        """Creates a new project on ModelMyWatershed.  At this time, a project
        is needed to access some data, including weather projections for a site.

        NOTE:  Projects created with this function **will not be usable** in the
        ModelMyWatershed web app, but they will appear under the projects for your user.
        In order not to have many unusable project, I _strongly_ recommend following
        any create_project actions with a delete_project action later in your script.

        YOU MUST BE LOGGED IN TO USE THIS FEATURE!

        Args:
            model_package (str): The model package to use.
                Must be either "gwlfe" or "tr-55"
            name (str): A name for the project, can be any text
            area_of_interest (Union[Dict, None]): A geojson dictionary with the
                shape of the area of interest for the project.  One of the
                area_of_interest, wkaoi, or huc must be given.
            wkaoi (Union[str, None]): A well known area of interest identifier
                for the project area.  One of the  area_of_interest, wkaoi, or huc
                must be given.
            huc (Union[str, None]): A HUC identifier code (HUC8, HUC10, or HUC12) for
                the project area.  One of the  area_of_interest, wkaoi, or huc must
                be given.
            mapshed_job_uuid (str): The UUID for the GWLF-E prepare (MapShed) job tied
                to this project, if applicable.
            subbasin_mapshed_job_uuid (str): The UUID for the subbasin GWLF-E prepare
                (MapShed) job tied to this project, if applicable.
            layer_overrides (ModemMyWatershedLayerOverride): Any layer overrides to
                use in the project


        Returns:
            Dict: A dictionary with information about the new project
        """

        request_endpoint = self.project_endpoint
        self._set_request_headers(request_endpoint)

        if huc is None and wkaoi is None and area_of_interest is None:
            self.api_logger.error(
                "\t***Either a HUC code, an WKAoI, or a geojson is required to create a project!***"
            )
            return {}

        payload = {
            "name": name,
            "model_package": model_package,
        }

        if area_of_interest is not None:
            payload["area_of_interest"] = area_of_interest
        elif huc is not None and huc != "":
            payload["huc"] = huc
        elif wkaoi is not None and wkaoi != "":
            payload["wkaoi"] = wkaoi

        if mapshed_job_uuid is not None and mapshed_job_uuid != "":
            payload["mapshed_job_uuid"] = mapshed_job_uuid
        if subbasin_mapshed_job_uuid is not None and subbasin_mapshed_job_uuid != "":
            payload["subbasin_mapshed_job_uuid"] = subbasin_mapshed_job_uuid

        if layer_overrides is not None:
            payload["layer_overrides"] = layer_overrides

        create_project_req: Request = Request(
            "POST", "{}/{}".format(self.mmw_host, request_endpoint), json=payload
        )
        create_project_resp = self._make_mmw_request(create_project_req, ["id"])

        if create_project_resp["succeeded"] == True:
            return create_project_resp["json_response"]

        return {}

    def delete_project(
        self,
        project_id: Union[str, int],
    ) -> None:
        """Deletes a ModelMW project.  If you're using the API to get information based
        on API-created skeleton projects, you probably want to be able to delete the
        project so as not to clutter up your user data with zillions of project
        YOU MUST BE LOGGED IN TO USE THIS FEATURE!

        Args:
                project_id (Union[str,int]): The project id.

        Returns:
            None
        """

        request_endpoint = self.project_endpoint + "{}".format(project_id)
        self._set_request_headers(request_endpoint)

        delete_project_req: Request = Request(
            "DELETE", "{}/{}".format(self.mmw_host, request_endpoint)
        )
        self._make_mmw_request(delete_project_req)

    def get_project_weather(
        self, project_id: Union[str, int], weather_layer: str
    ) -> Dict:
        """Get weather data for project given a category, if available.
        Current categories are NASA_NLDAS_2000_2019, RCP45_2080_2099 and
        RCP85_2080_2099, and only support shapes within the DRB. Given a project
        within the DRB, we find the N=2 nearest weather stations and
        average their values.

            Args:
                project_id (Union[str,int]): The project id.
                weather_layer (str) The weather layer to use, must be one of
                    "NASA_NLDAS_2000_2019", "RCP45_2080_2099" or "RCP85_2080_2099"


            Returns:
                Dict: Weather output, ready to be fed into a project GWLF-E modification run
        """

        request_endpoint = self.project_endpoint + "{}/weather/{}".format(
            project_id, weather_layer
        )
        self._set_request_headers(request_endpoint)

        weather_data_req = Request(
            "GET", "{}/{}".format(self.mmw_host, request_endpoint)
        )
        weather_data_resp = self._make_mmw_request(
            weather_data_req, ["output"]  # "WxYrBeg"
        )

        if weather_data_resp["succeeded"] == True:
            # # dump out the weather data
            # if self.save_path is not None:
            #     with open(
            #         self.json_dump_path
            #         + "{}_weather_{}.json".format(project_id, weather_layer),
            #         "w",
            #     ) as fp:
            #         json.dump(weather_data_resp["json_response"], fp, indent=2)
            return weather_data_resp["json_response"]

        self.api_logger.error("\t***ERROR GETTING WEATHER DATA***")
        return {}

    def get_subbasin_details(
        self,
        mapshed_job_uuid: str,
    ) -> list:
        """Gets information (geojson, metadata) about the HUC-12 subbasins within the results of a sub-basin preparation (MapShed) request.

        Args:
            mapshed_job_uuid (str): The UUID for the SUBBASIN GWLF-E prepare (MapShed) job tied to this project, if applicable.


        Returns:
            list: A list of geojsons for the HUC-12's in the subbasins contained in the MapShed job
        """

        request_endpoint = self.old_modeling_endpoint + "subbasins"
        self._set_request_headers(request_endpoint)

        params = {"mapshed_job_uuid": mapshed_job_uuid}

        subbasin_detail_req = Request(
            "POST", "{}/{}".format(self.mmw_host, request_endpoint), params=params
        )
        subbasin_detail_resp = self._make_mmw_request(subbasin_detail_req)
        subbasin_detail_resp_json = subbasin_detail_resp["json_response"]

        if (
            subbasin_detail_resp_json is not None
            and type(subbasin_detail_resp_json) is list
            and len(subbasin_detail_resp_json) > 0
            and (
                type(subbasin_detail_resp_json[0]) is dict
                or type(subbasin_detail_resp_json[0]) is OrderedDict
            )
            and "shape" in subbasin_detail_resp_json[0].keys()
            and subbasin_detail_resp_json[0]["shape"] is not None
        ):
            self.api_logger.info(
                "\tGot information about {} HUC-12 subbasins".format(
                    len(subbasin_detail_resp_json)
                )
            )

            # dump out the subbasins
            # if self.save_path is not None:
            #     with open(
            #         self.json_dump_path + mapshed_job_uuid + "_subbasin_geojsons.json",
            #         "w",
            #     ) as fp:
            #         json.dump(subbasin_detail_resp_json, fp, indent=2)
            return subbasin_detail_resp_json

        self.api_logger.error("\t***ERROR GETTING SUB-BASIN DETAILS***")
        return []

    def run_batch_analysis(
        self, list_of_aois: List, analysis_endpoint: str
    ) -> pd.DataFrame:
        """Given a list of areas of interest (AOIs), runs all of them for the same analysis endpoint.  Depending on the number of site in the list, this may take a very long time to return.

        Args:
            list_of_aois (List): A list of AOI's.  They can be strings or geojsons.
            analysis_endpoint (str): The analysis endpoint to use.

        Returns:
            pd.DataFrame: A pandas data frame with the results from all of the runs.
        """
        run_frames = []
        run_number: int = 1
        for aoi in list_of_aois:
            # TODO(SRGDamia1): validate strings
            # if it's a string with underscores, we're assuming it's a WKAoI from the hidden well-known area of interest table
            # this is not expected, but we'll support it
            if isinstance(aoi, str) and "__" in aoi:
                job_label = aoi
                payload = {"wkaoi": aoi}
            # if it doesn't have underscores, we're assuiming it's a HUC
            elif isinstance(aoi, str) and (
                len(aoi) == 8 or len(aoi) == 10 or len(aoi) == 12
            ):
                job_label = aoi
                payload = {"huc": aoi}
            # if it's not a string, hopefully it's a valid geojson
            # TODO(SRGDamia1): validate geojson!  Must be a valid single-ringed Multipolygon GeoJSON representation of the shape to analyze
            # NOTE:  In order to validate geojson, we'd need to add some sort of geo dependency.  I'm not sure if we want to add that.
            else:
                if (
                    isinstance(aoi, Dict)
                    and "properties" in aoi.keys()
                    and "name" in aoi["properties"].keys
                ):
                    job_label = aoi["properties"]["name"]
                else:
                    job_label = "shape_{}".format(run_number)
                payload = aoi

            try:
                req_dump = self.run_mmw_job(
                    request_endpoint=analysis_endpoint,
                    job_label=job_label,
                    payload=payload,
                )
                res_frame = pd.DataFrame(
                    copy.deepcopy(
                        req_dump["result_response"]["result"]["survey"]["categories"]
                    )
                )
            except Exception as ex:
                self.api_logger.warn("\tUnexpected exception:\n\t{}".format(ex))
                continue

            res_frame["job_label"] = job_label
            res_frame["request_endpoint"] = analysis_endpoint
            run_frames.append(res_frame)
            run_number += 1

        # join all of the frames together into one frame with the batch results
        lu_results = pd.concat(run_frames, ignore_index=True)
        return lu_results

    def run_batch_gwlfe(
        self, list_of_aois: List, layer_overrides: ModemMyWatershedLayerOverride = None
    ) -> Dict[str, pd.DataFrame]:
        """Given a list of areas of interest (AOIs), runs mapshed and GWLF-E on all of them.

        Args:
            list_of_aois (List): A list of AOI's.  They can be strings or geojsons.
            layer_overrides (ModemMyWatershedLayerOverride): Any layer overrides to use in the model

        Returns:
            Dict[str,pd.DataFrame]: A dictionary of dataframes with the GWLF-E model results.
        """
        # empty lists to hold results
        mapshed_z_files = []

        gwlfe_monthlies = []
        gwlfe_load_summaries = []
        gwlfe_lu_loads = []
        gwlfe_metas = []
        gwlfe_summaries = []

        run_number: int = 1
        for aoi in list_of_aois:
            mapshed_payload = {}
            if layer_overrides is not None:
                mapshed_payload["layer_overrides"] = layer_overrides

            # TODO(SRGDamia1): validate strings
            # if it's a string with underscores, we're assuming it's a WKAoI from the hidden well-known area of interest table
            # this is not expected, but we'll support it
            if isinstance(aoi, str) and "__" in aoi:
                job_label = aoi
                mapshed_payload["wkaoi"] = aoi
            # if it doesn't have underscores, we're assuiming it's a HUC
            elif isinstance(aoi, str) and (
                len(aoi) == 8 or len(aoi) == 10 or len(aoi) == 12
            ):
                job_label = aoi
                mapshed_payload["huc"] = aoi
            # if it's not a string, hopefully it's a valid geojson
            # TODO(SRGDamia1): validate geojson!  Must be a valid single-ringed Multipolygon GeoJSON representation of the shape to analyze
            # NOTE:  In order to validate geojson, we'd need to add some sort of geo dependency.  I'm not sure if we want to add that.
            else:
                if (
                    isinstance(aoi, Dict)
                    and "properties" in aoi.keys()
                    and "name" in aoi["properties"].keys
                ):
                    job_label = aoi["properties"]["name"]
                else:
                    job_label = "shape_{}".format(run_number)
                mapshed_payload["area_of_interest"] = aoi

            mapshed_job_id = None
            mapshed_result = None
            gwlfe_result = None

            mapshed_job_dict = self.run_mmw_job(
                request_endpoint=self.gwlfe_prepare_endpoint,
                job_label=job_label,
                payload=mapshed_payload,
            )
            if "result_response" in mapshed_job_dict.keys():
                mapshed_job_id = mapshed_job_dict["start_job_response"]["job_uuid"]
                mapshed_result = mapshed_job_dict["result_response"]["result"]

                mapshed_result["job_label"] = job_label
                mapshed_z_files.append(mapshed_result)

            ## NOTE:  Don't run GWLF-E if we don't get MapShed results
            if mapshed_job_id is not None and mapshed_result is not None:

                land_use_modification_set = "[{}]"

                gwlfe_payload = {
                    # NOTE:  The value of the inputmod_hash doesn't really matter here
                    # Internally, the ModelMW site uses the inputmod_hash in scenerios to
                    # determine whether it can use cached results or if it needs to
                    # re-run the job
                    "inputmod_hash": self.inputmod_hash,
                    "modifications": land_use_modification_set,
                    "job_uuid": mapshed_job_id,
                }
                gwlfe_job_dict = self.run_mmw_job(
                    request_endpoint=self.gwlfe_run_endpoint,
                    job_label=job_label,
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
                    frame["job_label"] = job_label
                gwlfe_monthlies.append(gwlfe_monthly)
                gwlfe_load_summaries.append(gwlfe_load_summary)
                gwlfe_lu_loads.append(gwlfe_lu_load)
                gwlfe_metas.append(gwlfe_meta)
                gwlfe_summaries.append(gwlfe_summary)

        # join various result
        gwlfe_results = {}
        gwlfe_results["gwlfe_monthly"] = pd.concat(gwlfe_monthlies, ignore_index=True)
        gwlfe_results["gwlfe_load_summaries"] = pd.concat(
            gwlfe_load_summaries, ignore_index=True
        )
        gwlfe_results["gwlfe_lu_loads"] = pd.concat(gwlfe_lu_loads, ignore_index=True)
        gwlfe_results["gwlfe_metadata"] = pd.concat(gwlfe_metas, ignore_index=True)
        gwlfe_results["gwlfe_summaries"] = pd.concat(gwlfe_summaries, ignore_index=True)
        return gwlfe_results

    def convert_predictions_to_modifications(
        self,
        modified_analysis_result_file: str,
        unmodified_mapshed_result_file: str,
    ) -> Union[str, None]:
        """Converts the Shippensburg generated predictions of land uses in 2100 within
        the Delaware River Watershed into a set of modifications that can be applied
        to other land use layers in order to model the 2100 predictions for an area.
        This function expects that you have already run both the analysis for the 2100
        predictions and a mapshed job for the underlying data and have saved both using
        the default file naming conventions used within this library.  The result from
        this function is *not* a new model result, but instead a dictionary of
        modifications that can be fed back in to a model job.

        NOTE:  The modifications applied by ModelMyWatershed for GWLF-E are **NOT**
        geolocated in the way the baselayers are.  This means that when the 2100
        predicted land uses are applied as modification, any information about the soil
        type or geology of the changed area is lost.

        Args:
            modified_analysis_result_file (str): The file name of a json file with the
               analysis output to be used for modifications.  This is expected to be a
               dump of a request to the future predictions endpoint.
            unmodified_mapshed_result_file (str): The file name of a MapShed (GWLF-E
                prepare) output on the **unmodified** layer

        Returns:
            Dict: a dictionary of land use modifications
        """

        _, lu_modifications = self.read_dumped_result(
            "",
            "",
            self.json_dump_path + modified_analysis_result_file,
            "survey",
        )
        _, mapshed_base = self.read_dumped_result(
            "",
            "",
            self.json_dump_path + unmodified_mapshed_result_file,
            "Area",
        )

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
        # mod_dict_preset = {
        #     "entry_landcover_preset": "drb_2100_land_{}".format(
        #         modified_land_use_source
        #     )
        # }
        # mod_dict_2 = dict(mod_dict_preset, **mod_dict)
        # mod_dict_dump = "[{}]".format(json.dumps(mod_dict_2).replace(" ", ""))
        mod_dict_dump = "[{}]".format(json.dumps(mod_dict).replace(" ", ""))

        return mod_dict_dump

    def analyse_protected_lands(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.protected_lands_endpoint, job_label, payload)

    def analyse_soil(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.soil_endpoint, job_label, payload)

    def analyse_terrain(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.terrain_endpoint, job_label, payload)

    def analyse_climate(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.climate_endpoint, job_label, payload)

    def analyse_point_sources(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.point_source_endpoint, job_label, payload)

    def analyse_animals(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.animal_endpoint, job_label, payload)

    def analyse_catchment_water_quality(
        self, job_label, payload
    ) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.catchment_water_quality_endpoint, job_label, payload
        )

    def analyse_land(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.land_endpoint.format("2019_2019"), job_label, payload
        )

    def analyse_2019_land(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.land_endpoint.format("2019_2019"), job_label, payload
        )

    def analyse_2016_land(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.land_endpoint.format("2019_2016"), job_label, payload
        )

    def analyse_2011_land(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.land_endpoint.format("2019_2011"), job_label, payload
        )

    def analyse_2006_land(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.land_endpoint.format("2019_2006"), job_label, payload
        )

    def analyse_2001_land(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.land_endpoint.format("2019_2001"), job_label, payload
        )

    def analyse_2011_2011_land(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.land_endpoint.format("2011_2011"), job_label, payload
        )

    def analyse_forcast(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.forcast_endpoint.format("centers"), job_label, payload
        )

    def analyse_forcast_centers(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.forcast_endpoint.format("centers"), job_label, payload
        )

    def analyse_forcast_corridors(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.forcast_endpoint.format("corridors"), job_label, payload
        )

    def analyse_streams(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.streams_endpoint.format("nhdhr"), job_label, payload
        )

    def analyse_streams_nhdhr(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(
            self.streams_endpoint.format("nhdhr"), job_label, payload
        )

    def analyse_streams_nhdmr(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.streams_endpoint.format("nhd"), job_label, payload)

    def analyse_streams_drb(self, job_label, payload) -> ModelMyWatershedJob:
        return self.run_mmw_job(self.streams_endpoint.format("drb"), job_label, payload)

    def get_dump_filename(self, request_endpoint: str, job_label: str) -> str:
        """Returns the expected generated file name for a json returned by ModelMyWatershed

        Args:
            request_endpoint (str): The endpoint of the request
            job_label (str): custom job label for the request

        Returns:
            str: a conventioned file name
        """
        return (
            self.json_dump_path
            + job_label.replace("/", "_").strip(" _")
            + "_"
            + self._pprint_endpoint(request_endpoint)
            + ".json"
        )

    def read_dumped_result(
        self,
        request_endpoint: str,
        job_label: str,
        alt_filename: str = "",
        needed_result_key: str = "",
    ) -> ModelMyWatershedJob:
        """Reads a json file saved by this library with its file naming convention back into memory.

        Args:
            request_endpoint (str): The request endpoint that was used
            job_label (str): the custom job label
            alt_filename (str, optional): an alternate file name to look for, if the file was saved with a name other than that generated by `get_dump_filename(...)`. Defaults to "".
            needed_result_key (str, optional): The key in the json for the results, if the json was saved external to this library.. Defaults to "".

        Returns:
            ModelMyWatershedJob: A python dictionary made from the saved file.
        """

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

            self.api_logger.info(
                "\tRead saved {} results for {} from JSON".format(
                    self._pprint_endpoint(request_endpoint),
                    job_label,
                )
            )
        return (req_dump, saved_result)
