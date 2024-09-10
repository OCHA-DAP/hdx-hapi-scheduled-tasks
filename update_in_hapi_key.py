#!/usr/bin/env python
# encoding: utf-8

import datetime
import json
import logging
import os
import time
import requests
from common.util import (
    fetch_data_from_hapi,
    fetch_data_from_ckan_api,
    get_app_identifier,
)

HAPI_BASE_URL = os.getenv("HAPI_BASE_URL")
HDX_API_KEY = os.getenv("HDX_BLUE_KEY", os.getenv("HDX_API_KEY"))
HDX_BASE_URL = os.getenv("HDX_BASE_URL")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process():
    print_banner(
        "Synchronising resources in HAPI with in_hapi flags on HDX",
        include_timestamp=True,
    )
    logger.info(f"HAPI_BASE_URL: {HAPI_BASE_URL}")
    logger.info(f"HDX_BASE_URL: {HDX_BASE_URL}")
    if HDX_API_KEY is not None:
        logger.info(f"HDX_API_KEY: [censored]{HDX_API_KEY[-10:]}")
    else:
        logger.info("HDX_API_KEY: None")

    # 1. from HAPI api get a list of all HDX resource IDs
    hapi_resource_ids = get_hapi_resource_ids()
    # 2. from HDX api (via package_search) get a list of all resource IDs in CKAN that are
    # marked as being in HAPI
    already_in_hapi_resource_ids = get_hdx_resources_with_in_hapi_flag()

    # 3. Process the two lists (actually sets):
    # a. add the flag to those resources that are in HAPI
    # b. remove the flag from the CKAN resources that are no longer in HAPI
    update_in_hapi_flag_in_hdx(hapi_resource_ids, already_in_hapi_resource_ids)


def update_in_hapi_flag_in_hdx(
    hapi_resource_ids: set, already_in_hapi_resource_ids: set
):
    t0 = time.time()
    print_banner("update_in_hapi_flag_in_hdx")
    logger.info(f"Number of resources in HAPI: {len(hapi_resource_ids)}")
    logger.info(
        f"Number of resources with in_hapi flag in HDX: {len(already_in_hapi_resource_ids)}"
    )
    resource_ids_to_add = hapi_resource_ids.difference(already_in_hapi_resource_ids)
    resource_ids_to_remove = already_in_hapi_resource_ids.difference(hapi_resource_ids)

    logger.info(f"Number of resource_ids_to_add = {len(resource_ids_to_add)}")
    logger.info(f"Number of resource_ids_to_remove = {len(resource_ids_to_remove)}")
    mark_resource_url = f"{HDX_BASE_URL}api/action/hdx_mark_resource_in_hapi"
    logger.info(f"Mark resource URL = {mark_resource_url}")
    headers = {
        "Authorization": HDX_API_KEY,
        "Content-Type": "application/json",
    }

    for state, resource_set in [
        ("yes", resource_ids_to_add),
        ("no-data", resource_ids_to_remove),
    ]:
        if state == "yes":
            logger.info(f"Adding `in_hapi` flags to {len(resource_set)} resources")
        elif state == "no-data":
            logger.info(f"Removing `in_hapi` flags from {len(resource_set)} resources")

        logger.info("index, resource_id, success, in_hapi value, error message")
        for i, resource_id in enumerate(resource_set):
            payload = json.dumps(
                {
                    "id": resource_id,
                    "in_hapi": state,  # This needs to be either "yes" or "no-data"
                }
            )

            response = requests.request(
                "POST", mark_resource_url, headers=headers, data=payload
            )

            if response.status_code == 404:
                print(response.json(), flush=True)
                print(
                    "Status code 404: "
                    f"Resource may be missing or API key invalid from {HDX_BASE_URL}",
                    flush=True,
                )
            elif response.status_code == 429:
                print(
                    "Status code 429: Too many requests, resolve by re-running",
                    flush=True,
                )
                return

            if not response.json()["success"]:
                logger.info(
                    f"{i}. {resource_id}, {response.json()['success']}, {state}, "
                    f"{response.json()['error']['message']}"
                )
                if response.json()["error"]["message"] == "Access denied":
                    raise PermissionError(
                        f"Access denied to {mark_resource_url} with API key "
                        f"ending [censored]{HDX_API_KEY[-10:]}"
                    )

            else:
                logger.info(
                    f"{i}. {resource_id}, {response.json()['success']}, {state}"
                )

    logger.info(
        f"{len(resource_ids_to_add)+len(resource_ids_to_remove)} add/remove operations "
        f"took {time.time() - t0:0.2f} seconds"
    )


def get_hdx_resources_with_in_hapi_flag() -> set:
    t0 = time.time()
    print_banner("get_hdx_resources_with_in_hapi_flag")
    package_search_url = f"{HDX_BASE_URL}api/action/package_search"
    query = {
        "fq": (
            "capacity:public +state:(active) "
            "+dataset_type: (dataset OR requestdata-metadata-only) "
            '-extras_archived:"true" +res_extras_in_hapi: [* TO *]'
        )
    }
    response = fetch_data_from_ckan_api(package_search_url, query)

    datasets_with_in_hapi = response["result"]["results"]
    already_in_hapi_resource_ids = set()
    if len(datasets_with_in_hapi) != 0:
        for dataset in datasets_with_in_hapi:
            for resource in dataset["resources"]:
                if resource.get("in_hapi", "no").lower() == "yes":
                    already_in_hapi_resource_ids.add(resource["id"])

    logger.info(
        (
            f"Found {len(already_in_hapi_resource_ids)} resources already marked as 'in_hapi', "
            f"took {time.time() - t0:0.2f} seconds"
        )
    )

    return already_in_hapi_resource_ids


def get_hapi_resource_ids() -> set:
    t0 = time.time()
    print_banner("Creating app identifier")
    theme = "metadata/resource"
    hapi_app_identifier = get_app_identifier(
        "hapi",
        email_address="ian.hopkinson@humdata.org",
        app_name="HDXINTERNAL_hapi_scheduled",
    )
    print_banner("Fetching data from HAPI resources endpoint")
    query_url = (
        f"{HAPI_BASE_URL}api/v1/{theme}?"
        f"output_format=json"
        f"&app_identifier={hapi_app_identifier}"
    )

    hapi_results = fetch_data_from_hapi(query_url, limit=1000)

    hapi_resource_ids = {x["resource_hdx_id"] for x in hapi_results}
    logger.info(
        f"Found {len(hapi_results)} resources in HAPI, took {time.time()-t0:0.2f} seconds"
    )
    return hapi_resource_ids


def print_banner(message: str, include_timestamp: bool = False):
    timestamp = f"Invoked at: {datetime.datetime.now().isoformat()}"
    if include_timestamp:
        width = max(len(message), len(timestamp))
    else:
        width = len(message)
    logger.info((width + 4) * "*")
    logger.info(f"* {message:<{width}} *")
    if include_timestamp:
        logger.info(f"* {timestamp:<{width}} *")
    logger.info((width + 4) * "*")


if __name__ == "__main__":
    process()
