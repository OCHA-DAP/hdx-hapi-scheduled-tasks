#!/usr/bin/env python
# encoding: utf-8

import json
import os
import requests
from common.util import (
    fetch_data_from_hapi,
    fetch_data_from_ckan_api,
    get_app_identifier,
)

HAPI_BASE_URL = os.getenv("HAPI_BASE_URL")
HDX_BLUE_KEY = os.getenv("HDX_BLUE_KEY")
HDX_BASE_URL = "https://blue.demo.data-humdata-org.ahconu.org/"


def main():
    print_banner("Synchronising resources in HAPI with in_hapi flags on HDX")
    # 1. from HAPI api get a list of all HDX resource IDs
    hapi_resource_ids = get_hapi_resource_ids()
    # print(hapi_resource_ids, flush=True)
    # 2. from HDX api (via package_search) get a list of all resource IDs in CKAN that are
    # marked as being in HAPI
    already_in_hapi_resource_ids = get_hdx_resources_with_in_hapi_flag()

    # 3. compare the 2 lists :
    # a. add the flag to those resources that are in HAPI
    # . remove the flag from the CKAN resources that are no longer in HAPI
    update_in_hapi_flag_in_hdx(hapi_resource_ids, already_in_hapi_resource_ids)


def update_in_hapi_flag_in_hdx(hapi_resource_ids, already_in_hapi_resource_ids):
    print_banner("update_in_hapi_flag_in_hdx")
    print(f"Number of resources in HAPI: {len(hapi_resource_ids)}", flush=True)
    print(
        f"Number of resources with in_hapi flag in HDX: {len(already_in_hapi_resource_ids)}",
        flush=True,
    )
    resource_ids_to_add = hapi_resource_ids.difference(already_in_hapi_resource_ids)
    resource_ids_to_remove = already_in_hapi_resource_ids.difference(hapi_resource_ids)

    print(f"Number of resource_ids_to_add = {len(resource_ids_to_add)}", flush=True)
    print(
        f"Number of resource_ids_to_remove = {len(resource_ids_to_remove)}", flush=True
    )
    mark_resource_url = f"{HDX_BASE_URL}api/action/hdx_mark_resource_in_hapi"
    print(mark_resource_url, flush=True)
    headers = {
        "Authorization": HDX_BLUE_KEY,
        "Content-Type": "application/json",
    }

    state = "yes"
    for resource_set in [resource_ids_to_add, resource_ids_to_remove]:
        if state == "yes":
            print(
                f"Adding `in_hapi` flags to {len(resource_set)} resources", flush=True
            )
        elif state == "no-data":
            print(
                f"Removing `in_hapi` flags from {len(resource_set)} resources",
                flush=True,
            )

        for i, resource_id in enumerate(resource_set):
            # if i > 1:
            #     break
            payload = json.dumps(
                {
                    "id": resource_id,
                    "in_hapi": state,  # This needs to be either "yes" or "no-data"
                }
            )

            response = requests.request(
                "POST", mark_resource_url, headers=headers, data=payload
            )
            print(
                f"{i}. {resource_id}, {response.json()['success']}, {state}", flush=True
            )
            if not response.json()["success"]:
                print(response.json(), flush=True)
        state = "no-data"


def get_hdx_resources_with_in_hapi_flag():
    print_banner("get_hdx_resources_with_in_hapi_flag")
    package_search_url = f"{HDX_BASE_URL}api/action/package_search"
    query = {
        "fq": 'capacity:public +state:(active) +dataset_type: (dataset OR requestdata-metadata-only) -extras_archived:"true" +res_extras_in_hapi: [* TO *]'
    }
    response = fetch_data_from_ckan_api(package_search_url, query)

    # print(response.json(), flush=True)
    datasets_with_in_hapi = response["result"]["results"]
    # print(json.dumps(datasets_with_in_hapi[0], indent=4), flush=True)
    already_in_hapi_resource_ids = set()
    if len(datasets_with_in_hapi) != 0:
        for dataset in datasets_with_in_hapi:
            for resource in dataset["resources"]:
                if resource.get("in_hapi", "no").lower() == "yes":
                    already_in_hapi_resource_ids.add(resource["id"])

    print(
        f"Found {len(already_in_hapi_resource_ids)} resources already marked as 'in_hapi'",
        flush=True,
    )

    return already_in_hapi_resource_ids


def get_hapi_resource_ids():
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

    print(f"Found {len(hapi_results)} resources in HAPI", flush=True)
    # for row in hapi_results:
    #     print(row["dataset_hdx_stub"], row["name"], row["resource_hdx_id"], flush=True)
    # for record in hapi_results:
    #     if record["resource_hdx_id"] == "3c9abf00-42ed-4676-a7d2-a85b3999a499":
    #         print(json.dumps(record, indent=4), flush=True)

    hapi_resource_ids = {x["resource_hdx_id"] for x in hapi_results}
    return hapi_resource_ids


def print_banner(message: str):
    width = len(message)
    print((width + 4) * "*", flush=True)
    print(f"* {message:<{width}} *", flush=True)
    print((width + 4) * "*", flush=True)


if __name__ == "__main__":
    main()
