#!/usr/bin/env python
# encoding: utf-8

import json
import os
import requests
from common.util import fetch_data_from_hapi, get_app_identifier

HAPI_BASE_URL = os.getenv("HAPI_BASE_URL")
HDX_PACKAGE_SEARCH_URL = os.getenv("HDX_PACKAGE_SEARCH_URL")
HDX_BLUE_KEY = os.getenv("HDX_BLUE_KEY")


def main():
    pass
    # 1. from HAPI api get a list of all HDX resource IDs
    theme = "metadata/resource"
    hapi_app_identifier = get_app_identifier(
        "hapi",
        email_address="ian.hopkinson@humdata.org",
        app_name="HDXINTERNAL_hapi_scheduled",
    )
    query_url = (
        f"{HAPI_BASE_URL}api/v1/{theme}?"
        f"output_format=json"
        f"&app_identifier={hapi_app_identifier}"
    )

    hapi_results = fetch_data_from_hapi(query_url, limit=1000)

    print(f"Found {len(hapi_results)} resources", flush=True)
    for row in hapi_results:
        print(row["dataset_hdx_stub"], row["name"], row["resource_hdx_id"], flush=True)

    hapi_resource_ids = {x["resource_hdx_id"] for x in hapi_results}
    print(hapi_resource_ids, flush=True)
    # 2. from HDX api (via package_search) get a list of all resource IDs in CKAN that are marked as being in HAPI

    payload = json.dumps(
        {
            "fq": 'capacity:public +state:(active) +dataset_type: (dataset OR requestdata-metadata-only) -extras_archived:"true" +res_extras_in_hapi: [* TO *]'
        }
    )
    headers = {
        "Authorization": HDX_BLUE_KEY,
        "Content-Type": "application/json",
    }

    response = requests.request(
        "POST", HDX_PACKAGE_SEARCH_URL, headers=headers, data=payload
    )
    print(response.json(), flush=True)


# 3. compare the 2 lists :
# a. add the flag to those resources that are in HAPI
# . remove the flag from the CKAN resources that are no longer in HAPI


if __name__ == "__main__":
    main()
