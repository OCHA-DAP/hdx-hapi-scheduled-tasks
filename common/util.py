import json
import logging
import os
import requests

from typing import List, Union

HAPI_BASE_URL = os.getenv("HAPI_BASE_URL")
HDX_API_KEY = os.getenv("HDX_BLUE_KEY", os.getenv("HDX_API_KEY"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_app_identifier(
    hapi_site: str,
    email_address: str = "ian.hopkinson%40humdata.org",
    app_name="HDXINTERNAL_hapi_scheduled",
) -> str:
    app_identifier_url = (
        f"https://{hapi_site}.humdata.org/api/v1/"
        f"encode_app_identifier?application={app_name}&email={email_address}"
    )
    app_identifier_response = fetch_data_from_hapi(app_identifier_url)

    app_identifier = app_identifier_response["encoded_app_identifier"]
    return app_identifier


def fetch_data_from_hapi(query_url: str, limit: int = 1000) -> Union[dict, List[dict]]:
    """
    Fetch data from the provided query_url with pagination support.

    Args:
    - query_url (str): The query URL to fetch data from.
    - limit (int): The number of records to fetch per request.

    Returns:
    - list: A list of fetched results.
    """

    if "encode_app_identifier" in query_url:
        json_response = requests.request("get", query_url).json()

        return json_response

    idx = 0
    results = []

    while True:
        offset = idx * limit
        url = f"{query_url}&offset={offset}&limit={limit}"

        logger.info(f"Getting results {offset} to {offset+limit-1}")

        response = requests.request("GET", url)
        json_response = response.json()

        results.extend(json_response["data"])
        # If the returned results are less than the limit,
        # it's the last page
        if len(json_response["data"]) < limit:
            break
        idx += 1

    return results


def fetch_data_from_ckan_api(query_url: str, query: dict) -> dict:
    headers = {
        "Authorization": HDX_API_KEY,
        "Content-Type": "application/json",
    }

    start = 0
    if "start" not in query.keys():
        query["start"] = start
    if "rows" not in query.keys():
        query["rows"] = 100
    payload = json.dumps(query)
    i = 1
    logger.info(f"{i}. Querying {query_url} with {payload}")
    response = requests.request("POST", query_url, headers=headers, data=payload)
    full_response_json = response.json()
    n_expected_result = full_response_json["result"]["count"]

    result_length = len(full_response_json["result"]["results"])

    if result_length != n_expected_result:
        while result_length != 0:
            i += 1
            start += 100
            query["start"] = start
            payload = json.dumps(query)
            logger.info(f"{i}. Querying {query_url} with {payload}")
            new_response = requests.request(
                "POST", query_url, headers=headers, data=payload
            )
            result_length = len(new_response.json()["result"]["results"])
            full_response_json["result"]["results"].extend(
                new_response.json()["result"]["results"]
            )
    else:
        logger.info(
            f"CKAN API returned all results ({result_length}) on first page of 100"
        )

    assert n_expected_result == len(full_response_json["result"]["results"])

    return full_response_json
