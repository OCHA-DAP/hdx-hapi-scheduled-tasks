import json
import time
from urllib import request


def get_app_identifier(
    hapi_site,
    email_address="ian.hopkinson%40humdata.org",
    app_name="HDXINTERNAL_hapi_scheduled",
):
    app_identifier_url = (
        f"https://{hapi_site}.humdata.org/api/v1/"
        f"encode_app_identifier?application={app_name}&email={email_address}"
    )
    print(app_identifier_url, flush=True)
    app_identifier_response = fetch_data_from_hapi(app_identifier_url)

    app_identifier = app_identifier_response["encoded_app_identifier"]
    return app_identifier


def fetch_data_from_hapi(query_url, limit=1000):
    """
    Fetch data from the provided query_url with pagination support.

    Args:
    - query_url (str): The query URL to fetch data from.
    - limit (int): The number of records to fetch per request.

    Returns:
    - list: A list of fetched results.
    """

    if "encode_app_identifier" in query_url:
        with request.urlopen(query_url) as response:
            json_response = json.loads(response.read())

        return json_response

    idx = 0
    results = []

    t0 = time.time()
    while True:
        offset = idx * limit
        url = f"{query_url}&offset={offset}&limit={limit}"

        with request.urlopen(url) as response:
            print(f"Getting results {offset} to {offset+limit-1}")
            print(f"{url}", flush=True)
            encoding = response.headers.get_content_charset()

            # print(response.headers, flush=True)
            if "output_format=json" in query_url:
                json_response = json.loads(response.read())

                results.extend(json_response["data"])
                # If the returned results are less than the limit,
                # it's the last page
                if len(json_response["data"]) < limit:
                    break
            else:
                raw = response.read().decode(encoding)
                csv_rows = raw.splitlines()
                # Don't include the header line except for the first file
                if len(results) == 0:
                    results.extend(csv_rows)
                else:
                    results.extend(csv_rows[1:])

                if len(csv_rows) < limit:
                    break
        idx += 1

    print(f"Download took {time.time()-t0:0.2f} seconds", flush=True)
    return results
