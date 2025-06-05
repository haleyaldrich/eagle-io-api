from datetime import datetime
from dotenv import load_dotenv
import json
import logging
import os
import requests

load_dotenv()

logger = logging.getLogger(__name__)


def handle_request(response: requests.Response) -> dict:
    """
    Handle HTTP responses from API calls.
    This function processes HTTP responses, returning JSON data for successful responses
    or raising appropriate exceptions for errors. Error details are logged before raising.
    Args:
        response (requests.Response): The HTTP response object to process
    Returns:
        dict: The JSON response data if request was successful (status code 200)
    Raises:
        Exception: If the response contains an error (non-200 status code), with details about the error
    """
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error Status Code: {response.status_code}")

        try:
            error_json = response.json()
            logger.error(
                f"API call error. Status {response.status_code}. JSON response: {json.dumps(error_json, indent=4)}"
            )
            raise Exception(f"Error: {error_json}")
        except json.JSONDecodeError:
            logger.error("No JSON response available")

        logger.error(f"Response text: '{response.text}'")
        logger.error(f"Reason: {response.reason}")

        if response.text is None or len(response.text) == 0:
            raise Exception(f"API call error: Empty response message")
        else:
            raise Exception(f"API call error: {response.text}")


def get_token() -> str:
    """Retrieves an OAuth access token for the iTwin platform API."""
    url = "https://ims.bentley.com/connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("ITWIN_IOT_CLIENT_ID"),
        "client_secret": os.getenv("ITWIN_IOT_CLIENT_SECRET"),
        "scope": "itwin-platform",
    }
    r = handle_request(requests.post(url, data=payload))
    return r["access_token"]


def get_all_nodes():
    """
    Gets all nodes from the iTwin platform.

    Args:
        itwin_id (str): The iTwin Project or Asset ID
        access_token (str): OAuth access token with itwin-platform scope

    Returns:
        dict: The JSON response containing all nodes
    """
    params = {"iTwinId": os.getenv("ITWIN_IOT_ASSET_ID")}

    headers = {
        "Authorization": f"Bearer {os.getenv('ITWIN_IOT_API_TOKEN')}",
        "Accept": "application/vnd.bentley.itwin-platform.v1+json",
    }

    url = f"https://api.bentley.com/sensor-data/integrations/nodes"

    return handle_request(requests.get(url, headers=headers, params=params))


def query_node_by_dates(
    sensor_id: str, start_date: str = None, end_date: str = None
) -> dict:
    """
    Retrieves sensor data from the iTwin platform API for the specified sensor ID
    and optional date range.
    Args:
        sensor_id (str): The ID of the sensor to query
        start_date (str, optional): The start date for the data query in ISO 8601 format
        end_date (str, optional): The end date for the data query in ISO 8601 format
    """
    headers = {
        "Authorization": f"Bearer {os.getenv('ITWIN_IOT_API_TOKEN')}",
        "Accept": "application/vnd.bentley.itwin-platform.v1+json",
    }

    url = "https://api.bentley.com/sensor-data/data/observations"

    body = {
        "sensorId": sensor_id,
        "startDate": start_date,
        "endDate": end_date,
        "units": {
            "f": "digits",
            "T": "C",
        },
    }
    response = handle_request(requests.post(url, headers=headers, json=body))

    if "data" in response:
        return response["data"]
    else:
        raise Exception("No data found for the given sensor ID and date range")


def _get_latest_date_from_data(data: dict) -> str:
    keys = list(data.keys())
    dates = [datetime.strptime(key, "%Y-%m-%dT%H:%M:%S.%fZ") for key in keys]
    s = max(dates).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return s.replace(".000000Z", ".000Z")  # Format to match API response


def query_node(sensor_id: str) -> dict:
    """
    Retrieves all the data for a specific sensor. Method recursively queries the API
    until all data is retrieved.

    .. note::
        startDate is inclusive meaning that the data for the start date is included in the response.
        Recursion stops when the latest date in the response is equal to the latest date in the previous response.
    """

    start_date = "2024-01-01T00:00:00.000Z"
    end_date = "2030-01-01T00:00:00.000Z"

    logger.info(f"Querying sensor: {sensor_id} for dates {start_date} to {end_date}")
    data = query_node_by_dates(sensor_id, start_date, end_date)
    latest_date = _get_latest_date_from_data(data)

    iterations = 0
    while True:
        try:
            logger.info(
                f"Querying sensor: {sensor_id} for dates {latest_date} to {end_date}"
            )
            data_i = query_node_by_dates(sensor_id, latest_date, end_date)
        except Exception as e:
            raise e
        else:
            data.update(data_i)
            latest_date_i = _get_latest_date_from_data(data_i)

            if latest_date_i == latest_date:
                break

        iterations += 1
        if iterations > 100:
            raise Exception("Too many iterations, check the API response")

    return data
