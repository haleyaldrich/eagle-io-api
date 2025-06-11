from datetime import datetime, timedelta
import json
import logging
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from bf_goodrich import itwin, compute, nwps
from log.logging_config import setup_logging
from eagleio.api import EagleIOWorkspace

os.environ["ITWIN_IOT_API_TOKEN"] = itwin.get_token()

setup_logging(log_level="INFO", log_directory="logs", app_name="bf-goodrich-piezos")
logger = logging.getLogger(__name__)

DEVICES = json.load(open(os.path.join(os.path.dirname(__file__), "devices.json"), "r"))


def get_latest_date_from_data(data: dict) -> str:
    """
    Returns the latest date from the data dictionary.
    Data is expected to be a dictionary in the format:

    {
        "2025-02-05T17:00:00.000Z": {
            "f": 7711.340224899999,
            "T": 17.303894496723217
        },
        "2025-02-05T18:00:00.000Z": {
            "f": 7711.701230024999,
            "T": 17.30318479921675
        },
        ...
    }

    """
    assert isinstance(data, dict), "Data must be a dictionary"
    keys = list(data.keys())
    dates = [datetime.strptime(key, "%Y-%m-%dT%H:%M:%S.%fZ") for key in keys]
    s = max(dates).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return s.replace(".000000Z", ".000Z")


def add_to_date(date: str, days: int) -> str:
    """
    Adds a specified number of days to a date string in ISO 8601 format.
    """
    dt = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
    new_date = dt + timedelta(days=days)
    new_date = new_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return new_date.replace(".000000Z", ".000Z")


def main():
    """

    .. note::
        iTwin IoT API startDate is inclusive meaning that the data for the start
        date is included in the response.

        Recursion stops when the latest date in the response is equal to the
        latest date in the previous response.
    """
    eagleio = EagleIOWorkspace(os.environ["BF_GOODRICH_EAGLEIO_KEY"])
    for device in DEVICES:

        start_date = "2025-01-15T00:00:00.000Z"
        logger.info(f"Processing device: {device}")
        while True:
            try:
                # Query data from iTwin platform
                end_date = add_to_date(start_date, 60)
                data = itwin.query_node_by_dates(
                    sensor_id=DEVICES[device]["id"],
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as e:
                raise e
            else:
                logger.info(
                    f"Data retrieved for {device} from {start_date} to {end_date}"
                )
                latest_date_i = get_latest_date_from_data(data)
                if start_date == latest_date_i:
                    break
                else:
                    start_date = latest_date_i

                # Load raw data to Eagle.io
                logger.info(f"Loading data to Eagle.io")
                eagleio.load_data_to_datasource(
                    name=device,
                    data=data,
                    names_mapper={"f": "Frequency (digits)", "T": "Temperature (C)"},
                    units={"f": "digits", "T": "C"},
                )

                # Calculate water elevation
                logger.info(f"Calculating water elevation")
                water_elevation = compute.compute_piezo_elevation(
                    timestamps=list(data.keys()),
                    frequency=[d["f"] for d in data.values()],
                    temperature=[d["T"] for d in data.values()],
                    sensor_info=DEVICES[device],
                )

                # Load water elevation to Eagle.io
                logger.info(f"Loading water elevation to Eagle.io")
                eagleio.load_data_to_datasource(
                    name=device,
                    data=water_elevation,
                    names_mapper={"water_elevation": "Water Elevation (ft)"},
                    units={"water_elevation": "ft"},
                )

    # Load NWPS data
    logger.info("Loading NWPS data")
    data = nwps.get_gauge_data()
    eagleio.load_data_to_datasource(
        name="River Elevation",
        data=data,
        names_mapper={"water_elevation": "Water Elevation (ft)"},
        units={"water_elevation": "ft"},
    )


if __name__ == "__main__":
    main()
