"""
ETL Script for ingestions sensor data from BF Goodrich project into Eagle.io

This script orchestrates the extraction, transformation, and loading (ETL) of
both sensor and manual water elevation data for the B.F. Goodrich project. It
consolidates data from multiple sources, processes it, and uploads it to the
Eagle.io platform for centralized monitoring and analysis.

DATA SOURCES:
- Piezometer data from iTwin IoT
- Water elevation data from the National Water Prediction Service (NWPS) API
- Manually collected data from monitoring wells (Excel format)

PIEZOMETER ETL:
- Gets the latest timestamp from Eagle.io for each device.
- Retrieves sensor data from iTwin IoT in 30-day increments for each device listed in `devices.json`.
- Computes water elevation using sensor-specific calibration factors.
- Loads raw sensor data and computed water elevation data into Eagle.io.

NWPS ETL:
- Retrieves water elevation data from the NOAA NWPS API for the gauge KYTK2.
- Loads the data into Eagle.io.
- Retrieves historical water elevation (API does not provide historical data) and loads it into Eagle.io.
  - Note: This step may be removed to avoid overwriting existing data, though it is currently harmless.

MANUAL DATA ETL:
- Reads water elevation data from an Excel file preformatted to match Eagle.io’s API requirements.
- Filters out records that are already present in Eagle.io.
- Uploads the new data to Eagle.io.
"""

from datetime import datetime, timedelta
from dateutil import parser
import json
import logging
import pandas as pd
from pytz import timezone
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


def get_manual_transducer_data(name: str, start_date: str) -> dict:
    """
    Retrieves manual transducer data from an Excel file formatted to Eagle.io
    API's standard.

    Only rows with timestamps after the specified start_date are included.

    The data is returned in the following format:

    {
        "2025-02-05T17:00:00.000Z": {
            "temperature": 17.303894496723217,
            "conductivity": 0.0,
            "water_elevation": 1.0
        },
        "2025-02-05T18:00:00.000Z": {
            "temperature": 17.30318479921675,
            "conductivity": 0.0,
            "water_elevation": 1.0
        },
        ...
    }
    """

    p = os.path.join(
        os.path.dirname(__file__),
        "data",
        "transducer_data.xlsx",
    )
    df = pd.read_excel(
        p,
        sheet_name=name,
        skiprows=13,
        dtype={
            "Date/Time": str,
            "TEMPERATURE": float,
            "CONDUCTIVITY": float,
            "compensated elevation": float,
        },
    )
    cols = ["Date/Time", "TEMPERATURE", "CONDUCTIVITY", "compensated elevation"]
    df = df[cols]
    df.columns = ["timestamp", "temperature", "conductivity", "water_elevation"]
    df.dropna(subset=["water_elevation"], inplace=True)

    data = {}
    est = timezone("US/Eastern")
    start_date = parser.parse(start_date)

    for _, row in df.iterrows():
        dt = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
        dt_est = est.localize(dt)
        dt_utc = dt_est.astimezone(timezone("UTC"))

        if start_date is not None and dt_utc < start_date:
            continue

        timestamp = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        data[timestamp] = {
            "temperature": row["temperature"],
            "conductivity": row["conductivity"],
            "water_elevation": row["water_elevation"],
        }
    return data


def get_start_date_from_eagleio(name: str) -> str:
    """
    Retrieves the latest timestamp from a datasource in Eagle.io by its name.
    This is used to determine the starting point for data retrieval from iTwin IoT.
    """
    eagleio = EagleIOWorkspace(os.environ["BF_GOODRICH_EAGLEIO_KEY"])
    try:
        start_date = eagleio.get_latest_timestamp_from_datasource_by_name(name)
    except ValueError as e:
        return "2022-01-01T00:00:00.000Z"  # Default start date if datasource not found
    start_date = parser.parse(start_date)
    start_date = start_date - timedelta(
        days=1
    )  # Start from one day before the latest date
    return start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ").replace(".000000Z", ".000Z")


def main():

    eagleio = EagleIOWorkspace(os.environ["BF_GOODRICH_EAGLEIO_KEY"])

    # Load Piezometer data from iTwin IoT #####################################
    for device in DEVICES:

        logger.info(f"Processing device: {device}")
        logger.info("Retrieving latest timestamp from Eagle.io")
        start_date = get_start_date_from_eagleio(device)
        logger.info(f"Latest timestamp in Eagle.io for {device}: {start_date}")

        while True:
            try:
                # Query data from iTwin platform
                end_date = add_to_date(start_date, 30)
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

    # Load NWPS data ##########################################################
    logger.info("Loading NWPS data")
    data = nwps.get_gauge_data()
    eagleio.load_data_to_datasource(
        name="River Elevation",
        data=data,
        names_mapper={"water_elevation": "Water Elevation (ft)"},
        units={"water_elevation": "ft"},
    )

    logger.info("Loading NWPS manual data")
    data = nwps.get_manual_data()
    eagleio.load_data_to_datasource(
        name="River Elevation",
        data=data,
        names_mapper={"water_elevation": "Water Elevation (ft)"},
        units={"water_elevation": "ft"},
    )

    # Load manual transducer data #############################################
    logger.info("Loading manual transducer data")
    # for device in ["LW-04", "LW-08", "LW-10", "LW-14", "LW-18", "LW-20", "Stilling Well"]:
    for device in ["Stilling Well"]:
        logger.info(f"Processing manual transducer data for: {device}")
        logger.info("Retrieving latest timestamp from Eagle.io")
        start_date = get_start_date_from_eagleio(device)
        logger.info(f"Latest timestamp in Eagle.io for {device}: {start_date}")

        data = get_manual_transducer_data(device, start_date)

        # Process data in batches of 5000 keys
        batch_size = 5000
        all_keys = list(data.keys())
        for i in range(0, len(all_keys), batch_size):
            batch_keys = all_keys[i : i + batch_size]
            batch_data = {key: data[key] for key in batch_keys}
            logger.info(
                f"Processing batch {i//batch_size + 1} with {len(batch_keys)} records"
            )
            eagleio.load_data_to_datasource(
                name=device,
                data=batch_data,
                names_mapper={
                    "temperature": "Temperature (C)",
                    "conductivity": "Conductivity (µS | cm)",
                    "water_elevation": "Water Elevation (ft)",
                },
                units={
                    "temperature": "C",
                    "conductivity": "µS/cm",
                    "water_elevation": "ft",
                },
            )


if __name__ == "__main__":
    main()
