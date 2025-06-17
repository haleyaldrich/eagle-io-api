from datetime import datetime
import os
import requests


def get_gauge_data():
    """
    Retrieves water elevation data from the NOAA NWPS API for the gauge KYTK2.
    The data is filtered to exclude points with negative primary values, which
    represent invalid or non-existent water levels.

    Output format is a dictionary with timestamps as keys and water elevation
    values as nested dictionaries:

    {
        "2023-10-01T00:00:00Z": {
            "water_elevation": 123.45
        },
        "2023-10-01T01:00:00Z": {
            "water_elevation": 124.56
        },
        ...
    }

    """

    url = "https://api.water.noaa.gov/nwps/v1/gauges/KYTK2/stageflow/observed"
    response = requests.get(url)
    data = response.json()["data"]
    data = {}
    for point in response.json()["data"]:

        if point["primary"] < 0:
            continue

        data[point["validTime"]] = {
            "water_elevation": point["primary"],
        }

    return data


def get_manual_data():
    """
    Retrieves manual water elevation data and transforms it into a dictionary
    with timestamps as keys and water elevation values as nested dictionaries:

    {
        "2023-10-01T00:00:00Z": {
            "water_elevation": 123.45
        },
        "2023-10-01T01:00:00Z": {
            "water_elevation": 124.56
        },
        ...
    }
    """
    p = os.path.join(
        os.path.dirname(__file__),
        "data",
        "river_elev.txt",
    )
    data = {}
    with open(p, "r") as f:
        lines = f.readlines()[1:]  # Skip the header line
        for line in lines:
            date, water_elevation, _ = line.strip().split(",")
            date = date + ".000Z"
            date = date.replace(" ", "T")
            data[date] = {"water_elevation": float(water_elevation)}

    return data
