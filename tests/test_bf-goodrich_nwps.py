import json
import os

from bf_goodrich import nwps

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

def test_get_gauge_data():
    data = nwps.get_gauge_data()
    assert type(data) is dict, "Data should be a dictionary"
    k = list(data.keys())[0]
    assert "water_elevation" in data[k], "Data should contain 'water_elevation' key"
    assert (
        type(data[k]["water_elevation"]) is float
    ), "Water elevation should be a float"
    assert k.endswith("Z"), "Timestamp should end with 'Z' indicating UTC time"
    p = os.path.join(OUTPUT_DIR, "nwps.json")
    with open(p, "w") as f:
        json.dump(data, f, indent=4)

def test_get_manual_data():
    data = nwps.get_manual_data()
    assert type(data) is dict, "Data should be a dictionary"
    k = list(data.keys())[0]
    assert "water_elevation" in data[k], "Data should contain 'water_elevation' key"
    assert (
        type(data[k]["water_elevation"]) is float
    ), "Water elevation should be a float"
    assert k.endswith("Z"), "Timestamp should end with 'Z' indicating UTC time"
