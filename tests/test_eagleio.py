import json
import os

from eagleio import api

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

e = api.EagleIOWorkspace(os.environ["BF_GOODRICH_EAGLEIO_KEY"])


def test_get_nodes():
    """
    Test the get_nodes function to ensure it retrieves nodes from the Eagle.io API.
    """
    nodes = e.get_nodes()
    assert len(nodes) > 0, "No nodes were retrieved from the Eagle.io API."
    p = os.path.join(OUTPUT_DIR, "eagle_io_nodes.json")
    with open(p, "w") as f:
        json.dump(nodes, f, indent=4)


def test_get_node_by_id():
    node = e.get_node_by_id("682f4ffae391c2c7fb81abec")
    assert node["name"] == "LW-02"
    assert node["_class"] == "io.eagle.models.node.location.Location"
    p = os.path.join(OUTPUT_DIR, "eagle_io_node.json")
    with open(p, "w") as f:
        json.dump(node, f, indent=4)


def test_get_datasource_id_by_name():

    # Non existent datasource
    try:
        e.get_datasource_id_by_name("Nonexistent Datasource")
    except ValueError as exc:
        assert (
            str(exc) == "No datasource found with name: Nonexistent Datasource"
        ), "Unexpected error message for nonexistent datasource"
    else:
        raise AssertionError(
            "Expected ValueError for nonexistent datasource was not raised."
        )

    # Specific datasource
    ds = e.get_datasource_id_by_name("LW-02S")
    p = os.path.join(OUTPUT_DIR, "eagle_io_datasource.json")
    with open(p, "w") as f:
        json.dump(ds, f, indent=4)
    assert isinstance(ds, str), "Datasource should be a string"

    # Handle a non datasource name
    try:
        e.get_datasource_id_by_name("LW-02")
    except ValueError as exc:
        assert (
            str(exc) == "No datasource found with name: LW-02"
        ), "Unexpected error message for non-datasource name"
    else:
        raise AssertionError(
            "Expected ValueError for non-datasource name was not raised."
        )


def test_ts_object_data_to_jts():

    data = {
        "2025-02-05T17:00:00.000Z": {"f": 1000, "T": 16},
        "2025-02-05T18:00:00.000Z": {"f": 1500, "T": 17},
        "2025-02-05T19:00:00.000Z": {"T": 12, "f": 4400},  # check for order
    }

    data = e._ts_object_data_to_jts(
        data, {"f": "Frequency", "T": "Temperature"}, {"f": "Hz", "T": "C"}
    )

    assert list(data["header"]["columns"].keys()) == [0, 1]
    assert data["header"]["columns"][0] == {
        "name": "Frequency",
        "dataType": "NUMBER",
        "units": "Hz",
    }
    assert data["header"]["columns"][1] == {
        "name": "Temperature",
        "dataType": "NUMBER",
        "units": "C",
    }
    assert data["data"][0] == {
        "ts": "2025-02-05T17:00:00.000Z",
        "f": {0: {"v": 1000}, 1: {"v": 16}},
    }
    assert data["data"][1] == {
        "ts": "2025-02-05T18:00:00.000Z",
        "f": {0: {"v": 1500}, 1: {"v": 17}},
    }
    assert data["data"][2] == {
        "ts": "2025-02-05T19:00:00.000Z",
        "f": {0: {"v": 4400}, 1: {"v": 12}},
    }


def test_load_data_to_datasource():
    data = {
        "2025-02-05T17:00:00.000Z": {"f": 1000, "T": 1},
        "2025-02-05T18:00:00.000Z": {"f": 1500, "T": 2},
        "2025-02-05T19:00:00.000Z": {"f": 4400, "T": 3},
    }

    names_mapper = {"f": "Frequency", "T": "Temperature (C)"}
    units = {"f": "Hz", "T": "C"}

    e.load_data_to_datasource("LW-02S", data, names_mapper, units)
