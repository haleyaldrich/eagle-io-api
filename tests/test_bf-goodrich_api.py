import json
import os

from bf_goodrich import itwin

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def test_get_token():
    """
    Test the get_token function to ensure it retrieves a valid access token.
    """
    token = itwin.get_token()
    assert isinstance(token, str)
    assert len(token) > 0


def test_get_all_nodes():
    """
    Test the get_all_nodes function to ensure it retrieves nodes from the iTwin platform.
    """
    nodes = itwin.get_all_nodes()
    assert isinstance(nodes, dict)
    assert "nodes" in nodes
    assert len(nodes["nodes"]) == 3


def test_query_node_by_dates():
    sensor = "/loadsensing/27990/node/dynamic/86313/device/vw1/sensor"
    start_date = "2025-01-01T00:00:00.000Z"
    end_date = "2025-05-10T00:00:00.000Z"
    data = itwin.query_node_by_dates(sensor, start_date, end_date)
    assert isinstance(data, dict)
    p = os.path.join(OUTPUT_DIR, "vw1_sensor.json")
    with open(p, "w") as f:
        json.dump(data, f, indent=4)
