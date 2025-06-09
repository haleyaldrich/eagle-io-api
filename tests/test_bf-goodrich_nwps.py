from bf_goodrich import nwps


def test_get_gauge_data():
    data = nwps.get_gauge_data()
    assert type(data) is dict, "Data should be a dictionary"
    k = list(data.keys())[0]
    assert "water_elevation" in data[k], "Data should contain 'water_elevation' key"
    assert (
        type(data[k]["water_elevation"]) is float
    ), "Water elevation should be a float"
