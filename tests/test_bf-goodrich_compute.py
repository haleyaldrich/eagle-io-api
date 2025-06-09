import json
import logging
import numpy as np
import os
import pandas as pd

from bf_goodrich import compute

logger = logging.getLogger(__name__)

TEST_DIR = os.path.join(os.path.dirname(__file__), "test_files")
DEVICES = json.load(
    open(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "bf_goodrich", "devices.json")
        ),
        "r",
    )
)


def get_sensor(name: str) -> dict:
    """Get the sensor information from devices.json."""
    if name not in DEVICES:
        raise Exception(f"Sensor {name} not found in devices.json")
    return DEVICES[name]


def get_all_sensors_names() -> dict:
    """Get all sensor names from devices.json."""
    return list(DEVICES.keys())


def test_simple_get_pressure_psi():
    # First row for LW-02S
    kwargs = {
        "frequency": 7950.10,
        "temperature": 17.6,
        "r0": 8964.40,
        "t0": 3.6,
        "poly_a": -2.49100e-08,
        "poly_b": -0.01306,
        "k": -0.002295,
    }

    p = compute.get_pressure_psi(**kwargs)
    expected = 13.6420
    assert abs(p - expected) < 0.001, f"Expected {expected}, got {p}"


def test_get_pressure_head():
    # First row for LW-02S
    kwargs = {
        "frequency": 7950.10,
        "temperature": 17.6,
        "r0": 8964.40,
        "t0": 3.6,
        "poly_a": -2.49100e-08,
        "poly_b": -0.01306,
        "k": -0.002295,
    }

    p = compute.get_pressure_head(**kwargs)
    expected = 31.481523
    assert abs(p - expected) < 0.0001, f"Expected {expected}, got {p}"


def test_get_pressure_psi_with_arrays():
    """
    Computes the pressure in psi for all sensors and compares the results
    with manual calculations in readings.txt.
    """
    path = os.path.join(TEST_DIR, "readings.txt")
    df = pd.read_csv(path, sep="\t", header=0)

    sensors = get_all_sensors_names()
    for sensor in sensors:
        logger.info(f"Calculating pressure for sensor: {sensor}")
        sensor_info = get_sensor(sensor)
        df_i = df[df["sensor"] == sensor]

        expected = df_i["pressure"].to_numpy()
        pressure = compute.get_pressure_psi(
            frequency=df_i["frequency"].values,
            temperature=df_i["temperature"].values,
            r0=sensor_info["r0"],
            t0=sensor_info["t0"],
            poly_a=sensor_info["poly_a"],
            poly_b=sensor_info["poly_b"],
            k=sensor_info["k"],
        )

        diff = np.isclose(pressure, expected)
        not_matching_indices = np.where(~diff)[0]
        if len(not_matching_indices) > 0:
            logger.error(f"Indices where values do not match: {not_matching_indices}")
            logger.error(
                f"Expected: {expected[not_matching_indices]}, got: {pressure[not_matching_indices]}"
            )
            raise AssertionError(f"Values do not match for sensor {sensor}")


def test_compute_piezo_elevation():

    timestamps = [
        "2025-02-05T17:00:00.000Z",
        "2025-02-05T18:00:00.000Z",
        "2025-02-05T19:00:00.000Z",
    ]
    frequencies = [7711, 7712, 7713]
    temperatures = [17, 18, 19]

    result = compute.compute_piezo_elevation(
        timestamps, frequencies, temperatures, get_sensor("LW-02S")
    )

    assert isinstance(result, dict), "Result should be a dictionary"
    assert isinstance(result["2025-02-05T17:00:00.000Z"], dict), "Result entry should be a dictionary"
    assert "water_elevation" in result["2025-02-05T17:00:00.000Z"], "Water elevation should be present"
