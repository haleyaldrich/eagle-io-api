"""
This module provides functionality to process the raw sensor data from the
piezometers.
"""

import pandas as pd


def get_pressure_psi(
    frequency: float,
    temperature: float,
    r0: float,
    t0: float,
    poly_a: float,
    poly_b: float,
    k: float,
) -> float:
    """
    Calculate the presure in psi from piezometer readings.

    Args:
        frequency (float): The vibrating wire frequency in digits.
        temperature (float): The temperature in degrees C.
        r0 (float): The initial field zero vibrating wire reading in digits.
        t0 (float): The initial field zero temperature reading in degrees C.
        poly_a (float): The Geokon polynomial correction coefficient for psi.
        poly_b (float): The Geokon polynomial correction coefficient for psi.
        k (float): The thermal factor (psi/deg C).

    Formula:
        P = A * R1^2 + B * R1 + C + K * (T - T0)

    Returns:
        float: The calculated head in psi.
    """
    # Calculate C by setting P=0 and R1=initial field zero reading into the polynomial equation
    poly_c = -poly_a * r0**2 - poly_b * r0
    t = (temperature - t0) * k
    p = poly_a * frequency**2 + poly_b * frequency + poly_c + t
    return p


def get_pressure_head(
    frequency: float,
    temperature: float,
    r0: float,
    t0: float,
    poly_a: float,
    poly_b: float,
    k: float,
) -> float:
    """
    Calculate the head in feet of water from piezometer readings.

    Args:
        frequency (float): The vibrating wire frequency in digits.
        temperature (float): The temperature in degrees C.
        r0 (float): The initial field zero vibrating wire reading in digits.
        t0 (float): The initial field zero temperature reading in degrees C.
        poly_a (float): The Geokon polynomial correction coefficient for psi.
        poly_b (float): The Geokon polynomial correction coefficient for psi.
        k (float): The thermal factor (psi/deg C).

    Returns:
        float: The calculated head in feet.
    """
    p = get_pressure_psi(frequency, temperature, r0, t0, poly_a, poly_b, k)
    head = p * 144 / 62.4  # Convert psi to feet of water
    return head


def compute_piezo_elevation(
    timestamps: list[str],
    frequency: list[float],
    temperature: list[float],
    sensor_info: dict,
) -> dict:
    """
    Calculate the water elevation for a piezometer based on frequency and
    temperature readings.

    Args:
        df (pd.DataFrame): The DataFrame containing frequency and temperature data.
        sensor_info (dict): The sensor information dictionary.

    Returns:
        dict: A dictionary with timestamps as keys and calculated water elevation
              {
                  "timestamp_1": {
                      "p_psi": value_1,
                      "p_ft_head": value_2,
                      "water_elevation": value_3,
                  },
                  "timestamp_2": {
                      "p_psi": value_1,
                      "p_ft_head": value_2,
                      "water_elevation": value_3,
                  },
                  ...
              }
            }
    """
    assert "r0" in sensor_info, "Sensor info must contain 'r0'"
    assert "t0" in sensor_info, "Sensor info must contain 't0'"
    assert "poly_a" in sensor_info, "Sensor info must contain 'poly_a'"
    assert "poly_b" in sensor_info, "Sensor info must contain 'poly_b'"
    assert "k" in sensor_info, "Sensor info must contain 'k'"
    assert "ground_elev" in sensor_info, "Sensor info must contain 'ground_elev'"
    assert "sensor_depth" in sensor_info, "Sensor info must contain 'sensor_depth'"

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "frequency": frequency,
            "temperature": temperature,
        }
    )

    df["pressure_psi"] = get_pressure_psi(
        df["frequency"],
        df["temperature"],
        sensor_info["r0"],
        sensor_info["t0"],
        sensor_info["poly_a"],
        sensor_info["poly_b"],
        sensor_info["k"],
    )

    df["ft_water"] = get_pressure_head(
        df["frequency"],
        df["temperature"],
        sensor_info["r0"],
        sensor_info["t0"],
        sensor_info["poly_a"],
        sensor_info["poly_b"],
        sensor_info["k"],
    )

    sensor_elevation = sensor_info["ground_elev"] - sensor_info["sensor_depth"]
    df["water_elevation"] = df["ft_water"] + sensor_elevation

    df.set_index("timestamp", inplace=True)
    return df[["water_elevation"]].to_dict(orient="index")
