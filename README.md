# eagle-io-api
This library provides a Python interface for interacting with the Eagle.io API. It allows users to authenticate with an API key, retrieve nodes and data sources, and upload time-series data in JSON Time Series (JTS) format.

The `EagleIOWorkspace` class includes:
- Methods to list and retrieve nodes from a workspace
- A utility to convert object-based time-series data into JTS format
- A method to load data into a specific Eagle.io datasource by name

This is useful for building ETL pipelines that automate the ingestion of IoT or sensor data into Eagle.io for visualization and analysis.

## BF-Goodrich ETL
This ETL pipeline integrates data from three distinct sources:
- Piezometer data from iTwin IoT
- Water elevation data from the National Water Prediction Service (NWPS) API
- Manually collected data from monitoring wells (Excel format)

### Piezometer ETL
- Retrieve raw sensor data from iTwin IoT
- Compute water elevation using sensor-specific calibration factors
- Upload both raw and computed values to the appropriate Eagle.io data sources

### NWPS ETL
- Fetch water elevation data via API from the nearest NWPS gauge
- Transform the data into Eagle.io's JSON Time Series (JTS) format
- Note: `river_elev.txt` is manually collected data for river elevation. The NWPS API does not support `start` or `end` date parameters and it returns data for approximately the last 30 days. This manual collected data goes back up to December 2024

### Manual Monitoring Wells ETL
- Convert manually collected Excel data into the JTS format required by Eagle.io
- Upload the processed data to the appropriate Eagle.io datasource
