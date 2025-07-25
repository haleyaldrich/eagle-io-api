# eagle-io-api
This library provides a Python interface for interacting with the Eagle.io API. It allows users to authenticate with an API key, retrieve nodes and data sources, and upload time-series data in JSON Time Series (JTS) format.

The `EagleIOWorkspace` class includes methods to:
- List and retrieve nodes from a workspace
- Convert object-based time-series data into JTS format
- Upload data to a specific Eagle.io datasource by name
- Retrieve the latest timestamp from a datasource

This library is designed to support ETL pipelines that automate the ingestion of IoT or sensor data into Eagle.io for visualization and analysis.

## BF-Goodrich ETL
This ETL pipeline uses the `eagle-io-api` library to automate the ingestion of water elevation data from various sources into Eagle.io.

This ETL pipeline integrates data from three distinct sources:
- Piezometer data from iTwin IoT
    - See `bf_goodrich/itwin.py` for the iTwin IoT client implementation
    - Sensors `"LW-02S", "LW-02D", "LW-05S", "LW-05D", "LW-07S", "LW-07D",  "LW-09S", "LW-09D", "LW-15S", "LW-15D", "LW-17S", "LW-17D", "LW-19S", "LW-19D"`
- Water elevation data from the National Water Prediction Service (NWPS) API
    - See `bf_goodrich/nwps.py` for the NWPS client implementation
- Manually collected data from monitoring wells (Excel format)
    - Data processing is handled in `bf_goodrich/etl.py`
    - `"LW-04", "LW-08", "LW-10", "LW-14", "LW-18", "LW-20", "Stilling Well"`
    - [Transducer Data Master Spreadsheet](https://haleyaldrich.sharepoint.com/:x:/r/sites/BFGoodrichSuperfundSiteRPs/Shared%20Documents/0210817.BF%20Goodrich%20RC06/_RC06_Barge%20Slip/Field/Field%20Investigation%20Tracking/Transducer%20Data%20-%20Monitoring/1_Transducer%20Data%20Master%20Spreadsheet.xlsx?d=wb485947e1e0f43178d1017f55b61ed01&csf=1&web=1&e=DWExkh)

### 🛑Implementation Assumption
The primary design assumption is a 1:1 mapping between Eagle.io datasources and the data sources in the ETL pipeline. Each datasource in Eagle.io corresponds directly to a single data source in the pipeline, with names that must match exactly.

The Eagle.io API requires a nodeId to upload data. However, since nodeIds are not meaningful in the context of this ETL pipeline, the system maintains an internal mapping between data source names and their corresponding Eagle.io nodeIds. This allows the pipeline to operate using descriptive names instead of raw identifiers.

### Batch Processing
The ETL pipeline is designed to process data in batches. It retrieves the latest timestamp from each datasource and only processes data that is newer than this timestamp. This ensures that the pipeline does not reprocess existing data, optimizing performance and reducing unnecessary API calls.

### Environment Setup
```
BF_GOODRICH_EAGLEIO_KEY=
ITWIN_IOT_CLIENT_ID=
ITWIN_IOT_CLIENT_SECRET=
ITWIN_IOT_ASSET_ID=
```

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