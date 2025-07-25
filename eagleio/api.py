from datetime import datetime, timedelta
from dateutil import parser
import requests


class EagleIOWorkspace:
    """Represents a workspace in the Eagle.io API."""

    def __init__(self, api_key):
        """
        Initializes the EagleIOWorkspace with the provided API key for that
        workspace.
        """
        self.api_key = api_key
        self._base_url = "https://api.eagle.io/api/v1"
        self.headers = {"X-Api-Key": self.api_key}
        self._nodes = self.get_nodes()

    def get_nodes(self) -> dict:
        """
        Fetch all nodes and a reduced set of attributes from the Eagle.io API.
        """
        url = f"{self._base_url}/nodes/"
        params = {
            "attr": "_id,_class,name,workspaceId,parentId",
        }

        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_node_by_id(self, node_id: str) -> dict:
        """
        Fetch a specific node by ID from the Eagle.io API.
        """
        url = f"{self._base_url}/nodes/{node_id}"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_datasource_id_by_name(self, name: str) -> dict:
        """
        Fetch a specific datasource by name from the Eagle.io API.
        Raises ValueError if no datasource is found or if multiple datasources
        are found with the same name.

        .. note::
            This function assumes that the datasource name is unique within the
            workspace.

            To retrieve a datasource we pass a filter to the API that
            matches the class to `io.eagle.models.node.source.data`. We could
            be more specific by using the class `io.eagle.models.node.source.data.Jts`,
            but this would not work for other types of datasources that might
            be added in the future.
        """
        url = f"{self._base_url}/nodes/"
        params = {
            "filter": f"name($eq:{name}),_class($match:io.eagle.models.node.source.data)"
        }
        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code == 200:
            r = response.json()
            if len(r) == 0:
                raise ValueError(f"No datasource found with name: {name}")
            elif len(r) > 1:
                ids = [item["_id"] for item in r]
                raise ValueError(
                    f"Multiple datasources found with name: {name}. IDs: {ids}"
                )
            return r[0]["_id"]
        else:
            response.raise_for_status()

    @staticmethod
    def _ts_object_data_to_jts(data: dict, names_mapper: dict, units: dict) -> dict:
        """
        Converts object-based timeseries data to JSON Time Series (JTS) format.

        .. note::
            Only handles numeric data types.

        Input data format:

            {
                "timestampt_1": {
                    "param_1": value_1,
                    "param_2": value_2,
                },
                "timestamp_2": {
                    "param_1": value_1,
                    "param_2": value_2,
                },
                ...
            }

        Args:
            data (list[dict]): The timeseries data to be converted.
            names_mapper (dict): Mapping of column names to storage names.
            units (dict): Mapping of column names to units.

        .. example::
            data = {
                "2025-02-05T17:00:00.000Z": {"f": 1000, "T": 16},
                "2025-02-05T18:00:00.000Z": {"f": 1500, "T": 17},
                "2025-02-05T19:00:00.000Z": {"T": 12, "f": 4400}, # check for order
            }
            jts_data = EagleIOWorkspace._ts_object_data_to_jts(
                data, {"f": "Frequency", "T": "Temperature"}, {"f": "Hz", "T": "C"}
            )
        """
        assert isinstance(data, dict), "Data must be a dictionary"
        assert isinstance(names_mapper, dict), "names_mapper must be a dictionary"
        assert isinstance(units, dict), "units must be a dictionary"

        first_key = list(data.keys())[0]
        attrs = list(data[first_key].keys())  # get attributes from the first entry
        assert isinstance(
            data[first_key], dict
        ), "Data must be a dictionary of dictionaries"

        # Header columns
        columns = {}
        for i, k in enumerate(attrs):
            try:
                n = names_mapper[k]
            except KeyError:
                raise KeyError(
                    f"Column name '{k}' not found in names_mapper. Available keys: {list(names_mapper.keys())}"
                )

            try:
                u = units[k]
            except KeyError:
                raise KeyError(
                    f"Column name '{k}' not found in units. Available keys: {list(units.keys())}"
                )

            columns[i] = {"name": n, "dataType": "NUMBER", "units": u}

        # Data manipulation - Regardless of the order of attributes, we need to
        # ensure that the data is in the correct format for JTS.
        timeseries = []
        for timestamp in data:
            row = {}
            for i, a in enumerate(attrs):
                row[i] = {"v": data[timestamp][a]}

            timeseries.append({"ts": timestamp, "f": row})

        jts_template = {
            "docType": "jts",
            "version": "1.0",
            "header": {"columns": columns},
            "data": timeseries,
        }
        return jts_template

    def load_data_to_datasource(
        self, name: str, data: dict, names_mapper: dict, units: dict
    ) -> None:
        """
        Loads numeric data to a specific datasource in the Eagle.io API.

        The Eagle.io API expects the data to be in JSON Time Series (JTS) format.

        Args:
            name (str): The datasource name to which the data will be loaded.
            data (dict): The data to be loaded into the datasource. Data must be
                time-series data structured as a nested JSON object. See example
                below.
            names_mapper (dict): Mapping of column names to storage names.
            units (dict): Mapping of column names to units.

        .. example::
            data = {
                "2025-02-05T17:00:00.000Z": {"f": 1000, "T": 16},
                "2025-02-05T18:00:00.000Z": {"f": 1500, "T": 17},
                "2025-02-05T19:00:00.000Z": {"f": 1800, "T": 18},
            }
            names_mapper = {"f": "Frequency", "T": "Temperature"} # Names to be used in Eagle.io
            units = {"f": "digits", "T": "C"} # Units to be used in Eagle.io

        Raises:
            ValueError: If the datasource is not found or if the API request fails.
        """
        datasource_id = self.get_datasource_id_by_name(name)
        jts = self._ts_object_data_to_jts(data, names_mapper, units)
        url = f"{self._base_url}/nodes/{datasource_id}/historic"
        response = requests.put(url, headers=self.headers, json=jts)

        if response.status_code != 202:
            raise ValueError(f"Failed to load data to datasource: {response.text}")

    def get_latest_timestamp_from_datasource_by_name(self, name: str) -> str:
        """
        Retrieves the latest timestamp(s) from all parameters of a datasource identified by its name.

        Note:
            - A datasource is a node with the class 'io.eagle.models.node.source.data.Jts'.
            - Each parameter is a child node of the datasource.
            - The method first retrieves the datasource ID by name, then finds all child nodes
              associated with that datasource ID.
        """
        end_date = datetime.now() + timedelta(days=1)
        end_date = end_date.strftime("%Y-%m-%d") + "T00:00:00.000Z"
        datasource_id = self.get_datasource_id_by_name(name)

        children_ids = []
        for node in self._nodes:
            if "parentId" in node and node["parentId"] == datasource_id:
                children_ids.append(node["_id"])

        if not children_ids:
            raise ValueError(f"No child nodes found for datasource: {name}")

        latest_dates = []
        for child_id in children_ids:
            url = f"{self._base_url}/nodes/{child_id}/historic"
            params = {"limit": "25", "endTime": end_date}
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                raise ValueError(f"Failed to query datasource by name: {response.text}")

            dates = [d["ts"] for d in response.json()["data"]]
            dates = [parser.parse(d) for d in dates]
            latest_dates.append(max(dates))

        return (
            min(latest_dates)
            .strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            .replace("000000Z", "000Z")
        )
