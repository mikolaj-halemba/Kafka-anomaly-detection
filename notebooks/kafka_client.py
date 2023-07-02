import json
import os

class KafkaClient:
    def _get_config(self, path: str) -> dict:
        """
        Load the configuration parameters from a JSON file.

        Args:
            path (str): The path to the JSON file.

        Returns:
            dict: The configuration parameters.
        """
        file_path =  os.path.abspath(path)
        with open(file_path, 'r') as f:
            config_json = json.load(f)
        return config_json
