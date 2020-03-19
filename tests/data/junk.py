import yaml
import json
import os
from jsonschema import validate as json_validate

with open("tests/data/example_config.yaml", "r") as y:
    config = yaml.safe_load(y)

json.dumps(config)

with open("config-schema.json") as f:
    config_schema = json.load(f)
config_path = os.path.join(".", "config.yaml")
config_path
json_validate(config, config_schema)
def load_and_validate_config(file_name="config.yaml", path="."):
    """
    Loads and validates the config
    """
    # load yaml or json
    config_path = os.path.join()
    if os.path.isfile("config.yaml"):
        ext = "yaml"
    elif os.path.isfile("config.yml"):
        ext = "yml"
    else:
        raise FileNotFoundError(
            "Expecting a file with the name config.json, config.yaml config.yml) in working dir."
        )

    with open(f"config.{ext}", "r") as f:
        config = yaml.safe_load(f)

    json_validate(config, config_schema)

    return config
