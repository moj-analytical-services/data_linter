import yaml
import json
import os
from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError

with open("tests/data/example_config_pass.yaml", "r") as y:
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
    config_path = os.path.join(path, file_name)
    if not os.path.isfile(config_path):
        config_path = config_path.replace("yaml", "yml")
        if not os.path.isfile(config_path):
            raise FileNotFoundError(
                f"Expecting a file in {path} with name {file_name}."
            )

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    json_validate(config, config_schema)

    return config

load_and_validate_config(path="tests/data", file_name="example_config_fail_table.yaml")