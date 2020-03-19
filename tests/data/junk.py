import yaml
import json
from jsonschema import validate as json_validate

with open("tests/data/example_config.yaml", "r") as y:
    config = yaml.safe_load(y)

json.dumps(config)

with open("config-schema.json") as f:
    config_schema = json.load(f)


json_validate(config, config_schema)

def load_and_validate_config():
    """
    Loads and validates the config
    """
    # load yaml or json
    if os.path.isfile("config.yaml"):
        ext = "yaml"
    elif os.path.isfile("config.yml"):
        ext = "yml"
    elif os.path.isfile("config.json"):
        ext = "json"
    else:
        raise FileNotFoundError(
            "Expecting a file with the name config.json or config.yaml in working dir."
        )

    with open(f"config.{ext}", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    with open("config-schema.json") as f:
        config_schema = json.load(f)

    json_validate(config, config_schema)

    return config
