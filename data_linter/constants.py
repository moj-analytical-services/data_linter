import json
import pkg_resources

config_schema = json.load(
    pkg_resources.resource_stream(__name__, "schemas/config-schema.json")
)
