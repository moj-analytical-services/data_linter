from python_scripts.logging_functions import logging_setup, write_to_log, upload_logs
import pytest
import json
from python_scripts.functions import load_and_validate_config


def test_logging():
    log = logging_setup()
    with open("tests/data/expected_pass.json", "r") as f:
        expected_pass = json.load(f)
    c = load_and_validate_config(
        path="tests/data", file_name="example_config_pass.yaml"
    )
    assert c == expected_pass

    upload_logs(body=log["log_stringio"].getvalue(),
        bucket="log-bucket",
        key=log["log_name_timestamped"],
    )