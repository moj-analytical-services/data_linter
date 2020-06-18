from data_linter.validation import load_and_validate_config
from jsonschema.exceptions import ValidationError
import json


def test_load_and_validate_config():
    test_fail_bucket_expected = "pattern"
    try:
        _ = load_and_validate_config(
            path="tests/data", file_name="example_config_fail_bucket.yaml"
        )
    except ValidationError as e:
        assert e.validator == test_fail_bucket_expected

    test_fail_table_expected = "required"
    try:
        _ = load_and_validate_config(
            path="tests/data", file_name="example_config_fail_table.yaml"
        )
    except ValidationError as e:
        assert e.validator == test_fail_table_expected

    with open("tests/data/expected_pass.json", "r") as f:
        expected_pass = json.load(f)
    c = load_and_validate_config(
        path="tests/data", file_name="example_config_pass.yaml"
    )
    assert c == expected_pass
