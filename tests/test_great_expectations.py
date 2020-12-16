import os
import json
import pytest

from data_linter.validators.great_expectations_validator import (
    GreatExpectationsValidator,
)


@pytest.mark.parametrize(
    "file_name,expected_result", [("table1_na_test.csv", [False, True])]
)
def test_great_expectations(file_name, expected_result):
    test_folder = "tests/data/great_expectations/"
    full_file_path = os.path.join(test_folder, file_name)

    table_name = file_name.split(".")[0].split("_")[0]
    with open(os.path.join(test_folder, f"meta_data/{table_name}.json")) as f:
        metadata = json.load(f)

    table_params = [
        {},
        {"pandas-kwargs": {"keep_default_na": False, "na_values": [""]}},
    ]

    all_tests = []
    for table_param in table_params:
        validator = GreatExpectationsValidator(full_file_path, table_param, metadata)
        validator.read_data_and_validate()
        table_response = validator.response.get_result()
        all_tests.append(table_response["valid"])

    assert expected_result == all_tests
