import os
import json
import pytest

from data_linter.validators.great_expectations_validator import (
    GreatExpectationsValidator,
)


@pytest.mark.parametrize(
    "file_name,table_param,expected_result",
    [
        ("table1_na_test.csv", {}, False),
        ("table1_na_test.csv", {"pandas-kwargs": {"keep_default_na": False, "na_values": [""]}}, True),
        ("table1_na_test.csv", {"pandas-kwargs": {"keep_default_na": False, "na_values": {"animal": [""]}}}, False)
    ],
    ids=["default_pd_na", "pd_new_na_list", "animal_specific_pd_new_na_list"]
)
def test_great_expectations(file_name, table_param, expected_result):
    test_folder = "tests/data/great_expectations/"
    full_file_path = os.path.join(test_folder, file_name)

    table_name = file_name.split(".")[0].split("_")[0]
    with open(os.path.join(test_folder, f"meta_data/{table_name}.json")) as f:
        metadata = json.load(f)

    validator = GreatExpectationsValidator(full_file_path, table_param, metadata)
    validator.read_data_and_validate()
    table_response = validator.response.get_result()
    assert table_response["valid"] == expected_result
