import os
import json
import pytest

from data_linter.validators.pandas_validator import (
    PandasValidator,
)


@pytest.mark.parametrize(
    "file_name,expected_result",
    [
        ("table1.csv", [False, True, True]),
        ("table1_mixed_headers.csv", [False, False, True]),
        ("table1_no_header.csv", [True, False, False]),
        ("table2.jsonl", [True, True, True]),
        ("table2_missing_keys.jsonl", [False, False, False]),
        ("table2_mixed_headers.jsonl", [False, False, True]),
        ("table2_wrong_headers.jsonl", [False, False, False]),
    ],
)
def test_headers(file_name, expected_result):
    """
    Tests files against the _read_data_and_validate function.
    runs each file and corresponding meta (table1 or table2).
    Against the additional table config params:
    - expected-headers is False
    - expected-headers is True and ignore-case is False
    - expected-headers is True and ignore-case is True
    In that order
    Args:
        file_name ([str]): The filename in the dir tests/data/headers/
        expected_results ([Tuple(bool)]): expected results for the 3
        different config params listed above
    """
    test_folder = "tests/data/headers/"
    full_file_path = os.path.join(test_folder, file_name)

    table_name = file_name.split(".")[0].split("_")[0]
    with open(os.path.join(test_folder, f"meta_data/{table_name}.json")) as f:
        metadata = json.load(f)

    table_params = [
        {"expect-header": False},
        {"expect-header": True, "headers-ignore-case": False},
        {"expect-header": True, "headers-ignore-case": True},
    ]

    all_tests = []
    for table_param in table_params:
        validator = PandasValidator(full_file_path, table_param, metadata)
        validator.read_data_and_validate()
        table_response = validator.response
        all_tests.append(table_response.result["valid"])

    assert expected_result == all_tests
