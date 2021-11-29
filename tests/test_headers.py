import os
import pytest

from mojap_metadata.metadata.metadata import Metadata
from data_linter.validators.pandas_validator import PandasValidator


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
    In that order.
    Args:
        file_name ([str]): The filename in the dir tests/data/headers/
        expected_results ([Tuple(bool)]): expected results for the 3
        different config params listed above
    """
    test_folder = "tests/data/headers/"
    full_file_path = os.path.join(test_folder, file_name)

    table_name = file_name.split(".")[0].split("_")[0]
    metadata = Metadata.from_json(
        os.path.join(test_folder, f"meta_data/{table_name}.json")
    )

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


@pytest.mark.parametrize("uppercase_meta", [True, False])
@pytest.mark.parametrize("uppercase_data", [True, False])
@pytest.mark.parametrize("headers_ignore_case", [True, False])
def test_header_case_with_meta(
    uppercase_meta: bool, uppercase_data: bool, headers_ignore_case: bool
):
    """
    Tests whether the correct result is given using headers_ignore_case and either the
    data or metadata has captilalised column names. The result is the same as:
        (uppercase_data == uppercase_meta) OR headers_ignore_case
    i.e. they're both the same case or headers_ignore_case is True.
    """

    test_folder = "tests/data/headers/"
    full_file_path = os.path.join(test_folder, "table1.csv")

    # get the meta and set the correct case for the col names
    metadata = Metadata.from_json(os.path.join(test_folder, "meta_data/table1.json"))
    if uppercase_meta:
        for c in metadata.columns:
            c["name"] = c["name"].upper()

    # get the data an set the correct case for the columns
    if uppercase_data:
        full_file_path = os.path.join(test_folder, "table1_uppercase.csv")
    else:
        full_file_path = os.path.join(test_folder, "table1.csv")

    # get the expected result
    expected_result = (uppercase_data == uppercase_meta) or headers_ignore_case

    # get the validator and validate
    table_params = {"headers-ignore-case": headers_ignore_case}
    pv = PandasValidator(full_file_path, table_params, metadata)
    pv.read_data_and_validate()

    # assert the result is as expected
    assert expected_result == pv.response.result["valid"]
