import os
import json
import yaml
from pprint import pprint
import pytest

from data_linter.validation import (
    _read_data_and_validate,
    match_files_in_land_to_config,
    load_and_validate_config,
    convert_meta_to_goodtables_schema,
)

expected_linting_result = {
    "table1_mixed_headers": True,
    "table1_strict_headers": False,
    "table1_no_header": True,
    "table2_missing_keys": True,
    "table2_wrong_headers": False,
    "table2_mixed_headers": True,
}


# method stolen from end_to_end1
def set_up_s3(mocked_s3, test_folder, config):
    from dataengineeringutils3.s3 import s3_path_to_bucket_key

    land_bucket, _ = s3_path_to_bucket_key(config.get("land-base-path", "s3://land/"))
    fail_bucket, _ = s3_path_to_bucket_key(config.get("fail-base-path", "s3://fail/"))
    pass_bucket, _ = s3_path_to_bucket_key(config.get("pass-base-path", "s3://pass/"))
    log_bucket, _ = s3_path_to_bucket_key(config.get("log-base-path", "s3://log/"))

    buckets = [
        land_bucket,
        fail_bucket,
        pass_bucket,
        log_bucket,
    ]
    for b in buckets:
        mocked_s3.meta.client.create_bucket(
            Bucket=b, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

    files = [
        f for f in os.listdir(test_folder) if f.endswith(".csv") or f.endswith(".jsonl")
    ]
    for filename in files:
        full_path = os.path.join(test_folder, filename)
        mocked_s3.meta.client.upload_file(full_path, land_bucket, filename)

@pytest.mark.parametrize(
    "file_name,expected_result",
    [
        ("table1.csv", [True, True, True]),
        ("table1_mixed_headers.csv", [True, False, True]),
        ("table1_no_header.csv", [True, False, False]),
        ("table2.jsonl", [True, True, True]),
        ("table2_missing_keys.jsonl", [True, True, True]),
        ("table2_mixed_headers.jsonl", [False, False, False]),
        ("table2_wrong_headers.jsonl", [False, False, False])
    ]
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
        expected_results ([Tuple(bool)]): expected results for the 3 different config params listed above
    """
    test_folder = "tests/data/headers/"
    full_file_path = os.path.join(test_folder, file_name)

    table_name = file_name.split(".")[0].split("_")[0]
    with open(os.path.join(test_folder, f"meta_data/{table_name}.json")) as f:
        metadata = json.load(f)
    
    schema = convert_meta_to_goodtables_schema(metadata)


    table_params = [
        {"expect-header": False},
        {"expect-header": True, "headers-ignore-case": False},
        {"expect-header": True, "headers-ignore-case": True},
    ]

    all_tests = []
    for table_param in table_params:
        response = _read_data_and_validate(
            full_file_path, schema, table_param, metadata
        )
        table_response = response["tables"][0]
        all_tests.append(table_response["valid"])

    assert expected_result == all_tests
