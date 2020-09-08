import os
import json
import yaml
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
    "table2_mixed_headers": False,
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
        mocked_s3.meta.client.create_bucket(Bucket=b)

    files = [
        f for f in os.listdir(test_folder) if f.endswith(".csv") or f.endswith(".jsonl")
    ]
    for filename in files:
        full_path = os.path.join(test_folder, filename)
        mocked_s3.meta.client.upload_file(full_path, land_bucket, filename)


def test_headers(s3):
    test_folder = "tests/data/headers/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    set_up_s3(s3, test_folder, config)

    config = load_and_validate_config(config_path)
    config = match_files_in_land_to_config(config)

    all_responses = {}

    for table_name, table_params in config["tables"].items():
        table_params["lint-response"] = []
        if table_params["matched_files"]:
            msg1 = f"Linting {table_name}"
            print(msg1)

            meta_file_path = table_params.get(
                "metadata", f"meta_data/{table_name}.json"
            )

            with open(meta_file_path) as sfile:
                metadata = json.load(sfile)
                schema = convert_meta_to_goodtables_schema(metadata)

            for i, matched_file in enumerate(table_params["matched_files"]):

                response = _read_data_and_validate(
                    matched_file, schema, table_params, metadata
                )

                table_response = response["tables"][0]

                all_responses[table_name] = table_response["valid"]
    assert all_responses == expected_linting_result
