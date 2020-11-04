import os
import yaml
import gzip
import tempfile

import pytest


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
            Bucket=b,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

    files = [
        f for f in os.listdir(test_folder) if f.endswith(".csv") or f.endswith(".jsonl")
    ]
    for filename in files:
        full_path = os.path.join(test_folder, filename)
        mocked_s3.meta.client.upload_file(full_path, land_bucket, filename)


def test_end_to_end(s3):

    from data_linter.validation import run_validation

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    set_up_s3(s3, test_folder, config)
    run_validation(config_path)
    os.system(f"python data_linter/command_line.py --config-path {config_path}")


@pytest.mark.parametrize("validator", ["frictionless", "great-expectations"])
def test_end_to_end_ge(s3, validator):

    from data_linter.validation import run_validation

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    config["validator-engine"] = validator
    set_up_s3(s3, test_folder, config)
    run_validation(config)


def test_end_to_end_no_creds_error():

    from data_linter.validation import run_validation
    from botocore.exceptions import ClientError

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")

    with pytest.raises(ClientError):
        run_validation(config_path)


def test_compression(s3):
    from data_linter.utils import compress_data

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    set_up_s3(s3, test_folder, config)
    test_file_uncompressed = "table2.jsonl"
    test_file_compressed = "table2.jsonl.gz"
    uncompressed_location = os.path.join(
        config["land-base-path"], test_file_uncompressed
    )
    compressed_location = os.path.join(config["pass-base-path"], test_file_compressed)

    compress_data(uncompressed_location, compressed_location)
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, test_file_compressed), "wb") as file1:
            s3.meta.client.download_fileobj("pass", test_file_compressed, file1)
        with gzip.GzipFile(
            os.path.join(d, test_file_compressed), "r"
        ) as compressed_json:
            json_bytes = compressed_json.read()

    compressed_json_str = json_bytes.decode("utf-8")

    with open(os.path.join(test_folder, test_file_uncompressed)) as uncompressed_json:
        assert (
            compressed_json_str == uncompressed_json.read()
        ), "uncompressed json doesn't contain the same data as compressed json"
