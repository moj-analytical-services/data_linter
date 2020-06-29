import pytest
import os
import yaml

from dataengineeringutils3.s3 import s3_path_to_bucket_key

def set_up_s3(mocked_s3, test_folder, config):

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

    files = [f for f in os.listdir(test_folder) if f.endswith(".csv") or f.endswith(".jsonl")]
    for filename in files:
        full_path = os.path.join(test_folder, filename)
        mocked_s3.meta.client.upload_file(full_path, land_bucket, filename)


def test_end_to_end(s3):

    import data_linter

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    set_up_s3(s3, test_folder, config)
    land_files = [o.key for o in s3.Bucket("land").objects.all()]
    # assert land_files == ["table1.csv", "table2.csv"] # Testing setup

    data_linter.run_validation(config_path)
