import os

import awswrangler as wr
import boto3
import pytest
from mojap_metadata import Metadata
from moto import mock_s3
from pyarrow.parquet import read_schema

import data_linter.validators.parquet_validator as pqv
from tests.helpers import mock_get_file

bucket = "dummy-bucket"


@mock_s3
@pytest.mark.parametrize(
    "filepath, mock_s3",
    [
        ("tests/data/parquet_validator/table1.parquet", True),
        ("tests/data/parquet_validator/table1.parquet", False),
    ],
)
def test_parquet_schema_reader(filepath, mock_s3, monkeypatch):
    if mock_s3:

        s3_client = boto3.client("s3")
        _ = s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

        full_path = f"s3://{bucket}/{filepath}"

        wr.s3.upload(filepath, full_path)

    else:
        full_path = filepath

    _ = monkeypatch.setattr(pqv, "S3FileSystem", mock_get_file)

    schema = pqv.ParquetValidator._read_schema(full_path)

    expected_schema = read_schema(filepath).remove_metadata()

    assert expected_schema == schema


@pytest.mark.parametrize(
    "meta_file,expected_pass",
    [
        ("table1_pass.json", True),
        ("table1_fail.json", False),
        ("table1_missing_columns.json", False),
    ],
)
def test_parquet_validator(meta_file, expected_pass):
    meta = Metadata.from_json(
        os.path.join("tests/data/parquet_validator/meta_data/", meta_file)
    )
    file_path = "tests/data/parquet_validator/table1.parquet"
    pv = pqv.ParquetValidator(filepath=file_path, table_params={}, metadata=meta)
    pv.read_data_and_validate()
    assert pv.response.result["valid"] == expected_pass
