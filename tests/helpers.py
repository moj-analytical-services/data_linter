import io
import os
import pathlib
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import boto3
from dataengineeringutils3.s3 import s3_path_to_bucket_key


def set_up_s3(mocked_s3, test_folder, config, ext_filter=None):
    """
    Used to setup mocked s3 before a run that expects data in S3
    """
    if ext_filter is None:
        ext_filter = (".csv", ".jsonl", ".parquet")
    from dataengineeringutils3.s3 import s3_path_to_bucket_key

    land_base_path = config.get("land-base-path", "s3://land/")
    fail_base_path = config.get("fail-base-path", "s3://fail/")
    pass_base_path = config.get("pass-base-path", "s3://pass/")
    log_base_path = config.get("log-base-path", "s3://log/")

    land_base_path_is_s3 = land_base_path.startswith("s3://")
    fail_base_path_is_s3 = fail_base_path.startswith("s3://")
    pass_base_path_is_s3 = pass_base_path.startswith("s3://")
    log_base_path_is_s3 = log_base_path.startswith("s3://")

    buckets = []

    if land_base_path_is_s3:
        land_bucket, _ = s3_path_to_bucket_key(land_base_path)
        buckets.append(land_bucket)
    if fail_base_path_is_s3:
        fail_bucket, _ = s3_path_to_bucket_key(fail_base_path)
        buckets.append(fail_bucket)
    if pass_base_path_is_s3:
        pass_bucket, _ = s3_path_to_bucket_key(pass_base_path)
        buckets.append(pass_bucket)
    if log_base_path_is_s3:
        log_bucket, _ = s3_path_to_bucket_key(log_base_path)
        buckets.append(log_bucket)

    for b in buckets:
        mocked_s3.meta.client.create_bucket(
            Bucket=b,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

    files = [f for f in os.listdir(test_folder)]

    if ext_filter:
        files = [f for f in files if f.endswith(ext_filter)]

    if land_base_path_is_s3:
        for filename in files:
            full_path = os.path.join(test_folder, filename)
            mocked_s3.meta.client.upload_file(full_path, land_bucket, filename)


class MockS3FilesystemReadInputStream:
    @staticmethod
    @contextmanager
    def open_input_stream(s3_file_path_in: str) -> io.BytesIO:
        s3_resource = boto3.resource("s3")
        bucket, key = s3_path_to_bucket_key(s3_file_path_in)
        obj_bytes = s3_resource.Object(bucket, key).get()["Body"].read()
        obj_io_bytes = io.BytesIO(obj_bytes)
        try:
            yield obj_io_bytes
        finally:
            obj_io_bytes.close()

    @staticmethod
    @contextmanager
    def open_input_file(s3_file_path_in: str):
        s3_client = boto3.client("s3")
        bucket, key = s3_path_to_bucket_key(s3_file_path_in)
        tmp_file = NamedTemporaryFile(suffix=pathlib.Path(key).suffix)
        s3_client.download_file(bucket, key, tmp_file.name)
        yield tmp_file.name


def mock_get_file(*args, **kwargs):
    return MockS3FilesystemReadInputStream()
