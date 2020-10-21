import os
import shutil
import boto3
import gzip
import tempfile

from typing import Union
from pathlib import Path

from dataengineeringutils3.s3 import (
    s3_path_to_bucket_key,
    write_local_file_to_s3,
)


def download_data(s3_path: str, local_path: str):
    s3_client = boto3.client("s3")
    dirname = os.path.dirname(local_path)
    Path(dirname).mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        b, o = s3_path_to_bucket_key(s3_path)
        s3_client.download_fileobj(b, o, f)


def compress_data(s3_download_path: str, s3_upload_path: str):
    s3_client = boto3.client("s3")
    with tempfile.TemporaryDirectory() as temp_dir:
        bucket, key = s3_path_to_bucket_key(s3_download_path)
        temp_file = os.path.join(temp_dir, key.split("/")[-1])
        with open(temp_file, "wb") as opened_temp_file:
            s3_client.download_fileobj(bucket, key, opened_temp_file)
        with open(temp_file, "rb") as f_in, gzip.open(temp_file + ".gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        write_local_file_to_s3(temp_file + ".gz", s3_upload_path, overwrite=True)


def get_out_path(
    basepath: str,
    table: str,
    ts: str,
    filename: str,
    compress: bool = False,
    filenum: int = 0,
    timestamp_partition_name: Union[str, None] = None,
) -> str:
    filename_only, ext = filename.split(".", 1)
    final_filename = f"{filename_only}-{filenum}-{ts}.{ext}"
    if compress and not ext.endswith(".gz"):
        final_filename += ".gz"

    if timestamp_partition_name:
        out_path = os.path.join(
            basepath, table, f"{timestamp_partition_name}={ts}", final_filename
        )
    else:
        out_path = os.path.join(
            basepath, table, final_filename
        )
    return out_path


def get_log_path(basepath: str, table: str, ts: str, filenum: int = 0) -> str:
    final_filename = f"log-{table}-{filenum}-{ts}.json"

    out_path = os.path.join(basepath, table, final_filename)
    return out_path


def local_file_to_s3(local_path: str, s3_path: str):
    s3_client = boto3.client("s3")

    if (not local_path.endswith(".gz")) and (s3_path.endswith(".gz")):
        new_path = local_path + ".gz"
        with open(local_path, "rb") as f_in, gzip.open(new_path, "wb") as f_out:
            f_out.writelines(f_in)
        local_path = new_path

    b, o = s3_path_to_bucket_key(s3_path)
    with open(local_path, "rb") as f:
        s3_client.upload_fileobj(f, b, o)
