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
    copy_s3_object,
    check_for_s3_file,
)


def download_data(s3_path: str, local_path: str):
    s3_client = boto3.client("s3")
    dirname = os.path.dirname(local_path)
    Path(dirname).mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        b, o = s3_path_to_bucket_key(s3_path)
        s3_client.download_fileobj(b, o, f)


def compress_data(download_path: str, upload_path: str):

    download_path_is_s3 = download_path.startswith("s3://")
    upload_path_is_s3 = upload_path.startswith("s3://")

    if download_path_is_s3:
        s3_client = boto3.client("s3")

    if not upload_path_is_s3:
        upload_path_dir = os.path.dirname(upload_path)
        if not os.path.exists(upload_path_dir):
            os.makedirs(upload_path_dir, exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:

        if download_path_is_s3:
            bucket, key = s3_path_to_bucket_key(download_path)
            temp_file = os.path.join(temp_dir, key.split("/")[-1])
            with open(temp_file, "wb") as opened_temp_file:
                s3_client.download_fileobj(bucket, key, opened_temp_file)
        else:
            temp_file = os.path.join(temp_dir, download_path.split(os.path.sep)[-1])
            with open(temp_file, "wb") as opened_temp_file:
                shutil.copy(download_path, temp_file)

        with open(temp_file, "rb") as f_in, gzip.open(temp_file + ".gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        if upload_path_is_s3:
            write_local_file_to_s3(temp_file + ".gz", upload_path, overwrite=True)
        else:
            shutil.copy(temp_file + ".gz", upload_path)


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
        out_path = os.path.join(basepath, table, final_filename)
    return out_path


def get_table_log_path(basepath: str, table: str, ts: str, filenum: int = 0) -> str:
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


def s3_to_local(s3_path: str, local_path: str):
    s3_client = boto3.client("s3")
    bucket, key = s3_path_to_bucket_key(s3_path)

    with open(local_path, "wb") as opened_file:
        s3_client.download_fileobj(bucket, key, opened_file)


def copy_data(src_path: str, dst_path: str):
    src_path_is_s3 = src_path.startswith("s3://")
    dst_path_is_s3 = dst_path.startswith("s3://")

    if not dst_path_is_s3:
        dst_path_dir = os.path.dirname(dst_path)
        if not os.path.exists(dst_path_dir):
            os.makedirs(dst_path_dir, exist_ok=True)

    if src_path_is_s3 and dst_path_is_s3:
        copy_s3_object(src_path, dst_path)
    elif src_path_is_s3 and not dst_path_is_s3:
        s3_to_local(src_path, dst_path)
    elif not src_path_is_s3 and dst_path_is_s3:
        write_local_file_to_s3(src_path, dst_path, overwrite=True)
    elif not src_path_is_s3 and not dst_path_is_s3:
        shutil.copyfile(src_path, dst_path)


def get_filepaths_from_local_folder(
    land_base_path: str,
    file_extension: Union[None, str] = None,
    exclude_zero_byte_files: bool = True,
) -> list:

    ret_file_paths = []

    while land_base_path.endswith(os.path.sep):
        land_base_path = land_base_path[:-1]

    for curr_dir, _, file_names in os.walk(land_base_path):
        file_names = [i for i in file_names if not i.startswith(".")]

        if file_extension:
            file_names = [i for i in file_names if i.endswith(file_extension)]

        file_paths = [curr_dir + os.path.sep + i for i in file_names]

        if exclude_zero_byte_files:
            file_paths = [i for i in file_paths if os.stat(i).st_size != 0]

        ret_file_paths.extend(file_paths)

    return ret_file_paths


def read_all_file_body(file_path: str) -> str:
    """
    Returns the text content of a file (will decode bytes if file read is bytes like)

    Args:
        file_path: A string specifying the location of the file to load text from.
        can be s3 or local
    """
    file_path_is_s3 = file_path.startswith("s3://")

    if file_path_is_s3:
        s3_client = boto3.client("s3")
        if not check_for_s3_file(file_path):
            raise FileNotFoundError("Path to config: {file_path}. Not found.")
        bucket, key = s3_path_to_bucket_key(file_path)
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        file_obj_body = file_obj["Body"].read()
    else:
        with open(file_path) as f_in:
            file_obj_body = f_in.read()

    if isinstance(file_obj_body, bytes):
        return file_obj_body.decode("utf-8")
    else:
        return file_obj_body
