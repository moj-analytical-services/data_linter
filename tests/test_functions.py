import json
from frictionless import Schema

from data_linter.validators.frictionless_validator import (
    convert_meta_to_goodtables_schema,
)
from data_linter.utils import get_out_path


def test_convert_meta_type_to_goodtable_type():
    with open("tests/data/inputs/basic_meta_data.json") as f:
        meta = json.load(f)

    gt_schema = convert_meta_to_goodtables_schema(meta)

    assert Schema(gt_schema).metadata_valid is True


def test_get_out_path():
    o1 = get_out_path(
        basepath="base_path/", table="table1", ts=1000, filename="file.csv"
    )
    e1 = "base_path/table1/file-0-1000.csv"
    assert o1 == e1

    o2 = get_out_path(
        basepath="base_path/two/",
        table="table1",
        ts=1234567,
        filename="file.jsonl",
        compress=True,
        filenum=20,
        timestamp_partition_name="mojap_file_land_timestamp",
    )
    e2 = (
        "base_path/two/table1/mojap_file_land_timestamp=1234567/"
        "file-20-1234567.jsonl.gz"
    )
    assert o2 == e2

    o3 = get_out_path(
        basepath="base_path/two/",
        table="table1",
        ts=1234567,
        filename="file.other-ext.jsonl",
        compress=True,
        filenum=20,
        timestamp_partition_name="mojap_file_land_timestamp",
    )
    e3 = (
        "base_path/two/table1/mojap_file_land_timestamp=1234567/"
        "file-20-1234567.other-ext.jsonl.gz"
    )
    assert o3 == e3
