import pytest
import jsonschema
import json
from tableschema import Schema

from data_linter.validation import convert_meta_to_goodtables_schema


def test_convert_meta_type_to_goodtable_type():
    with open("tests/data/basic_meta_data.json") as f:
        meta = json.load(f)

    gt_schema = convert_meta_to_goodtables_schema(meta)
    assert Schema(gt_schema).valid == True
