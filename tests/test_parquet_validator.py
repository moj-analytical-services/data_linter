import pytest
import os
from data_linter.validators.parquet_validator import ParquetValidator
from mojap_metadata import Metadata


@pytest.mark.parametrize(
    "meta_file,expected_pass", [("table1_pass.json", True), ("table1_fail.json", False)]
)
def test_parquet_validator(meta_file, expected_pass):
    meta = Metadata.from_json(
        os.path.join("tests/data/parquet_validator/meta_data/", meta_file)
    )
    file_path = "tests/data/parquet_validator/table1.parquet"
    pv = ParquetValidator(filepath=file_path, table_params={}, metadata=meta)
    pv.read_data_and_validate()
    print(pv.response.result)
    assert pv.response.result["valid"] == expected_pass
