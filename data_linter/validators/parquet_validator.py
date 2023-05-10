import logging
import os
from typing import Union

import pyarrow.parquet as pq
from dataengineeringutils3.s3 import s3_path_to_bucket_key
from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter
from pyarrow import Schema
from pyarrow.fs import S3FileSystem

from data_linter.validators.base import BaseTableValidator

log = logging.getLogger("root")
default_date_format = "%Y-%m-%d"
default_datetime_format = "%Y-%m-%d %H:%M:%S"
aws_default_region = os.getenv(
    "AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "eu-west-1")
)


class ParquetValidator(BaseTableValidator):
    """
    Validator for checking that a parquet file's schema matches a given Metadata.
    For validating the data itself, use the Pandas validator.
    """

    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: Union[dict, str, Metadata],
        **kwargs,
    ):
        super().__init__(filepath, table_params, metadata)

    @staticmethod
    def _read_schema(filepath: str) -> Schema:
        if filepath.startswith("s3://"):
            s3fs = S3FileSystem(region=aws_default_region)
            b, k = s3_path_to_bucket_key(filepath)
            pa_pth = os.path.join(b, k)
            with s3fs.open_input_file(pa_pth) as file:
                schema = pq.read_schema(file).remove_metadata()
        else:
            schema = pq.read_schema(filepath).remove_metadata()
        return schema

    def read_data_and_validate(self):
        table_arrow_schema = self._read_schema(self.filepath)
        ac = ArrowConverter()
        metadata_arrow_schema = ac.generate_from_meta(self.metadata).remove_metadata()
        metas_match = table_arrow_schema.equals(metadata_arrow_schema)

        cols_in_meta_not_in_file = list(
            set([c.name for c in metadata_arrow_schema])
            - set([c.name for c in table_arrow_schema])
        )

        cols_in_file_not_in_meta = list(
            set([c.name for c in table_arrow_schema])
            - set([c.name for c in metadata_arrow_schema])
        )

        cols_with_different_types = {
            c.name: {
                "meta_field": str(metadata_arrow_schema[i].type),
                "table_field": str(table_arrow_schema[i].type),
            }
            for i, c in enumerate(metadata_arrow_schema)
            if not metadata_arrow_schema[i].equals(table_arrow_schema[i])
        }

        result_dict = {
            "valid": metas_match,
            "cols_in_meta_not_in_file": cols_in_meta_not_in_file,
            "cols_in_file_not_in_meta": cols_in_file_not_in_meta,
            "cols_with_different_types": cols_with_different_types,
        }

        self.response.add_table_test("check_schema_conforms", result_dict)

    def write_validation_errors_to_log(self):
        table_result = self.response.get_result()
        if not table_result["valid"]:
            failed_cols = self.response.get_names_of_column_failures()
            err_msg = (
                "Table failed validation. "
                f"Col failures: {failed_cols}. "
                "See response error log for more details."
            )
            log.error(err_msg, extra={"context": "VALIDATION"})
            log.debug(str(table_result), extra={"context": "VALIDATION"})
