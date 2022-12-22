import logging


from data_linter.validators.base import (
    BaseTableValidator,
)
from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter
from typing import Union
import pyarrow.parquet as pq


log = logging.getLogger("root")
default_date_format = "%Y-%m-%d"
default_datetime_format = "%Y-%m-%d %H:%M:%S"


class ParquetValidator(BaseTableValidator):
    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: Union[dict, str, Metadata],
        **kwargs
    ):
        super().__init__(filepath, table_params, metadata)

    def read_data_and_validate(self):
        table_arrow_schema = pq.read_schema(self.filepath).remove_metadata()
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
                "meta_field": metadata_arrow_schema[i].type,
                "table_field": table_arrow_schema[i].type,
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
