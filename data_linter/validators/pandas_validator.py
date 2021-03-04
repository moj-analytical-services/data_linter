import logging
import pandas as pd

from arrow_pd_parser.parse import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
)

from typing import Union

from arrow_pd_parser.pa_pd import arrow_to_pandas

from pyarrow import parquet as pq, fs

from data_linter.validators.base import (
    BaseTableValidator,
)


class PandasValidator(BaseTableValidator):
    """
    Validator using Pandas
    """

    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: dict,
        ignore_missing_cols=False,
    ):
        super().__init__(filepath, table_params, metadata)

        self.ignore_missing_cols = ignore_missing_cols

    @property
    def valid(self):
        return self.response.result["valid"]

    def write_validation_errors_to_log(self, log: logging.Logger):
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

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.

        Data is read using pd_arrow_parser.
        """

        df = self.read_data()  # KARIK TODO
        self.validate_df(df)

    def get_response_dict(self):
        self.response.get_result()

    def read_data(self) -> pd.DataFrame:  # KARIK TODO
        """
        Reads in the data from the given filepath and returns
        a dataframe
        """

        if self.filepath.startswith("s3://"):
            reader_fs = fs.S3FileSystem(region="eu-west-1")
            fp_for_file_reader = self.filepath.replace("s3://", "", 1)

        else:
            reader_fs = fs.LocalFileSystem()
            fp_for_file_reader = self.filepath

        with reader_fs.open_input_stream(fp_for_file_reader) as f:
            if "csv" in self.metadata.data_format:
                df = pa_read_csv_to_pandas(
                    input_file=f,
                    schema=None,  # Needs actual schema
                    expect_full_schema=False,
                )
            elif "json" in self.metadata.data_format:
                df = pa_read_json_to_pandas(
                    input_file=f,
                    schema=None,  # Needs actual schema
                    expect_full_schema=False,
                )
            elif "parquet" in self.metadata.data_format:
                df = arrow_to_pandas(pq.read_table(f))
            else:
                raise ValueError(
                    f"Unknown data_format in metadata: {self.metadata.data_format}."
                )
        return df

    def validate_df(self, df):  # STEPHEN TODO

        meta_cols = [col for col in self.metadata["columns"]]

        for i, (_, col) in enumerate(df.iteritems()):
            self.validate_col(col, meta_cols[i])

    def validate_col(self, col, meta_col):

        col_name = meta_col["name"]

        mi = meta_col.get("minimum")
        ma = meta_col.get("maximum")
        if mi or ma:
            self.response.add_test_to_col(
                col_name, "min_max", self.min_max_test(col, col_name, mi, ma)
            )

        mil = meta_col.get("minLength")
        mal = meta_col.get("maxLength")
        if mil or mal:
            self.response.add_test_to_col(
                col_name,
                "min_max_length",
                self.min_max_length_test(col, col_name, mil, mal),
            )

        if meta_col.get("pattern"):
            self.response.add_test_to_col(
                col_name,
                "regex_match",
                self.pattern_test(col, col_name, meta_col["pattern"]),
            )

        if meta_col.get("enum"):
            self.response.add_test_to_col(
                col_name, "enum_match", self.enum_test(col, col_name, meta_col["enum"])
            )

        if not meta_col.get("nullable", True):
            self.response.add_test_to_col(
                col_name, "nullabe", self.nullable_test(col, col_name)
            )

    def min_max_test(self, col: pd.Series, col_name: str, mi: int, ma: int) -> dict:

        res_kwargs = {"column": col_name, "minimum_value": mi, "maximum_value": ma}
        res_dict = self.__result_dict("value_is_between", res_kwargs)

        col_oob = self._get_min_max_series_out_of_bounds_col(col, col_name, mi, ma)

        return self.__fill_res_dict(col, col_oob, res_dict)

    def min_max_length_test(self, col: pd.Series, col_name, mil, mal) -> dict:

        res_kwargs = {"column": col_name, "minimum_length": mil, "maximum_length": mal}
        res_dict = self.__result_dict("string_between_length", res_kwargs)

        col_oob = self._get_min_max_series_out_of_bounds_col(
            col.str.len(), col_name, mil, mal
        )

        return self.__fill_res_dict(col, col_oob, res_dict)

    def pattern_test(self, col: pd.Series, col_name: str, pattern: str) -> dict:

        res_kwargs = {"column": col_name, "regex": pattern}

        res_dict = self.__result_dict("srting matches regex", res_kwargs)

        col_oob = ~col.str.match(pattern)

        return self.__fill_res_dict(col, col_oob, res_dict)

    def enum_test(self, col: pd.Series, col_name: str, enum: list) -> dict:

        res_kwargs = {"column": col_name, "enum_value_set": enum}

        res_dict = self.__result_dict("value in enum list", res_kwargs)

        col_oob = ~col.isin(enum)

        return self.__fill_res_dict(col, col_oob, res_dict)

    def nullable_test(self, col: pd.Series, col_name) -> dict:

        res_kwargs = {
            "column": col_name,
        }

        res_dict = self.__result_dict("value is not null", res_kwargs)

        col_oob = col.isnull()

        return self.__fill_res_dict(col, col_oob, res_dict)

    def datetime_test(self, col: pd.Series, col_name, dt_type) -> dict:
        pass

    def __result_dict(self, expectation_type: str, res_kwargs: dict) -> dict:

        d = {
            "result": {},
            "valid": False,
            "expectation_config": {
                "expectation_type": expectation_type,
                "kwargs": res_kwargs,
            },
        }

        return d

    def __fill_unexpected(self, col, col_oob, res_dict) -> dict:

        unexpected_index_list = col_oob.index[col_oob].tolist()
        unexpected_list = col[unexpected_index_list].tolist()

        res_dict["result"]["unexpected_index_list"] = unexpected_index_list
        res_dict["result"]["unexpected_list"] = unexpected_list

        return res_dict

    def __fill_res_dict(self, col, col_oob, res_dict) -> dict:

        valid = not col_oob.any()
        res_dict["valid"] = valid

        if not valid:
            unexpected_index_list = col_oob.index[col_oob].tolist()
            unexpected_list = col[unexpected_index_list].tolist()

            res_dict["result"]["unexpected_index_list"] = unexpected_index_list
            res_dict["result"]["unexpected_list"] = unexpected_list

        return res_dict

    def _get_min_max_series_out_of_bounds_col(
        self, col: pd.Series, colname: str, mi: Union[int, None], ma: Union[int, None]
    ) -> pd.Series:

        # Test if values out of bounds
        if mi is not None and ma is None:
            col_oob = col < mi
        elif ma is not None and mi is None:
            col_oob = col > ma
        elif ma is not None and mi is not None:
            col_oob = ~col.between(mi, ma)
        else:
            raise ValueError(f"invalid min/max values for column: {colname}")
        return col_oob
