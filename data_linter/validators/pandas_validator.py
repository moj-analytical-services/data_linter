import logging
import pandas as pd
import functools
import inspect
from typing import Union, Tuple

from arrow_pd_parser.parse import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
    pa_read_csv_to_pandas,
)

from arrow_pd_parser.pa_pd import arrow_to_pandas

from pyarrow import parquet as pq, fs

from data_linter.validators.base import (
    BaseTableValidator,
    ValidatorResult,
)


def check_run_validation_for_meta(func):
    """
    Wrapper function to test if test function should be called based
    on meta_col keys.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get parameters from function to check if function should run
        sig = inspect.signature(func)
        argmap = sig.bind_partial(*args, **kwargs).arguments
        mc = argmap.get("meta_col")
        if func.__name__ == "test_min_max" and _check_meta_has_params(["minimum", "maximum"], mc):
            return func(*args, **kwargs)
        elif func.__name__ == "test_min_max_length" and _check_meta_has_params([], mc):
            return func(*args, **kwargs)
        else:
            pass
    return wrapper


def _check_meta_has_params(any_of: list, meta_col:dict):
    return any([a in meta_col for a in any_of])


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
            reader_fs = fs.S3FileSystem(region='eu-west-1')
            fp_for_file_reader = self.filepath.replace("s3://", "", 1)

        else:
            reader_fs = fs.LocalFileSystem()
            fp_for_file_reader = self.filepath

        with reader_fs.open_input_stream(fp_for_file_reader) as f:
            if "csv" in self.metadata.data_format:
                df = pa_read_csv_to_pandas(
                    input_file=f,
                    schema=None,  # Needs actual schema
                    expect_full_schema=False
                )
            elif "json" in self.metadata.data_format:
                df = pa_read_json_to_pandas(
                    input_file=f,
                    schema=None,  # Needs actual schema
                    expect_full_schema=False
                )
            elif "parquet" in self.metadata.data_format:
                df = arrow_to_pandas(
                    pq.read_table(f)
                )
            else:
                raise ValueError(f"Unknown data_format in metadata: {self.metadata.data_format}.")
        return df


    def validate_df(self, df):  # STEPHEN TODO
        
        meta_cols = [ col for col in self.metadata["columns"]]

        for i, (_, series) in enumerate(df.iteritems()):
            self.validate_col(series, meta_cols[i], self.response)


    def validate_col(self, col, meta_col, result): # result: ValidatorResult

        col_name = meta_col["name"]
        
        mi = meta_col.get("minimum")
        ma = meta_col.get("maximum")
        if mi or ma:
            result.add_test_to_col(
                col_name,
                "min_max",
                self.min_max_test(col, col_name, mi, ma)
            )

        mil = meta_col.get("minLength")
        mal = meta_col.get("maxLength")
        if mil or mal:
            result.add_test_to_col(
                col_name,
                "min_max_length",
                self.min_max_length_test(col, col_name, mil, mal)
            )

    @check_run_validation_for_meta
    def min_max_test(self, col: pd.Series, meta_col: dict):

        colname = meta_col.get("name")
        mi = meta_col.get("minimum")
        ma = meta_col.get("maximum")

        res_dict = _init_get_low_level_result_dict()
        res_dict["details"]["minimum_value"] = mi
        res_dict["details"]["maximum_value"] = ma

        col_oob = _get_min_max_series_out_of_bounds_col(col, mi, ma)
        res_dict["valid"] = ~(col_oob.any())

        if not res_dict["valid"]:
            uil, ul = _get_list_of_col_values_and_index_from_oob(col, col_oob)
            res_dict["details"]["unexpected_index_list"] = uil
            res_dict["details"]["unexpected_list"] = ul

        self.add_test_to_col(colname, "min_max_test", res_dict)


    def min_max_length_test(self, col, meta_col):

        colname = meta_col.get("name")
        mil = meta_col.get("minLength")
        mal = meta_col.get("maxLength")

        res_dict = _init_get_low_level_result_dict()
        res_dict["details"]["minLength_value"] = mil
        res_dict["details"]["maxLength_value"] = mal

        col_str_lens = col.str.len()
        col_oob = _get_min_max_series_out_of_bounds_col(col_str_lens, mil, mal)

        res_dict["valid"] = ~(col_oob.any())

        if not res_dict["valid"]:
            uil, ul = _get_list_of_col_values_and_index_from_oob(col, col_oob)
            res_dict["details"]["unexpected_index_list"] = uil
            res_dict["details"]["unexpected_list"] = ul

        self.add_test_to_col(colname, "min_max_test", res_dict)


def _get_min_max_series_out_of_bounds_col(
    col: pd.Series,
    colname: str,
    mi: Union[int, None] = None,
    ma: Union[int, None] = None,
) -> pd.Series:
    # Test if values out of bounds
    if mi is not None and ma is None:
        col_oob = (col < mi)
    elif ma is not None and mi is None:
        col_oob = (col > ma)
    elif ma is not None and mi is not None:
        col_oob = ~col.between(mi, ma)
    else:
        raise ValueError(f"invalid min/max values for column: {colname}")
    return col_oob


def _get_list_of_col_values_and_index_from_oob(
    col: pd.Series,
    col_oob: pd.Series,
) -> Tuple(list, list):

    unexpected_index_list = col_oob.index[col_oob].tolist()
    unexpected_list = col[col_oob].tolist()
    return (unexpected_index_list, unexpected_list)

def _init_get_low_level_result_dict():
    d = {
        "details" : {},
        "valid" : None,
    }
    return d
