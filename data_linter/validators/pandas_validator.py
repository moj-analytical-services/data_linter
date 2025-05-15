import logging
import inspect
import traceback

from functools import wraps
from datetime import datetime
from mojap_metadata import Metadata
from typing import Union

import pandas as pd

from arrow_pd_parser import reader
from arrow_pd_parser.caster import cast_pandas_table_to_schema

from data_linter.validators.base import (
    BaseTableValidator,
)

log = logging.getLogger("root")
default_date_format = "%Y-%m-%d"
default_datetime_format = "%Y-%m-%d %H:%M:%S"
global_log_verbosity = None


class ColumnError(Exception):
    pass


class PandasValidator(BaseTableValidator):
    """
    Validator using Pandas
    """

    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: Union[dict, str, Metadata],
        log_verbosity: int = None,
        ignore_missing_cols: bool = False,
    ):
        super().__init__(filepath, table_params, metadata)
        global global_log_verbosity
        global_log_verbosity = table_params.get("log_verbosity", log_verbosity)
        self.ignore_missing_cols = ignore_missing_cols

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

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.

        Data is read using pd_arrow_parser.
        """
        fail_response_dict = {self.response.vvkn: False}
        try:
            df, self.metadata = _parse_data_to_pandas(
                self.filepath, self.table_params, self.metadata
            )
        except Exception:
            traceback_message = traceback.format_exc()
            fail_response_dict["traceback"] = traceback_message
            self.response.add_table_test("parse_data_to_pandas", fail_response_dict)
            log.error(traceback_message)
            df = None

        if df is not None:
            try:
                self.validate_df(df)
            except Exception:
                self.response.add_table_test("overall_validation", fail_response_dict)
                log.error(traceback.format_exc())

    def validate_df(self, df):
        for m in self.metadata.columns:
            self.validate_col(df[m["name"]], m)

    def validate_col(self, col, meta_col):

        self.min_max_test(col, meta_col)
        self.min_max_length_test(col, meta_col)
        self.pattern_test(col, meta_col)
        self.enum_test(col, meta_col)
        self.nullable_test(col, meta_col)
        self.datetime_format_test(col, meta_col)
        self.date_format_test(col, meta_col)

    def min_max_test(self, col, meta_col):
        res_dict = _min_max_test(col, meta_col)
        col_name = meta_col["name"]
        if res_dict is not None:
            self.response.add_test_to_col(col_name, "min_max_test", res_dict)

    def min_max_length_test(self, col, meta_col):
        res_dict = _min_max_length_test(col, meta_col)
        col_name = meta_col["name"]
        if res_dict is not None:
            self.response.add_test_to_col(col_name, "min_max_length_test", res_dict)

    def pattern_test(self, col, meta_col):
        res_dict = _pattern_test(col, meta_col)
        col_name = meta_col["name"]
        if res_dict is not None:
            self.response.add_test_to_col(col_name, "pattern_test", res_dict)

    def enum_test(self, col, meta_col):
        res_dict = _enum_test(col, meta_col)
        col_name = meta_col["name"]
        if res_dict is not None:
            self.response.add_test_to_col(col_name, "enum_test", res_dict)

    def nullable_test(self, col, meta_col):
        res_dict = _nullable_test(col, meta_col)
        col_name = meta_col["name"]
        if res_dict is not None:
            self.response.add_test_to_col(col_name, "nullable_test", res_dict)

    def datetime_format_test(self, col, meta_col):
        res_dict = _datetime_format_test(col, meta_col)
        col_name = meta_col["name"]
        if res_dict is not None:
            self.response.add_test_to_col(col_name, "datetime_format_test", res_dict)

    def date_format_test(self, col, meta_col):
        res_dict = _date_format_test(col, meta_col)
        col_name = meta_col["name"]
        if res_dict is not None:
            self.response.add_test_to_col(col_name, "date_format_test", res_dict)


def check_run_validation_for_meta(func):
    """
    Wrapper for each validation test. Will get inputs to function
    and check if function should be called based on the inputs.
    (Most likely this will be done based on the properties of the supplied
    metadata).

    Will return nothing if function should not be called.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        sig = inspect.signature(func)
        argmap = sig.bind_partial(*args, **kwargs).arguments
        mc = argmap.get("meta_col")

        # Check if column is string or str dtype
        col_is_str = _check_pandas_series_is_str(argmap.get("col"))

        call_method = False
        if func.__name__ == "_min_max_test" and _check_meta_has_params(
            ["minimum", "maximum"], mc
        ):
            call_method = True
        elif func.__name__ == "_min_max_length_test" and _check_meta_has_params(
            ["minLength", "maxLength"], mc
        ):
            call_method = True
        elif func.__name__ == "_pattern_test" and _check_meta_has_params(
            ["pattern"], mc
        ):
            call_method = True
        elif func.__name__ == "_enum_test" and _check_meta_has_params(["enum"], mc):
            call_method = True
        elif func.__name__ == "_nullable_test" and not _check_meta_has_params(
            [None, True], [mc.get("nullable")]
        ):
            call_method = True
        elif func.__name__ == "_date_format_test" and mc.get("type", "").startswith(
            "date"
        ):
            if col_is_str:
                call_method = True
            else:
                msg = (
                    f"Column {mc['name']} not tested. "
                    "Tests for datetime encoded columns are not yet implemented."
                )
                log.info(msg)
        elif func.__name__ == "_datetime_format_test" and mc.get("type", "").startswith(
            "timestamp"
        ):
            if col_is_str:
                call_method = True
            else:
                msg = (
                    f"Column {mc['name']} not tested. "
                    "Tests for datetime encoded columns are not yet implemented."
                )
                log.info(msg)

        else:
            pass

        return func(*args, **kwargs) if call_method else None

    return wrapper


@check_run_validation_for_meta
def _min_max_test(col: pd.Series, meta_col: dict) -> dict:

    col_name = meta_col["name"]
    mi = meta_col.get("minimum")
    ma = meta_col.get("maximum")

    test_inputs = {"column": col_name, "minimum_value": mi, "maximum_value": ma}
    res_dict = _result_dict("min max numerical", test_inputs)

    col_oob = _get_min_max_series_out_of_bounds_col(col, col_name, mi, ma)

    return _fill_res_dict(col, col_oob, res_dict)


@check_run_validation_for_meta
def _min_max_length_test(col: pd.Series, meta_col: dict) -> dict:

    col_name = meta_col["name"]
    mil = meta_col.get("minLength")
    mal = meta_col.get("maxLength")

    test_inputs = {"column": col_name, "minimum_length": mil, "maximum_length": mal}
    res_dict = _result_dict("min max length", test_inputs)

    col_oob = _get_min_max_series_out_of_bounds_col(col.str.len(), col_name, mil, mal)

    return _fill_res_dict(col, col_oob, res_dict)


@check_run_validation_for_meta
def _pattern_test(col: pd.Series, meta_col: dict) -> dict:

    col_name = meta_col["name"]
    pattern = meta_col.get("pattern")

    test_inputs = {"column": col_name, "regex": pattern}

    res_dict = _result_dict("regex", test_inputs)

    col_oob = ~col.str.match(pattern)

    return _fill_res_dict(col, col_oob, res_dict)


@check_run_validation_for_meta
def _enum_test(col: pd.Series, meta_col: dict) -> dict:

    col_name = meta_col["name"]
    enum = meta_col.get("enum")

    test_inputs = {"column": col_name}

    res_dict = _result_dict("enum", test_inputs)

    if meta_col.get("nullable", True):
        col_oob = ~col.fillna(enum[0]).isin(enum)
    else:
        col_oob = ~col.isin(enum)

    return _fill_res_dict(col, col_oob, res_dict)


@check_run_validation_for_meta
def _nullable_test(col: pd.Series, meta_col: dict) -> dict:

    col_name = meta_col.get("name")

    test_inputs = {
        "column": col_name,
    }

    res_dict = _result_dict("nullable", test_inputs)

    col_oob = col.isnull()

    return _fill_res_dict(col, col_oob, res_dict)


@check_run_validation_for_meta
def _date_format_test(col: pd.Series, meta_col) -> dict:

    col_name = meta_col["name"]

    datetime_format = meta_col.get("datetime_format", default_date_format)
    test_inputs = {"column": col_name, "datetime_format": datetime_format}

    res_dict = _result_dict("datetime_format", test_inputs)

    col_oob = ~col.apply(
        lambda x: _valid_date_or_datetime_conversion(x, datetime_format, True)
    )
    return _fill_res_dict(col, col_oob, res_dict)


@check_run_validation_for_meta
def _datetime_format_test(col: pd.Series, meta_col):

    col_name = meta_col["name"]

    datetime_format = meta_col.get("datetime_format", default_datetime_format)
    test_inputs = {"column": col_name, "datetime_format": datetime_format}

    res_dict = _result_dict("datetime_format", test_inputs)

    col_oob = ~col.apply(
        lambda x: _valid_date_or_datetime_conversion(x, datetime_format)
    )
    return _fill_res_dict(col, col_oob, res_dict)


def _valid_date_or_datetime_conversion(
    date_or_datetime_str: str, dt_format: str, check_for_no_time_component=False
):
    if pd.isna(date_or_datetime_str) or not bool(date_or_datetime_str):
        return True
    else:
        try:
            dt = datetime.strptime(date_or_datetime_str, dt_format)
            if check_for_no_time_component:
                return _check_no_time_component_in_expected_date(dt)
            else:
                return True
        except ValueError:
            return False


def _check_no_time_component_in_expected_date(dt: datetime):
    result = dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0
    return result


def _result_dict(test_name: str, test_inputs: dict) -> dict:

    d = {
        "valid": None,
        "test_inputs": test_inputs,
    }

    return d


def _fill_res_dict(col: pd.Series, col_oob: pd.Series, res_dict: dict) -> dict:

    valid = not col_oob.any()
    res_dict["valid"] = valid

    if not valid:
        col_oob = col_oob.fillna(False)
        n = global_log_verbosity

        # get the unexpected values
        unexpected_index = col_oob.index[col_oob]
        unexpected_values = col[unexpected_index].astype(str)

        res_dict["percentage_of_column_is_error"] = (
            len(unexpected_index) / len(col) * 100
        )

        if n is not None:
            # if the global_log_verbosity is not 0, sample
            if n != 0:
                # asking for a higher sample than is there?
                if global_log_verbosity > len(unexpected_values):
                    n = len(unexpected_values)
                # sample the requested amount
                unexpected_values = unexpected_values.sample(n=n)
                unexpected_index = unexpected_values[unexpected_values.index]
            # log the required unexpected values
            res_dict["unexpected_index_sample"] = unexpected_index.tolist()
            res_dict["unexpected_values_sample"] = unexpected_values.tolist()

    return res_dict


def _get_min_max_series_out_of_bounds_col(
    col: pd.Series, colname: str, mi: Union[int, None], ma: Union[int, None]
) -> pd.Series:

    # Test if values out of bounds
    if mi is not None and ma is None:
        return col < mi
    elif ma is not None and mi is None:
        return col > ma
    elif mi is not None and ma is not None:
        return (col < mi) | (col > ma)
    else:
        raise ValueError(f"invalid min/max values for column: {colname}")


def _check_meta_has_params(any_of: list, meta_col: dict):
    return any([a in meta_col for a in any_of])


def _parse_data_to_pandas(filepath: str, table_params: dict, metadata: Metadata):
    """
    Reads in the data from the given filepath and returns
    a dataframe
    """

    # get the required sets of column names
    meta_col_names = [
        c["name"] for c in metadata.columns if c["name"] not in metadata.partitions
    ]

    pandas_kwargs = table_params.get("pandas-kwargs", {})

    # read data (and do headers stuff if csv)
    if filepath.lower().endswith("csv"):
        expect_header = table_params.get("expect-header", True)
        header = 0 if expect_header else None
        df = reader.read(filepath, header=header, low_memory=False, **pandas_kwargs)
        if not expect_header:
            df.columns = meta_col_names
    else:
        df = reader.read(filepath, **pandas_kwargs)

    # eliminate case sensitivity, if requested
    if table_params.get("headers-ignore-case"):
        for c in metadata.columns:
            c["name"] = c["name"].lower()
        df.columns = [c.lower() for c in df.columns]
        meta_col_names = [c.lower() for c in meta_col_names]

    allow_missing_cols = table_params.get("allow-missing-cols", False)
    allow_unexpected_data = table_params.get("allow-unexpected-data", False)

    cols_in_meta_but_not_data = [c for c in meta_col_names if c not in df.columns]
    cols_in_data_but_not_meta = [c for c in df.columns if c not in meta_col_names]
    cols_in_data_and_meta = [c for c in df.columns if c in meta_col_names]

    # error if there are no common columns
    if not cols_in_data_and_meta:
        raise ColumnError("There is no commonality between the data and metadata")

    # this is so that both mitigations can be checked and both errors are made visible
    raise_column_error = False
    err_msg = ""

    # remove columns from meta that aren't in the data if allowed
    msg_1 = f"columns present in metadata but not in data: {cols_in_meta_but_not_data}"
    if (not allow_missing_cols) and cols_in_meta_but_not_data:
        err_msg += msg_1
        raise_column_error = True
    elif allow_missing_cols and cols_in_meta_but_not_data:
        for col in cols_in_meta_but_not_data:
            metadata.remove_column(col)
        log.info("not testing " + msg_1)

    # error if there is unexepcted data, unless allowed
    msg_2 = f"columns present in data but not in metadata: {cols_in_data_but_not_meta}"
    if (not allow_unexpected_data) and cols_in_data_but_not_meta:
        err_msg += f"\n{msg_2}"
        raise_column_error = True
    elif allow_unexpected_data and cols_in_data_but_not_meta:
        log.info("not testing " + msg_2)
        df = df[cols_in_data_and_meta]

    # raise the error with all details, if required
    if raise_column_error:
        raise ColumnError(err_msg)

    # sample the data, if required
    row_limit = table_params.get("row-limit", None)
    if row_limit:
        row_limit = row_limit if row_limit <= len(df) else len(df)
        df = df.sample(row_limit)

    if metadata.file_format not in ["parquet", "snappy.parquet"]:
        df = cast_pandas_table_to_schema(df, metadata)

    return df, metadata


def _check_pandas_series_is_str(s: pd.Series, na_as=True):
    """
    Checks if a pandas series is a str or string. No I can't use
    pd.api.types.is_string_dtype. See issue #164.

    Args:
        s (pd.Series): A Pandas Series to check
        na_as (bool, optional): How you want to treat NAs or None.
          True (default) means any NA is a string. This is fine because
          Any other value that is not missing should be accurately
          determined as a string or not a string.
    """

    def check(x):
        if pd.isna(x) or x is None:
            return na_as
        else:
            return isinstance(x, tuple([str, pd.StringDtype]))

    out = s.apply(check)
    return out.all()
