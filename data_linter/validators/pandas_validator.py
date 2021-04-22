import logging
import pandas as pd
import inspect

from functools import wraps

from datetime import datetime

from arrow_pd_parser.parse import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
)

from typing import Union

from arrow_pd_parser.pa_pd import arrow_to_pandas

import pyarrow as pa
from pyarrow import parquet as pq, fs, csv, json

from data_linter.validators.base import (
    BaseTableValidator,
)

from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter

log = logging.getLogger("root")
default_date_format = "%Y-%m-%d"
default_datetime_format = "%Y-%m-%d %H:%M:%S"


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

        df = _parse_data_to_pandas(self.filepath, self.table_params, self.metadata)
        self.validate_df(df)

    def get_response_dict(self):
        return self.response.get_result()

    def validate_df(self, df):

        meta_cols = [col for col in self.metadata["columns"] if col["name"] in df]

        for m in meta_cols:
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
        if func.__name__ == "_min_max_test" and _check_meta_has_params(
            ["minimum", "maximum"], mc
        ):
            return func(*args, **kwargs)
        elif func.__name__ == "_min_max_length_test" and _check_meta_has_params(
            ["minLength", "maxLength"], mc
        ):
            return func(*args, **kwargs)
        elif func.__name__ == "_pattern_test" and _check_meta_has_params(
            ["pattern"], mc
        ):
            return func(*args, **kwargs)
        elif func.__name__ == "_enum_test" and _check_meta_has_params(["enum"], mc):
            return func(*args, **kwargs)
        elif func.__name__ == "_nullable_test" and not _check_meta_has_params(
            [None, True], [mc.get("nullable")]
        ):
            return func(*args, **kwargs)
        elif func.__name__ == "_date_format_test" and mc.get("type", "").startswith(
            "date"
        ):
            return func(*args, **kwargs)
        elif func.__name__ == "_datetime_format_test" and mc.get("type", "").startswith(
            "timestamp"
        ):
            return func(*args, **kwargs)
        else:
            pass

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


def _fill_res_dict(col, col_oob, res_dict) -> dict:

    valid = not col_oob.any()
    res_dict["valid"] = valid

    if not valid:
        col_oob = col_oob.fillna(False)
        unexpected_index_list = col_oob.index[col_oob].tolist()
        unexpected_list = col[unexpected_index_list].tolist()

        res_dict["unexpected_index_list"] = unexpected_index_list
        res_dict["unexpected_list"] = unexpected_list

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


def _parse_data_to_pandas(filepath: str, table_params: dict, metadata: dict):
    """
    Reads in the data from the given filepath and returns
    a dataframe
    """

    meta_col_names = [
        c["name"]
        for c in metadata["columns"]
        if c["name"] not in metadata.get("partitions", [])
    ]

    # For string based file types convert make arrow readers read them in as strings
    # validators will still treat these as dates but will run validation against strings
    # cols expecting values to match a timestamp format
    if "json" in metadata["file_format"] or "csv" in metadata["file_format"]:
        md_obj = Metadata.from_dict(metadata)
        cols = md_obj.columns

        cols_to_force_str_read_in = []
        for c in cols:
            if c["type"].startswith("time") or c["type"].startswith("date"):
                c["type"] = "string"
                c["type_category"] = "string"
                cols_to_force_str_read_in.append(c["name"])

        md_obj.columns = cols
        ac = ArrowConverter()
        arrow_schema = ac.generate_from_meta(md_obj)

        ts_as_str_schema = pa.schema([])
        for cname in cols_to_force_str_read_in:
            ts_as_str_schema = ts_as_str_schema.append(arrow_schema.field(cname))

    # Set the reader type
    if filepath.startswith("s3://"):
        reader_fs = fs.S3FileSystem(region="eu-west-1")
        fp_for_file_reader = filepath.replace("s3://", "", 1)

    else:
        reader_fs = fs.LocalFileSystem()
        fp_for_file_reader = filepath

    with reader_fs.open_input_stream(fp_for_file_reader) as f:
        if "csv" in metadata["file_format"]:

            # Safer CSV load for newlines_in_values set to True
            if table_params.get("expect-header", True):
                po = csv.ParseOptions(newlines_in_values=True)
            else:
                po = csv.ParseOptions(
                    newlines_in_values=True, column_names=meta_col_names
                )

            if ts_as_str_schema:
                co = csv.ConvertOptions(column_types=ts_as_str_schema)
            else:
                co = None

            df = pa_read_csv_to_pandas(
                input_file=f,
                schema=arrow_schema,
                expect_full_schema=False,
                parse_options=po,
                convert_options=co,
            )
            # dates/datetimes == string

        elif "json" in metadata["file_format"]:

            po = json.ParseOptions(
                newlines_in_values=True,
                explicit_schema=ts_as_str_schema if ts_as_str_schema else None,
            )

            df = pa_read_json_to_pandas(
                input_file=f,
                schema=arrow_schema,
                expect_full_schema=False,
                parse_options=po,
            )
            # dates/datetimes == string

        elif "parquet" in metadata["file_format"]:
            df = arrow_to_pandas(pq.read_table(f))
            # dates/datetimes == datetime / date

        else:
            raise ValueError(
                f"Unknown file_format in metadata: {metadata['file_format']}."
            )

    if table_params.get("row-limit"):
        df = df.sample(table_params.get("row-limit"))

    if table_params.get("headers-ignore-case"):
        df_cols = [c.lower() for c in df.columns]
        df.columns = df_cols

    if table_params.get("only-test-cols-in-metadata", False):
        keep_cols = [c for c in df.columns if c in meta_col_names]
        df = df[keep_cols]

    return df
