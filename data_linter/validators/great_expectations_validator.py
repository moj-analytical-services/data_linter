import logging
from data_linter.validators.base import BaseTableValidator
from copy import deepcopy
from typing import List

optional_import_errors = ""
try:
    import pandas as pd
    import numpy as np
except ImportError as e:
    optional_import_errors += " " + str(e)

try:
    import awswrangler as wr
except ImportError as e:
    optional_import_errors += " " + str(e)


try:
    import great_expectations as ge
except ImportError as e:
    optional_import_errors += " " + str(e)

numerical_conversion = {
    "int": "Int64",
    "long": "Int64",
    "double": np.float,
    "float": np.float,
}

default_pd_in_type = "str"

pd_conversion = deepcopy(numerical_conversion)
pd_conversion["datetime"] = "str"
pd_conversion["date"] = "str"
pd_conversion["boolean"] = "str"
pd_conversion["character"] = "str"


log = logging.getLogger("root")


class ValidatorResult(object):
    """
    Little class to manage adding to validator dict
    """

    def __init__(self, result_dict={}):
        if result_dict:
            self.result = result_dict
        else:
            self.result = {"valid": True}

    def get_result(self, copy=True):
        if copy:
            return deepcopy(self.result)
        else:
            return self.result

    def init_col(self, colname):
        if colname not in self.result:
            self.result[colname] = {"valid": True}

    def get_names_of_column_failures(self, test_names: List[str] = []):
        """

        Return col names which have an overall fail. If test_names is given
        only returns cols that failed those particular tests is given.
        Args:
            test_name (List[str], optional): [description]. List of tests to
            check against Defaults to [].
        """
        non_column_names = ["valid", "validator-table-test-"]
        failed_cols = []
        for colname in self.result:
            if colname in non_column_names:
                continue

            if test_names:
                overall_success = True
                for k, v in self.result[colname].items():
                    if k in test_names:
                        overall_success = overall_success and v.get("success", True)
            else:
                overall_success = self.result[colname].get("valid", True)

            if not overall_success:
                failed_cols.append(colname)

        return failed_cols

    def add_table_test(self, testname, test_result):
        # Same setup treats overall test as a colname
        self.init_col(testname)
        self.result[testname] = test_result
        if "success" in test_result:
            self.result["valid"] = self.result["valid"] and test_result["success"]

    def add_test_to_col(self, colname, testname, test_result):
        self.init_col(colname)

        self.result[colname][testname] = test_result
        if "success" in test_result:
            self.result["valid"] = self.result["valid"] and test_result["success"]


class GreatExpectationsValidator(BaseTableValidator):
    """
    Great expectations data validator
    """

    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: dict,
    ):
        super().__init__(filepath, table_params, metadata)

        if optional_import_errors:
            imp_err = (
                "This validator requires optional packages that are not installed. "
                f"Please see specific errors: {optional_import_errors}"
            )
            raise ImportError(imp_err)

        self.default_result_fmt = "COMPLETE"
        self.col_type_conversion_failures = []
        self.result = ValidatorResult()

    def write_validation_errors_to_log(self):
        table_result = self.result.get_result()
        if not table_result["valid"]:
            log.error(str(table_result), extra={"context": "VALIDATION"})

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.

        Using Great Expectations.
        """
        df = _parse_data_to_pandas(
            self.filepath,
            self.table_params,
            self.metadata,
            nrows=self.table_params.get("row-limit"),
        )

        if self.metadata["data_format"] != "parquet":
            df = _convert_df_to_meta_for_testing(df, self.metadata, self.result)

        validate_df_with_ge(df, self.metadata, self.result)
        return self.result.get_result()


def _convert_df_to_meta_for_testing(df, metadata, result: ValidatorResult):

    df_cols = list(df.columns)
    cols = [c for c in metadata["columns"] if c["name"] in df_cols]

    for c in cols:
        try:
            if c["type"] in numerical_conversion:
                df[c["name"]] = pd.to_numeric(df[c["name"]])
                df[c["name"]] = df[c["name"]].astype(
                    numerical_conversion[c["type"]]
                )  # in case pandas converts to int rather than float
            else:
                df[c["name"]] = df[c["name"]].astype("string")
            result.add_test_to_col(
                c["name"], "test-type-conversion", {"success": True, "desc": None}
            )
        except Exception as e:
            t = numerical_conversion.get(c["type"], "string")
            e = (
                f"Column {c['name']} could not be cast to pandas type {t}."
                f"Error: {str(e)}"
            )
            log.error(e, extra={"context": "VALIDATION"})
            result.add_test_to_col(
                c["name"], "test-type-conversion", {"success": False, "desc": e}
            )

    return df


def _parse_data_to_pandas(
    filepath: str, table_params: dict, metadata: dict, nrows: int = None
):

    meta_col_names = [
        c["name"]
        for c in metadata["columns"]
        if c["name"] not in metadata.get("partitions", [])
    ]
    if metadata["data_format"] == "csv":
        names = None
        header = "infer" if table_params.get("expect-header", True) else None
        if header is None:
            names = meta_col_names

        if filepath.startswith("s3://"):
            df = wr.s3.read_csv(
                [filepath],
                header=header,
                dtype=default_pd_in_type,
                names=names,
                nrows=nrows,
            )
        else:
            df = pd.read_csv(
                filepath,
                header=header,
                dtype=default_pd_in_type,
                names=names,
                nrows=nrows,
            )

    elif metadata["data_format"] == "json":
        if filepath.startswith("s3://"):
            df = wr.s3.read_json(
                [filepath], lines=True, dtype=default_pd_in_type, nrows=nrows
            )
        else:
            df = pd.read_json(
                filepath, lines=True, dtype=default_pd_in_type, nrows=nrows
            )

    elif metadata["data_format"] == "parquet":
        if filepath.startswith("s3://"):
            df = wr.s3.read_parquet([filepath], nrows=nrows)
        else:
            df = pd.read_parquet(filepath, nrows=nrows)

    else:
        data_fmt = metadata["data_format"]
        raise ValueError(
            f"metadata data_format ({data_fmt}) not supported for GE validator."
        )

    if table_params.get("headers-ignore-case"):
        df_cols = [c.lower() for c in df.columns]
        df.columns = df_cols

    if table_params.get("only-test-cols-in-metadata", False):
        keep_cols = [c for c in df.columns if c in meta_col_names]
        df = df[keep_cols]

    return df


def validate_df_with_ge(df, metadata, result: ValidatorResult, result_format="BASIC"):
    dfe = ge.dataset.PandasDataset(df)

    # Skip cols that could not be cast properly
    cols_to_skip = result.get_names_of_column_failures("test-type-conversion")

    # Get cols to test
    metacols = [
        col
        for col in metadata["columns"]
        if col["name"] not in (metadata.get("partitions", []) + cols_to_skip)
    ]
    metacol_names = [c["name"] for c in metacols]
    header_tests = validate_headers(df, metacol_names)
    result.add_table_test("validator-table-test-header", header_tests)

    for c in metacols:
        if c["name"] in list(df.columns):
            column_validation(dfe, c, result, result_format)


def validate_headers(df, metacols, ignore_missing_cols=False):
    extra_info = {}
    df_cols = list(df.columns)
    full_match = metacols == df_cols
    df_missing = set(df_cols).difference(metacols)
    df_extra = set(metacols).difference(df_cols)

    if ignore_missing_cols:
        overall_pass = not df_extra
        if df_missing:
            warn_msg1 = f"data missing headers: {df_missing}"
            log.warning(warn_msg1, extra={"context": "VALIDATION"})
    else:
        overall_pass = full_match

    if not overall_pass:
        if df_missing:
            err_msg1 = f"data missing headers: {df_missing}"
            log.error(err_msg1, extra={"context": "VALIDATION"})

        if df_extra:
            err_msg2 = f"data has extra columns: {df_extra}"
            log.error(err_msg2, extra={"context": "VALIDATION"})

        extra_info["missing"] = list(df_missing)
        extra_info["extra"] = list(df_extra)

    return {"success": overall_pass, "details": extra_info}


def column_validation(dfe, metacol, result: ValidatorResult, result_format):
    n = metacol["name"]

    # unique test
    if metacol.get("unique"):
        result.add_test_to_col(n, "unique", ge_unique_test(dfe, n, result_format))

    # nullable test
    if not metacol.get("nullable", True):
        result.add_test_to_col(n, "nullable", ge_nullable_test(dfe, n, result_format))

    if metacol["type"] in ["date", "datetime"]:
        result.add_test_to_col(
            n,
            "datetime-format",
            ge_test_datetime_format(
                dfe, n, metacol["type"], metacol.get("format"), result_format
            ),
        )

    # min / max numerical test
    mi = metacol.get("minimum")
    ma = metacol.get("maximum")
    if mi or ma:
        result.add_test_to_col(
            n, "min-max", ge_min_max_test(dfe, n, mi, ma, result_format)
        )

    # pattern/regex test
    if metacol.get("pattern"):
        result.add_test_to_col(
            n, "pattern", ge_pattern_test(dfe, n, metacol.get("pattern"), result_format)
        )

    # enum test
    if metacol.get("enum"):
        result.add_test_to_col(
            n, "enum", ge_enum_test(dfe, n, metacol.get("enum"), result_format)
        )

    # min / max length test
    mil = metacol.get("minLength")
    mal = metacol.get("maxLength")
    if mil or mal:
        result.add_test_to_col(
            n, "min-max-length", ge_min_max_length_test(dfe, n, mil, mal, result_format)
        )


def ge_test_datetime_format(dfe, colname, coltype, date_format, result_format=None):
    if date_format is None:
        date_format = "%Y-%m-%d" if coltype == "date" else "%Y-%m-%d %H:%M:%S"
    result = dfe.expect_column_values_to_match_strftime_format(
        colname,
        strftime_format=date_format,
        result_format={"result_format": result_format},
    )
    if not result.success:
        log.error(
            f"col: {colname} not between min/max length values",
            extra={"context": "VALIDATION"},
        )
    return result.to_json_dict()


def ge_min_max_length_test(dfe, colname, min_length, max_length, result_format=None):
    result = dfe.expect_column_value_lengths_to_be_between(
        colname,
        min_value=min_length,
        max_value=max_length,
        result_format={"result_format": result_format},
    )
    if not result.success:
        log.error(
            f"col: {colname} not between min/max length values",
            extra={"context": "VALIDATION"},
        )
    return result.to_json_dict()


def ge_enum_test(dfe, colname, enum, result_format=None):
    result = dfe.expect_column_values_to_be_in_set(
        colname, value_set=enum, result_format={"result_format": result_format}
    )
    if not result.success:
        log.error(
            f"col: {colname} has values outside of enum set",
            extra={"context": "VALIDATION"},
        )
    return result.to_json_dict()


def ge_pattern_test(dfe, colname, pattern, result_format=None):
    result = dfe.expect_column_values_to_match_regex(
        colname, regex=pattern, result_format={"result_format": result_format}
    )
    if not result.success:
        log.error(
            f"col: {colname} did not match regex pattern",
            extra={"context": "VALIDATION"},
        )
    return result.to_json_dict()


def ge_min_max_test(dfe, colname, minimum, maximum, result_format=None):

    result = dfe.expect_column_values_to_be_between(
        colname,
        min_value=minimum,
        max_value=maximum,
        result_format={"result_format": result_format},
    )
    if not result.success:
        log.error(
            f"col: {colname} not between min/max values",
            extra={"context": "VALIDATION"},
        )
    return result.to_json_dict()


def ge_unique_test(dfe, colname, result_format=None):
    result = dfe.expect_column_values_to_be_unique(
        colname, result_format={"result_format": result_format}
    )
    if not result.success:
        log.error(f"col: {colname} not unique", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_nullable_test(dfe, colname, result_format=None):
    result = dfe.expect_column_values_to_not_be_null(
        colname, result_format={"result_format": result_format}
    )
    if not result.success:
        log.error(f"col: {colname} contains nulls", extra={"context": "VALIDATION"})
    return result.to_json_dict()
