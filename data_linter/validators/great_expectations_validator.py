import logging
from data_linter.validators.base import BaseTableValidator

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

pd_conversion = copy.deepcopy(numerical_conversion)
pd_conversion["datetime"] = "str"
pd_conversion["date"] = "str"
pd_conversion["boolean"] = "str"
pd_conversion["character"] = "str"


log = logging.getLogger("root")


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

    def write_validation_errors_to_log(self):
        if not self.response["valid"]:
            for colname, col_results in self.response.items():
                for test_name, test_result in col_results.items():
                    if not test_result.get("success", True):
                        if "partial_unexpected_list" in test_result:
                            vals = test_result["partial_unexpected_list"]
                            inds = test_result.get("partial_unexpected_index_list", "Unknown")
                            examples = f"Examples {vals} at rows {inds}"
                        else:
                            examples = "Observation: " + test_result.get("observed_value", "unknown check full results")

                        err = f"{test_name} failed. {examples}"
                        log.error(err, extra={"context": "VALIDATION"})

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.

        Using Great Expectations.
        """
        df = _parse_data_to_pandas(
            self.filepath,
            self.table_params,
            self.metadata,
            row_limit = self.table_params.get("row-limit")
        )

        if self.metadata["data_format"] != "parquet":
            df = _convert_df_to_meta_for_testing(df, self.metadata)

        response = validate_df_with_ge(df, self.metadata)
        return response


### FUNCTION DEFINITIONS ###

def _convert_df_to_meta_for_testing(df, metadata):
    df_cols = list(df.columns)
    cols = [c for c in metadata["columns"] if c["name"] in df_cols]
    # results = []
    for c in cols:
        if c["type"] in numerical_conversion:
            df[c["name"]] = pd.to_numeric(df[c["name"]])
            df[c["name"]] = df[c["name"]].astype(numerical_conversion[c["type"]]) # in case pandas converts to int rather than float
        else:
            df[c["name"]] = df[c["name"]].astype("string")

    return df


def _parse_data_to_pandas(
    filepath: str, table_params: dict, metadata: dict, nrows: int = None
):

    meta_col_names = [c["name"] for c in metadata["columns"] if c["name"] not in metadata.get("partitions", [])]
    if metadata["data_format"] == "csv":
        names = None
        header = "infer" if table_params.get("expect-header", True) else None
        if header is None:
            names = meta_col_names

        if filepath.startswith("s3://"):
            df = wr.s3.read_csv([filepath], header=header, dtype=default_pd_in_type, names=names, nrows=nrows)
        else:
            df = pd.read_csv(filepath, header=header, dtype=default_pd_in_type, names=names, nrows=nrows)

    elif metadata["data_format"] == "json":
        if filepath.startswith("s3://"):
            df = wr.s3.read_json([filepath], lines=True, dtype=default_pd_in_type, nrows=nrows)
        else:
            df = pd.read_json(filepath, lines=True, dtype=default_pd_in_type, nrows=nrows)

    elif metadata["data_format"] == "parquet":
        if filepath.startswith("s3://"):
            df = wr.s3.read_parquet([filepath], nrows=nrows)
        else:
            df = pd.read_parquet(filepath, nrows=nrows)

    else:
        data_fmt = metadata["data_format"]
        raise ValueError(f"metadata data_format ({data_fmt}) not supported for GE validator.")

    if table_params.get("headers-ignore-case"):
        df_cols = [c.lower() for c in df.columns]
        df.columns = df_cols

    if table_params.get("only-test-cols-in-metadata", False):
        keep_cols = [c for c in df.columns if c in meta_col_names]
        df = df[keep_cols]

    return df


def validate_df_with_ge(df, metadata, cols_to_skip=[], result_format="BASIC"):
    dfe = ge.dataset.PandasDataset(df)

    overall_pass = True

    table_results = {}
    metacols = [
        col for col in metadata["columns"]
        if col["name"] not in (metadata.get("partitions", []) + cols_to_skip)
    ]
    metacol_names = [c["name"] for c in metacols]
    table_results["header-tests"] = validate_headers(df, metacol_names)
    overall_pass = overall_pass and table_results["ge-header-tests"]["valid"]

    for c in metacols:
        table_results[c["name"]] = None
        if c["name"] in list(df.columns):
            table_results[c["name"]] = column_validation(dfe, c, result_format)
            overall_pass = overall_pass and table_results[c["name"]]["valid"]

    table_results["valid"] = overall_pass
    return table_results


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

    return {"valid": overall_pass, "details": extra_info}


def column_validation(dfe, metacol, result_format):
    n = metacol["name"]
    col_results = {}
    overall_pass = True

    # unique test
    if metacol.get("unique"):
        col_results["unique"] = ge_unique_test(dfe, n, result_format)

    # nullable test
    if not metacol.get("nullable", True):
        col_results["nullable"] = ge_nullable_test(dfe, n, result_format)

    # min / max numerical test
    mi = metacol.get("minimum")
    ma = metacol.get("maximum")
    if mi or ma:
        col_results["min-max"] = ge_min_max_test(dfe, n, mi, ma, result_format)

    # pattern/regex test
    if metacol.get("pattern"):
        col_results["pattern"] = ge_pattern_test(dfe, n, metacol.get("pattern"), result_format)

    # enum test
    if metacol.get("enum"):
        col_results["enum"] = ge_enum_test(dfe, n, metacol.get("enum"), result_format)

    # min / max length test
    mil = metacol.get("minLength")
    mal = metacol.get("maxLength")
    if mil or mal:
        col_results["min-max-length"] = ge_min_max_length_test(dfe, n, mil, mal, result_format)

    for k, v in col_results.items():
        overall_pass = overall_pass and v["success"]

    col_results["valid"] = overall_pass
    return col_results


def ge_min_max_length_test(dfe, colname, min_length, max_length, result_format=None):
    result = dfe.expect_column_value_lengths_to_be_between(
        colname,
        min_value=min_length,
        max_value=max_length,
        result_format={'result_format': result_format}
    )
    if not result.success:
        log.error(f"col: {colname} not between min/max length values", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_enum_test(dfe, colname, enum, result_format=None):
    result = dfe.expect_column_values_to_be_in_set(
        colname,
        value_set=enum,
        result_format={'result_format': result_format}
    )
    if not result.success:
        log.error(f"col: {colname} has values outside of enum set", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_pattern_test(dfe, colname, pattern, result_format=None):
    result = dfe.expect_column_values_to_match_regex(
        colname,
        regex=pattern,
        result_format={'result_format': result_format}
    )
    if not result.success:
        log.error(f"col: {colname} did not match regex pattern", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_min_max_test(dfe, colname, minimum, maximum, result_format=None):

    result = dfe.expect_column_values_to_be_between(
        colname,
        min_value=minimum,
        max_value=maximum,
        result_format={'result_format': result_format}
    )
    if not result.success:
        log.error(f"col: {colname} not between min/max values", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_unique_test(dfe, colname, result_format=None):
    result = dfe.expect_column_values_to_be_unique(
        colname,
        result_format={'result_format': result_format}
    )
    if not result.success:
        log.error(f"col: {colname} not unique", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_nullable_test(dfe, colname, result_format=None):
    result = dfe.expect_column_values_to_not_be_null(
        colname,
        result_format={'result_format': result_format}
    )
    if not result.success:
        log.error(f"col: {colname} contains nulls", extra={"context": "VALIDATION"})
    return result.to_json_dict()
