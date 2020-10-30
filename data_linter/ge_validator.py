# Add ons so allowed to skip if need be
import logging
import copy

log = logging.getLogger("root")

try:
    import great_expectations as ge
except ImportError as e:
    log.warning(str(e))

try:
    import pandas as pd
    import numpy as np
except ImportError as e:
    log.warning(str(e))

try:
    import awswrangler as wr
except ImportError as e:
    log.warning(str(e))


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


def _convert_df_to_meta_for_testing(df, metadata):
    df_cols = list(df.columns)
    cols = [c for c in metadata["columns"] if c["name"] in df_cols]
    for c in cols:
        if c["type"] in numerical_conversion:
            df[c["name"]] = pd.to_numeric(df[c["name"]])
            df[c["name"]] = df[c["name"]].astype(numerical_conversion[c["type"]]) # in case pandas converts to int rather than float
        else:
            df[c["name"]] = df[c["name"]].astype("string")
    return df


def _parse_data_to_pandas(
    filepath: str, table_params: dict, metadata: dict
):

    meta_col_names = [c["name"] for c in metadata["columns"] if c["name"] not in metadata.get("partitions", [])]
    if metadata["data_format"] == "csv":
        names = None
        header = "infer" if table_params.get("expect-header", True) else None
        if header is None:
            names = meta_col_names
        
        if filepath.startswith("s3://"):
            df = wr.s3.read_csv([filepath], header=header, dtype=default_pd_in_type, names=names)
        else:
            df = pd.read_csv(filepath, header=header, dtype=default_pd_in_type, names=names)

    elif metadata["data_format"] == "json":
        if filepath.startswith("s3://"):
            df = wr.s3.read_json([filepath], lines=True, dtype=default_pd_in_type)
        else:
            df = pd.read_json(filepath, lines=True, dtype=default_pd_in_type)

    elif metadata["data_format"] == "parquet":
        if filepath.startswith("s3://"):
            df = wr.s3.read_parquet([filepath])
        else:
            df = pd.read_parquet(filepath)

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


def validate_df_with_ge(df, metadata):
    dfe = ge.dataset.PandasDataset(df)

    overall_pass = True

    col_tests = {}
    metacols = [col for col in metadata["columns"] if col["name"] not in metadata.get("partitions", [])]
    metacol_names = [c["name"] for c in metacols]
    col_tests["ge-header-tests"] = validate_headers(df, metacol_names)
    overall_pass = overall_pass and col_tests["ge-header-tests"]["valid"]

    for c in metacols:
        if c["name"] in list(df.columns):
            col_tests[c["name"]] = {}
            col_tests[c["name"]]["results"] = column_validation(dfe, c)
            overall_pass = overall_pass and col_tests[c["name"]]["results"]["valid"]

    col_tests["valid"] = overall_pass
    return col_tests


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


def column_validation(dfe, metacol):
    n = metacol["name"]
    results = {}
    overall_pass = True

    # type test
    results["type"] = ge_type_test(dfe, n, metacol["type"], metacol.get("format"))

    # unique test
    if metacol.get("unique"):
        results["unique"] = ge_unique_test(dfe, n)

    # nullable test
    if not metacol.get("nullable", True):
        results["nullable"] = ge_nullable_test(dfe, n)

    # min / max numerical test
    mi = metacol.get("minimum")
    ma = metacol.get("maximum")
    if mi or ma:
        results["min-max"] = ge_min_max_test(dfe, n, mi, ma)

    # pattern/regex test
    if metacol.get("pattern"):
        results["pattern"] = ge_pattern_test(dfe, n, metacol.get("pattern"))

    # enum test
    if metacol.get("enum"):
        results["enum"] = ge_enum_test(dfe, n, metacol.get("enum"))

    # min / max length test
    mil = metacol.get("minLength")
    mal = metacol.get("maxLength")
    if mil or mal:
        results["min-max-length"] = ge_min_max_length_test(dfe, n, mil, mal)

    for k, v in results.items():
        overall_pass = overall_pass and v["success"]

    results["valid"] = overall_pass
    return results


def ge_type_test(dfe, colname, coltype, dt_fmt=None):

    try:
        if coltype in ["date", "datetime"]:
            if dt_fmt is None:
                dt_fmt = "%Y-%m-%d" if coltype == "date" else "%Y-%m-%d %H:%M:%S" 
            result = dfe.expect_column_values_to_match_strftime_format(colname, strftime_format=dt_fmt, result_format="SUMMARY")
        else:
            result = dfe.expect_column_values_to_be_of_type(colname, pd_conversion.get(coltype, coltype), result_format="SUMMARY")
    except Exception as e:
        log.error(f"col: {colname}, type: {coltype} error. {str(e)}", extra={"context": "VALIDATION"})
        return {"success": False}
    else:
        if not result.success:
            log.error(f"col: {colname} was not of the expected type", extra={"context": "VALIDATION"})
        return result.to_json_dict()


def ge_min_max_length_test(dfe, colname, min_length, max_length):
    result = dfe.expect_column_value_lengths_to_be_between(
        colname,
        min_value=min_length,
        max_value=max_length,
        result_format="SUMMARY"
    )
    if not result.success:
        log.error(f"col: {colname} not between min/max length values", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_enum_test(dfe, colname, enum):
    result = dfe.expect_column_values_to_be_in_set(
        colname,
        value_set=enum,
        result_format="SUMMARY"
    )
    if not result.success:
        log.error(f"col: {colname} has values outside of enum set", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_pattern_test(dfe, colname, pattern):
    result = dfe.expect_column_values_to_match_regex(
        colname,
        regex=pattern
    )
    if not result.success:
        log.error(f"col: {colname} did not match regex pattern", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_min_max_test(dfe, colname, minimum, maximum):

    result = dfe.expect_column_values_to_be_between(
        colname,
        min_value=minimum,
        max_value=maximum,
        result_format="SUMMARY"
    )
    if not result.success:
        log.error(f"col: {colname} not between min/max values", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_unique_test(dfe, colname):
    result = dfe.expect_column_values_to_be_unique(colname, result_format="SUMMARY")
    if not result.success:
        log.error(f"col: {colname} not unique", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def ge_nullable_test(dfe, colname):
    result = dfe.expect_column_values_to_not_be_null(colname, result_format="SUMMARY")
    if not result.success:
        log.error(f"col: {colname} contains nulls", extra={"context": "VALIDATION"})
    return result.to_json_dict()


def _ge_read_data_and_validate(filepath: str, table_params: dict, metadata: dict):

    df = _parse_data_to_pandas(
        filepath,
        table_params,
        metadata
    )

    if metadata["data_format"] != "parquet":
        df = _convert_df_to_meta_for_testing(df, metadata)

    response = validate_df_with_ge(df, metadata)
    return response
