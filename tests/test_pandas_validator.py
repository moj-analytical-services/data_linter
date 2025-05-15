import os
import pytest
from data_linter.validators import pandas_validator as pv
from datetime import datetime

import numpy as np
import pandas as pd

from mojap_metadata.metadata.metadata import Metadata

int_not_null = pd.Series([1, 2, 3, 4, 5], dtype=pd.Int64Dtype())
int_is_null = pd.Series([1, 2, None, 4, 5], dtype=pd.Int64Dtype())

double_not_null = pd.Series([1.0, 2.23545, 3.532513, 4.35, 5.93567])
double_is_null = pd.Series([1.0, 2.23545, 3.532513, None, 5.93567])

str_not_null = pd.Series(
    ["cat", "dog", "fish", "apple", "pineapple"], dtype=pd.StringDtype()
)
str_is_null = pd.Series(
    ["cat", "dog", None, "apple", "pineapple"], dtype=pd.StringDtype()
)

date_str_not_null = pd.Series(
    ["01/01/2020", "29/12/3000", "25/06/1903"], dtype=pd.StringDtype()
)
date_str_is_null = pd.Series([None, "29/12/3000", "25/06/1903"], dtype=pd.StringDtype())

datetime_str_not_null = pd.Series(
    ["2020-01-01 10:00:31", "3000-12-29 09:12:23", "1903-06-25 12:00:00"],
    dtype=pd.StringDtype(),
)
datetime_str_is_null = pd.Series(
    [None, "3000-12-29 09:12:23", "1903-06-25 12:00:00"], dtype=pd.StringDtype()
)

datetime_str_not_null_midnight = pd.Series(
    ["2020-01-01 00:00:00", "3000-12-29 00:00:00", "1903-06-25 00:00:00"],
    dtype=pd.StringDtype(),
)
datetime_str_is_null_midnight = pd.Series(
    [None, "3000-12-29 00:00:00", "1903-06-25 00:00:00"], dtype=pd.StringDtype()
)


@pytest.mark.parametrize(
    "col,expected_valid",
    [
        (int_is_null, False),
        (int_not_null, True),
        (double_is_null, False),
        (double_not_null, True),
        (str_is_null, False),
        (str_not_null, True),
    ],
)
def test_nullable_validation(col, expected_valid):
    """
    Check that nullable validation test gives expected result T/F
    """
    res = pv._nullable_test(col, {"name": "test_col", "nullable": False})
    assert isinstance(res, dict)
    assert expected_valid == res["valid"]


@pytest.mark.parametrize(
    "col",
    [
        int_is_null,
        int_not_null,
        double_is_null,
        double_not_null,
    ],
)
@pytest.mark.parametrize(
    "meta_col",
    [
        {"name": "test_col", "minimum": 0, "maximum": 6},
        {"name": "test_col", "minimum": -6},
        {"name": "test_col", "maximum": 6},
        {"name": "test_col", "minimum": 0.0, "maximum": 6.0},
        {"name": "test_col", "minimum": -6.1},
        {"name": "test_col", "maximum": 6.2},
    ],
)
def test_min_max_validation_pass(col, meta_col):
    """
    Check that every combination of pandas series and
    metadata col passes min/max validation test
    """
    res = pv._min_max_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"]


@pytest.mark.parametrize(
    "col",
    [
        int_is_null,
        int_not_null,
        double_is_null,
        double_not_null,
    ],
)
@pytest.mark.parametrize(
    "meta_col",
    [
        {"name": "test_col", "minimum": 5, "maximum": 6},
        {"name": "test_col", "minimum": 5},
        {"name": "test_col", "maximum": 3},
    ],
)
def test_min_max_validation_fail(col, meta_col):
    """
    Check that every combination of pandas series and
    metadata col fails min/max validation test
    """
    res = pv._min_max_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"] is False


@pytest.mark.parametrize(
    "col",
    [
        str_is_null,
        str_not_null,
    ],
)
@pytest.mark.parametrize(
    "meta_col",
    [
        {"name": "test_col", "minLength": 0, "maxLength": 10},
        {"name": "test_col", "minLength": 0},
        {"name": "test_col", "maxLength": 10},
    ],
)
def test_min_max_length_test_pass(col, meta_col):
    """
    Check that every combination of pandas series and
    metadata col passes min/max length validation test
    """
    res = pv._min_max_length_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"]


@pytest.mark.parametrize(
    "col",
    [
        str_is_null,
        str_not_null,
    ],
)
@pytest.mark.parametrize(
    "meta_col",
    [
        {"name": "test_col", "minLength": 5, "maxLength": 6},
        {"name": "test_col", "minLength": 5},
        {"name": "test_col", "maxLength": 6},
    ],
)
def test_min_max_length_test_fail(col, meta_col):
    """
    Check that every combination of pandas series and
    metadata col passes min/max length validation test
    """
    res = pv._min_max_length_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"] is False


@pytest.mark.parametrize(
    "col_values,expected_valid",
    [
        (["abc-1235", "xyz-4468", "xyz-0284", "acx-8936"], True),
        (["abc-1235", None, "xyz-0284", "acx-8936"], True),
        (["abc-1235", "xyz-4468", "xyz-0284", "1acx-8936"], False),
        (["abc-1235", "xyz-4468", None, "cx-8936"], False),
    ],
)
def test_pattern_test_pass(col_values, expected_valid):
    """
    Check that pattern validation test gives expected result T/F
    """
    col = pd.Series(col_values, dtype=pd.StringDtype())
    meta_col = {"name": "c", "pattern": "^\\D{3}-\\d{4}$"}
    res = pv._pattern_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"] == expected_valid


@pytest.mark.parametrize(
    "col",
    [
        str_is_null,
        str_not_null,
    ],
)
@pytest.mark.parametrize(
    "meta_col,expected_valid",
    [
        (
            {"name": "test_col", "enum": ["cat", "dog", "fish", "apple", "pineapple"]},
            True,
        ),
        ({"name": "test_col", "enum": ["cat", "dog", "fish", "robot"]}, False),
    ],
)
def test_enum_test(col, meta_col, expected_valid):
    """
    Check that pattern validation test gives expected meta and result T/F
    For all nullable and non-nullable strings.
    """
    res = pv._enum_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"] == expected_valid


@pytest.mark.parametrize(
    "col",
    [
        date_str_is_null,
        date_str_not_null,
    ],
)
def test_date_format_test_pass(col):
    meta_col = {"name": "test_col", "type": "date64", "datetime_format": "%d/%m/%Y"}
    res = pv._date_format_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"]


@pytest.mark.parametrize(
    "col",
    [
        datetime_str_is_null_midnight,
        datetime_str_not_null_midnight,
    ],
)
def test_datetime_format_test_pass(col):
    meta_col = {
        "name": "test_col",
        "type": "date32",
        "datetime_format": "%Y-%m-%d %H:%M:%S",
    }
    res = pv._date_format_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"]


@pytest.mark.parametrize(
    "col,meta_col",
    [
        (
            # expects iso by default
            date_str_is_null,
            {"name": "test_col", "type": "date64"},
        ),
        (
            # expects iso by default
            date_str_not_null,
            {"name": "test_col", "type": "date64"},
        ),
        (
            # datetime strs with time component not date
            datetime_str_is_null,
            {
                "name": "test_col",
                "type": "date64",
                "datetime_format": "%Y/%m/%d %H:%M:%S",
            },
        ),
        (
            # datetime strs with time component not date
            datetime_str_not_null,
            {
                "name": "test_col",
                "type": "date64",
                "datetime_format": "%Y/%m/%d %H:%M:%S",
            },
        ),
    ],
)
def test_date_format_test_fail(col, meta_col):
    res = pv._date_format_test(col, meta_col)
    assert isinstance(res, dict)
    assert not res["valid"]


@pytest.mark.parametrize(
    "col",
    [
        datetime_str_is_null,
        datetime_str_not_null,
    ],
)
@pytest.mark.parametrize(
    "datetime_format,expected_valid",
    [
        (None, True),
        ("%Y-%m-%d %H:%M:%S", True),
        ("%d/%m/%Y %H:%M:%S", False),
    ],
)
def test_datetime_format_test(col, datetime_format, expected_valid):
    meta_col = {"name": "test_col", "type": "timestamp(s)"}
    if datetime_format:
        meta_col["datetime_format"] = datetime_format

    res = pv._datetime_format_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"] == expected_valid


def test_validation_function_skips():
    assert pv._nullable_test(str_is_null, {"name": "c"}) is None
    assert pv._nullable_test(str_is_null, {"name": "c", "nullable": True}) is None
    assert pv._min_max_test(str_is_null, {"name": "c"}) is None
    assert pv._min_max_length_test(str_is_null, {"name": "c"}) is None
    assert pv._pattern_test(str_is_null, {"name": "c"}) is None
    assert pv._enum_test(str_is_null, {"name": "c"}) is None
    assert pv._datetime_format_test(str_is_null, {"name": "c"}) is None
    assert pv._date_format_test(str_is_null, {"name": "c"}) is None


@pytest.mark.parametrize(
    "s, expected",
    [
        # str dtype
        (pd.Series(["string", "another string"], dtype=str), True),
        (pd.Series(["string", np.nan], dtype=str), True),
        # string dtype
        (pd.Series(["string", pd.NA], dtype=pd.StringDtype()), True),
        (pd.Series(["string", "another string"], dtype=pd.StringDtype()), True),
        # object dtype (should be same as str but you never know)
        (pd.Series(["string", "another string"], dtype=object), True),
        (pd.Series(["string", np.nan], dtype=object), True),
        # object dtype (but with datetime / date objects)
        (pd.Series([datetime(2021, 1, 1), None], dtype=object), False),
        (pd.Series([datetime(2021, 1, 1, 12, 11, 10), None], dtype=object), False),
        (pd.Series([datetime(2021, 1, 1).date(), None], dtype=object), False),
        # Timestamp[ns] dtype
        (pd.to_datetime(pd.Series(["2021-01-01 12:11:10", None])), False),
    ],
)
def test_check_pandas_series_is_str(s, expected):
    assert pv._check_pandas_series_is_str(s) == expected


@pytest.mark.parametrize(
    "file_name, expect_header, row_limit, exp_row_limit",
    [
        (
            "table1.csv",
            True,
            9,
            9,
        ),
        ("table1.csv", True, 10, 10),
        ("table1.csv", True, 100, 10),
        (
            "table1_no_header.csv",
            False,
            9,
            9,
        ),
        ("table1_no_header.csv", False, 10, 10),
        ("table1_no_header.csv", False, 100, 10),
        (
            "table2.jsonl",
            False,
            9,
            9,
        ),
        ("table2.jsonl", False, 10, 10),
        ("table2.jsonl", False, 100, 10),
    ],
)
def test_row_limits(
    file_name: str, expect_header: bool, row_limit: int, exp_row_limit: int
):
    """
    Tests files against the _read_data_and_validate function.
    runs each file and corresponding meta (table1 or table2)
    with different row limits, to ensure no exceptions are
    raised when the records are sampled.
    Args:
        file_name ([str]): The filename in the dir tests/data/headers/
        row_limit (int): Number of rows supplied to function to sample
        exp_row_limit (int): Expected actual number of rows sampled
    """
    test_folder = "tests/data/headers/"
    full_file_path = os.path.join(test_folder, file_name)

    table_name = file_name.split(".")[0].split("_")[0]
    metadata = Metadata.from_json(
        os.path.join(test_folder, f"meta_data/{table_name}.json")
    )

    table_params = {"expect-header": expect_header, "row-limit": row_limit}
    df, _ = pv._parse_data_to_pandas(full_file_path, table_params, metadata)

    assert len(df) == exp_row_limit
