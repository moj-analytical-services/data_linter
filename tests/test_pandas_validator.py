import pytest
from data_linter.validators import pandas_validator as pv
import pandas as pd

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
    res = pv._nullable_test(col, {"name": "test_col", "nullable": False})
    assert isinstance(res, dict)
    assert expected_valid == res["valid"]

    # assert pv._nullable_test(col, {"name": "test_col", "nullable": True})


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
    res = pv._min_max_length_test(col, meta_col)
    assert isinstance(res, dict)
    assert res["valid"] is False


def test_pattern_test():
    pass


def test_enum_test():
    pass


def test_datetime_format_test():
    pass


def test_date_format_test():
    pass


def test_validation_function_skips():
    assert pv._nullable_test(str_is_null, {"name": "c"}) is None
    assert pv._nullable_test(str_is_null, {"name": "c", "nullable": True}) is None
    assert pv._min_max_test(str_is_null, {"name": "c"}) is None
    assert pv._min_max_length_test(str_is_null, {"name": "c"}) is None
    assert pv._pattern_test(str_is_null, {"name": "c"}) is None
    assert pv._enum_test(str_is_null, {"name": "c"}) is None
    assert pv._datetime_format_test(str_is_null, {"name": "c"}) is None
    assert pv._date_format_test(str_is_null, {"name": "c"}) is None
