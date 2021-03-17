import pytest
import toml
import data_linter as dl
from data_linter.validation import load_and_validate_config
from jsonschema.exceptions import ValidationError
import json
import os


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("example_config_fail_required.yaml", r"required"),
        ("example_config_fail_enum.yaml", r"enum"),
        ("example_config_fail_type.yaml", r"type"),
    ],
)
def test_load_and_validate_config_fail(test_input, expected):
    with pytest.raises(ValidationError, match=expected) as e:
        _ = load_and_validate_config(os.path.join("tests/data/inputs", test_input))
        assert e.validator == expected


def test_load_and_validate_config_pass():
    with open("tests/data/expected/expected_pass.json", "r") as f:
        expected_pass = json.load(f)
    c = load_and_validate_config("tests/data/inputs/example_config_pass.yaml")
    assert c == expected_pass


def test_pyproject_toml_matches_version():
    with open("pyproject.toml") as f:
        proj = toml.load(f)
    assert dl.__version__ == proj["tool"]["poetry"]["version"]
