import pytest
import yaml
import tempfile
from data_linter.validation import load_and_validate_config
from data_linter.iam import generate_iam_config
import os


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("config_generate_iam_with_fail_path.yaml", "test_iam_fail_path.yaml"),
        ("config_generate_iam_without_fail_path.yaml", "test_iam_no_fail_path.yaml"),
    ],
)
def test_generate_iam_config(test_input, expected):
    config = load_and_validate_config(os.path.join("tests/data/inputs", test_input))
    with open(f"tests/data/expected/{expected}") as f:
        expected_output = yaml.safe_load(f)

    with tempfile.TemporaryDirectory() as d:
        generate_iam_config(config, f"{d}/test_iam.yaml", overwrite_config=True)

        with open(f"{d}/test_iam.yaml") as f:
            test_output = yaml.safe_load(f)

    assert expected_output == test_output
