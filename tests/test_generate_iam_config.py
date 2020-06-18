import pytest
import yaml
from python_scripts.functions import load_and_validate_config, generate_iam_config

@pytest.mark.parametrize("test_input, expected", [
    ("config_generate_iam_with_fail_path.yaml", "test_iam_fail_path.yaml"),
    ("config_generate_iam_without_fail_path.yaml", "test_iam_no_fail_path.yaml")
])
def test_generate_iam_config(test_input, expected):
    config = load_and_validate_config("tests/data/", test_input)
    with open(f"tests/data/{expected}") as f:
        expected_output = yaml.safe_load(f)
    
    generate_iam_config(config, "tests/data/test_iam.yaml", overwrite_config=True)

    with open("tests/data/test_iam.yaml") as f:
        test_output = yaml.safe_load(f)

    assert expected_output == test_output
