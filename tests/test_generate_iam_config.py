import pytest
import yaml
from python_scripts.functions import load_and_validate_config, generate_iam_config



def test_generate_iam_config():
    config = load_and_validate_config("tests/data/config.yaml")
    with open("tests/data/iam_config.yaml") as f:
        expected_output = yaml.safe_load(f)
    
    generate_iam_config(config,"tests/data/test_iam.yaml")

    with open("tests/data/test_iam.yaml") as f:
        test_output = yaml.safe_load(f)
    
    assert expected_output == test_output