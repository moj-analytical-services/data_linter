from python_scripts.functions import load_and_validate_config, generate_iam_config

config = load_and_validate_config()
generate_iam_config(config)