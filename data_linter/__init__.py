import os
import atexit
import io
from data_linter.logging_functions import upload_log, logging_setup

from data_linter.validation import (
    load_and_validate_config,
    validate_data,
    get_validator_name,
)


def run_validation():
    # set up logging
    log, log_stringio = logging_setup()

    log.info("Loading config")
    config = load_and_validate_config()
    log_path = os.path.join(config["log-base-path"], get_validator_name() + ".log")
    log.info("Running validation")
    validate_data(config)

    atexit.register(upload_log, body=log_stringio.getvalue(), s3_path=log_path)