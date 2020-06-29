import os
import atexit
import io
import sys

from data_linter.logging_functions import upload_log, logging_setup

from data_linter.validation import (
    load_and_validate_config,
    validate_data,
    get_validator_name,
)


def run_validation(config_path="config.yaml"):
    # set up logging
    log, log_stringio = logging_setup()

    log.info("Loading config")
    try:
        config = load_and_validate_config(config_path)
        log_path = os.path.join(config["log-base-path"], get_validator_name() + ".log")
        log.info("Running validation")
        validate_data(config)
    except Exception as e:
        upload_log(body=log_stringio.getvalue(), s3_path=log_path)
        log_msg = "Unexpected error hit. Uploading log to {log_path}. Before raising error."
        error_msg = str(e)

        log.error(log_msg)
        log.error(error_msg)

        upload_log(body=log_stringio.getvalue(), s3_path=log_path)

        raise type(e)(str(e)).with_traceback(sys.exc_info()[2])
    else:
        upload_log(body=log_stringio.getvalue(), s3_path=log_path)
