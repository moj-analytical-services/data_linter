import os

import logging
from datetime import datetime
import atexit
import io
from goodtables import validate

from logging_functions import upload_log, logging_setup

from functions import (
    load_and_validate_config,
    validate_data,
    get_validator_name,
)


def main():
    # set up logging
    log, log_stringio = logging_setup()

    log.info("Loading config")
    config = load_and_validate_config()
    log_path = os.path.join(config["log-base-path"], get_validator_name() + ".log")
    log.info("Running validation")
    validate_data(config)

    atexit.register(upload_log, body=log_stringio.getvalue(), s3_path=log_path)


if __name__ == "__main__":
    main()
