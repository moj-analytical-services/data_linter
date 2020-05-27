from goodtables import validate
from gluejobutils import s3

from functions import load_and_validate_config, validate_data

import logging
from datetime import datetime
import atexit
import io
from logging_functions import upload_logs, logging_setup


def main():
    # set up logging
    log = logging_setup()

    config = load_and_validate_config()
    validate_data(config)

    atexit.register(
        upload_logs,
        body=log["log_stringio"].getvalue(),
        bucket=config["log-base-path"],
        key=log["log_name_timestamped"],
    )


if __name__ == "__main__":
    main()
