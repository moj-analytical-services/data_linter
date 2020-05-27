from goodtables import validate
from gluejobutils import s3

from functions import load_and_validate_config, validate_data, upload_log

import logging
from datetime import datetime
import atexit
import io
from logging_functions import upload_logs, logging_setup

def main():
    # set up logging
    logging_setup()
    
    config = load_and_validate_config()
    validate_data(config)

    atexit.register(upload_logs, body=log_stringio.get_value(), bucket=config["log-base-path"], key=log_name_timestamped)


if __name__ == "__main__":
    main()
