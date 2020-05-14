from goodtables import validate
from gluejobutils import s3

from functions import load_and_validate_config, validate_data, upload_log

import logging
from datetime import datetime
import atexit
import io

def main():
    # set up logging
    log_name = "log.csv"
    log_name_timestamped = f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} log.csv"
    
    log = logging.getLogger(log_name)
    log_stringio = io.StringIO()
    handler = logging.StreamHandler(log_stringio)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.formatter = logging.Formatter(format="%(levelname)s,%(asctime)s,%(message)s",datefmt="%Y-%m-%d %H:%M:%S")
    
    config = load_and_validate_config()
    validate_data(config)

    atexit.register(upload_log, body=log_stringio.get_value(), bucket=config["log-base-path"], key=log_name_timestamped)


if __name__ == "__main__":
    main()
