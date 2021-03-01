import logging
import io
import boto3
import os
from typing import Tuple
from datetime import datetime

from dataengineeringutils3.s3 import s3_path_to_bucket_key


class ContextFilter(logging.Filter):
    """
    This is just overkill to apply a default context param to the log.
    But it does mean I don't have to define extra everytime I wanna log.
    So keeping it.
    """

    def filter(self, record):
        if not getattr(record, "context", None):
            record.context = "PROCESSING"
        return True


def logging_setup() -> Tuple[logging.Logger, io.StringIO]:

    log = logging.getLogger("root")
    log.setLevel(logging.DEBUG)

    log_stringio = io.StringIO()
    handler = logging.StreamHandler(log_stringio)

    log_formatter = logging.Formatter(
        fmt="%(asctime)s | %(funcName)s | %(levelname)s | %(context)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(log_formatter)
    log.addHandler(handler)

    # Add console output
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(log_formatter)
    log.addHandler(console)

    cf = ContextFilter()
    log.addFilter(cf)

    return log, log_stringio


def upload_log(log: logging.Logger, log_stringio: io.StringIO, log_path: str):
    log_path_is_s3 = log_path.startswith("s3://")

    if log_path:
        if log_path_is_s3:
            s3_client = boto3.client("s3")
            b, k = s3_path_to_bucket_key(log_path)
            s3_client.put_object(Body=log_stringio.getvalue(), Bucket=b, Key=k)
        else:
            dir_out = os.path.dirname(log_path)
            if not os.path.exists(dir_out):
                os.makedirs(dir_out, exist_ok=True)
            with open(log_path, "w") as log_out:
                log_out.write(log_stringio.getvalue())
    else:
        log.error(
            "An error occurred but no log path registered, "
            "likely due to issue with config, so logs not saved."
        )


def get_log_fn() -> str:
    return f"data-linter-{int(datetime.utcnow().timestamp())}.log"


def get_temp_log_basepath(config: dict) -> str:
    """
        Defines temp log basepath for parallel runs

    Args:
        config (dict): A data linter config

    Returns:
        str: tmp base path for log for a parallelised run
    """
    temp_log_basepath = os.path.join(
        config["log-base-path"], "data_linter_temporary_fs/"
    )
    return temp_log_basepath


def get_temp_log_path_from_config(config: dict) -> str:
    """
        Defines temp log path for parallel runs

    Args:
        config (dict): A data linter config

    Returns:
        str: tmp path for log for a parallelised run
    """
    temp_log_path = os.path.join(get_temp_log_basepath(config), "init", get_log_fn())
    return temp_log_path


def get_main_log_path_from_config(config: dict) -> str:
    """
        Defines main log file path for a data linter run

    Args:
        config (dict): A data linter config

    Returns:
        str: master log file path for a datalinter run
    """
    log_path = os.path.join(
        config["log-base-path"], "data-linter-main-logs", get_log_fn()
    )
    return log_path
