import logging
from datetime import datetime
import io
import boto3
from typing import Tuple

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
    log_stringio = io.StringIO()
    handler = logging.StreamHandler(log_stringio)
    log.setLevel(logging.INFO)

    log_formatter = logging.Formatter(
        fmt="%(asctime)s | %(module)s | %(levelname)s | %(context)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(log_formatter)
    log.addHandler(handler)

    cf = ContextFilter()
    log.addFilter(cf)

    return log, log_stringio


def upload_log(body: str, s3_path: str):
    s3_client = boto3.client("s3")
    b, k = s3_path_to_bucket_key(s3_path)
    s3_client.put_object(Body=body, Bucket=b, Key=k)
