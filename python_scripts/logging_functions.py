import logging
from datetime import datetime
import io

import boto3

s3_client = boto3.client("s3")

def logging_setup():
    log_name = "log.csv"
    log_name_timestamped = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} log.csv"
    
    log = logging.getLogger(log_name)
    log_stringio = io.StringIO()
    handler = logging.StreamHandler(log_stringio)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.formatter = logging.Formatter(fmt="%(levelname)s,%(asctime)s,%(message)s",datefmt="%Y-%m-%d %H:%M:%S")

    output = {"log": log,
              "log_name_timestamped": log_name_timestamped,
              "log_stringio": log_stringio}

    return output


def upload_logs(bucket, key, body):
    s3_client.put_object(Body=body, Bucket=bucket, Key=key)

def write_to_log(log, identifier, message, level='INFO'):
    log.log(getattr(logging, level), f"{identifier}, {message}")