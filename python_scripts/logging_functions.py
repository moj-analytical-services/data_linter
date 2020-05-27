import logging


def logging_setup():
    log_name = "log.csv"
    log_name_timestamped = f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} log.csv"
    
    log = logging.getLogger(log_name)
    log_stringio = io.StringIO()
    handler = logging.StreamHandler(log_stringio)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.formatter = logging.Formatter(format="%(levelname)s,%(asctime)s,%(message)s",datefmt="%Y-%m-%d %H:%M:%S")

    return log


def upload_logs(bucket, key, body):
    s3_client.put_object(Body=body, Bucket=bucket, Key=key)

def write_to_log(logger, identifier, message, level='INFO'):
    logger.log(getattr(logging, level), f"{identifier}, {message}")