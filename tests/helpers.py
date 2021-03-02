import os


def set_up_s3(mocked_s3, test_folder, config):
    """
    Used to setup mocked s3 before a run that expects data in S3
    """
    from dataengineeringutils3.s3 import s3_path_to_bucket_key

    land_bucket, _ = s3_path_to_bucket_key(config.get("land-base-path", "s3://land/"))
    fail_bucket, _ = s3_path_to_bucket_key(config.get("fail-base-path", "s3://fail/"))
    pass_bucket, _ = s3_path_to_bucket_key(config.get("pass-base-path", "s3://pass/"))
    log_bucket, _ = s3_path_to_bucket_key(config.get("log-base-path", "s3://log/"))

    buckets = [
        land_bucket,
        fail_bucket,
        pass_bucket,
        log_bucket,
    ]
    for b in buckets:
        mocked_s3.meta.client.create_bucket(
            Bucket=b,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

    files = [
        f for f in os.listdir(test_folder) if f.endswith(".csv") or f.endswith(".jsonl")
    ]
    for filename in files:
        full_path = os.path.join(test_folder, filename)
        mocked_s3.meta.client.upload_file(full_path, land_bucket, filename)
