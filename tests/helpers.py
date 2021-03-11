import os


def set_up_s3(mocked_s3, test_folder, config):
    """
    Used to setup mocked s3 before a run that expects data in S3
    """
    from dataengineeringutils3.s3 import s3_path_to_bucket_key

    land_base_path = config.get("land-base-path", "s3://land/")
    fail_base_path = config.get("fail-base-path", "s3://fail/")
    pass_base_path = config.get("pass-base-path", "s3://pass/")
    log_base_path = config.get("log-base-path", "s3://log/")

    land_base_path_is_s3 = land_base_path.startswith("s3://")
    fail_base_path_is_s3 = fail_base_path.startswith("s3://")
    pass_base_path_is_s3 = pass_base_path.startswith("s3://")
    log_base_path_is_s3 = log_base_path.startswith("s3://")

    buckets = []

    if land_base_path_is_s3:
        land_bucket, _ = s3_path_to_bucket_key(land_base_path)
        buckets.append(land_bucket)
    if fail_base_path_is_s3:
        fail_bucket, _ = s3_path_to_bucket_key(fail_base_path)
        buckets.append(fail_bucket)
    if pass_base_path_is_s3:
        pass_bucket, _ = s3_path_to_bucket_key(pass_base_path)
        buckets.append(pass_bucket)
    if log_base_path_is_s3:
        log_bucket, _ = s3_path_to_bucket_key(log_base_path)
        buckets.append(log_bucket)

    for b in buckets:
        mocked_s3.meta.client.create_bucket(
            Bucket=b,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

    files = [
        f for f in os.listdir(test_folder) if f.endswith(".csv") or f.endswith(".jsonl")
    ]

    if land_base_path_is_s3:
        for filename in files:
            full_path = os.path.join(test_folder, filename)
            mocked_s3.meta.client.upload_file(full_path, land_bucket, filename)
