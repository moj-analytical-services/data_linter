import os


def test_download_fileobj(s3_client):
    # s3 is a fixture defined above that yields a boto3 s3 client.
    from dataengineeringutils3.s3 import s3_path_to_bucket_key
    s3_client.create_bucket(Bucket="somebucket")
    s3_download_path = "somebucket/"
    bucket, key = s3_path_to_bucket_key(s3_download_path)

    table1 = "table1.csv"
    test_path = "tests/data/end_to_end1/"
    full_path = os.path.join(test_path, table1)
    s3_client.upload_file(full_path, bucket, table1)

    with open(table1, "wb") as downloaded_file:
        s3_client.download_fileobj(bucket, table1, downloaded_file)

    my_file = open(table1, "rb").read()

    assert my_file == open(full_path, "rb").read()
