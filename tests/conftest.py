import os

import boto3
from moto import mock_s3
import pytest


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    mocked_envs = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
    ]
    for menv in mocked_envs:
        os.environ[menv] = "testing"

    yield  # Allows us to close down envs on exit

    for menv in mocked_envs:
        del os.environ[menv]


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.resource("s3", region_name="eu-west-1")


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-1")
