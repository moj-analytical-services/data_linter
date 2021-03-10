import os
import yaml
import gzip
import tempfile
import pytest

from tests.helpers import set_up_s3

# class MockS3FileSystem:
#     @staticmethod
#     def open_input_stream(f):
        
def test_end_to_end(s3):

    from data_linter.validation import run_validation

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    set_up_s3(s3, test_folder, config)
    run_validation(config_path)
    os.system(f"python data_linter/command_line.py --config-path {config_path}")


@pytest.mark.parametrize("validator", ["pandas", "frictionless", "great-expectations"])
def test_end_to_end_all_validators(s3, validator, monkeypatch):

    from data_linter.validation import run_validation

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    config["validator-engine"] = validator
    set_up_s3(s3, test_folder, config)
    run_validation(config)


def test_end_to_end_no_creds_error():

    from data_linter.validation import run_validation
    from botocore.exceptions import NoCredentialsError

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")

    with pytest.raises(NoCredentialsError):
        run_validation(config_path)


def test_compression(s3):

    from data_linter.utils import compress_data

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    set_up_s3(s3, test_folder, config)
    test_file_uncompressed = "table2.jsonl"
    test_file_compressed = "table2.jsonl.gz"
    uncompressed_location = os.path.join(
        config["land-base-path"], test_file_uncompressed
    )
    compressed_location = os.path.join(config["pass-base-path"], test_file_compressed)

    compress_data(uncompressed_location, compressed_location)
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, test_file_compressed), "wb") as file1:
            s3.meta.client.download_fileobj("pass", test_file_compressed, file1)
        with gzip.GzipFile(
            os.path.join(d, test_file_compressed), "r"
        ) as compressed_json:
            json_bytes = compressed_json.read()

    compressed_json_str = json_bytes.decode("utf-8")

    with open(os.path.join(test_folder, test_file_uncompressed)) as uncompressed_json:
        assert (
            compressed_json_str == uncompressed_json.read()
        ), "uncompressed json doesn't contain the same data as compressed json"


@pytest.mark.parametrize("land_path", ["s3://land/", "tests/data/end_to_end1/"])
@pytest.mark.parametrize("fail_path", ["s3://fail/", "fail"])
@pytest.mark.parametrize("pass_path", ["s3://pass/", "pass"])
@pytest.mark.parametrize("log_path", ["s3://log/", "log"])
def test_end_to_end_full_path_spectrum(
    s3, tmpdir_factory, land_path, fail_path, pass_path, log_path
):

    from data_linter.validation import run_validation

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")

    with open(config_path) as yml:
        config = yaml.safe_load(yml)

    if not fail_path.startswith("s3://"):
        fail_path = tmpdir_factory.mktemp(fail_path)
    if not pass_path.startswith("s3://"):
        pass_path = tmpdir_factory.mktemp(pass_path)
    if not log_path.startswith("s3://"):
        log_path = tmpdir_factory.mktemp(log_path)

    config["land_path"] = land_path
    config["fail_path"] = fail_path
    config["pass_path"] = pass_path
    config["log_path"] = log_path

    set_up_s3(s3, test_folder, config)

    run_validation(config)


@pytest.mark.parametrize("land_path", ["s3://land/"])
@pytest.mark.parametrize("fail_path", ["s3://fail/", "fail"])
@pytest.mark.parametrize("pass_path", ["s3://pass/", "pass"])
@pytest.mark.parametrize("log_path", ["s3://log/", "log"])
def test_end_to_end_full_path_spectrum_parallel(
    s3,
    tmpdir_factory,
    land_path,
    fail_path,
    pass_path,
    log_path,
):

    from data_linter import validation

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    max_bin_count = 3

    with open(config_path) as yml:
        config = yaml.safe_load(yml)

    if not fail_path.startswith("s3://"):
        fail_path = tmpdir_factory.mktemp(fail_path)
    if not pass_path.startswith("s3://"):
        pass_path = tmpdir_factory.mktemp(pass_path)
    if not log_path.startswith("s3://"):
        log_path = tmpdir_factory.mktemp(log_path)

    config["land_path"] = land_path
    config["fail_path"] = fail_path
    config["pass_path"] = pass_path
    config["log_path"] = log_path

    set_up_s3(s3, test_folder, config)

    validation.para_run_init(max_bin_count, config_path)
    for i in range(max_bin_count):
        validation.para_run_validation(i, config_path)
    validation.para_collect_all_status(config_path)
    validation.para_collect_all_logs(config_path)


@pytest.mark.parametrize("max_bin_count", [1, 3, 10])
def test_bin_count(s3, max_bin_count):

    from data_linter import validation

    test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")

    with open(config_path) as yml:
        config = yaml.safe_load(yml)

    set_up_s3(s3, test_folder, config)

    validation.para_run_init(max_bin_count, config_path)
    for i in range(max_bin_count):
        validation.para_run_validation(i, config_path)
    validation.para_collect_all_status(config_path)
    validation.para_collect_all_logs(config_path)


def test_end_to_end_single_file_config(s3):

    from data_linter import validation

    test_folder = "tests/data/end_to_end1/"

    config = {
        "land-base-path": "s3://land/",
        "fail-base-path": "s3://fail/",
        "pass-base-path": "s3://pass/",
        "log-base-path": "s3://log/",
        "compress-data": True,
        "remove-tables-on-pass": True,
        "all-must-pass": True,
        "tables": {
            "table1": {
                "required": True,
                "metadata": "tests/data/end_to_end1/meta_data/table1.json",
                "expect-header": True,
                "matched-files": ["s3://land/table1.csv"],
            }
        },
    }

    set_up_s3(s3, test_folder, config)

    validation.para_run_init(1, config)
    validation.para_run_validation(0, config)
    validation.para_collect_all_status(config)
    validation.para_collect_all_logs(config)


@pytest.mark.parametrize("max_bin_count", [1, 2, 3, 4, 10])
def test_bin_pack_configs(s3, max_bin_count):

    from data_linter import validation
    from data_linter.utils import read_all_file_body
    from dataengineeringutils3.s3 import get_filepaths_from_s3_folder
    from botocore.exceptions import ClientError

    test_folder = test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config_matched_files.yml")

    with open(config_path) as yml:
        config = yaml.safe_load(yml)

    set_up_s3(s3, test_folder, config)

    validation.bin_pack_configs(config, max_bin_count)

    land_base_path = config["land-base-path"]

    all_bin_packed_configs = get_filepaths_from_s3_folder(
        f"{land_base_path}/data_linter_temporary_storage/configs"
    )

    for i, file_path in enumerate(all_bin_packed_configs):
        bin_pack_path = os.path.join(
            test_folder, f"bin_pack/config_{max_bin_count}_{i}.yml"
        )
        with open(bin_pack_path) as yml:
            pre_bin_packed = yaml.safe_load(yml)

        try:
            actual_bin_pack = yaml.safe_load(read_all_file_body(file_path))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                assert pre_bin_packed is None
        else:
            assert actual_bin_pack == pre_bin_packed


@pytest.mark.parametrize("land_path", ["s3://land/", "tests/data/end_to_end1/"])
def test_read_all_file_body(s3, land_path):

    from data_linter.utils import read_all_file_body

    test_folder = test_folder = "tests/data/end_to_end1/"
    config_path = os.path.join(test_folder, "config.yaml")
    table_1_path = os.path.join(test_folder, "table1.csv")

    with open(config_path) as yml:
        config = yaml.safe_load(yml)
    with open(table_1_path) as f_in:
        table_1_body_actual = f_in.read()

    config["land-base-path"] = land_path

    set_up_s3(s3, test_folder, config)
    land_base_path = config["land-base-path"]
    table_1_body = read_all_file_body(f"{land_base_path}table1.csv")

    # Unix new line is \r\n, Mac new line is \n
    table_1_body = table_1_body.replace("\r\n", "\n")
    table_1_body_actual = table_1_body_actual.replace("\r\n", "\n")

    assert table_1_body == table_1_body_actual
