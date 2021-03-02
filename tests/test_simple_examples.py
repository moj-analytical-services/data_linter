import os
import yaml
from copy import deepcopy
from tests.helpers import set_up_s3

simple_yaml_config = """
land-base-path: s3://land/
fail-base-path: s3://fail/
pass-base-path: s3://pass/
log-base-path: s3://log/

compress-data: false
remove-tables-on-pass: true
all-must-pass: true

# Tables to validate
tables:
  table1:
    required: true
    metadata: tests/data/end_to_end1/meta_data/table1.json
    expect-header: true

  table2:
    required: true
    pattern: ^table2
    metadata: tests/data/end_to_end1/meta_data/table2.json
"""


def test_validation_single_worker(s3):
    """
    Simple example on how to run DL for a single worker.

    [init] -> [worker]x1 -> [closedown]
    """
    from data_linter import validation
    from dataengineeringutils3.s3 import get_filepaths_from_s3_folder

    test_folder = "tests/data/end_to_end1/"
    config = yaml.safe_load(simple_yaml_config)

    # Only required for mocked tests
    set_up_s3(s3, test_folder, config)

    validation.para_run_init(1, config)
    validation.para_run_validation(0, config)
    validation.para_collect_all_status(config)
    validation.para_collect_all_logs(config)

    # Assert that files have moved from land -> pass and nothing failed
    land_files = get_filepaths_from_s3_folder(config["land-base-path"])
    pass_files = get_filepaths_from_s3_folder(config["pass-base-path"])
    fail_files = get_filepaths_from_s3_folder(config["fail-base-path"])
    assert (not land_files and not fail_files) and pass_files


def test_validation_multiple_workers(s3):
    """
    Simple example on how to run DL for multiple worker.

    [init] -> [worker]x4 -> [closedown]
    """
    from data_linter import validation
    from dataengineeringutils3.s3 import get_filepaths_from_s3_folder

    test_folder = "tests/data/end_to_end1/"
    config = yaml.safe_load(simple_yaml_config)

    # Only required for mocked tests
    set_up_s3(s3, test_folder, config)

    validation.para_run_init(4, config)

    # although ran sequencially this can be ran in parallel
    for i in range(4):
        validation.para_run_validation(i, config)

    validation.para_collect_all_status(config)
    validation.para_collect_all_logs(config)

    # Assert that files have moved from land -> pass and nothing failed
    land_files = get_filepaths_from_s3_folder(config["land-base-path"])
    pass_files = get_filepaths_from_s3_folder(config["pass-base-path"])
    fail_files = get_filepaths_from_s3_folder(config["fail-base-path"])
    assert (not land_files and not fail_files) and pass_files


def test_validation_multiple_workers_no_init(s3):
    """
    Simple example on how to run DL for multiple workers.
    But without using the init. You would want to do this
    if you want to specify which worker works on what specific dataset.
    In the example below we run 1 worker per table validation

    [init] -> [worker]x2 -> [closedown]
    """

    import boto3
    from data_linter import validation
    from data_linter.logging_functions import get_temp_log_basepath

    from dataengineeringutils3.s3 import (
        s3_path_to_bucket_key,
        get_filepaths_from_s3_folder,
    )

    s3_client = boto3.client("s3")

    test_folder = "tests/data/end_to_end1/"
    config = yaml.safe_load(simple_yaml_config)

    # Only required for mocked tests
    set_up_s3(s3, test_folder, config)

    worker_config_path = os.path.join(get_temp_log_basepath(config), "configs")
    log_bucket, worker_base_key = s3_path_to_bucket_key(worker_config_path)

    config = validation.load_and_validate_config(config)
    config = validation.match_files_in_land_to_config(config)

    # Create a config for worker 0 to only process table1
    # (aka drop other tables in config)
    # and write to worker 0 config to s3
    worker0_conf = deepcopy(config)
    del worker0_conf["tables"]["table2"]
    s3_client.put_object(
        Body=yaml.dump(worker0_conf).encode("utf-8"),
        Bucket=log_bucket,
        Key=f"{worker_base_key}/0/config.yml",
    )

    # Create a config for worker 1 to only process table2
    # and write to worker 1 config to s3
    worker1_conf = deepcopy(config)
    del worker1_conf["tables"]["table1"]
    s3_client.put_object(
        Body=yaml.dump(worker1_conf).encode("utf-8"),
        Bucket=log_bucket,
        Key=f"{worker_base_key}/1/config.yml",
    )

    validation.para_run_validation(0, config)
    validation.para_run_validation(1, config)

    validation.para_collect_all_status(config)
    validation.para_collect_all_logs(config)

    # Assert that files have moved from land -> pass and nothing failed
    land_files = get_filepaths_from_s3_folder(config["land-base-path"])
    pass_files = get_filepaths_from_s3_folder(config["pass-base-path"])
    fail_files = get_filepaths_from_s3_folder(config["fail-base-path"])
    assert (not land_files and not fail_files) and pass_files
