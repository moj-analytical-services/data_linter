import os
import yaml
import json
import re
import logging

import copy
from typing import Union

from datetime import datetime

from jsonschema import validate as json_validate

from dataengineeringutils3.s3 import (
    get_filepaths_from_s3_folder,
    copy_s3_object,
    delete_s3_object,
    write_json_to_s3,
)

import boto3
from botocore.client import Config

from data_linter.constants import config_schema

from data_linter.logging_functions import (
    upload_log,
    logging_setup,
)

from data_linter.utils import (
    get_out_path,
    get_log_path,
    compress_data,
)

from data_linter.validators import (
    FrictionlessValidator,
    GreatExpectationsValidator,
)

boto3_config = Config(read_timeout=120)
s3_client = boto3.client("s3", config=boto3_config)

log = logging.getLogger("root")

get_validator = {
    "frictionless": FrictionlessValidator,
    "great-expectations": GreatExpectationsValidator
}


def get_validator_name() -> str:
    validator_name = os.getenv("VALIDATOR_NAME")
    if not validator_name:
        validator_name = "de-data-validator"
    validator_name += f"-{int(datetime.utcnow().timestamp())}"
    return validator_name


def load_and_validate_config(config_path: str = "config.yaml") -> dict:
    """
    Loads and validates the config
    """

    # load yaml or json
    if not os.path.isfile(config_path):
        config_path = config_path.replace("yaml", "yml")
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Expecting a file in path given {config_path}.")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return validate_and_clean_config(config)


def validate_and_clean_config(config: dict) -> dict:
    """Validates a config as a dict. And adds default
    properties to that config.

    Args:
        config (dict): Config for data linter validation run.

    Returns:
        dict: The same config but with default params added.
    """
    json_validate(config, config_schema)

    for table_name, params in config["tables"].items():
        if (not params.get("expect-header")) and params.get("headers-ignore-case"):
            log.warning(
                f"Table '{table_name}' has a 'headers-ignore-case' parameter "
                "but no 'expect-header'. Setting 'expect-header' to True."
            )
            params["expect-header"] = True

    return config


def match_files_in_land_to_config(config) -> dict:
    """
    Takes config and matches files in S3 to the corresponding table list in config.
    Checks against other config parameters and raise error if config params not met.
    """
    land_base_path = config["land-base-path"]
    land_files = get_filepaths_from_s3_folder(land_base_path)

    if not land_files and config.get("fail-no-files", False):
        raise FileNotFoundError(f"No files found in the s3 path: {land_base_path}")
    else:
        total_files = len(land_files)
        log.info(f"Found {total_files} in {land_base_path}")

    # Check for requrired tables
    all_matched = []
    for table_name, table_params in config["tables"].items():
        if table_params.get("pattern"):
            table_params["matched_files"] = [
                land_file
                for land_file in land_files
                if re.match(
                    table_params.get("pattern"), land_file.replace(land_base_path, "")
                )
            ]
        else:
            table_params["matched_files"] = [
                land_file
                for land_file in land_files
                if land_file.replace(land_base_path, "").startswith(table_name)
            ]

        if not table_params["matched_files"] and table_params.get("required"):
            raise FileNotFoundError(
                f"Config states file for {table_name} must exist but no files matched."
            )

        all_matched.extend(table_params["matched_files"])

    if len(all_matched) != len(set(all_matched)):
        large_error_traceback = ""
        for table_name, table_params in config["tables"].items():
            large_error_traceback += f"{table_name}: {table_params['matched_files']} \n"
        raise FileExistsError(
            f"We matched the same files to multiple tables.\n{large_error_traceback}"
        )

    # Fail if expecting no unknown files
    if "fail-unknown-files" in config:
        file_exeptions = config["fail-unknown-files"].get("exceptions", [])
        land_diff = set(land_files).difference(all_matched)
        land_diff = land_diff.difference(file_exeptions)
        if land_diff:
            raise FileExistsError(
                "Config states no unknown should exist. "
                f"The following were unmatched: {land_diff}"
            )

    return config


def validate_data(config: dict):

    utc_ts = int(datetime.utcnow().timestamp())
    land_base_path = config["land-base-path"]
    all_must_pass = config.get("all-must-pass", False)
    pass_base_path = config["pass-base-path"]
    log_base_path = config["log-base-path"]
    fail_base_path = config.get("fail-base-path")
    remove_on_pass = config.get("remove-tables-on-pass")
    compress = config.get("compress-data")
    timestamp_partition_name = config.get("timestamp-partition-name")
    validator_engine = config.get("validator-engine", "frictionless")
    config = match_files_in_land_to_config(config)

    # If all the above passes lint each file
    all_table_responses = []
    all_matched_files = []
    overall_pass = True
    for table_name, table_params in config["tables"].items():
        table_params["lint-response"] = []
        if table_params["matched_files"]:
            log.info(f"Linting {table_name}")

            meta_file_path = table_params.get(
                "metadata", f"meta_data/{table_name}.json"
            )

            with open(meta_file_path) as sfile:
                metadata = json.load(sfile)

            for i, matched_file in enumerate(table_params["matched_files"]):
                all_matched_files.append(matched_file)
                log.info(f"...file {i+1} of {len(table_params['matched_files'])}")
                file_basename = os.path.basename(matched_file)

                validator = get_validator[validator_engine](
                    matched_file, table_params, metadata
                )
                validator.read_data_and_validate()
                validator.write_validation_errors_to_log()

                # response - needs to be standardised see issue #100
                table_response = {
                    "valid": validator.valid,
                    "response": copy.deepcopy(validator.response),
                    "s3-original-path": matched_file,
                    "table-name": table_name
                }

                # Write data to s3 on pass or elsewhere on fail
                if table_response["valid"]:
                    final_outpath = get_out_path(
                        pass_base_path,
                        table_name,
                        utc_ts,
                        file_basename,
                        compress=compress,
                        filenum=i,
                        timestamp_partition_name=timestamp_partition_name,
                    )

                    table_response["archived-path"] = final_outpath
                    if not all_must_pass:
                        msg2 = f"...file passed. Writing to {final_outpath}"
                        log.info(msg2)
                        if compress:
                            compress_data(matched_file, final_outpath)
                        else:
                            copy_s3_object(matched_file, final_outpath)
                        if remove_on_pass:
                            log.info(f"Removing {matched_file}")
                            delete_s3_object(matched_file)
                    else:
                        log.info("File passed")

                elif fail_base_path:
                    overall_pass = False
                    final_outpath = get_out_path(
                        fail_base_path,
                        table_name,
                        utc_ts,
                        file_basename,
                        compress=compress,
                        filenum=i,
                        timestamp_partition_name=timestamp_partition_name,
                    )
                    table_response["archived-path"] = final_outpath
                    if not all_must_pass:
                        msg3 = f"...file failed. Writing to {final_outpath}"
                        log.warning(msg3)
                        if compress:
                            compress_data(matched_file, final_outpath)
                        else:
                            copy_s3_object(matched_file, final_outpath)

                else:
                    overall_pass = False
                    table_response["archived-path"] = None

                # Write reponse log
                log_outpath = get_log_path(log_base_path, table_name, utc_ts, filenum=i)

                # Write log to s3
                write_json_to_s3(table_response, log_outpath)
                all_table_responses.append(table_response)

        else:
            msg4 = f"SKIPPING {table_name}. No files found."
            log.info(msg4)

    if overall_pass:
        log.info("All tables passed")
        if all_must_pass:
            msg5 = f"Copying data from {land_base_path} to {pass_base_path}"
            log.info(msg5)

            for resp in all_table_responses:
                msg6 = f"Copying {resp['s3-original-path']} to {resp['archived-path']}"
                log.info(msg6)
                if compress:
                    compress_data(resp["s3-original-path"], resp["archived-path"])
                else:
                    copy_s3_object(resp["s3-original-path"], resp["archived-path"])

                if remove_on_pass:
                    log.info(f"Removing data in land: {resp['s3-original-path']}")
                    delete_s3_object(resp["s3-original-path"])

    elif all_must_pass:
        if fail_base_path:
            m0 = "Copying files"
            log.info(m0)
        for resp in all_table_responses:
            if resp["archived-path"]:
                if compress:
                    log.info(
                        f"Compressing file from {resp['s3-original-path']} to \
                            {resp['archived-path']}"
                    )
                    compress_data(resp["s3-original-path"], resp["archived-path"])
                else:
                    log.info(
                        f"Copying file from {resp['s3-original-path']} to \
                            {resp['archived-path']}"
                    )
                    copy_s3_object(resp["s3-original-path"], resp["archived-path"])
            log.error("The following tables failed:")
            if not resp["valid"]:
                m1 = f"{resp['table-name']} failed"
                m2 = f"... original path: {resp['s3-original-path']}"
                m3 = f"... out path: {resp['archived-path']}"
                log.error(m1)
                log.error(m2)
                log.error(m3)

        m4 = f"Logs that show failed data: {log_base_path}"
        m5 = (
            "Tables that passed but not written due to other table failures"
            f"are stored here: {land_base_path}"
        )
        log.info(m4)
        log.info(m5)
        raise ValueError("Tables did not pass linter")

    else:
        m6 = "Some tables failed but all_must_pass set to false. Check logs for details"
        log.info(m6)


def run_validation(config: Union[str, dict] = "config.yaml"):
    """
    Runs end to end validation based on config.

    Args:
        config (Union[str, dict], optional): Either a string specifying the path to a
        config yaml. Or a dict of a config in memory. Defaults to "config.yaml".

    Raises:
        Error: States where log is written if error is hit in validation and then raises
        traceback.
    """

    # set up logging
    log, log_stringio = logging_setup()

    log.info("Loading config")
    log_path = None
    try:
        if isinstance(config, str):
            config = load_and_validate_config(config)
        elif isinstance(config, dict):
            config = validate_and_clean_config(config)
        else:
            raise TypeError("Input 'config' must be a str or dict.")

        log_path = os.path.join(config["log-base-path"], get_validator_name() + ".log")
        log.info("Running validation")
        validate_data(config)

    except Exception as e:
        log_msg = (
            f"Unexpected error. Uploading log to {log_path} before raising error."
        )
        error_msg = str(e)

        log.error(log_msg)
        log.error(error_msg)

        upload_log(log, log_stringio, log_path)

        raise e.with_traceback(e.__traceback__)
    else:
        upload_log(log, log_stringio, log_path)
