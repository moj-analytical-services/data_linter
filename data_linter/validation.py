import os
import yaml
import json
import re
import logging
import tempfile
import boto3

from typing import Union

from datetime import datetime

from jsonschema import validate as json_validate

from awswrangler.s3 import delete_objects

from copy import deepcopy

from dataengineeringutils3.s3 import (
    get_filepaths_from_s3_folder,
    delete_s3_object,
    write_json_to_s3,
    s3_path_to_bucket_key,
)

from data_linter.constants import config_schema

from data_linter.logging_functions import (
    upload_log,
    logging_setup,
)

from data_linter.utils import (
    get_out_path,
    get_log_path,
    compress_data,
    copy_data,
    get_filepaths_from_local_folder,
    local_file_to_s3,
)

from data_linter.validators import (
    FrictionlessValidator,
    GreatExpectationsValidator,
)

log = logging.getLogger("root")

get_validator = {
    "frictionless": FrictionlessValidator,
    "great-expectations": GreatExpectationsValidator,
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


def match_files_in_land_to_config(config: dict) -> dict:
    """
    Takes config and matches files in S3 to the corresponding table list in config.
    Checks against other config parameters and raise error if config params not met.
    """
    land_base_path = config["land-base-path"]
    if land_base_path.startswith("s3://"):
        land_files = get_filepaths_from_s3_folder(land_base_path)
        # delete temp storage, incase it hasn't been already:
        delete_objects(f"{land_base_path}/data_linter_temporary_fs/")
    else:
        land_files = get_filepaths_from_local_folder(land_base_path)

    if not land_files and config.get("fail-no-files", False):
        raise FileNotFoundError(f"No files found in the path: {land_base_path}")
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

        config = match_files_in_land_to_config(config)

        log_path = os.path.join(config["log-base-path"], get_validator_name() + ".log")
        log.info("Running validation")

        paralell = True
        if paralell:
            bin_pack_configs(config)
            validate_from_chunked_configs(config)
        else:
            validate_data(config)

        collect_all_status(config)

    except Exception as e:
        log_msg = f"Unexpected error. Uploading log to {log_path} before raising error."
        error_msg = str(e)

        log.error(log_msg)
        log.error(error_msg)

        upload_log(log, log_stringio, log_path)

        raise e.with_traceback(e.__traceback__)
    else:
        upload_log(log, log_stringio, log_path)


def chunk_config_and_save(config: dict):
    land_base_path = config["land-base-path"]

    land_base_path_is_s3 = land_base_path.startswith("s3://")

    if land_base_path_is_s3:
        bin_pack_configs(config)

    else:
        pass
        # do local equivelent


def bin_pack_configs(config: dict):
    """
    write a docstring pls
    """

    land_base_path = config.get("land-base-path")
    land_base_path_is_s3 = land_base_path.startswith("s3://")
    max_bin_count = 4

    if land_base_path_is_s3:
        while land_base_path.endswith("/"):
            land_base_path = land_base_path[:-1]
        s3_temp_path = f"{land_base_path}/data_linter_temporary_fs/configs"
        file_list = []

        # create a list of dictionaries, for each file with all attributes
        for table_name, table in config["tables"].items():
            table_sans_files = deepcopy(table)
            mfiles = table_sans_files.pop("matched_files")

            for file_name in mfiles:
                table_sans_files["file-name"] = file_name
                table_sans_files["table-name"] = table_name
                file_list.append(deepcopy(table_sans_files))

        # get the size of them all
        acum_file_size = 0
        for i, file_dict in enumerate(file_list):
            s3_client = boto3.client("s3")
            file_name = file_dict["file-name"]
            bucket, key = s3_path_to_bucket_key(file_name)
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            file_size = obj.get("ContentLength")
            file_list[i]["file-size"] = file_size
            acum_file_size += file_size

        target_bin_size = acum_file_size / max_bin_count

        # sort them in descending order
        file_list.sort(key=lambda x: -x["file-size"])

        bins = [None for i in range(max_bin_count)]

        offset = 0
        for i in range(max_bin_count):
            curr_bin = []
            curr_bin_size = 0
            has_been_binned = False
            for j in range(offset, len(file_list)):
                if len(curr_bin) == 0:
                    curr_bin.append(file_list[j])
                    curr_bin_size += file_list[j]["file-size"]
                    offset += 1
                else:
                    if curr_bin_size <= target_bin_size:
                        curr_bin.append(file_list[j])
                        curr_bin_size += file_list[j]["file-size"]
                        offset += 1
                    else:
                        bins[i] = curr_bin
                        has_been_binned = True
                        break
            if not has_been_binned:
                bins[i] = curr_bin

        bins[i] = curr_bin
        bins = [i for i in bins if i != [] or i is not None]

        # create the configs for the given bins
        for i, packed_bin in enumerate(bins):
            config_n = deepcopy(config)
            config_n.pop("tables")
            config_n["tables"] = {}

            for table in packed_bin:
                curr_table_name = table.pop("table-name")

                if config_n["tables"].get(curr_table_name):
                    # it exists, so just add to matched files
                    config_n["tables"][curr_table_name]["matched_files"].append(
                        table["file-name"]
                    )
                else:
                    # it doesn't exist, do a full copy of all attributes
                    mfile = table.pop("file-name")
                    table.pop("file-size")
                    config_n["tables"][curr_table_name] = deepcopy(table)
                    config_n["tables"][curr_table_name]["matched_files"] = []
                    config_n["tables"][curr_table_name]["matched_files"].append(mfile)

            # upload the config to temp storage, into config
            with tempfile.NamedTemporaryFile(
                suffix=".yml", prefix="config_"
            ) as tmp_file:

                with open(tmp_file.name, "w") as yaml_out:
                    yaml.dump(config_n, yaml_out, default_flow_style=False)

                tmp_file_name = tmp_file.name.split("/")[-1]
                local_file_to_s3(tmp_file.name, f"{s3_temp_path}/{i}/{tmp_file_name}")

    else:
        pass
        # do local equiv maybe lol


def validate_from_chunked_configs(config: dict):
    land_base_path = config["land-base-path"]
    land_base_path_is_s3 = land_base_path.startswith("s3://")

    if land_base_path_is_s3:

        while land_base_path.endswith("/"):
            land_base_path = land_base_path[:-1]
        s3_temp_path = f"{land_base_path}/data_linter_temporary_fs/configs"

        config_file_paths = get_filepaths_from_s3_folder(s3_temp_path)

        s3_client = boto3.client("s3")

        all_configs = []
        for config_file_path in config_file_paths:
            bucket, key = s3_path_to_bucket_key(config_file_path)
            config_file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            all_configs.append(yaml.safe_load(config_file_obj["Body"].read()))

        for config in all_configs:
            validate_data(config)


def validate_data(config: dict):

    land_base_path = config["land-base-path"]
    validator_engine = config.get("validator-engine", "frictionless")
    validator_params = config.get("validator-engine-params", {})

    # config = match_files_in_land_to_config(config)

    all_table_responses = []

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

                log.info(f"...file {i+1} of {len(table_params['matched_files'])}")
                validator = get_validator[validator_engine](
                    matched_file, table_params, metadata, **validator_params
                )

                validator.read_data_and_validate()
                validator.write_validation_errors_to_log()

                table_response = {
                    "valid": validator.valid,
                    "response": validator.get_response_dict(),
                    "original-path": matched_file,
                    "table-name": table_name,
                }

                if table_response["valid"]:
                    log.info("...file passed.")
                else:
                    log.info("...file failed.")

                all_table_responses.append(table_response)

        else:
            msg4 = f"SKIPPING {table_name}. No files found."
            log.info(msg4)

    if all_table_responses:
        save_completion_status(land_base_path, all_table_responses)


def save_completion_status(land_base_path: str, all_table_responses: list):
    land_base_path_is_s3 = land_base_path.startswith("s3://")

    for table_response in all_table_responses:
        if land_base_path_is_s3:

            while land_base_path.endswith("/"):
                land_base_path = land_base_path[:-1]
            s3_temp_path = f"{land_base_path}/data_linter_temporary_fs/status/"

            og_file_name = table_response["original-path"].split("/")[-1].split(".")[0]

            with tempfile.NamedTemporaryFile(
                suffix=".json", prefix=og_file_name
            ) as tmp_file:

                with open(tmp_file.name, "w") as json_out:
                    json.dump(table_response, json_out)

                s3_temp_path += tmp_file.name.split("/")[-1]

                local_file_to_s3(tmp_file.name, s3_temp_path)

        else:
            tmp_dir = os.path.join(land_base_path, "tmp")
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir, exist_ok=True)
            tmp_file_resp = tempfile.mkstemp(suffix=".json", dir=tmp_dir)
            tmp_filename = tmp_file_resp[1]
            with open(tmp_filename, "w") as json_out:
                json.dump(table_response, json_out)


def collect_all_status(config: dict):
    utc_ts = int(datetime.utcnow().timestamp())
    land_base_path = config["land-base-path"]
    all_must_pass = config.get("all-must-pass", False)
    pass_base_path = config["pass-base-path"]
    log_base_path = config["log-base-path"]
    fail_base_path = config.get("fail-base-path")
    remove_on_pass = config.get("remove-tables-on-pass")
    compress = config.get("compress-data")
    timestamp_partition_name = config.get("timestamp-partition-name")

    land_base_path_is_s3 = land_base_path.startswith("s3://")

    if land_base_path_is_s3:

        while land_base_path.endswith("/"):
            land_base_path = land_base_path[:-1]
        s3_temp_path = f"{land_base_path}/data_linter_temporary_fs/status/"
        status_file_paths = get_filepaths_from_s3_folder(s3_temp_path)

        s3_client = boto3.client("s3")

        all_table_response = []
        for status_file_path in status_file_paths:
            bucket, key = s3_path_to_bucket_key(status_file_path)
            status_file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            all_table_response.append(json.loads(status_file_obj["Body"].read()))

        all_tables_passed = True

        pass_count = sum([i["valid"] for i in all_table_response])

        if pass_count != len(all_table_response):
            all_tables_passed = False

        there_was_a_fail = False
        all_tables_to_fail = False
        all_tables_to_respective = False

        if all_tables_passed:
            all_tables_to_respective = True
        else:
            if all_must_pass:
                all_tables_to_fail = True
            else:
                all_tables_to_respective = True

        for i, table_response in enumerate(all_table_response):
            table_name = table_response.get("table-name")
            matched_file = table_response.get("original-path")
            file_basename = os.path.basename(matched_file)

            if all_tables_to_fail:
                there_was_a_fail = True
                final_outpath = get_out_path(
                    fail_base_path,
                    table_name,
                    utc_ts,
                    file_basename,
                    compress=compress,
                    filenum=i,
                    timestamp_partition_name=timestamp_partition_name,
                )
                if compress:
                    log.info(f"Compressing file from {matched_file} to {final_outpath}")
                    compress_data(matched_file, final_outpath)
                else:
                    log.info(f"Copying file from {matched_file} to {final_outpath}")
                    copy_data(matched_file, final_outpath)
            elif all_tables_to_respective:
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
                    if compress:
                        log.info(
                            f"Compressing file from {matched_file} to {final_outpath}"
                        )
                        compress_data(matched_file, final_outpath)
                    else:
                        log.info(f"Copying file from {matched_file} to {final_outpath}")
                        copy_data(matched_file, final_outpath)
                    if remove_on_pass:
                        log.info(f"Removing data in land: {matched_file}")
                        delete_s3_object(matched_file)
                else:
                    there_was_a_fail = True
                    final_outpath = get_out_path(
                        pass_base_path,
                        table_name,
                        utc_ts,
                        file_basename,
                        compress=compress,
                        filenum=i,
                        timestamp_partition_name=timestamp_partition_name,
                    )
                    if compress:
                        log.info(
                            f"Compressing file from {matched_file} to {final_outpath}"
                        )
                        compress_data(matched_file, final_outpath)
                    else:
                        log.info(f"Copying file from {matched_file} to {final_outpath}")
                        copy_data(matched_file, final_outpath)
            table_response["archived-path"] = final_outpath

            # write (table specific) log
            log_outpath = get_log_path(log_base_path, table_name, utc_ts, filenum=i)
            write_json_to_s3(table_response, log_outpath)
            log.info(f"log for {matched_file} uploaded to {log_outpath}")

        if there_was_a_fail and all_must_pass:
            log.info("The following tables have failed: ")
            for failed_table in [i for i in all_table_response if not i["valid"]]:
                log.info(f"{failed_table['table-name']} failed")
                log.info(f"...original path: {failed_table['original-path']}")
                log.info(f"...out path: {failed_table['archived-path']}")
            raise ValueError("Tables did not pass linter")

        if not all_must_pass and there_was_a_fail:
            msg6 = "Some tables failed but all_must_pass set to false."
            msg6 += " Check logs for details"
            log.info(msg6)

        delete_objects(f"{land_base_path}/data_linter_temporary_fs/")

    else:
        # do the same for local
        pass
