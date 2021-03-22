import os
import yaml
import json
import re
import tempfile
import boto3
import shutil
import io

from typing import Union, List

from datetime import datetime

from jsonschema import validate as json_validate

from copy import deepcopy

from dataengineeringutils3.s3 import (
    get_filepaths_from_s3_folder,
    delete_s3_object,
    write_json_to_s3,
    s3_path_to_bucket_key,
    delete_s3_folder_contents,
)

from data_linter.constants import config_schema

from data_linter.logging_functions import (
    upload_log,
    logging_setup,
    get_temp_log_path_from_config,
    get_main_log_path_from_config,
    get_temp_log_basepath,
)

from data_linter.utils import (
    get_out_path,
    get_table_log_path,
    compress_data,
    copy_data,
    get_filepaths_from_local_folder,
    local_file_to_s3,
    read_all_file_body,
)

from data_linter.validators import (
    FrictionlessValidator,
    GreatExpectationsValidator,
    PandasValidator,
)

log, log_stringio = logging_setup()

get_validator = {
    "pandas": PandasValidator,
    "frictionless": FrictionlessValidator,
    "great-expectations": GreatExpectationsValidator,
}


def load_and_validate_config(config: Union[str, dict] = "config.yaml") -> dict:
    """
    Loads and validates the config
    """

    if isinstance(config, str):
        config_raw_text = read_all_file_body(config)
        config = yaml.safe_load(config_raw_text)
    elif isinstance(config, dict):
        pass
    else:
        raise TypeError("Input 'config' must be a str or dict.")

    return _validate_and_clean_config(config)


def _validate_and_clean_config(config: dict) -> dict:
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

    log.info("Loading config")
    log_path = None
    try:
        config = load_and_validate_config(config)
        log_path = get_main_log_path_from_config(config)

        temp_log_path = get_temp_log_path_from_config(config)
        if temp_log_path.startswith("s3://"):
            delete_s3_folder_contents(temp_log_path)
        else:
            shutil.rmtree(temp_log_path, ignore_errors=True)

        log.info("Running validation")

        config = match_files_in_land_to_config(config)

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


def bin_pack_configs(config: dict, max_bin_count: int):
    """
    creates up to max_bin_count of config files by splitting the files from the config
    by size and grouping them into or below the average size of all the files

    Args:
        config: a config file specifying the files to be linted
        max_bin_count: the maximum of bins to split the files up into - optimal number
        is equal to the amount of workers available
    """

    log_base_path = config.get("log-base-path")
    log_base_path_is_s3 = log_base_path.startswith("s3://")

    if log_base_path_is_s3:
        tmp_log_bp = get_temp_log_basepath(config)
        s3_temp_path = os.path.join(tmp_log_bp, "configs")
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

        bins = [None] * max_bin_count

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
        bins = [i for i in bins if i != []]

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
                s3_out_path = os.path.join(s3_temp_path, str(i), tmp_file_name)
                local_file_to_s3(tmp_file.name, s3_out_path)

    else:
        raise ValueError("Local land path not supported for parrallel running")


def validate_from_chunked_configs(config: dict, config_num: int) -> bool:

    land_base_path = config["land-base-path"]
    land_base_path_is_s3 = land_base_path.startswith("s3://")

    if land_base_path_is_s3:
        tmp_log_bp = get_temp_log_basepath(config)
        s3_temp_path = os.path.join(tmp_log_bp, "configs", str(config_num))

        config_file_paths = get_filepaths_from_s3_folder(s3_temp_path)
        if not config_file_paths:
            return False

        s3_client = boto3.client("s3")

        all_configs = []
        for config_file_path in config_file_paths:
            bucket, key = s3_path_to_bucket_key(config_file_path)
            config_file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            all_configs.append(yaml.safe_load(config_file_obj["Body"].read()))

        for config in all_configs:
            validate_data(config)

        return True

    else:
        raise ValueError("Local land path not supported for parrallel running")


def validate_data(config: dict):

    validator_engine = config.get("validator-engine", "pandas")
    validator_params = config.get("validator-engine-params", {})

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
        save_completion_status(config, all_table_responses)


def save_completion_status(config: dict, all_table_responses: List[dict]):
    """
    saves the status of the table linting to a file to be colleted later

    Args:
    config: A data linter config
    all_table_responses: a list of dictionaries detailing whether it passed or failied
    linting, the validator response, the file linted, and the table name
    """

    log_base_path_is_s3 = config["log-base-path"].startswith("s3://")

    temp_status_basepath = os.path.join(get_temp_log_basepath(config), "status")
    for table_response in all_table_responses:
        if log_base_path_is_s3:
            og_file_name = os.path.basename(table_response["original-path"]).split(".")[
                0
            ]

            with tempfile.NamedTemporaryFile(
                suffix=".json", prefix=og_file_name
            ) as tmp_file:

                with open(tmp_file.name, "w") as json_out:
                    json.dump(table_response, json_out)

                tmp_file_name = os.path.basename(tmp_file.name)
                s3_temp_path = os.path.join(temp_status_basepath, tmp_file_name)

                local_file_to_s3(tmp_file.name, s3_temp_path)

        else:
            if not os.path.exists(temp_status_basepath):
                os.makedirs(temp_status_basepath, exist_ok=True)
            tmp_file_resp = tempfile.mkstemp(suffix=".json", dir=temp_status_basepath)
            tmp_filename = tmp_file_resp[1]
            with open(tmp_filename, "w") as json_out:
                json.dump(table_response, json_out)


def collect_all_status(config: dict):
    """
    collects the status files saved and determines whether the linting was a succes or
    not and copies/removes/compresses the files to and from the correct places

    Args:
    config: the config as given at the beggining with the paths of where to collect and
    save data from as well as compression, remove-on-pass etc.
    """

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
    log_base_path_is_s3 = log_base_path.startswith("s3://")
    temp_status_basepath = os.path.join(get_temp_log_basepath(config), "status")
    if log_base_path_is_s3:
        status_file_paths = get_filepaths_from_s3_folder(temp_status_basepath)

        s3_client = boto3.client("s3")

        all_table_response = []
        for status_file_path in status_file_paths:
            bucket, key = s3_path_to_bucket_key(status_file_path)
            status_file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            all_table_response.append(json.loads(status_file_obj["Body"].read()))

    else:
        status_file_paths = get_filepaths_from_local_folder(temp_status_basepath)

        all_table_response = []
        for status_file_path in status_file_paths:
            with open(status_file_path) as json_in:
                all_table_response.append(json.load(json_in))

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
                    log.info(f"Compressing file from {matched_file} to {final_outpath}")
                    compress_data(matched_file, final_outpath)
                else:
                    log.info(f"Copying file from {matched_file} to {final_outpath}")
                    copy_data(matched_file, final_outpath)
                if remove_on_pass:
                    log.info(f"Removing data in land: {matched_file}")
                    if land_base_path_is_s3:
                        delete_s3_object(matched_file)
                    else:
                        os.remove(matched_file)

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
                    log.info(f"Compressing file from {matched_file} to {final_outpath}")
                    compress_data(matched_file, final_outpath)
                else:
                    log.info(f"Copying file from {matched_file} to {final_outpath}")
                    copy_data(matched_file, final_outpath)
        table_response["archived-path"] = final_outpath

        # write (table specific) log
        log_outpath = get_table_log_path(log_base_path, table_name, utc_ts, filenum=i)
        if log_base_path_is_s3:
            write_json_to_s3(table_response, log_outpath)
        else:
            path_name = os.path.dirname(log_outpath)
            os.makedirs(path_name, exist_ok=True)
            with open(log_outpath, "w") as json_out:
                json.dump(table_response, json_out)
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

    if log_base_path_is_s3:
        delete_s3_folder_contents(temp_status_basepath)
    else:
        shutil.rmtree(temp_status_basepath, ignore_errors=True)


def para_run_init(max_bin_count: int, config: Union[str, dict] = "config.yaml"):

    log.info("Loading config for paralellisation")
    log_path = None
    try:
        config = load_and_validate_config(config)
        temp_log_path = get_temp_log_path_from_config(config)
        if get_filepaths_from_s3_folder(temp_log_path):
            log.info(
                f"Found temp logs in {temp_log_path}."
                "Deleting data in folder before run."
            )
            delete_s3_folder_contents(temp_log_path)

        log_path = get_main_log_path_from_config(config)

        config = match_files_in_land_to_config(config)

        bin_pack_configs(config, max_bin_count)

        log.info("Running validation")

    except Exception as e:
        log_msg = f"Unexpected error. Uploading log to {log_path} before raising error."
        error_msg = str(e)

        log.error(log_msg)
        log.error(error_msg)

        upload_log(log, log_stringio, log_path)

        raise e.with_traceback(e.__traceback__)
    else:
        upload_log(log, log_stringio, temp_log_path)


def para_run_validation(config_num: int, config: Union[str, dict] = "config.yaml"):

    try:
        config = load_and_validate_config(config)

        log.info(f"Worker {config_num} loading config for validaiton")
        log_path = None

        temp_log_path = get_temp_log_path_from_config(config)
        log_path = get_main_log_path_from_config(config)
        there_was_a_config = validate_from_chunked_configs(config, config_num)

        if not there_was_a_config:
            log.info(f"worker {config_num} had no work - moving on")

    except Exception as e:
        log_msg = f"Unexpected error. Uploading log to {log_path} before raising error."
        error_msg = str(e)

        log.error(log_msg)
        log.error(error_msg)

        upload_log(log, log_stringio, log_path)

        raise e.with_traceback(e.__traceback__)
    else:
        upload_log(log, log_stringio, temp_log_path)


def para_collect_all_status(config: Union[str, dict] = "config.yaml"):

    log_path = None
    try:
        config = load_and_validate_config(config)

        temp_log_path = get_temp_log_path_from_config(config)
        log_path = get_main_log_path_from_config(config)

        log.info("collating table status")
        collect_all_status(config)
        log.info(f"collating all logs in {config['log-base-path']}")

    except Exception as e:
        log_msg = f"Unexpected error. Uploading log to {log_path} before raising error."
        error_msg = str(e)

        log.error(log_msg)
        log.error(error_msg)

        upload_log(log, log_stringio, log_path)

        raise e.with_traceback(e.__traceback__)
    else:
        upload_log(log, log_stringio, temp_log_path)


def para_collect_all_logs(config: Union[str, dict] = "config.yaml"):

    config = load_and_validate_config(config)

    log_base_path = config["log-base-path"]
    log_path_fin = get_main_log_path_from_config(config)
    log_base_path_is_s3 = log_base_path.startswith("s3://")

    tmp_log_base_path = get_temp_log_basepath(config)
    init_log_path = os.path.join(tmp_log_base_path, "init")
    val_log_path = os.path.join(tmp_log_base_path, "val")
    status_log_path = os.path.join(tmp_log_base_path, "status")

    if log_base_path_is_s3:
        init_log_paths = get_filepaths_from_s3_folder(init_log_path)
        val_log_paths = get_filepaths_from_s3_folder(val_log_path)
        status_log_paths = get_filepaths_from_s3_folder(status_log_path)
    else:
        init_log_paths = get_filepaths_from_local_folder(init_log_path)
        val_log_paths = get_filepaths_from_local_folder(val_log_path)
        status_log_paths = get_filepaths_from_local_folder(status_log_path)

    log_string_list = []
    for init_log_path in init_log_paths:
        log_string_list.append(read_all_file_body(init_log_path))
    for val_log_path in val_log_paths:
        log_string_list.append(read_all_file_body(val_log_path))
    for status_log_path in status_log_paths:
        log_string_list.append(read_all_file_body(status_log_path))

    log_io = io.StringIO()
    for log_str in log_string_list:
        log_io.write(log_str)
    upload_log(log, log_io, log_path_fin)

    log_path_del = os.path.join(log_base_path, "data_linter_temporary_fs")

    if log_base_path_is_s3:
        delete_s3_folder_contents(log_path_del)
    else:
        shutil.rmtree(log_path_del, ignore_errors=True)
