import os
import yaml
import json
import re
import gzip
import logging

from io import BytesIO
from datetime import datetime

from jsonschema import validate as json_validate
from dataengineeringutils3 import s3
import boto3

from goodtables import validate

import logging

from data_linter.constants import config_schema

from data_linter.logging_functions import (
    upload_log,
    logging_setup,
)

from data_linter.utils import (
    download_data,
    get_out_path,
    local_file_to_s3,
    get_log_path,
)

s3_client = boto3.client("s3")

log = logging.getLogger("root")


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

    json_validate(config, config_schema)

    return config


def match_files_in_land_to_config(config) -> dict:
    """
    Takes config and matches files in S3 to the corresponding table list in config.
    Checks against other config parameters and will raise error if config params not met.
    """
    land_base_path = config["land-base-path"]
    land_files = s3.get_filepaths_from_s3_folder(land_base_path)

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
                l
                for l in land_files
                if re.match(table_params.get("pattern"), l.replace(land_base_path, ""))
            ]
        else:
            table_params["matched_files"] = [
                l
                for l in land_files
                if l.replace(land_base_path, "").startswith(table_name)
            ]

        if not table_params["matched_files"] and table_params.get("required"):
            raise FileNotFoundError(
                "Config states file must exist but not files matched."
            )

        all_matched.extend(table_params["matched_files"])

    if len(all_matched) != len(set(all_matched)):
        raise FileExistsError("We matched the same files to multiple tables")

    # Fail if expecting no unknown files
    if "fail-unknown-files" in config:
        file_exeptions = config["fail-unknown-files"].get("exceptions", [])
        land_diff = set(land_files).difference(all_matched)
        land_diff = land_diff.difference(file_exeptions)
        if land_diff:
            raise FileExistsError(
                f"Config states no unknown should exist. The following were unmatched: {land_diff}"
            )

    return config


def convert_meta_type_to_goodtable_type(meta_type: str) -> str:
    """
    Converts string name for etl_manager data type and converts it to a goodtables data type

    Parameters
    ----------
    meta_type: str
        Column type of the etl_manager metadata

    Returns
    -------
    str:
        Column type of the goodtables_type https://frictionlessdata.io/specs/table-schema/
    """
    meta_type = meta_type.lower()

    lookup = {
        "character": "string",
        "int": "integer",
        "long": "integer",
        "float": "number",
        "double": "number",
        "date": "date",
        "datetime": "datetime",
        "boolean": "boolean",
    }

    if meta_type in lookup:
        gt_type = lookup[meta_type]
    elif meta_type.startswith("array"):
        gt_type = "array"
    elif meta_type.startswith("struct"):
        gt_type = "object"
    else:
        raise TypeError(
            f"Given meta_type: {meta_type} but this matches no goodtables equivalent"
        )

    return gt_type


def convert_meta_to_goodtables_schema(meta: dict) -> dict:
    """
    Should take our metadata file and convert it to a goodtables schema

    Parameters
    ----------
    meta: dict
        Takes a metadata dictionary (see etl_manager) then converts that to a particular schema for linting

    Returns
    -------
    dict:
        A goodtables schema
    """

    gt_template = {
        "$schema": "https://frictionlessdata.io/schemas/table-schema.json",
        "fields": [],
        "missingValues": [""],
    }

    gt_constraint_names = [
        "unique",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
        "pattern",
        "enum",
    ]

    for col in meta["columns"]:
        gt_constraints = {}

        gt_type = convert_meta_type_to_goodtable_type(col["type"])
        gt_format = col.get("format", "default")

        if gt_type in ["date", "datetime"] and "format" not in col:
            gt_format = "any"

        if "nullable" in col:
            gt_constraints["required"] = not col["nullable"]

        contraint_params_in_col = [g for g in gt_constraint_names if g in col]

        for gt_constraint_name in contraint_params_in_col:
            gt_constraints[gt_constraint_name] = col[gt_constraint_name]

        gt_template["fields"].append(
            {
                "name": col["name"],
                "type": gt_type,
                "format": gt_format,
                "constraints": gt_constraints,
            }
        )

    return gt_template


def log_validation_result(log: logging.Logger, table_resp: dict):
    for e in table_resp["errors"]:
        log.error(e["message"], extra={"context": "VALIDATION"})


def _spoof_onetable_datapackage(name, s3_path, schema, data_format):
    dummypackage = {
        "name": "spoof-datapackage-single-table",
        "resources": [{"name": name, "path": s3_path, "schema": schema,}],
    }
    return dummypackage


def validate_data(config: dict):

    utc_ts = int(datetime.utcnow().timestamp())
    land_base_path = config["land-base-path"]
    all_must_pass = config.get("all-must-pass", False)
    pass_base_path = config["pass-base-path"]
    log_base_path = config["log-base-path"]
    fail_base_path = config.get("fail-base-path")

    #  If all tables must pass before
    if all_must_pass:
        pass_base_path += "__tmp__/"
        log_base_path += "__tmp__/"

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
                schema = convert_meta_to_goodtables_schema(metadata)
                file_type = metadata["data_format"]

            # Assume header is first line if not headerless
            if table_params.get("expect-header", False) or file_type == "json":
                headers = [c["name"] for c in metadata["columns"]]
            else:
                headers = 1

            for i, matched_file in enumerate(table_params["matched_files"]):
                all_matched_files.append(matched_file)
                log.info(f"...file {i+1} of {len(table_params['matched_files'])}")
                file_basename = os.path.basename(matched_file)

                response = validate(
                    matched_file,
                    schema=schema,
                    headers=headers,
                    **table_params.get("gt-kwargs", {}),
                )

                log.info(str(response["tables"]))
                table_response = response["tables"][0]
                table_response["s3-original-path"] = matched_file
                table_response["table-name"] = table_name

                log_validation_result(log, table_response)
                # Write data to s3 on pass or elsewhere on fail
                # log.info(f"TEST {response}")
                if table_response["valid"]:
                    final_outpath = get_out_path(
                        config["pass-base-path"],
                        table_name,
                        utc_ts,
                        file_basename,
                        compress=config["compress-data"],
                        filenum=i,
                    )
                    if all_must_pass:
                        tmp_outpath = get_out_path(
                            pass_base_path,
                            table_name,
                            utc_ts,
                            file_basename,
                            compress=config["compress-data"],
                            filenum=i,
                        )
                    else:
                        tmp_outpath = final_outpath

                    table_response["archived-path"] = final_outpath
                    log.info(f"...file passed. Writing to {tmp_outpath}")
                    s3.copy_s3_object(table_response["s3-original-path"], tmp_outpath)

                    if not all_must_pass:
                        s3.delete_s3_object(matched_file)

                # Failed paths don't need a temp path
                elif fail_base_path:
                    final_outpath = get_out_path(
                        fail_base_path,
                        table_name,
                        utc_ts,
                        file_basename,
                        compress=config["compress-data"],
                        filenum=i,
                    )
                    table_response["archived-path"] = final_outpath
                    log.warning(f"...file failed. Writing to {final_outpath}")
                else:
                    table_response["archived-path"] = None

                # Write reponse log
                log_outpath = get_log_path(log_base_path, table_name, utc_ts, filenum=i)

                # Write log to s3
                s3.write_json_to_s3(table_response, log_outpath)
                all_table_responses.append(table_response)

        else:
            log.info(f"SKIPPING {table_name}. No files found.")

    if overall_pass:
        log.info("All tables passed")
        if all_must_pass:
            log.info("Moving data from tmp into land-base-path")

            s3.copy_s3_folder_contents_to_new_folder(
                land_base_path, config["land-base-path"]
            )
            s3.delete_s3_folder_contents(land_base_path)

            log.info("Moving data from tmp into log-base-path")
            s3.copy_s3_folder_contents_to_new_folder(
                log_base_path, config["log-base-path"]
            )

            log.info("Removing data in land")
            for matched_file in all_matched_files:
                s3.delete_s3_object(matched_file)

    else:
        log.error("The following tables failed:")
        for resp in all_table_responses:
            m1 = f"resp {resp['table-name']}"
            m2 = f"... original path: {resp['s3-original-path']}"
            m3 = f"... out path: {resp['archived-path']}"
            log.error(m1)
            log.error(m2)
            log.error(m3)

        if all_must_pass:
            log.info(f"Logs that show failed data: {land_base_path}")
            log.info(
                f"Tables that passed but not written due to other table failures are stored here: {log_base_path}"
            )

    if not overall_pass:
        raise ValueError("Tables did not pass linter. Check logs.")


def run_validation(config_path="config.yaml"):
    """
    Runs end to end validation based on config.

    Args:
        config_path (str, optional): [description]. Defaults to "config.yaml".

    Raises:
        Error: States where log is written if error is hit in validation and then raises
        traceback.
    """

    # set up logging
    log, log_stringio = logging_setup()

    log.info("Loading config")
    try:
        config = load_and_validate_config(config_path)
        log_path = os.path.join(config["log-base-path"], get_validator_name() + ".log")
        log.info("Running validation")
        validate_data(config)
    except Exception as e:
        upload_log(body=log_stringio.getvalue(), s3_path=log_path)
        log_msg = (
            "Unexpected error hit. Uploading log to {log_path}. Before raising error."
        )
        error_msg = str(e)

        log.error(log_msg)
        log.error(error_msg)

        upload_log(body=log_stringio.getvalue(), s3_path=log_path)

        raise type(e)(str(e)).with_traceback(sys.exc_info()[2])
    else:
        upload_log(body=log_stringio.getvalue(), s3_path=log_path)
