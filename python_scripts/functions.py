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

from iam_builder.iam_builder import build_iam_policy

s3_client = boto3.client("s3")

log = logging.getLogger("log.csv")

def load_and_validate_config(path=".", file_name="config.yaml"):

    """
    Loads and validates the config
    """
    # load yaml or json
    config_path = os.path.join(path, file_name)
    if not os.path.isfile(config_path):
        config_path = config_path.replace("yaml", "yml")
        if not os.path.isfile(config_path):
            raise FileNotFoundError(
                f"Expecting a file in {path} with name {file_name}."
            )

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    with open("config-schema.json") as f:
        config_schema = json.load(f)

    json_validate(config, config_schema)

    return config


def download_data(s3_path, local_path):
    with open(local_path, "rb") as f:
        b, o = s3.s3_path_to_bucket_key(s3_path)
        s3_client.download_fileobj(b, o, f)


def match_files_in_land_to_config(config):
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
        log.info(f"{land_base_path},Found {total_files} in {land_base_path}")

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

        all_matched = all_matched.extend(table_params["matched_files"])

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


def convert_meta_type_to_goodtable_type(meta_type):
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


def convert_meta_to_goodtables_schema(meta):
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




def validate_data(config):

    utc_ts = int(datetime.utcnow())
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
            log.info(f"{land_base_path},Linting {table_name}")

            meta_file_path = table_params.get("metadata", f"metadata/{table_name}.json")

            with open(meta_file_path) as sfile:
                metadata = json.load(sfile)
                schema = convert_meta_to_goodtables_schema(metadata)

            for i, matched_file in enumerate(table_params["matched_files"]):
                all_matched_files.append(matched_file)
                log.info(f"{land_base_path},...file {i+1} of {len(table_params['matched_files'])}")
                file_basename = os.path.basename(matched_file)
                local_path = f"data_tmp/{file_basename}"
                download_data(matched_file, local_path)
                response = validate(
                    local_path, schema=schema, **table_params.get("gt-kwargs")
                )
                table_response = response["tables"][0]
                table_response["s3-original-path"] = matched_file
                table_response["table-name"] = table_name

                # Write data to s3 on pass or elsewhere on fail
                if table_params["lint-response"]["valid"]:
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
                    log.info(f"{land_base_path},...file passed. Writing to {tmp_outpath}")
                    local_file_to_s3(local_path, tmp_outpath)
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
                    logging.error(f"{land_base_path},...file failed. Writing to {final_outpath}")

                else:
                    table_response["archived-path"] = None

                # Write reponse log
                log_outpath = get_log_path(log_base_path, table_name, utc_ts, filenum=i)

                # Write log to s3
                s3.write_json_to_s3(table_response, log_outpath)
                os.remove(local_path)
                all_table_responses.append(table_response)

        else:
            logging.warning(f"{land_base_path},SKIPPING {table_name}. No files found.")

    if overall_pass:
        log.info(f"{land_base_path},All tables passed")
        if all_must_pass:
            log.info(f"{land_base_path},Moving data from tmp into land-base-path")
            s3.copy_s3_folder_contents_to_new_folder(
                land_base_path, config["land-base-path"]
            )
            s3.delete_s3_folder_contents(land_base_path)

            log.info(f"{land_base_path},Moving data from tmp into log-base-path")
            s3.copy_s3_folder_contents_to_new_folder(
                log_base_path, config["log-base-path"]
            )

            log.info(f"{land_base_path},Removing data in land")
            for matched_file in all_matched_files:
                s3.delete_s3_object(matched_file)

    else:
        log.error("The following tables failed:")
        for resp in all_table_responses:
            if not resp["valid"]:
                log.error(f"{land_base_path},... {resp['table-name']}: \n original path: {resp['s3-original-path']}; \n out path: {resp['archived-path']}")
        if all_must_pass:
            log.info(f"{land_base_path},Logs that show failed data: {land_base_path}")
            log.info(
                f"{land_base_path},Tables that passed but not written due to other table failures are stored here: {log_base_path}"
            )

    if not overall_pass:
        raise ValueError("Tables did not pass linter. Check logs.")


def get_out_path(basepath, table, ts, filename, compress=False, filenum=0):
    filename_only, ext = filename.split(".", 1)
    final_filename = f"{filename_only}-{ts}-{filenum}.{ext}"
    if compress and not ext.endswith(".gz"):
        final_filename += ".gz"

    out_path = os.path.join(
        basepath, table, f"mojap_fileland_timestamp={ts}", final_filename
    )
    return out_path


def get_log_path(basepath, table, ts, filenum=0):
    final_filename = f"log-{table}-{ts}-{filenum}.json"

    out_path = os.path.join(basepath, table, final_filename)
    return out_path


def local_file_to_s3(local_path, s3_path):
    if (not local_path.endswith(".gz")) and (s3_path.endswith(".gz")):
        new_path = local_path + ".gz"
        with open(local_path, "rb") as f_in, gzip.open(new_path, "wb") as f_out:
            f_out.writelines(f_in)
        local_path = new_path

    b, o = s3.s3_path_to_bucket_key(s3_path)
    with open(local_path, "rb") as f:
        s3_client.upload_fileobj(f, b, o)



def generate_iam_config(
    config, iam_config_path="iam_config.yaml", iam_policy_path="iam_policy.json"
):
    """
    Takes file paths from config and generates an iam_config, and optionally an iam_policy

    Parameters
    ----------

    config: dict
        A config loaded from load_and_validate_config()
    
    iam_config_path: str
        Path to where you want to output the iam_config
    
    iam_policy_path: str
        Optional path to output the iam policy json generated from the iam_config just generated



    """

    out_iam = {
        "iam-role-name": config["iam-role-name"],
        "athena": {"write": True},
        "s3": {
            "write_only": [os.path.join(config["log-base-path"], "*")],
            "read_write": [
                os.path.join(config["land-base-path"], "*"),
                os.path.join(config["fail-base-path"], "*"),
                os.path.join(config["pass-base-path"], "*"),
            ],
        },
    }

    with open(iam_config_path, "w") as file:
        yaml.dump(out_iam, file)

    if iam_policy_path:
        if iam_policy_path.endwith(".json"):
            with open(iam_policy_path, "w") as file:
                iam_policy = build_iam_policy(out_iam)
                json.dump(iam_policy, iam_policy_path, indent=4, separators=(",", ": "))
        else:
            raise ValueError("iam_policy_path should be a json file")

def upload_logs(bucket, key, body):
    s3_client.put_object(Body=body, Bucket=bucket, Key=key)
