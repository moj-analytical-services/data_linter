import os
import yaml
import json
from iam_builder.iam_builder import build_iam_policy


def generate_iam_config(
    config,
    iam_config_output="iam_config.yaml",
    iam_policy_output=None,
    overwrite_config=False,
):
    """
    Takes file paths from config and generates an iam_config,
    and optionally an iam_policy

    Parameters
    ----------

    config: dict
        A config loaded from load_and_validate_config()

    iam_config_path: str
        Path to where you want to output the iam_config

    iam_policy_path: str
        Optional path to output the iam policy json generated from the
        iam_config just generated
    """

    if os.path.exists(iam_config_output) and overwrite_config is not True:
        raise ValueError(
            f"{iam_config_output} exists: to overwrite set overwrite_config=True"
        )

    log_path = config["log-base-path"].replace("s3://", "")
    land_path = config["land-base-path"].replace("s3://", "")
    pass_path = config["pass-base-path"].replace("s3://", "")

    read_write = [os.path.join(land_path, "*"), os.path.join(pass_path, "*")]

    if config["fail-base-path"]:
        fail_path = config["fail-base-path"].replace("s3://", "")
        read_write.append(os.path.join(fail_path, "*"))

    out_iam = {
        "iam-role-name": config["iam-role-name"],
        "athena": {"write": True},
        "s3": {"write_only": [os.path.join(log_path, "*")], "read_write": read_write},
    }

    with open(iam_config_output, "w") as f:
        yaml.dump(out_iam, f)

    if iam_policy_output:
        if iam_policy_output.endswith(".json"):

            with open(iam_policy_output, "w") as f:
                iam_policy = build_iam_policy(out_iam)
                json.dump(iam_policy, f, indent=4, separators=(",", ": "))
        else:
            raise ValueError("iam_policy_path should be a json file")
