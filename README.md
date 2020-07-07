# Data Linter

A python package to to allow automatic validation of data as part of a Data Engineering pipeline. It is designed to automate the process of moving data from Land to Raw-History as described in the [ETL pipline guide](https://github.com/moj-analytical-services/etl-pipeline-example)

The validation is based on the `goodtables` package, from the fine folk at Frictionless Data. More information can be found at [their website.](https://frictionlessdata.io/tooling/goodtables/#check-it-out)

## Installation

```bash
pip install data_linter
```

## Usage

This package takes a `yaml` based config file written by the user (see example below), and validates data in the specified Land bucket against specified metadata. If the data conforms to the metadata, it is moved to the specified Raw bucket for the next step in the pipeline. Any failed checks are passed to a separate bucket for testing. The package also generates logs to allow you to explore issues in more detail.

To run the validation, at most simple you can use the following:

```python
from data_linter import run_validation

config_path = "config.yaml"

run_validation(config_path)
```

## Example config file

```yaml
land-base-path: s3://land-bucket/my-folder/ # Where to get the data from
fail-base-path: s3://fail-bucket/my-folder/ # Where to write the data if failed validation
pass-base-path: s3://pass-bucket/my-folder/ # Where to write the data if passed validation
log-base-path: s3://log-bucket/my-folder/ # Where to write logs - necessary should be centralised? based on repo names maybe
compress-data: True # Compress data when moving elsewhere
remove-tables-on-pass: True # Delete the tables if pass 
all-must-pass: True # Only move data if all tables have passed
fail-unknown-files:
    exceptions: 
      - additional_file.txt
      - another_additional_file.txt

# Tables to validate
tables:
    - table1:
        kwargs: null
        required: True # Does the table have to exist
        pattern: null # Assumes file is called table1
        metadata: null # May not be necessary could be infered
        linter: goodtables # jsonschema?
        gt-kwargs:
            # kwargs specific to goodtables - not sure about this. Might be better to 
            # put into the file shema

    - table2:
        kwargs: null
        required: True
        pattern: ^table2
        metadata: metadata/table2.json # Should be an overwrite the input here is what it should infered as if set to None
```

## How to update

When updating this package, make sure to change the version number in `pyproject.toml` and describe the change in CHANGELOG.md.

If you have changed any dependencies in `pyproject.toml`, run `poetry update` to update `poetry.lock`. 
Then run `poetry export -f requirements.txt -o requirements.txt` to update the requirements.txt to get picked up by the `Dockerfile`.

Once you have created a release in GitHub, to publish the latest version to PyPI, run:

```bash
poetry build
poetry publish -u <username>
```

Here, you should substitute <username> for your PyPI username. In order to publish to PyPI, you must be an owner of the project.