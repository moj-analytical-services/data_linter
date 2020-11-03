# Data Linter

A python package to to allow automatic validation of data as part of a Data Engineering pipeline. It is designed to automate the process of moving data from Land to Raw-History as described in the [ETL pipline guide](https://github.com/moj-analytical-services/etl-pipeline-example)

This package implements different validators using different packages:

- `frictionless`: Uses Frictionless data to validate the data against our metadata schemas. More information can be found [here](https://github.com/frictionlessdata/frictionless-py/)
- `great-expectations`: Uses the Great Expectations data to validate the data against our metadata schemas. More information can be found [here](https://github.com/frictionlessdata/frictionless-py/)


## Installation

```bash
pip install data_linter # frictionless only
```

```bash
pip install data_linter[ge] # To include packages required for teh great-expectations validator
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
land-base-path: s3://land-bucket/my-folder/  # Where to get the data from
fail-base-path: s3://fail-bucket/my-folder/  # Where to write the data if failed
pass-base-path: s3://pass-bucket/my-folder/  # Where to write the data if passed
log-base-path: s3://log-bucket/my-folder/  # Where to write logs
compress-data: true  # Compress data when moving elsewhere
remove-tables-on-pass: true  # Delete the tables in land if validation passes
all-must-pass: true  # Only move data if all tables have passed
fail-unknown-files:
    exceptions:
        - additional_file.txt
        - another_additional_file.txt

validator-engine: frictionless # will default to this if unspecified

# Tables to validate
tables:
    table1:
        required: true  # Does the table have to exist
        pattern: null  # Assumes file is called table1
        metadata: meta_data/table1.json

    table2:
        required: true
        pattern: ^table2
        metadata: meta_data/table2.json
```

You can also run the validator as part of a python script, where you might want to dynamically generate your config:

```python
from data_linter.validation import run_validation

base_config = {
    "land-base-path": "s3://my-bucket/land/",
    "fail-base-path": "s3://my-bucket/fail/",
    "pass-base-path": "s3://my-bucket/pass/",
    "log-base-path": "s3://my-bucket/log/",
    "compress-data": False,
    "remove-tables-on-pass": False,
    "all-must-pass": False,
    "validator-engine": "great-expectations",
    "validator-engine-params": {"default_result_fmt": "BASIC", "ignore_missing_cols": True},
    "tables": {}
}

def get_table_config(table_name):
    d = {
        "required": False,
        "expect-header": True,
        "metadata": f"metadata/{table_name}.json",
        "pattern": r"^{}\.jsonl$".format(table_name),
        "headers-ignore-case": True,
        "only-test-cols-in-metadata": True # Only currently supported by great-expectations validator
    }
    return d

for table in ["table1", "table2"]:
    base_config["tables"][table_name] = get_table_config(table_name)

run_validation(base_config) # Then watch that log go...
```

## Validators

### Frictionless

Known errors / gotchas:
- Frictionless will drop cols in a jsonl files if keys are not present in the first row (would recommend using the `great-expectations` validator for jsonl as it uses pandas to read in the data). [Link to raised issue](https://github.com/frictionlessdata/frictionless-py/issues/490).


### Great Expectations

Known errors / gotchas:
- When setting the "default_result_fmt" to "COMPLETE" current default behavour of codebase. You may get errors due to the fact that the returned result from great expectations tries to serialise a `pd.NA` (as a value sample in you row that failed an expectation) when writing the result as a json blob. This can be avoided by setting the "default_result_fmt" to "BASIC" as seen in the Python example above. [Link to raised issue](https://github.com/great-expectations/great_expectations/issues/2029).


#### Additional Parameters

- `default_result_fmt`: This is passed to the GE validator, if unset default option is to set the value to `"COMPLETE"`. This value sets out how much information to be returned in the result from each "expectation". For more information [see here](https://docs.greatexpectations.io/en/v0.4.0/result_format.html). Also note the safest option is to set it to `"BASIC"` for reasons discussed in the gotcha section above.

- `ignore_missing_cols`: Will only lint columns in the corresponding metadata. This is useful if you just want to make sure new data won't break deployed systems (i.e. everything else is fine if I drop that column before I push it elsewhere)


## Process Diagram

How logic works

![](images/data_linter_process.png)

## How to update

We have tests that run on the current state of the `poetry.lock` file (i.e. the current dependencies). We also run tests based on the most up to date dependencies allowed in `pyproject.toml`. This allows us to see if there will be any issues when updating dependences. These can be run locally in the `tests` folder.

When updating this package, make sure to change the version number in `pyproject.toml` and describe the change in CHANGELOG.md.

If you have changed any dependencies in `pyproject.toml`, run `poetry update` to update `poetry.lock`.

Once you have created a release in GitHub, to publish the latest version to PyPI, run:

```bash
poetry build
poetry publish -u <username>
```

Here, you should substitute <username> for your PyPI username. In order to publish to PyPI, you must be an owner of the project.