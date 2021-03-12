# Data Linter

A python package to to allow automatic validation of data as part of a Data Engineering pipeline. It is designed to read in and validate tabular data against a given schema for the data. The schemas provided adhere to [our metadata schemas standards](https://github.com/moj-analytical-services/mojap-metadata/) for data. This package can also be used to manage movement of data from a landing area (s3 or locally) to a new location based on the result of the validation.

This package implements different validators using different packages based on the users preference:
- `pandas`: (default) Uses our own lightweight pandas dataframe operations to run simple validation tests on the columns based on the datatype and additional tags in the metadata. Utilises pyarrow for reading data.
- `frictionless`: Uses Frictionless data to validate the data against our metadata schemas. More information can be found [here](https://github.com/frictionlessdata/frictionless-py/)
- `great-expectations`: Uses the Great Expectations data to validate the data against our metadata schemas. More information can be found [here](https://github.com/great-expectations/great_expectations)


## Installation

```bash
pip install data_linter # pandas validator only
```

Or to install the necessary dependencies for the non-default validators.

```bash
pip install data_linter[frictionless] # To include packages required for the frictionless validator

# OR

pip install data_linter[ge] # To include packages required for teh great-expectations validator
```


## Usage

This package takes a `yaml` based config file written by the user (see example below), and validates data in the specified s3 folder path against specified metadata. If the data conforms to the metadata, it is moved to the specified s3 folder path for the next step in the pipeline (note can also provide local paths for running locally). Any failed checks are passed to a separate location for testing. The package also generates logs to allow you to explore issues in more detail.

### Simple Use

To run the validation, at most simple you can use the following:

**In Python:**

```python
from data_linter import run_validation

config_path = "config.yaml"

run_validation(config_path)
```

**Via command line:**

```bash
data_linter --config_path config.yaml
```

### Example config file

```yaml
land-base-path: s3://testing-bucket/land/  # Where to get the data from
fail-base-path: s3://testing-bucket/fail/  # Where to write the data if failed
pass-base-path: s3://testing-bucket/pass/  # Where to write the data if passed
log-base-path: s3://testing-bucket/logs/  # Where to write logs
compress-data: true  # Compress data when moving elsewhere (only applicable from CSV/JSON)
remove-tables-on-pass: true  # Delete the tables in land if validation passes
all-must-pass: true  # Only move data if all tables have passed
fail-unknown-files:
    exceptions:
        - additional_file.txt
        - another_additional_file.txt

validator-engine: pandas # will default to this if unspecified
# (but other options are `frictionless` and `great-expectations`)

# Tables to validate
tables:
    table1:
        required: true  # Does the table have to exist
        pattern: null  # Assumes file is called table1 (same as key)
        metadata: meta_data/table1.json # local path to metadata

    table2:
        required: true
        pattern: ^table2
        metadata: meta_data/table2.json
        row-limit: 10000 # for big tables - only take the first x rows
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

### Validating a single file

> Without all the bells and whistles

If you do not need `data_linter` to match files to a specified config, log the process and then move data around based on the outcome of the validation you can just use the validators themselves:

```python
# Example using simple pandas validatior (without added data_linter features)
import json
from data_linter.validators import PandasValidator

filepath = "tests/data/end_to_end1/land/table1.csv"
table_params = {
    "expect-header": True
}
with open("tests/data/end_to_end1/meta_data/table1.json") as f:
    metadata = json.load(f)

pv = PandasValidator(filepath, table_params, metadata)
pv.read_data_and_validate()

pv.valid  # True (data in table1.csv is valid against metadata)
pv.response.get_result()  # Returns dict of all tests ran against data

# The response object of for the PandasValidator in itself, and has it's own functions
pv.get_names_of_column_failures()  # [], i.e. no cols failed
```


## Validators


### Pandas
```
# - Add notes on dates/datetimes
# - Add data format notes
```

### Frictionless

Known errors / gotchas:
- Frictionless will drop cols in a jsonl files if keys are not present in the first row (would recommend using the `great-expectations` validator for jsonl as it uses pandas to read in the data). [Link to raised issue](https://github.com/frictionlessdata/frictionless-py/issues/490).


### Great Expectations

Known errors / gotchas:
- When setting the "default_result_fmt" to "COMPLETE" current default behavour of codebase. You may get errors due to the fact that the returned result from great expectations tries to serialise a `pd.NA` (as a value sample in you row that failed an expectation) when writing the result as a json blob. This can be avoided by setting the "default_result_fmt" to "BASIC" as seen in the Python example above. [Link to raised issue](https://github.com/great-expectations/great_expectations/issues/2029).


#### Additional Parameters

- `default_result_fmt`: This is passed to the GE validator, if unset default option is to set the value to `"COMPLETE"`. This value sets out how much information to be returned in the result from each "expectation". For more information [see here](https://docs.greatexpectations.io/en/v0.4.0/result_format.html). Also note the safest option is to set it to `"BASIC"` for reasons discussed in the gotcha section above.

- `ignore_missing_cols`: Will not fail if columns don't exist in data but do in metadata (it ignores this).


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