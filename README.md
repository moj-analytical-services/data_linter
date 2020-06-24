# de-docker-data-validator
Docker image used to automatically validate data 

Gets a config and validates data based on that config.

Data that passes is written and stored in archived S3 folders, failed data is stored in an archive for testing. 

This docker image should also output standard logs that are querable via Athena.

My thinking around the config atm

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