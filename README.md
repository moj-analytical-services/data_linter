# de-docker-data-validator
Docker image used to automatically validate data 

Gets a config and validates data based on that config.

Data that passes is written and stored in archived S3 folders, failed data is stored in an archive for testing. 

This docker image should also output standard logs that are querable via Athena.

My thinking around the config atm

```yaml
land-base-path: s3://land-bucket/my-folder/
fail-base-path: s3://fail-bucket/my-folder/
pass-base-path: s3://pass-bucket/my-folder/
log-base-path: s3://log-bucket/my-folder/
compress-data: True
remove-tables-on-pass: True
must-all-pass: True

tables:
    - table1:
        kwargs: null
        required: True
        pattern: null
        file-schema: None # May not be necessary could be infered
    - table2:
        kwargs: null
        required: True
        pattern: null
        file-schema: None
```