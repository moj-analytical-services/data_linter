# de-docker-data-validator
Docker image used to automatically validate data 

Gets a config and validates data based on that config.

Data that passes is written and stored in archived S3 folders, failed data is stored in an archive for testing. 

This docker image should also output standard logs that are querable via Athena.
