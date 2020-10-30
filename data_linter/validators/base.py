import logging


class BaseTableValidator(object):
    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: dict,
    ):
        """Base class for validators. Not a useable,
        but used to be inherited for other validators.

        Args:
            filepath (str): path to data to validate
            table_params (dict): Parameters which define how data is validated.
            Taken from config. metadata (dict): Standard metadata for the table
            to validate.
        """
        self.filepath = filepath
        self.table_params = table_params
        self.metadata = metadata
        self.valid = None
        self.response = None

    def write_validation_result_to_log(self, log: logging.Logger):
        """Writes a table response to log provided.
        Default behavior is to just write str representation
        to log.

        Args:
            log (logging.Logger): A python log
            table_resp (dict): A dictionary that will be written as
        """
        log.error(self.get_error_response_str(), extra={"context": "VALIDATION"})

    def write_validation_errors_to_log(self, log: logging.Logger):
        raise NotImplementedError("Needs to be overwritten")

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.
        Should set classes valid parameter to True/False depending
        on outcome.

        Boilerplate to be overwritten.

        Args:
            filepath (str): Path to data
            table_params (dict): Table params
            metadata (dict): [description]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError("Needs to be overwritten")
