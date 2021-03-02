import logging
from copy import deepcopy


class ValidatorResult(object):
    """
    Little class to manage adding to validator dict
    """

    def __init__(self, result_dict={}):
        if result_dict:
            self.result = result_dict
        else:
            self.result = {"valid": True}

    def get_result(self, copy=True):
        if copy:
            return deepcopy(self.result)
        else:
            return self.result

    def init_col(self, colname):
        if colname not in self.result:
            self.result[colname] = {"valid": True}

    def get_names_of_column_failures(self, test_names: List[str] = []):
        """

        Return col names which have an overall fail. If test_names is given
        only returns cols that failed those particular tests is given.
        Args:
            test_name (List[str], optional): [description]. List of tests to
            check against Defaults to [].
        """
        non_column_names = ["valid", "validator-table-test-"]
        failed_cols = []
        for colname in self.result:
            if colname in non_column_names:
                continue

            if test_names:
                overall_success = True
                for k, v in self.result[colname].items():
                    if k in test_names:
                        overall_success = overall_success and v.get("success", True)
            else:
                overall_success = self.result[colname].get("valid", True)

            if not overall_success:
                failed_cols.append(colname)

        return failed_cols

    def add_table_test(self, testname, test_result):
        # Same setup - treats overall test as a colname
        self.init_col(testname)
        self.result[testname] = test_result
        if "success" in test_result:
            self.result["valid"] = self.result["valid"] and test_result["success"]

    def add_test_to_col(self, colname, testname, test_result):
        self.init_col(colname)

        self.result[colname][testname] = test_result
        if "success" in test_result:
            self.result["valid"] = self.result["valid"] and test_result["success"]
            self.result[colname]["valid"] = (
                self.result[colname]["valid"] and test_result["success"]
            )


class BaseTableValidator:
    def __init__(self, filepath: str, table_params: dict, metadata: dict, **kwargs):
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
        self.response = ValidatorResult()

    def write_validation_result_to_log(self, log: logging.Logger):
        """Writes a the validators response to log provided.
        Default behavior is to just write str representation
        to log.

        Args:
            log (logging.Logger): A python log
            table_resp (dict): A dictionary that will be written as
        """
        log.error(str(self.response), extra={"context": "VALIDATION"})

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

    def get_response_dict(self):
        """
        Returns the response object as a dictionary
        """
        return deepcopy(self.response)
