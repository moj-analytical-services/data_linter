import logging
from copy import deepcopy
from typing import List

from mojap_metadata import Metadata
from jsonschema.exceptions import ValidationError


class ValidatorResult:
    """
    Little class to manage adding to validator dict
    """

    def __init__(self, result_dict=None, validator_valid_key_name=None):
        if result_dict:
            if not isinstance(result_dict, dict):
                raise TypeError("result_dict must be a dict type")
            self.result = result_dict
        else:
            self.result = {"valid": True}

        if validator_valid_key_name:
            if not isinstance(validator_valid_key_name, str):
                raise TypeError("validator_valid_key_name must be a str type")
            self.vvkn = validator_valid_key_name
        else:
            self.vvkn = "valid"

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, meta_dict: dict):
        try:
            meta_obj = Metadata.from_dict(meta_dict)
            meta_obj.set_col_type_category_from_types()
            self._metadata = meta_obj.to_dict()

            if "file_format" not in self.metadata:
                raise ValidationError("metadata given must have a file_format property")
        except ValidationError as e:
            error_msg = (
                "Pandas validator requires schemas that conform "
                "to those found in the mojap_metadata package. "
                f"Metadata given failed validation: {str(e)}"
            )
            raise ValidationError(error_msg)

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
        failed_cols = []
        for colname in self.result:
            if colname == "valid" or colname.startswith("validator-table-test-"):
                continue

            if test_names:
                overall_success = True
                for k, v in self.result[colname].items():
                    if k in test_names:
                        overall_success = overall_success and v.get(self.vvkn, True)
            else:
                overall_success = self.result[colname].get("valid", True)

            if not overall_success:
                failed_cols.append(colname)

        return failed_cols

    def add_table_test(self, testname, test_result):
        # Same setup - treats overall test as a colname
        self.init_col(testname)
        self.result[testname] = test_result
        if self.vvkn in test_result:
            self.result["valid"] = self.result["valid"] and test_result[self.vvkn]

    def add_test_to_col(self, colname, testname, test_result):
        self.init_col(colname)

        self.result[colname][testname] = test_result
        if self.vvkn in test_result:
            self.result["valid"] = self.result["valid"] and test_result[self.vvkn]
            self.result[colname]["valid"] = (
                self.result[colname]["valid"] and test_result[self.vvkn]
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

        self.response = ValidatorResult(
            result_dict=kwargs.get("result_dict"),
            validator_valid_key_name=kwargs.get("validator_valid_key_name"),
        )

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
