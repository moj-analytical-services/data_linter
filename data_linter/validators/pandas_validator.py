import logging
import pandas as pd

from arrow_pd_parser.parse import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
    pa_read_csv_to_pandas,
)

from arrow_pd_parser.pa_pd import arrow_to_pandas

from pyarrow import parquet as pq

from data_linter.validators.base import (
    BaseTableValidator,
    ValidatorResult,
)

class PandasValidator(BaseTableValidator):
    """
    Validator using Pandas
    """

    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: dict,
        ignore_missing_cols=False,
    ):
        super().__init__(filepath, table_params, metadata)

        self.ignore_missing_cols = ignore_missing_cols

    @property
    def valid(self):
        return self.response.result["valid"]

    def write_validation_errors_to_log(self, log: logging.Logger):
        table_result = self.response.get_result()
        if not table_result["valid"]:
            failed_cols = self.response.get_names_of_column_failures()
            err_msg = (
                "Table failed validation. "
                f"Col failures: {failed_cols}. "
                "See response error log for more details."
            )
            log.error(err_msg, extra={"context": "VALIDATION"})
            log.debug(str(table_result), extra={"context": "VALIDATION"})

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.

        Data is read using pd_arrow_parser.
        """

        df = self.read_data()  # KARIK TODO
        self.validate_df(df)

    def get_response_dict(self):
        self.response.get_result()

    def read_data(self) -> pd.DataFrame:  # KARIK TODO
        """
        Reads in the data from the given filepath and returns
        a dataframe
        """

        if self.filepath.startswith("s3://"):
            df = None  # TODO
        else:
            if "csv" in self.metadata.data_format:
                df = pa_read_csv_to_pandas(
                    input_file=self.filepath,
                    schema=None,  # Needs actual schema
                    expect_full_schema=False
                )
            elif "json" in self.metadata.data_format:
                df = pa_read_json_to_pandas(
                    input_file=self.filepath,
                    schema=None,  # Needs actual schema
                    expect_full_schema=False
                )
            elif "parquet" in self.metadata.data_format:
                df = arrow_to_pandas(
                    pq.read_table(self.filepath)
                )

        return df

    def validate_df(self, df):  # STEPHEN TODO
        pass
