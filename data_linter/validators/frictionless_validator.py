import logging
from data_linter.validators.base import BaseTableValidator


optional_import_errors = ""
try:
    from goodtables import validate
except ImportError as e:
    optional_import_errors += " " + str(e)

try:
    from tabulator import Stream
except ImportError as e:
    optional_import_errors += " " + str(e)

log = logging.getLogger("root")


class FrictionlessValidator(BaseTableValidator):
    """
    Frictionless data validator
    """
    def __init__(
        self,
        filepath: str,
        table_params: dict,
        metadata: dict
    ):
        super().__init__(filepath, table_params, metadata)
        self.schema = convert_meta_to_goodtables_schema(metadata)
        if optional_import_errors:
            imp_err = (
                "This validator requires optional packages that are not installed. "
                f"Please see specific errors: {optional_import_errors}"
            )
            raise ImportError(imp_err)

    def write_validation_errors_to_log(self, log: logging.Logger):
        for e in self.response["errors"]:
            log.error(e["message"], extra={"context": "VALIDATION"})

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.

        Using frictionless.
        """

        if " " in self.filepath:
            raise ValueError("The filepath must not contain a space")
        with Stream(self.filepath) as stream:
            if self.table_params.get("expect-header") and self.metadata["data_format"] != "json":
                # Get the first line from the file if expecting a header
                headers = next(stream.iter())
                if self.table_params.get("headers-ignore-case"):
                    headers = [h.lower() for h in headers]
            else:
                headers = [c["name"] for c in self.metadata["columns"]]

            # This has to be added for jsonl
            # This forces the validator to put the headers in the right order
            # and inform it ahead of time what all the headers should be.
            # If not specified the iterator reorders the columns.
            stream.headers = headers
            if self.metadata["data_format"] == "json":
                skip_checks = ["missing-value"]
            else:
                skip_checks = []

            resp = validate(
                stream.iter, schema=self.schema, headers=headers, skip_checks=skip_checks
            )
            self.response = resp["tables"][0]


def convert_meta_type_to_goodtable_type(meta_type: str) -> str:
    """
    Converts string name for etl_manager data type
    and converts it to a goodtables data type

    Parameters
    ----------
    meta_type: str
        Column type of the etl_manager metadata

    Returns
    -------
    str:
        Column type of the goodtables_type
        https://frictionlessdata.io/specs/table-schema/
    """
    meta_type = meta_type.lower()

    lookup = {
        "character": "string",
        "int": "integer",
        "long": "integer",
        "float": "number",
        "double": "number",
        "date": "date",
        "datetime": "datetime",
        "boolean": "boolean",
    }

    if meta_type in lookup:
        gt_type = lookup[meta_type]
    elif meta_type.startswith("array"):
        gt_type = "array"
    elif meta_type.startswith("struct"):
        gt_type = "object"
    else:
        raise TypeError(
            f"Given meta_type: {meta_type} but this matches no goodtables equivalent"
        )

    return gt_type


def convert_meta_to_goodtables_schema(meta: dict) -> dict:
    """
    Should take our metadata file and convert it to a goodtables schema

    Parameters
    ----------
    meta: dict
        Takes a metadata dictionary (see etl_manager)
        then converts that to a particular schema for linting

    Returns
    -------
    dict:
        A goodtables schema
    """

    gt_template = {
        "$schema": "https://frictionlessdata.io/schemas/table-schema.json",
        "fields": [],
        "missingValues": [],
    }

    gt_constraint_names = [
        "unique",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
        "pattern",
        "enum",
    ]

    for col in meta["columns"]:
        gt_constraints = {}

        gt_type = convert_meta_type_to_goodtable_type(col["type"])
        gt_format = col.get("format", "default")

        if gt_type in ["date", "datetime"] and "format" not in col:
            gt_format = "any"

        if "nullable" in col:
            gt_constraints["required"] = not col["nullable"]

        contraint_params_in_col = [g for g in gt_constraint_names if g in col]

        for gt_constraint_name in contraint_params_in_col:
            gt_constraints[gt_constraint_name] = col[gt_constraint_name]

        gt_template["fields"].append(
            {
                "name": col["name"],
                "type": gt_type,
                "format": gt_format,
                "constraints": gt_constraints,
            }
        )

    return gt_template
