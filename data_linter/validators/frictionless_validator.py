import logging

from data_linter.validators.base import BaseTableValidator

optional_import_errors = ""
try:
    from frictionless import (
        validate,
        Table,
        dialects,
        Query,
    )
except ImportError as e:
    optional_import_errors += " " + str(e)

log = logging.getLogger("root")


class FrictionlessValidator(BaseTableValidator):
    """
    Frictionless data validator
    """

    def __init__(self, filepath: str, table_params: dict, metadata: dict):
        super().__init__(filepath, table_params, metadata)
        self.response = None  # Not yet integrated with ValidatorResult class
        self.schema = convert_meta_to_goodtables_schema(metadata)
        if optional_import_errors:
            imp_err = (
                "This validator requires optional packages that are not installed. "
                f"Please see specific errors: {optional_import_errors}"
            )
            raise ImportError(imp_err)

    def write_validation_errors_to_log(self):
        for e in self.response["errors"]:
            log.error(e["message"], extra={"context": "VALIDATION"})

    def read_data_and_validate(self):
        """Reads data from filepath and validates it.

        Using frictionless.
        """

        log.info(f"Reading and validating: {self.filepath}")

        skip_errors = []

        # assert the correct dialect and checks
        header_case = not self.table_params.get("headers-ignore-case", False)
        if "json" in self.metadata["file_format"]:
            expected_headers = [
                c["name"]
                for c in self.metadata["columns"]
                if c not in self.metadata.get("partitions", [])
            ]
            dialect = dialects.JsonDialect(keys=expected_headers)
            if (
                "headers-ignore-case" in self.table_params
                or "expect-header" in self.table_params
            ):
                conf_warn = (
                    "jsonl files do not support header options. If keys "
                    "in json lines do not match up exactly (i.e. case sensitive) "
                    "with meta columns then keys will be nulled"
                )
                log.warning(conf_warn)
        else:  # assumes CSV
            dialect = dialects.Dialect(header_case=header_case)
            if not self.table_params.get("expect-header"):
                skip_errors.append("#head")

        query = None
        row_limit = self.table_params.get("row-limit", False)

        if row_limit:
            query = Query(limit_rows=row_limit)

        if " " in self.filepath:
            raise ValueError("The filepath must not contain a space")

        with Table(self.filepath, dialect=dialect, query=query) as table:
            resp = validate(
                table.row_stream,
                schema=self.schema,
                dialect=dialect,
                skip_errors=skip_errors,
            )

        self.valid = resp.valid
        # Returns a class so lazily converting it to dict
        self.response = dict(resp.tables[0])


def convert_meta_type_to_goodtable_type(meta_col: dict) -> str:
    """
    Converts string name for etl_manager data type
    and converts it to a goodtables data type

    Parameters
    ----------
    meta_col: dict
        Column dict from the metadata columns

    Returns
    -------
    str:
        Column type of the goodtables_type
        https://frictionlessdata.io/specs/table-schema/
    """
    meta_type = meta_col["type"].lower()
    meta_type_category = meta_col["type_category"].lower()

    lookup = {
        "string": "string",
        "integer": "integer",
        "float": "number",
        "boolean": "boolean",
    }

    if meta_type_category in lookup:
        gt_type = lookup[meta_type_category]
    elif meta_type_category.startswith("timestamp"):
        if meta_type.startswith("date"):
            gt_type = "date"
        elif meta_type.startswith("timestamp"):
            gt_type = "datetime"
        else:
            pass
    elif meta_type_category == "list":
        gt_type = "array"
    elif meta_type_category == "struct":
        gt_type = "object"
    else:
        raise TypeError(
            f"Given meta with type:{meta_type} and type_category:"
            f"{meta_type_category} but this matches no goodtables equivalent."
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

        gt_type = convert_meta_type_to_goodtable_type(col)
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
