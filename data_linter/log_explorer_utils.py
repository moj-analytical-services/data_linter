import os

from arrow_pd_parser import reader
from data_linter.validation import load_and_validate_config
from IPython.display import Markdown


def summary_of_all_tables(config_path: str):
    """
    Summary measures:
        - overall validity
        - total number files that have failed as a percentage and number
        - count of failures per table
    """
    # get the config
    config = load_and_validate_config(config_path)
    # make the logs path
    pull_logs_from = os.path.join(config["log-base-path"], "tables")
    # pull logs as df
    logs_df = reader.read(pull_logs_from, file_format="jsonl")
    # get overall valid
    overall_valid = "✅" if logs_df["valid"].all() else "❌"
    total = len(logs_df["valid"])
    count_successes = logs_df["valid"].sum()
    # get number of failures
    count_fails = total - count_successes
    # get percentage of files that failed
    percentage_fails = (count_fails / total) * 100
    count_fails = logs_df["valid"].value_counts().to_dict().get(False, 0)
    # make the summary markdown
    summary_markdown = (
        "overall valid | fail percentage | fail count\n"
        "--- | --- | ---\n"
        f"{overall_valid} | {percentage_fails}% | {count_fails}"
    )
    # get list of tables
    table_list = list(logs_df["table-name"].unique())
    # get the failure count per table
    table_fails_markdown = (
        "table | percentage of files failed | number of failed files\n"
        "--- | --- | ---\n"
    )
    for table_name in table_list:
        # just get this tables deets
        table_log_df = logs_df[logs_df["table-name"] == table_name]
        # get percentage of fails
        table_percentage_fails = (
            table_log_df["valid"]
            .value_counts(normalize=True)
            .mul(100)
            .to_dict()
            .get(False, 0.0)
        )
        # get count of fails
        table_count_fails = table_log_df["valid"].value_counts().to_dict().get(False, 0)
        # add results to markdown
        table_fails_markdown += (
            f"{table_name} | {table_percentage_fails} | {table_count_fails}\n"
        )

    return Markdown(
        f"### overall summary \n{summary_markdown}\n"
        f"### per table summary \n{table_fails_markdown}\n"
    )


def get_failed_files(config_path: str, table_name: str = None) -> Markdown:
    # set the table name
    table_name = "" if not table_name else table_name
    # get the config
    config = load_and_validate_config(config_path)
    # get the path of the logs required to read
    pull_logs_from = os.path.join(config["log-base-path"], "tables", table_name)
    # read the logs
    logs_df = reader.read(pull_logs_from, file_format="jsonl")
    # get all the failed paths
    trimmed = logs_df[logs_df["valid"] is False][["table-name", "original-path"]]
    # return it as markdown
    return Markdown(trimmed.to_markdown())


def get_all_errors_for_file(config_path: str, file_path: str):
    # get the config
    config = load_and_validate_config(config_path)
    # get the path of the logs required to read
    pull_logs_from = os.path.join(config["log-base-path"], "tables")
    # read the logs
    logs_df = reader.read(pull_logs_from, file_format="jsonl")
    # get the errors for the file in question from all the logs
    file_logs = logs_df[logs_df["original-path"] == file_path]
    # if the file logs has more than one entry, then it probably contains logs from more
    # than one lint run, lets tell the user that
    if len(file_logs) > 1:
        print(
            "More than one log for file, output may contain duplicate entries\n\n"
            "Entries show most recent first"
        )
    # extract the timestamps from the log files
    file_logs["ts"] = file_logs["archived-path"].apply(
        lambda x: os.path.splitext(os.path.basename(x))[0].rsplit("-", 1)[1]
    )
    # sort in descending order
    file_logs = file_logs.sort_values(by="ts", ascending=False)
    # use this to collect the markdown tables
    list_of_markdown_tables = []
    # for each file, generate a markdown table in descending order of the timestamp
    for i in range(len(file_logs)):
        # get the response dict
        current_response_dict = file_logs["response"][0]
        # make the markdown header template
        file_markdown = (
            f"**file:** {file_logs['original-path'][i]}\n"
            f"**timestamp of run:** {file_logs['ts'][i]}\n\n"
            "column | test name | test result | percentage error | traceback/error\n"
            "--- | --- | --- | --- | ---\n"
        )
        # add each column and test to the this files markdown table
        for col, tests in current_response_dict.items():
            if col == "valid":
                continue
            # for each test in this column, make the markdown for it
            for test_name, test_result in tests.items():
                if test_name == "valid":
                    continue
                test_valid = "✅" if test_result["valid"] else "❌"
                percentage_error = test_result.get(
                    "percentage_of_column_is_error", "n/a"
                )
                tb = test_result.get("traceback", "n/a")
                file_markdown += (
                    f"{col} | {test_name} | {test_valid} | {percentage_error} | {tb}\n"
                )
        list_of_markdown_tables.append(file_markdown + "\n\n")
    return Markdown("\n\n".join(list_of_markdown_tables))
