import pytest

from data_linter.logging_functions import logging_setup


@pytest.mark.parametrize("context", ["", "VALIDATION"])
def test_logger(context):
    log, log_stringio = logging_setup()
    test_message = "test message"
    expected_str_end = f"| PROCESSING | {test_message}\n"
    if context:
        expected_str_end = expected_str_end.replace("PROCESSING", context)
        log.info(test_message, extra={"context": context})
    else:
        log.info(test_message)

    test_out = log_stringio.getvalue()

    assert test_out.endswith(expected_str_end)
