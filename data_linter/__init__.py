import os
import atexit
import io
import sys

import poetry_version

from data_linter.validation import run_validation

__version__ = "1.0.0"

# poetry_version.extract(source_file=__file__)
