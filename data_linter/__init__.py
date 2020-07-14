import os
import atexit
import io
import sys

if sys.version_info[1] < 8:
    from importlib_metadata import version
else:
    from importlib.metadata import version
    
from data_linter.validation import run_validation

__version__ = version("data_linter")
