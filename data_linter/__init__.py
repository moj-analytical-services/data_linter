import os
import atexit
import io
import sys

from importlib.metadata import version
from data_linter.validation import run_validation

__version__ = version("data_linter")
