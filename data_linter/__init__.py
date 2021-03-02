import sys

if sys.version_info[1] < 8:
    from importlib_metadata import version
else:
    from importlib.metadata import version

try:
    __version__ = version("data_linter")
except Exception:
    __version__ = "1776"
