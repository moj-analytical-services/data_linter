import sys
import poetry_version

if sys.version_info[1] < 8:
    from importlib_metadata import version
else:
    from importlib.metadata import version

__version__ = poetry_version.extract(source_file=__file__)
