from data_linter import __version__
from data_linter.validation import run_validation

import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )
    parser.add_argument("-c", "--config_path", help="Path to a config.yaml")
    args = parser.parse_args()
    run_validation(args.config_path)


if __name__ == "__main__":
    main()
