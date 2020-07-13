from data_linter import run_validation, __version__
import poetry_version
import argparse

def main():
    __version__ = poetry_version.extract(source_file=__file__)
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
