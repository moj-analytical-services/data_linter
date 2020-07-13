from data_linter import run_validation, __version__
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_path", help="Path to a config.yaml")
    parser.add_argument("-v", "--version", action="version", version=__version__)

    args = parser.parse_args()
    config_path = args.config_path

    run_validation(config_path)


if __name__ == "__main__":
    main()
