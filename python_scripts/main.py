from goodtables import validate
from gluejobutils import s3

from functions import load_and_validate_config, validate_data


def main():
    config = load_and_validate_config()
    validate_data(config)


if __name__ == "__main__":
    main()
