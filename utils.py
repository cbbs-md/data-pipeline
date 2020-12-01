import json
from pathlib import Path
from typing import Union
import yaml


class ConfigError(Exception):
    """Raised when there are missing parameters in the configuration"""
    pass


def get_config(filename: str) -> dict:
    """ Read config from file.

    Args:
        filename: File to read the config from.
    Returns:
        The configuration as dictionary.
    """
    config_file = Path(filename)

    with config_file.open('r') as config_f:
        config = yaml.safe_load(config_f)

    return config


def read_subjects(filename: str) -> Union[list,dict]:
    """ Read in a json file

    Args:
        filename: The file to read.
    Returns:
        The contant of the file as
    """

    print("Reading file", filename)
    sub_file = Path(filename)

    with sub_file.open("r") as my_file:
        data = my_file.read()
        subjects = json.loads(data)

    for i in subjects:
        print(i)

    return subjects


def write_subjects(subjects: list, filename: str):
    """ Write subject in a file.

    Args:
        subjects: list of subject dictionaries
            e.g. [{"anon-sub": "20","acq": "cn85_3942"0},
                  {"anon-sub": "21","acq": "test"}]
        filename: The filename where to write the subjects to.
    """

    print("Write", filename)

    with open(filename, "w") as my_file:
        json.dump(subjects, my_file, indent=4, sort_keys=True)

