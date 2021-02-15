""" Collection of general utilities """

import contextlib
import json
import logging
import os
from pathlib import Path
from typing import Union
import yaml


class ConfigError(Exception):
    """Raised when there are missing parameters in the configuration"""


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


def setup_logging(log_level=logging.DEBUG):
    """ Set up a logger.

    Args:
        log_level: The logging level to end in the log handler.
    """

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
            # default:
            # '%(levelname)s:%(name)s:%(message)s'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(log_level)

    return logger


def read_subjects(filename: str) -> Union[list, dict]:
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


def read_spec(file_name: str or Path) -> list:
    """ Reads a datalad spec file and converts it into proper python objects

    Args:
        file_name: the studyspec file name.
    """

    # allow string
    file_name = Path(file_name)

    # strip: file may contain empty lines
    lines = file_name.read_text().strip().split("\n")
    return list(map(json.loads, lines))


class ChangeWorkingDir(contextlib.ContextDecorator):
    """ Change the working directory temporaly """

    def __init__(self, new_wd):
        self.current_wd = None
        self.new_wd = new_wd

    def __enter__(self):
        self.current_wd = Path.cwd()
        os.chdir(self.new_wd)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.current_wd)
        # signal that the exception was handled and the program should continue
        return True


def get_logger_name() -> str:
    """ Returns a common logger name

    This should make it easier to identify the modules as part of the
    data-pipeline tool set.
    """
    return "data-pipeline"


def get_logger(my_class, postfix=None) -> logging.Logger:
    """Return a logger with a name corresponding to the tool set.

    Will return a logger of the the name data-pipeline.<class_module>.<class>
    or if postfix is set data-pipeline.<class_module>.<class><postfix>

    Args:
        my_class: class descriptor opject __class__
        postfix: optional postfix to be added after the class logger name
    """
    name = "{}.{}.{}".format(get_logger_name(),
                             my_class.__module__,
                             my_class.__name__)
    if postfix is not None:
        name += postfix

    return logging.getLogger(name)
