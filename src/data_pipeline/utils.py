""" Collection of general utilities """

import contextlib
import json
import logging
import os
from pathlib import Path
import subprocess
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


def run_cmd(cmd: list, log: logging.Logger, error_message: str = None) -> str:
    """ Runs a command via subprocess and returns the output

    Args:
        cmd: A list of strings definied as for subpocess.run method
        log: a logging logger
        error_message: Message to user when an error occures
    """

    try:
        # pylint: disable=subprocess-run-check
        proc = subprocess.run(cmd, capture_output=True)
    except Exception:
        if error_message:
            log.error(error_message)
        else:
            log.error("Something went wrong when calling subprocess run",
                      exc_info=True)
        raise

    # capture return code explicitely instead of useing subprocess
    # parameter check=True to be able to log error message
    if proc.returncode:
        if proc.stdout:
            log.info(proc.stdout.decode("utf-8"))

        log.debug("cmd: %s", " ".join(cmd))
        if error_message:
            log.error("%s, error was: %s", error_message,
                      proc.stderr.decode("utf-8"))
            raise Exception(error_message)

        log.error("Command failed with error %s",
                  proc.stderr.decode("utf-8"))
        raise Exception()

    return proc.stdout.decode("utf-8")


def run_cmd_piped(cmds: list,
                  log: logging.Logger,
                  error_message: str = None) -> str:
    """ Runs piped commands via subprocess and return the output

    Args:
        cmds: A list of commands, where a command is a list of strings definied
              as for subpocess.run method
        log: a logging logger
        error_message: Message to user when an error occures
    """

    if not cmds:
        return ""

    proc = subprocess.Popen(cmds[0], stdout=subprocess.PIPE)
    try:
        # pylint: disable=subprocess-run-check
        for cmd in cmds:
            proc = subprocess.Popen(cmd, stdin=proc.stdout,
                                    stdout=subprocess.PIPE)
        output, errors = proc.communicate()
    except Exception:
        if error_message:
            log.error(error_message)
        else:
            log.error("Something went wrong when calling subprocess "
                      "communicate", exc_info=True)
        raise

    # capture return code explicitely instead of useing subprocess
    # parameter check=True to be able to log error message
    if errors:
        if output:
            log.info(output.decode("utf-8"))

        log.debug("cmds: %s", " ".join(cmds))
        if error_message:
            log.error("%s, error was: %s", error_message,
                      errors.decode("utf-8"))
            raise Exception(error_message)

        log.error("Command failed with error %s", errors.decode("utf-8"))
        raise Exception()

    return output.decode("utf-8")
