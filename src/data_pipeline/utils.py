""" Collection of general utilities """

import contextlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
from typing import Union
import yaml


class ConfigError(Exception):
    """Raised when there are missing parameters in the configuration"""


class NotPossible(Exception):
    """ Raised when the requested action is not possible """


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


def write_config(config: dict, filename: str):
    """ Write config to to file.

    Args:
        config: The configuration to write.
        filename: File to write the config to.
    """
    config_file = Path(filename)

    with config_file.open('w') as config_f:
        yaml.dump(config, config_f)


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


def read_spec(file_name: Union[str, Path]) -> list:
    """ Reads a datalad spec file and converts it into proper python objects

    Args:
        file_name: the studyspec file name.
    """

    # allow string
    file_name = Path(file_name)

    # strip: file may contain empty lines
    lines = file_name.read_text().strip().split("\n")
    return list(map(json.loads, lines))


def copy_template(template: Union[str, Path], target: Union[str, Path]):
    """ Copies the template file to the target path

    Args:
        template: The path of the template file. Can be either absolute or
            relative inside the current toolbox
        target: The file path where the template should be copied to. If the
            target file does already exist it is not overwritten.
    """
    template = Path(template)
    target = Path(target)

    if not target.exists():
        # account for relative paths for template file
        if not template.is_absolute():
            template = Path(__file__).parent.absolute()/template

        shutil.copy(template, target)


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


def get_logger(my_class, postfix: str = None) -> logging.Logger:
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


def run_cmd(cmd: list, log: logging.Logger, error_message: str = None,
            raise_exception: bool = True, env: dict = None,
            surpress_output: bool = False) -> str:
    """ Runs a command via subprocess and returns the output

    Args:
        cmd: A list of strings definied as for subpocess.run method
        log: a logging logger
        error_message: Message to user when an error occures
        raise_exception: Optional; If an exception should be raised or not in
            case something went wrong during command execution.
        env: Optional; In case the command should be exectued in a special
            environment
        surpress_output: Optional; In case the calling application want to
            control the output separately, it can be disabled.
    """

    try:
        # pylint: disable=subprocess-run-check
        proc = subprocess.run(cmd, capture_output=True, env=env)
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
        if not surpress_output:
            if proc.stdout:
                log.info(proc.stdout.decode("utf-8"))

            log.debug("cmd: %s", " ".join(cmd))
        if error_message:
            log.error("%s, error was: %s", error_message,
                      proc.stderr.decode("utf-8"))
            raise Exception(error_message)

        if raise_exception:
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


def check_cmd(cmd: list) -> bool:
    """ Runs the command and checks if it runs through

    Args:
        cmd: The command to run in subprocess syntax, i.e. as a list of
            strings.
    Returns:
        True if the command worked, False if not.
    """
    try:
        subprocess.run(cmd, check=True, capture_output=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False


def show_side_by_side(left: list, right: list) -> str:
    """ Shows two lists side by side

    For example:
    left = ["first line left"]
    right = ["first line right", second line right]
    will result in
    "llllllllllllllllll  rrrrrrrrrrrrrr\n
    llllllll            r\n
                        rrrrrrrrrr\n"

    Args:
        left: Content of the left column
        right: Content of the right column

    Return:
        A string where both lists are printed side by side.
    """

    col_width = max(len(line) for line in left) + 2  # padding

    max_len = max(len(left), len(right))
    left.extend([""] * (max_len - len(left)))
    right.extend([""] * (max_len - len(right)))

    result = ""
    for row in zip(left, right):
        result += "".join(word.ljust(col_width) for word in row) + "\n"

    return result
