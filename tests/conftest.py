""" General fixtures """

from importlib import reload
from pathlib import Path
import shutil

from click.testing import CliRunner
import pytest

import data_pipeline
from data_pipeline.data_pipeline import main


@pytest.fixture(autouse=True)
def reset_confighandler():
    """ Reset ConfigHandler which is a Singleton """
    # disable fixture by using @pytest.mark.noautofixt
    reload(data_pipeline.config_handler)


@pytest.fixture
def config_file(tmp_path):
    """ Create a template config file """
    config_path = tmp_path / "config.yaml"
    config_template = Path(
        Path(data_pipeline.__file__).parent.absolute(),
        "templates", "config_template.yaml"
    )
    shutil.copy(config_template, config_path)

    return config_path


@pytest.fixture
def project(tmp_path):
    """ Sets up a proper project dir """
    CliRunner().invoke(main, ["--project", tmp_path, "--setup"])
    reload(data_pipeline.config_handler)

    return tmp_path
