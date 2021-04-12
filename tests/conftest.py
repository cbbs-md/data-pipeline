""" General fixtures """

from importlib import reload
from pathlib import Path
import shutil

import pytest

import data_pipeline


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
