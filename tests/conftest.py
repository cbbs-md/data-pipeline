""" General fixtures """

from importlib import reload
import pytest

import data_pipeline


@pytest.fixture(autouse=True)
def reset_confighandler():
    """ Reset ConfigHandler which is a Singleton """
    # disable fixture by using @pytest.mark.noautofixt
    reload(data_pipeline.config_handler)
