""" Test the ConfigHandler class """

from unittest import mock

import pytest

from data_pipeline.config_handler import ConfigHandler
import data_pipeline.utils as utils


def test_multiple_instanciation(config_file):
    """ Check that instanciating multiple instances is not possible """

    ConfigHandler(config_file=config_file)
    with pytest.raises(utils.UsageError):
        ConfigHandler(config_file=config_file)


def test_validate():
    """ Test validation of a configuration schema """

    config = {
        "name": "Eggs",
        "price": 12.34
    }

    schema = {
        "type": "object",
        "properties": {
            "price": {"type": "number"},
            "name": {"type": "string"},
        },
    }

    with mock.patch.object(ConfigHandler, "get"):
        config_handler = ConfigHandler(config_file="no_file")
        config_handler.validate(config=config, schema=schema)

    # TODO test other different combinations


def test_write():
    """ Check writing of a configuration """


def test_update_parameter():
    """ Check if a paramater is correctly updated """
