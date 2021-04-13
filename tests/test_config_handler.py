""" Test the ConfigHandler class """

# pylint: disable=missing-function-docstring, no-self-use

import copy
from unittest import mock

import pytest

from data_pipeline.config_handler import ConfigHandler
import data_pipeline.utils as utils


@pytest.fixture(name="config")
def config_fixture():
    return {
        "name": "Eggs",
        "price": 12.34
    }


@pytest.fixture(name="multi_config")
def multi_config_fixture(config):
    return {
        "test_module": config,
        "test_module2": copy.deepcopy(config)
    }


@pytest.fixture(name="schema")
def schema_fixture():
    return {
        "type": "object",
        "properties": {
            "price": {"type": "number"},
            "name": {"type": "string"},
        }
    }


@pytest.fixture(name="multi_schema")
def multi_schema_fixture(schema):
    return {
        "type": "object",
        "properties": {
            "test_module": schema,
            "test_module2": copy.deepcopy(schema)
        },
        "required": []
    }


@pytest.fixture(name="config_handler")
def config_handler_fixture():
    with mock.patch.object(ConfigHandler, "get"):
        return ConfigHandler(config_file="no_file")


def test_multiple_instanciation(config_file):
    """ Check that instanciating multiple instances is not possible """

    ConfigHandler(config_file=config_file)
    with pytest.raises(utils.UsageError):
        ConfigHandler(config_file=config_file)


def test_add_schema(config_handler, schema):
    config_handler.add_schema(module="test_module", schema=schema)

    assert config_handler.schema["properties"]["test_module"] == schema
    assert "test_module" in config_handler.schema["required"]


class TestValidate:
    """ Collection of tests concering the validate method """

    def test_external_config(self, config_handler, schema, config):
        config_handler.validate(config=config, schema=schema)

    def test_wrong_config(self, config_handler, schema, config):
        config["price"] = "some_string"
        with pytest.raises(utils.ConfigError):
            config_handler.validate(config=config, schema=schema)

    def test_specific_module(self, config_handler, multi_schema, config):
        config_handler.schema = multi_schema
        config_handler.validate(config=config, module="test_module")

    def test_internal_config(self, config_handler, multi_schema, multi_config):
        config_handler.schema = multi_schema
        config_handler.config = multi_config
        config_handler.validate()

    def test_all_modules(self, config_handler, multi_schema, multi_config):
        (multi_schema["properties"]["test_module2"]
                     ["properties"]["price"]["type"]) = "string"
        multi_config["test_module2"]["price"] = "123"

        config_handler.schema = multi_schema
        config_handler.config = multi_config
        config_handler.validate()

    def test_wrong_modules(self, config_handler, multi_schema, multi_config):
        multi_config["test_module2"]["price"] = "123"

        config_handler.schema = multi_schema
        config_handler.config = multi_config
        with pytest.raises(utils.ConfigError):
            config_handler.validate()

    def test_not_existing_module(self, config_handler, multi_schema,
                                 multi_config):
        config_handler.schema = multi_schema
        config_handler.config = multi_config
        with pytest.raises(KeyError):
            config_handler.validate(module="not_existing_module")


def test_write():
    """ Check writing of a configuration """


def test_update_parameter():
    """ Check if a paramater is correctly updated """
