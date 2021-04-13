""" Test the ConfigHandler class """

# pylint: disable=missing-function-docstring, no-self-use

import copy
from unittest import mock

import pytest
import yaml

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
    """ Collection of tests concerning the validate method """

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


@pytest.fixture(name="write_config")
def write_config_fixture(tmp_path):
    def _write_config(config: dict) -> str:
        config_file = tmp_path / "config.yaml"
        with config_file.open('w') as config_f:
            yaml.dump(config, config_f)

        return config_file

    return _write_config


def read_config(config_file):
    with config_file.open('r') as config_f:
        return yaml.safe_load(config_f)


class TestGet:
    """ Collection of tests concerning the get method """

    def test_complete_config(self, config_handler, write_config, config):
        config_handler.config_file = write_config(config)
        module_config = config_handler.get()
        assert module_config == config

    def test_module_config(self, config_handler, write_config, multi_config):
        config_handler.config_file = write_config(multi_config)
        module_config = config_handler.get("test_module")
        assert module_config == multi_config["test_module"]


class TestWrite():
    """ Collection of tests concerning the write method """

    def test_write(self, tmp_path, config_handler, config):

        config_file = tmp_path / "config.yaml"
        config_handler.config_file = config_file
        config_handler.write(config)

        assert config_file.exists()
        assert read_config(config_file) == config

    def test_not_valid_config(self, tmp_path, config_handler, config, schema):

        config_file = tmp_path / "config.yaml"
        config_handler.config_file = config_file
        config_handler.schema = schema
        config["price"] = "some_string"

        with pytest.raises(utils.ConfigError):
            config_handler.write(config)
        assert not config_file.exists()


class TestUpdateParameter:
    """ Collection of tests concering the update_parameter method """

    @pytest.fixture
    def config_handler(self, multi_config, multi_schema, write_config):
        config_file = write_config(multi_config)
        config_handler = ConfigHandler(config_file=config_file)
        config_handler.schema = multi_schema

        return config_handler

    def test_update_parameter(self, config_handler):

        config_handler.update_parameter(module="test_module",
                                        parameter="price",
                                        value=5)
        new_config = read_config(config_handler.config_file)
        assert new_config["test_module"]["price"] == 5

    def test_invalid_value(self, config_handler):

        with pytest.raises(utils.ConfigError):
            config_handler.update_parameter(module="test_module",
                                            parameter="price",
                                            value="something")

    def test_invalid_module(self, config_handler):
        with pytest.raises(KeyError):
            config_handler.update_parameter(module="test_module_invalid",
                                            parameter="price",
                                            value=5)
