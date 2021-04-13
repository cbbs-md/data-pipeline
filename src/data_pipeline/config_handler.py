""" Implements a singleton for the config handling"""

from typing import Any

import jsonschema

from data_pipeline import utils


class ConfigHandler():
    """ A Singleton handling all configuration file access """

    _instance = None

    def __init__(self, config_file="config.yaml"):
        """ Virtually private constructor. """

        if ConfigHandler._instance is not None:
            raise utils.UsageError(
                "There can only be one ConfigHandler instance. "
                "Use get_instance to get it"
            )

        ConfigHandler._instance = self

        self.log = utils.get_logger(__class__)  # type: ignore

        self.config_file = config_file
        self.schema = {
            "type": "object",
            "properties": {},
            "required": []
        }
        self.config = self.get()

    @staticmethod
    def get_instance():
        """ Static access method. """

        if ConfigHandler._instance is None:
            ConfigHandler()

        return ConfigHandler._instance

    def add_schema(self, module: str, schema: dict):
        """ Appends an schema to test the configuration against.

        This is mainly used for modules to be added to the pipeline
        (e.g. bids conversion).

        Args:
            module: The module this schema belongs to
            schema: The additional schema to add
        """

        self.schema["properties"][module] = schema
        self.schema["required"].append(module)

    def validate(self, config: dict = None, module: str = None,
                 schema: dict = None):
        """ Validate the configuration

        Checks that the configuration contains all parameters required for
        the bids configuration

        Args:
            config: Optional; The configuration to validate. If nothing is
                set, `self.config` is used
            module: Optional; Do not check against the complete schema but only
                against the one from module.
            schema: Optional: The schema to validate against. If nothing is
                set, `self.schema` is used.
        """

        if config is None:
            config = self.config

        if schema is not None:
            schema_to_check = schema
        elif module is not None:
            schema_to_check = self.schema["properties"][module]
        else:
            schema_to_check = self.schema

        try:
            jsonschema.validate(config, schema_to_check)
        except jsonschema.exceptions.ValidationError as excp:
            self.log.exception("Validating jsonschema failed")
            raise utils.ConfigError(excp) from excp

    def get(self, module: str = None) -> dict:
        """ Reads the configuration from the config file

        Args:
            module: Optional; The module of which the configuration should be
                loaded.
        Returns:
            Either the whole configuration or if module is set, only the
            configuration of the module.
        """
        self.config = utils.get_config(filename=self.config_file)
        self.validate()

        if module is not None:
            return self.config[module]

        return self.config

    def update_parameter(self, module: str, parameter: str, value: Any):
        """ Updates a module parameter in the config file.

        Args:
            module: The module of which the configuration should be updated.
            parameter: The parameter to update
            procedures: The procedures to activate
        """
        config = self.get()
        config[module][parameter] = value

        self.validate(config, module)
        self.write(config)

        self.config = config

    def write(self, config: dict):
        """ Write a new configuration into the config file

        Args:
            config: The new configuration. This has to be match the defined
                    schema.
        """
        self.validate(config)
        utils.write_config(config, filename=self.config_file)

        self.config = config
