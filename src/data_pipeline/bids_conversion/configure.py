""" Configure BIDS conversion """

from pathlib import Path
from typing import Tuple

import questionary

from data_pipeline.setup_datalad import SetupDatalad
from data_pipeline.config_handler import ConfigHandler
from .source_configuration import (
    SourceConfiguration, BidsGitHandling, ProcedureHandling
)
from .bids_configuration import BidsConfiguration


def configure(project_dir):
    """ Sets up a datalad dataset and prepares the conversion """

    schema = {
        "type": "object",
        "properties": {
            "active_procedures": {"type": "object"},
            "source": {
                "type": "object",
                "properties": {
                    "dataset_name": {"type": "string"},
                    "setup_procedures": {"type": "array"},
                    "patches": {"type": "array"}
                },
                "required": [
                    "dataset_name",
                    "setup_procedures"
                ]
            },
            "bids": {
                "type": "object",
                "properties": {
                    "dataset_name": {"type": "string"},
                    "setup_procedures": {"type": "array"},
                    "patches": {"type": "array"}
                },
                "required": [
                    "dataset_name",
                    "setup_procedures"
                ]
            },
            "default_procedure_dir": {"type": "string"},
            "procedure_python_template": {"type": "string"},
            "procedure_shell_template": {"type": "string"},
            "rule_dir": {"type": "string"},
            "rule_name": {"type": "string"},
            "rule_template": {"type": "string"},
            "validator_container_name": {"type": "string"},
            "validator_image_url": {"type": "string"},
            "container_dir": {"type": "string"},
            "config_acqid": {"type": "string"},
            "config_anon_subject": {"type": "string"},
        },
        "required": [
            "source",
            "bids",
            "default_procedure_dir",
            "procedure_python_template",
            "procedure_shell_template",
            "rule_dir",
            "rule_name",
            "rule_template",
            "validator_container_name",
            "validator_image_url",
            "container_dir",
            "config_acqid",
            "config_anon_subject",
        ]
    }
    config_handler = ConfigHandler.get_instance()
    config_handler.add_schema("bids_conversion", schema)
    config = config_handler.get("bids_conversion")

    # create source dataset
    source_setup = SetupDatalad(project_dir, config["source"])
    if not source_setup.dataset_path.exists():
        source_setup.run()

    # create bids dataset
    bids_setup = SetupDatalad(project_dir, config["bids"])
    if not bids_setup.dataset_path.exists():
        bids_setup.run()

    repo = BidsGitHandling(source_setup.dataset_path)

    while True:
        try:
            answers, choices = _ask_questions()
            if not answers or answers["step_select"] == "Exit":
                break

            repo.checkout_config_branch()

            switch = StepSwitcher(source_setup.dataset_path,
                                  bids_setup.dataset_path,
                                  choices, answers, repo)
            choices_reverted = {v: k for k, v in choices.items()}
            getattr(switch, choices_reverted[answers["step_select"]])()
        finally:
            repo.checkout_starting_branch()
            # commit changes in .datalad/config, rules, procedures
            repo.commit()


def _ask_questions() -> Tuple[dict, dict]:
    """ Define and ask the questionary for the user"""

    choices = dict(
        import_data="Import data",
        register_rule="Configure and register rule »",
        add_procedure="Add procedure »",
        preview="Generate preview",
        check="Check for BIDS conformity",
        cleanup="Cleanup",

        rule_create="Create new rule",
        rule_import="Import rule",

        proc_change="Change active procedures",
        proc_create="Create new procedure",
        proc_import="Import procedure",
    )

    questions = [
        {
            "type": "select",
            "name": "step_select",
            "message": "What do you want to do?",
            "choices": [
                # Sets up a datalad dataset and prepares the convesion
                choices["import_data"],
                # Registers and applies datalad hirni rule
                choices["register_rule"],
                # Create, import or register procedures
                choices["add_procedure"],
                # Generates the BIDS converion
                choices["preview"],
                choices["check"],
                # Remove imported and converted
                choices["cleanup"],
                questionary.Separator(),
                "Help",
                "Exit"
            ],
            "use_shortcuts": True,
            "default": "Exit",

        },
        {
            "type": "path",
            "name": "data_path",
            "message": "Path to the data tar ball:",
            "when": lambda x: x["step_select"] == choices["import_data"],
        },
        {
            "type": "select",
            "name": "rule_select",
            "message": "What do you want to do?",
            "when": lambda x: x["step_select"] == choices["register_rule"],
            "choices": [
                choices["rule_create"],
                choices["rule_import"],
                questionary.Separator(),
                "Return"
            ],
            "use_shortcuts": True,
            "default": "Return",
        },
        {
            "type": "path",
            "name": "rule_file",
            "message": "Path to the rule:",
            "when": (lambda x: x["step_select"] == choices["register_rule"]
                     and x["rule_select"] == choices["rule_import"]),
        },
        {
            "type": "select",
            "name": "procedure_select",
            "message": "What do you want to do?",
            "when": lambda x: x["step_select"] == choices["add_procedure"],
            "choices": [
                choices["proc_change"],
                choices["proc_create"],
                choices["proc_import"],
                questionary.Separator(),
                "Return",
            ],
            "use_shortcuts": True,
            "default": "Return",
        },
        {
            "type": "select",
            "name": "procedure_type",
            "message": "What type of procedure do you want to create?",
            "when": (lambda x: x["step_select"] == choices["add_procedure"]
                     and x["procedure_select"] == choices["proc_create"]),
            "choices": [
                "shell",
                "python",
                questionary.Separator(),
                "Return",
            ],
            "use_shortcuts": True,
            "default": "Return",
        },
        {
            "type": "text",
            "name": "procedure_name",
            "message": "How should the procedure be called?",
            "when": (lambda x: x["step_select"] == choices["add_procedure"]
                     and x["procedure_select"] == choices["proc_create"]
                     and x["procedure_type"] != "Return"),
        },
        {
            "type": "path",
            "name": "procedure_file",
            "message": "Path to the procedure:",
            "when": (lambda x: x["step_select"] == choices["add_procedure"]
                     and x["procedure_select"] == choices["proc_import"]),
        },
        {
            "type": "text",
            "name": "procedure_file_name",
            "message": "Name of the procedure:",
            "when": (lambda x: x["step_select"] == choices["add_procedure"]
                     and x["procedure_select"] == choices["proc_import"]
                     and x["procedure_file"]),
            "default": lambda x: Path(x["procedure_file"]).stem
        },
    ]
    return questionary.prompt(questions), choices


class StepSwitcher():
    """ Switcher for procedure action

    The purpose of this class is to simplify the questionary checking.
    Insead of checking each value manually the according Switcher method
    can be called. E.g.
    Use
       getattr(StepSwitcher, "import_data"])()
    Instead of
       if answers["step_select"] == choices["import_data"]:
           ...
    """

    def __init__(self, source_dataset_path, bids_dataset_path,
                 choices, answers, git_repo):
        self.source_dataset_path = source_dataset_path
        self.choices = choices
        self.answers = answers
        self.git_repo = git_repo
        self.src_conf = SourceConfiguration(self.source_dataset_path)
        self.bids_conf = BidsConfiguration(bids_dataset_path)

    def import_data(self):
        """ Create dataset and import data"""
        self.src_conf.import_data(tarball=self.answers["data_path"])

    def register_rule(self):
        """ Handle all rule relevant answers"""
        if self.answers["rule_select"] == "Return":
            return

        switch = RuleSwitcher(self.src_conf, self.answers)
        choices_reverted = {v: k for k, v in self.choices.items()}
        getattr(switch, choices_reverted[self.answers["rule_select"]])()

    def add_procedure(self):
        """ Handle all procedure relevant answers"""
        if any(self.answers.get(key, None) == "Return"
               for key in ["procedure_select", "procedure_type"]):
            return

        switch = ProcSwitcher(self.source_dataset_path, self.answers)
        choices_reverted = {v: k for k, v in self.choices.items()}
        getattr(switch, choices_reverted[self.answers["procedure_select"]])()

    def preview(self):
        """ Wrapper around ProcedureHandling """
        # to make conversion easier since the container has not be reloaded
        # after every drop
        self.src_conf.get_heudiconv_container()

        proc_handler = ProcedureHandling(self.source_dataset_path)
        active_procedures = proc_handler.get_active_procedures()
        self.bids_conf.generate_preview(
            source_dataset=self.src_conf.dataset_path,
            active_procedures=active_procedures
        )

    def check(self):
        """ Wrapper around BidsConfiguration """
        self.bids_conf.run_bids_validator()

    def cleanup(self):
        """ Wrapper around BidsConfiguration """
        self.src_conf.cleanup(self.git_repo)
        self.bids_conf.cleanup()


class RuleSwitcher():
    """ Switcher for rule action

    The purpose of this class is to simplify the questionary checking.
    Insead of checking each value manually the according Switcher method
    can be called. E.g.
    Use
       getattr(Switcher, "rule_create"])()
    Instead of
       if answers["rule_select"] == choices["rule_create"]:
           proc_handler.create_procedure(...)
    """

    def __init__(self, src_conf, answers):
        self.src_conf = src_conf
        self.answers = answers

    def rule_create(self):
        """ Wrapper around ProcedureHandler """
        self.src_conf.register_rule()

    def rule_import(self):
        """ Wrapper around ProcedureHandler """
        self.src_conf.import_rule(self.answers["rule_file"])


class ProcSwitcher():
    """ Switcher for procedure action

    The purpose of this class is to simplify the questionary checking.
    Insead of checking each value manually the according Switcher method
    can be called. E.g.
    Use
       getattr(Switcher, "proc_create"])()
    Instead of
       if answers["procedure_select"] == choices["proc_create"]:
           proc_handler.create_procedure(...)
    """

    def __init__(self, dataset_path, answers):
        self.proc_handler = ProcedureHandling(dataset_path)
        self.answers = answers

    def proc_create(self):
        """ Wrapper around ProcedureHandler """
        self.proc_handler.create_procedure(self.answers["procedure_type"],
                                           self.answers["procedure_name"])

    def proc_import(self):
        """ Wrapper around ProcedureHandler """
        self.proc_handler.import_procedure(self.answers["procedure_file"],
                                           self.answers["procedure_file_name"])

    def proc_change(self):
        """ Activate or deactivate procedures """

        active_procedures = self.proc_handler.get_active_procedures()
        available_procedures = self.proc_handler.get_available_procedures()

        active_proc_names = list(active_procedures.keys())

        # determine defaults for choices
        choices = []
        for proc in sorted(available_procedures):
            if proc in active_proc_names:
                choices.append(questionary.Choice(proc, checked=True))
            else:
                choices.append(proc)

        # get procedure names: select from all available procedures
        chosen_procs = questionary.checkbox(
            "Select active procedures",
            choices=choices
        ).ask() or []

        # activate or deactivate accordingly
        procs_to_activate = set(chosen_procs) - set(active_proc_names)
        if procs_to_activate:
            complete_procs = {}
            for proc in procs_to_activate:
                has_params = questionary.confirm(
                    "Does procedure {} have parameter?".format(proc),
                    default=False
                ).ask()

                if has_params:
                    parameters = questionary.text("Parameters:").ask()
                else:
                    parameters = ""

                complete_procs[proc] = {"parameters": parameters}

            self.proc_handler.activate_procedures(complete_procs)

        procs_to_deactivate = set(active_proc_names) - set(chosen_procs)
        if procs_to_deactivate:
            complete_procs = {proc: active_procedures[proc]
                              for proc in procs_to_deactivate}
            self.proc_handler.deactivate_procedures(complete_procs)

        # TODO should a new procedure that is added automatically be active?
