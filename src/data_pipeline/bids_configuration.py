""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import copy
import json
import os
from pathlib import Path
import re
import shutil
from typing import Tuple, Union

import click
import datalad.api as datalad
import git
import questionary

from data_pipeline.setup_datalad import SetupDatalad
import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler
from data_pipeline.git_handler import GitBase


class NotPossible(Exception):
    """ Raised when the requested action is not possible """


class BidsConfiguration():
    """ Enables configuration of rules to for bids convertions """

    def __init__(self, dataset_path: Union[str, Path]):
        self.dataset_path = Path(dataset_path)

        self.log = utils.get_logger(__class__)  # type: ignore
        self.dataset = datalad.Dataset(self.dataset_path)
        # TODO replace with datalad require_dataset?

        self.config = ConfigHandler.get_instance().get("bids")

        self.acqid = self.config["config_acqid"]
        self.spec_file = Path(self.dataset_path, self.acqid, "studyspec.json")
        self.anon_subject = self.config["config_anon_subject"]

    def import_data(self, tarball: str):
        """ Import tarball as subdataset

        Args:
            tarball: path to tarball to import
        """

        # datalad hirni-import-dcm --anon-subject "$ANON" \
        #   ../../original/sourcedata.tar.gz sourcedata

        self.log.info("Import %s: anon-subject=%s, aquisition=%s",
                      tarball, self.anon_subject, self.acqid)

        # creates a subdataset <acqid> under sourcedata/dicoms
        datalad.hirni_import_dcm(
            dataset=self.dataset,
            anon_subject=self.anon_subject,
            # subject=
            path=tarball,
            acqid=self.acqid,
            # properties=
        )

    def register_rule(self):
        """Register datalad hirni rule"""

        # TODO get these from config
        rule_dir = Path(self.config["rule_dir"])
        rule = self.config["rule_name"]
        rule_template = self.config["rule_template"]

        rule_file = Path(rule_dir, rule)

        config = self.dataset.config
        # cases
        # 1. no rule defined -> add rule, clear studyspec, copy rule template
        # 2. rule definied + same rule file
        #   -> clear studyspec, copy rule template
        # 3. rule definied + different rule file
        #   -> overwrite rule, clear studyspec, copy rule template

        if (not config.has_option("datalad.hirni.dicom2spec", "rules")
                or config.get("datalad.hirni.dicom2spec.rules") != rule_file):
            config.set("datalad.hirni.dicom2spec.rules",
                       rule_file, where="dataset")

        self._reset_studyspec()

        # create costume rule dir
        abs_rule_dir = Path(self.dataset_path, rule_dir)
        if not abs_rule_dir.exists():
            abs_rule_dir.mkdir(parents=True)

        # copy rule_base and rule template
        rules_to_copy = [
            # (source file name, target file name)
            ("rules_base.py", "rules_base.py"),
            ("custom_rules.py", rule),
            # TODO use correct template, this is only for dev
            # (rule_template, rule),
        ]

        # TODO patch hirni and put rule_base inside of it
        for src_name, target_name in rules_to_copy:
            target = Path(self.dataset_path, rule_dir, src_name)
            if not target.exists():
                source = Path(Path(__file__).parent.absolute(),
                              "templates", target_name)
                shutil.copy(source, target)

        # edit rule template
        abs_rule_file = Path(self.dataset_path, rule_file)
        self.log.info("Opening %s", abs_rule_file)
        click.edit(filename=abs_rule_file)

    def _reset_studyspec(self):
        """ reset studyspec to avoid problems with next imported dataset

        dicomseries:all entry is needed for hirni to convert data properly
        (this dataset: default spec merged with rule,
        next dataset: only rule)
        """

        spec_list = utils.read_spec(self.spec_file)
        dicomseries_all = [i for i in spec_list
                           if i["type"] == "dicomseries:all"]

        # write dicomseries:all
        with self.spec_file.open("w") as f:
            for i in dicomseries_all:
                f.write(json.dumps(i) + "\n")

        datalad.save(path=self.spec_file, dataset=self.dataset,
                     message="Reset studyspec file")

    def generate_preview(self, active_procedures: dict):
        """ Generade bids converion

        Args:
            active_procedures: The procedures to execute in addition, including
                the parameters to run them e.g. addtional procedures for
                getting the event_files. Format is
                {<proc_name>: {"parameters": <parameters>}, ...}
        """

        spec = self.spec_file.relative_to(self.dataset_path)
        self._create_studyspec(spec)

        # Clean up old bids convertion
        bids_dir = self._get_bids_dir()
        if bids_dir.exists():
            self.log.info("Target directory %s for bids conversion already "
                          "exists. Remove and reuse it.", bids_dir)
            shutil.rmtree(bids_dir)

        self.log.info("Convert to BIDS based on study specification")

        # since logging can not be controlled when using the datalad api, the
        # console output will be flooded -> sircument it by using the command
        # line interface
        with utils.ChangeWorkingDir(self.dataset_path):
            utils.run_cmd(
                [
                    "datalad",
                    "hirni-spec2bids",
                    "--anonymize",
                    spec
                ],
                self.log
            )

        # datalad hirni-spec2bids --anonymize sourcedata/studyspec.json
#        datalad.hirni_spec2bids(
#            specfile=spec,
#            dataset=self.dataset,
#            anonymize=True,
#            # only_type=
#        )

        # run procedures
        for procedure, values in active_procedures.items():
            proc_spec = "{} {}".format(procedure, values["parameters"])
            # replace {anon_subject}, ...
            proc_spec = proc_spec.format(anon_subject=self.anon_subject)
            self.log.info("Execute procedure %s", proc_spec)
            datalad.run_procedure(proc_spec, dataset=self.dataset)

        self._print_preview(bids_dir)

    def _get_bids_dir(self):
        return self.dataset_path/"sub-{}".format(self.anon_subject)

    def _print_preview(self, bids_dir):

        # imported data
        src_data_dir = Path(self.dataset_path, self.acqid, "dicoms",
                            "sourcedata")
        src_tree = utils.run_cmd(
            ["tree", "-d", src_data_dir], self.log
        ).split("\n")

        # converted data
        bids_tree = utils.run_cmd_piped(
           [["tree", bids_dir], ["sed", "s/-> .*//"]], self.log
        ).split("\n")

        # Generate nice output
        src_tree[0] = "source:"
        bids_tree[0] = "result:"

        self.log.info("Preview:\n %s",
                      utils.show_side_by_side(src_tree, bids_tree))

    def _create_studyspec(self, spec):

        self.log.info("Generate study specification file")

        # Fix needed since dicom2spec only looks for rule file in current dir
        # and not in dataset dir
        with utils.ChangeWorkingDir(self.dataset_path):
            # datalad hirni-dicom2spec -s bids_rule_config/studyspec.json \
            #     bids_rule_config/dicoms
            datalad.hirni_dicom2spec(
                path=str(Path(self.acqid, "dicoms")),
                spec=spec,
                dataset=self.dataset,
                # subject=
                # anon_subject=
                # acquisition=
                # properties=
            )

    def run_bids_validator(self):
        """ Checks the dataset for bids conformity"""

        name = self.config["validator_container_name"]
        image_url = self.config["validator_image_url"]
        container_dir = Path(self.config["container_dir"])
        if not container_dir.is_absolute():
            container_dir = Path(self.dataset_path, container_dir)

        if not container_dir.exists():
            container_dir.mkdir()

        container_path = Path(container_dir, name)
        if not container_path.exists():
            # modify environment only for exectued command and not whole
            # process
            environment = copy.deepcopy(os.environ)
            environment["SINGULARITY_PULLFOLDER"] = str(container_dir)

            utils.run_cmd(
                [
                    "singularity", "pull", "--name",
                    name, image_url,
                ],
                self.log,
                env=environment
            )

        # singularity run --no-home --containall --bind $DIR_TO_CHECK:/data
        #     $CONTAINER_PATH /data
        utils.run_cmd(
            [
                "singularity", "run",
                "--no-home",
                "--containall",
                "--bind", "{}:/data".format(self.dataset_path),
                str(container_path), "/data"
            ],
            self.log,
            raise_exception=False
        )

    def cleanup(self, git_repo):
        """ Cleanup the configuration data

        Cleans up the imported sourcedata and the converted bids data.
        """
        # remove imported source data
        source_dir = self.dataset_path/self.acqid
        if source_dir.exists():
            self.log.info("Remove %s", source_dir)
            # datalad remove bids_rule_config
            datalad.remove(dataset=self.dataset_path, path=self.acqid,
                           recursive=True, if_dirty="ignore")

            # cleanup submodule entry in git (bug in datalad)
            repo = git.Repo(self.dataset_path)
            with repo.config_writer() as writer:
                writer.remove_section('submodule "bids_rule_config/dicoms"')

        # cleanup generated bids data
        bids_dir = self.dataset_path/"sub-{}".format(self.anon_subject)
        if bids_dir.exists():
            self.log.info("Remove %s", bids_dir)
            shutil.rmtree(bids_dir)

        git_repo.checkout_starting_branch()
        git_repo.remove_config_branch()


class ProcedureHandling():
    """ Handles everything concerning procedures """

    def __init__(self, dataset_path):
        self.dataset_path = dataset_path
        self.dataset = datalad.Dataset(self.dataset_path)

        self.confhandler = ConfigHandler.get_instance()
        self.config = self.confhandler.get("bids")

        self.log = utils.get_logger(__class__)

    def get_available_procedures(self):
        """ Show all procedures known to datalad """

        # self.log.info("Available procedures are:")
        # datalad.run_procedure(dataset=self.dataset, discover=True)

        # run the command instead of the api to have more control about the
        # output, same reason as in generate_preview
        with utils.ChangeWorkingDir(self.dataset_path):
            output = utils.run_cmd(["datalad", "run-procedure", "--discover"],
                                   self.log)

            regex = r"(?P<name>.+?) \((?P<path>.+?)\) \[(?P<type>.+?)\]"
            procs = {}
            for proc in output.strip().split("\n"):
                # an entry look like this:
                # 'cfg_bids (<path/to/procedure>/cfg_bids.py) [python_script]'
                match = re.fullmatch(regex, proc)
                if match:
                    procedure = match.groupdict()
                    procs[procedure["name"]] = {
                        "path": procedure["path"],
                        "type": procedure["type"]
                    }

        return procs

    def create_procedure(self, procedure_type: str, procedure_name: str):
        """ Create a new procedure from a template to be modified.

        The default procedure dir is registered in datalad and depending of the
        procedure type defined, a template is copied into it with the defined
        name. Then the file is opened to be edited by the user.

        Args:
            procedure_type: what type of procedure to add
                            (python or shell)
            procedure_name: how the procedure should be called
        """

        proc_dir = self.config["default_procedure_dir"]
        proc_file = Path(proc_dir, procedure_name)

        # determine it from proc_type and template
        if procedure_type == "python":
            procedure_name += ".py"
            procedure_template = self.config["procedure_python_template"]
        elif procedure_type == "shell":
            procedure_name += ".sh"
            procedure_template = self.config["procedure_shell_template"]
        else:
            self.log.exception("Procedure %s type is not supported",
                               procedure_type)

        # create procedure dir
        abs_proc_dir = Path(self.dataset_path, proc_dir)
        if not abs_proc_dir.exists():
            abs_proc_dir.mkdir(parents=True)

        # register procedure dir in datalad
        self._register_proc_dir(proc_dir)

        # copy template
        target = Path(self.dataset_path, proc_file)
        if not target.exists():
            source = Path(Path(__file__).parent.absolute(), procedure_template)
            shutil.copy(source, target)

        # edit procedure template
        self.log.info("Opening %s", target)
        click.edit(filename=str(target))

    def _register_proc_dir(self, proc_dir: Union[str, Path],
                           overwrite: bool = False):
        """ Register procedure dir in datalad

        Args:
            proc_dir: The directory to register
            overwrite: If the procedure dir entry in the datalad config should
                       be overwritten in case an already register directory
                       differs from proc_dir
        """

        section = "datalad.locations"
        option = "dataset-procedures"

        config = self.dataset.config
        # cases
        # 1. no procedure defined -> add proc
        # 2. proc definied + same proc dir -> do nothing
        # 3. proc definied + different proc dir -> add proc dir

        if config.has_option(section, option):
            # convert to Path to handle writespaces et. all
            registered_dir = Path(config.get(section + "." + option))
            if registered_dir == Path(proc_dir):
                # nothing to do
                return
            if not overwrite:
                msg = "Different procedure dir %s already registered."
                self.log.error(msg, registered_dir)
                raise NotPossible(msg, "")

        config.set(section + "." + option, proc_dir, where="dataset")

    def import_procedure(self, procedure_path: str, procedure_file_name):
        """ Imports a file into the procedure dir

        Args:
            procedure_path: The file to be imported
            procedure_file_name: The new name of the imported procedure
        """

        proc_dir = self.config["default_procedure_dir"]

        # create procedure dir
        abs_proc_dir = Path(self.dataset_path, proc_dir)
        if not abs_proc_dir.exists():
            abs_proc_dir.mkdir(parents=True)

        # register procedure dir in datalad
        try:
            self._register_proc_dir(proc_dir)
        except NotPossible:
            self.log.error("Import of procedure not possible since default "
                           "procedure dir could not be registered")
            return

        # copy file into procedure dir
        proc_type = Path(procedure_path).suffix
        target = Path(self.dataset_path, proc_dir, procedure_file_name)
        target = target.with_suffix(proc_type)
        if target.exists():
            self.log.exception("Procedure with the name %s alread exists",
                               procedure_file_name)
            return

        shutil.copy(procedure_path, target)

    def register_procedure_dir(self, procedure_dir: str,
                               overwrite: bool = False):
        """ Registers a procedure dir in datalad

        Args:
            procedure_dir: The directory to register
            overwrite: Optional; In case another procedure dir is already
                       registered, if it should be overwritten.
        """

        # check if additional procedure_dir exist
        # (also check for relative paths)
        if (not Path(procedure_dir).exists()
                and not Path(self.dataset_path, procedure_dir).exists()):
            self.log.warning("Procedure dir %s, does not exist", procedure_dir)

        # register additional procedure dir in datalad
        self._register_proc_dir(procedure_dir, overwrite)

    def get_active_procedures(self) -> dict:
        """ Get all procedures registered to be executed in the conversion

        Returns:
            The active procedures including the set parameters in the form
            {proc1: {"parameters": params1}, ... }
        """

        # open config file and read procedures
        active_procedures = (
            self.confhandler.get("bids").get("active_procedures", {})
        )

        return active_procedures

    def activate_procedures(self, procedures: dict):
        """ Activate procedures in the config file

        The config file is used to store active procedures to keep track of
        them even if the application is restarted.

        Args:
            procedures: The procedures to activate
        """

        self._change_active_procedures(procedures, action="activate")

    def deactivate_procedures(self, procedures: dict):
        """ Remove procedures from the active procedure entry in the config file

        Args:
            procedures: The procedures to deactivate.
        """

        self._change_active_procedures(procedures, action="deactivate")

    def _change_active_procedures(self, procedures: dict, action: str):
        """ Add/Remove procedures from the active list

        Args:
            procedures: The procedures to activate/deactivate.
            action: What to do with the procedures:
                activate: They will be activated.
                deactivate: They will be deactivated.
        """

        if not procedures:
            self.log.warning("No procedure chosen.")

        # read procedures from config file
        active_procedures = self.get_active_procedures()

        for proc in procedures:
            # check if contained already
            if action == "activate":
                if proc in active_procedures:
                    self.log.info("Procedure %s is already active", proc)
                    continue

                active_procedures[proc] = procedures[proc]

            if action == "deactivate":
                if proc not in active_procedures:
                    self.log.info("Procedure %s is not active", proc)
                    continue

                del active_procedures[proc]

            self.log.info("Procedure to %s '%s'", action, proc)

        # add and write back into config
        self.confhandler.update_parameter(
            module="bids",
            parameter="active_procedures",
            value=active_procedures
        )


class BidsGitHandling(GitBase):
    """ To switch between starting and config branch """

    def __init__(self, dataset_path):
        super().__init__()

        self.dataset_path = dataset_path
        self.log = utils.get_logger(__class__)  # type: ignore
        self.starting_branch = self._get_current_branch()
        self.config_branch = "bids_config_branch"

    def _get_current_branch(self):
        with utils.ChangeWorkingDir(self.dataset_path):
            return super()._get_current_branch()

    def checkout_config_branch(self):
        """ Switch to branch dedicated for bids config """

        with utils.ChangeWorkingDir(self.dataset_path):
            self.checkout_branch(self.config_branch,
                                 rebase_branch=self.starting_branch,
                                 do_create=True)

    def checkout_starting_branch(self):
        """ Switch back to starting branch """

        self.stash()
        with utils.ChangeWorkingDir(self.dataset_path):
            self.checkout_branch(self.starting_branch)
        self.stash(pop=True)

    def stash(self, pop: bool = False):
        """Wrapper anound GitBase """
        with utils.ChangeWorkingDir(self.dataset_path):
            super().stash(pop)

    def check_if_to_be_committed(self, path: str):
        with utils.ChangeWorkingDir(self.dataset_path):

            if self.is_tracked(path):
                # path is already tracked but was changed
                return self.was_changed(path)
            if Path(path).exists():
                # path is not tracked yet
                return True
            # path does not exist
            return False

    def commit(self):
        # ass config and hirni changes
        path = ".datalad/config"
        if self.check_if_to_be_committed(path):
            datalad.save(
                path,
                dataset=self.dataset_path,
                message=("Modify datalad config for custom rule and "
                            "procedures")
            )

        # add rule
        try:
            rule_file = self.determine_dir(section="datalad.hirni.dicom2spec",
                                           option="rules")
            does_exist = True
        except Exception:
            does_exist = False

        if does_exist and self.check_if_to_be_committed(rule_file):
            datalad.save(rule_file, dataset=self.dataset_path,
                         message="Add/modify custom rule", to_git=True)
        #   message="Register and add custom rules to dataset configuration"

            rule_base_file = Path(rule_file).with_name("rules_base.py")
            if self.check_if_to_be_committed(rule_base_file):
                datalad.save(rule_base_file, dataset=self.dataset_path,
                             message="Add rule_base file", to_git=True)

        # add procedures
        try:
            procedure_dir = self.determine_dir(section="datalad.locations",
                                               option="dataset-procedures")
        except Exception:
            does_exist = False
        if does_exist and self.check_if_to_be_committed(procedure_dir):
            datalad.save(procedure_dir, dataset=self.dataset_path,
                         message="Add procedures", to_git=True)

    def determine_dir(self, section: str, option: str) -> Union[str, Path]:
        """ Get dir from datalad config """

        config = datalad.Dataset(self.dataset_path).config

        if config.has_option(section, option):
            configured_dir = Path(config.get(section + "." + option))

            if not configured_dir.is_absolute():
                configured_dir = self.dataset_path/configured_dir

            return configured_dir

        raise Exception("No entry in datalad config")

    def remove_config_branch(self):
        """ Remove the config branch """
        with utils.ChangeWorkingDir(self.dataset_path):
            super().remove_branch(self.config_branch)


def _ask_questions() -> Tuple[dict, dict]:
    """ Define and ask the questionary for the user"""

    config = ConfigHandler.get_instance().get("bids")

    choices = dict(
        import_data="Import data",
        register_rule="Configure and register rule",
        add_procedure="Add procedure »",
        preview="Generate preview",
        check="Check for BIDS conformity",
        cleanup="Cleanup",

        proc_change="Change active procedures",
        proc_create="Create new procedure",
        proc_import="Import procedure",
        proc_register="Register additional procedure location",
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
            "name": "procedure_select",
            "message": "What do you want to do?",
            "when": lambda x: x["step_select"] == choices["add_procedure"],
            "choices": [
                choices["proc_change"],
                choices["proc_create"],
                choices["proc_import"],
                choices["proc_register"],
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
                     and x["procedure_select"] == choices["proc_create"]),
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
        {
            "type": "path",
            "name": "procedure_dir",
            "message": "Path to the procedures:",
            "when": (lambda x: x["step_select"] == choices["add_procedure"]
                     and x["procedure_select"] == choices["proc_register"]),
            "only_directories": True,
            "default": config["default_procedure_dir"]
        },
    ]
    return questionary.prompt(questions), choices


def configure_bids_conversion(project_dir):
    """ Sets up a datalad dataset and prepares the convesion """

    schema = {
        "type": "object",
        "properties": {
            "bids": {
                "type": "object",
                "properties": {
                    "active_procedures": {"type": "object"},
                    "dataset_name": {"type": "string"},
                    "patches": {"type": "array"},
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
                    "dataset_name",
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
            },
        },
        "required": ["bids"]
    }
    ConfigHandler.get_instance().add_schema("bids", schema)

    setup = SetupDatalad(project_dir)
    if not setup.dataset_path.exists():
        setup.run()

    repo = BidsGitHandling(setup.dataset_path)
    repo.checkout_config_branch()

    try:
        while True:
            answers, choices = _ask_questions()
            if not answers or answers["step_select"] == "Exit":
                break

            switch = StepSwitcher(setup.dataset_path, choices, answers, repo)
            choices_reverted = {v: k for k, v in choices.items()}
            getattr(switch, choices_reverted[answers["step_select"]])()
    finally:
        repo.checkout_starting_branch()
        # commit changes in .datalad/config, rules, procedures
        repo.commit()


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

    def __init__(self, dataset_path, choices, answers, git_repo):
        self.dataset_path = dataset_path
        self.choices = choices
        self.answers = answers
        self.git_repo = git_repo
        self.conv = BidsConfiguration(self.dataset_path)

    def import_data(self):
        """ Create dataset and import data"""
        self.conv.import_data(tarball=self.answers["data_path"])

    def register_rule(self):
        """ Wrapper around BidsConfiguration"""
        self.conv.register_rule()

    def add_procedure(self):
        """ Handle all procedure relevant answers"""
        if self.answers["procedure_select"] == "Return":
            return

        switch = ProcSwitcher(self.dataset_path, self.answers)
        choices_reverted = {v: k for k, v in self.choices.items()}
        getattr(switch, choices_reverted[self.answers["procedure_select"]])()

    def preview(self):
        """ Wrapper around BidsConfiguration """
        proc_handler = ProcedureHandling(self.dataset_path)
        active_procedures = proc_handler.get_active_procedures()
        self.conv.generate_preview(active_procedures)

    def check(self):
        """ Wrapper around BidsConfiguration """
        self.conv.run_bids_validator()

    def cleanup(self):
        """ Wrapper around BidsConfiguration """
        self.conv.cleanup(self.git_repo)


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

    def proc_register(self):
        """ Wrapper around ProcedureHandler """
        try:
            self.proc_handler.register_procedure_dir(
                self.answers["procedure_dir"]
            )
        except NotPossible:
            if questionary.confirm("Overwrite?").ask():
                self.proc_handler.register_procedure_dir(
                    self.answers["procedure_dir"],
                    overwrite=True
                )

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

                complete_procs[proc] = {"parameters": parameters}

            self.proc_handler.activate_procedures(complete_procs)

        procs_to_deactivate = set(active_proc_names) - set(chosen_procs)
        if procs_to_deactivate:
            complete_procs = {proc: active_procedures[proc]
                              for proc in procs_to_deactivate}
            self.proc_handler.deactivate_procedures(complete_procs)
