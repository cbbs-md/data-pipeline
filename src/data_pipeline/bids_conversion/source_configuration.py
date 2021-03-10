""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import json
from pathlib import Path
import re
import shutil
from typing import Union

import click
import datalad.api as datalad

import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler
from data_pipeline.git_handler import GitBase

from .bids_conversion import SourceHandler


class SourceConfiguration():
    """ Enables configuration of rules to for bids convertions """

    def __init__(self, dataset_path: Union[str, Path]):
        self.dataset_path = Path(dataset_path)

        self.log = utils.get_logger(__class__)  # type: ignore
        self.dataset = datalad.Dataset(self.dataset_path)
        # TODO replace with datalad require_dataset?

        self.config = ConfigHandler.get_instance().get("bids_conversion")

        self.acqid = self.config["config_acqid"]
        self.spec_file = Path(self.dataset_path, self.acqid, "studyspec.json")
        self.anon_subject = self.config["config_anon_subject"]

        self.source_handler = SourceHandler(self.dataset_path)

    def import_data(self, tarball: str):
        """ Import tarball as subdataset

        Args:
            tarball: path to tarball to import
        """
        self.source_handler.import_data(
            tarball=tarball,
            anon_subject=self.anon_subject,
            acqid=self.acqid
        )

    def get_heudiconv_container(self):
        """ Wrapper around SourceHandler """
        self.source_handler.get_heudiconv_container()

    def register_rule(self):
        """Register datalad hirni rule"""

        rule_template = Path(self.config["rule_template"])
        rule_file = self._register_and_add_rule(rule_template)

        # edit rule
        self.log.info("Opening %s", rule_file)
        click.edit(filename=rule_file)

        self._create_studyspec()

    def _register_and_add_rule(self, rule_template):
        """Register datalad hirni rule"""

        # TODO get these from config
        rule_dir = Path(self.config["rule_dir"])
        rule = self.config["rule_name"]

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
            ("templates/rules_base.py", "rules_base.py"),
            (rule_template, rule),
        ]

        # TODO patch hirni and put rule_base inside of it
        for src_name, target_name in rules_to_copy:
            utils.copy_template(
                template=src_name,
                target=Path(self.dataset_path, rule_dir, target_name),
                # use template dir inside of bids_conversion
                this_file_path=Path(__file__)
            )

        return Path(self.dataset_path, rule_file)

    def _reset_studyspec(self):
        """ reset studyspec to avoid problems with next imported dataset

        If the studyspec is not reset the result of rule applications is not
        deterministic:
        this dataset: default spec merged with rule
        next dataset: only rule

        dicomseries:all entry is needed for hirni to convert data properly
        """

        if not self.spec_file.exists():
            # Nothing to do
            return

        spec_list = utils.read_spec(self.spec_file)
        dicomseries_all = [i for i in spec_list
                           if i["type"] == "dicomseries:all"]

        # write dicomseries:all
        with self.spec_file.open("w") as f:
            for i in dicomseries_all:
                f.write(json.dumps(i) + "\n")

        datalad.save(path=self.spec_file, dataset=self.dataset,
                     message="Reset studyspec file")

    def _create_studyspec(self):
        self.log.info("Generate study specification file")

        spec = self.spec_file.relative_to(self.dataset_path)

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

    def import_rule(self, rule: Union[str, Path]):
        """Import datalad hirni rule"""

        # handle "~/" paths
        rule = Path(rule).expanduser().resolve()

        self._register_and_add_rule(rule)
        self._create_studyspec()

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

        git_repo.checkout_starting_branch()
        git_repo.remove_config_branch()


class ProcedureHandling():
    """ Handles everything concerning procedures """

    def __init__(self, dataset_path: Union[str, Path]):
        self.dataset_path = dataset_path
        self.dataset = datalad.Dataset(self.dataset_path)

        self.confhandler = ConfigHandler.get_instance()
        self.config = self.confhandler.get("bids_conversion")

        self.log = utils.get_logger(__class__)  # type: ignore

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

        proc_dir = self.config["default_procedure_dir"]
        proc_file = Path(proc_dir, procedure_name)

        # create procedure dir
        abs_proc_dir = Path(self.dataset_path, proc_dir)
        if not abs_proc_dir.exists():
            abs_proc_dir.mkdir(parents=True)

        # register procedure dir in datalad
        self._register_proc_dir(proc_dir)

        # copy template
        target = Path(self.dataset_path, proc_file)
        utils.copy_template(
            template=procedure_template,
            target=target,
            # use template dir inside of bids_conversion
            this_file_path=Path(__file__)
        )

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
                raise utils.NotPossible(msg, "")

        config.set(section + "." + option, proc_dir, where="dataset")

    def import_procedure(self, procedure_path: Union[str, Path],
                         procedure_file_name):
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
        except utils.NotPossible:
            self.log.error("Import of procedure not possible since default "
                           "procedure dir could not be registered")
            return

        # handle "~/" paths
        procedure_path = Path(procedure_path).expanduser().resolve()

        # copy file into procedure dir
        proc_type = Path(procedure_path).suffix
        target = Path(self.dataset_path, proc_dir, procedure_file_name)
        target = target.with_suffix(proc_type)
        if target.exists():
            self.log.exception("Procedure with the name %s alread exists",
                               procedure_file_name)
            return

        shutil.copy(procedure_path, target)

    def get_active_procedures(self) -> dict:
        """ Get all procedures registered to be executed in the conversion

        Returns:
            The active procedures including the set parameters in the form
            {proc1: {"parameters": params1}, ... }
        """

        # open config file and read procedures
        active_procedures = (
            self.confhandler.get("bids_conversion")
            .get("active_procedures", {})
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
            module="bids_conversion",
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
        """ Check if a path has changed and should be committed """
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
        """ Commit changes done during bids configuration """

        with utils.ChangeWorkingDir(self.dataset_path):
            # add config and hirni changes
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
                rule_file = self.determine_dir(
                    section="datalad.hirni.dicom2spec", option="rules"
                )
                does_exist = True
            except Exception:
                does_exist = False

            if does_exist and self.check_if_to_be_committed(rule_file):
                datalad.save(rule_file, dataset=self.dataset_path,
                             message="Add/modify custom rule", to_git=True)

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
                # TODO check what happens if one procedure is only modified

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
