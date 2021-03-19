""" Converts tar ball into bids compatible dataset using datalad and hirni"""

from pathlib import Path
from typing import Union

import datalad.api as datalad

import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler


def get_dataset_path(project_dir: Union[Path, str],
                     dataset_name: Union[Path, str]) -> Path:
    """ Creates the dataset full path

    Args:
        project_dir: The directory of the dataset
        dataset_name: the name of the dataset
    Returns:
        The full path of the dataset
    """
    return Path(project_dir, dataset_name).expanduser()


class SetupDatalad():
    """ Set up a datalad dataset and preconfigure it"""

    def __init__(self, project_dir: Path, config: dict):
        """

        Args:
            dataset_path: Absolute path of the dataset
        """

        self.config = self._get_config(config)

        self.project_dir = project_dir
        self.dataset_path = get_dataset_path(self.project_dir,
                                             self.config["dataset_name"])

        self.log = utils.get_logger(__class__)  # type: ignore
        self.dataset = None

        self.gitignore_template = "templates/gitignore_template"

    @staticmethod
    def _get_config(config):

        schema = {
            "type": "object",
            "properties": {
                "dataset_name": {"type": "string"},
                "setup_procedures": {"type": "array"},
                "patches": {"type": "array"},
                "add_gitignore": {"type": "boolean"}
            },
            "required": ["dataset_name", "setup_procedures"]
        }
        # TODO make setup_procedure optional

        ConfigHandler.get_instance().validate(config=config, schema=schema)

        # set default values for optional parameters
        config["patches"] = config.get("patches", [])
        config["add_gitignore"] = config.get("add_gitignore", False)

        return config

    def run(self):
        "Set up dataset to be used and apply patches"

        if self.dataset_path.exists():
            raise Exception("ERROR: dataset under {} already exists"
                            .format(self.dataset_path))

        self.log.info("Create dataset %s", self.dataset_path)

        try:
            procs = self.config["setup_procedures"]
            # IMPORTANT: name of the procedure differs depending if it is an
            # argument for create or used in run_procedure:
            # create and command line use: hirni
            # run_procedure use full name: cfg_hirni
#            prefix = "cfg_"
#            proc = proc[len(prefix):] if proc.startswith(prefix) else proc
#            with utils.ChangeWorkingDir(self.project_dir):
#                utils.check_cmd(
#                    ["datalad", "create",
#                     "-c", proc,
#                     self.dataset_path]
#                )
#            self.dataset = datalad.Dataset(self.dataset_path)

            self.dataset = datalad.create(str(self.dataset_path))
            for spec in procs:
                # use command line to suppress output
                with utils.ChangeWorkingDir(self.dataset_path):
                    utils.check_cmd(
                        ["datalad", "run-procedure", spec]
                    )
#                datalad.run_procedure(spec=spec, dataset=self.dataset)

            self._apply_patches()

            if self.config["add_gitignore"]:
                target = self.dataset_path/".gitignore"
                utils.copy_template(template=self.gitignore_template,
                                    target=target)
                datalad.save(path=target, dataset=self.dataset_path,
                             message="Add gitignore")

            self._commit_hirni_patches()

        except Exception:
            self.log.error("Some error occurred", exc_info=True)
            self._remove_all()
            raise

    def _apply_patches(self):

        # for readability
        patches = self.config["patches"]

        if not patches:
            self.log.debug("No patches to apply")
            return

        self.log.debug("patches %s", patches)
        for patch in patches:

            patch = patch.format(
                data_pipeline_path=Path(__file__).parent.absolute()
            )
            patch = Path(patch).expanduser()  # to be able to cope with ~

            cmd = ["patch", "-p0", "-d", str(self.dataset_path),
                   "-i", str(patch)]
            # -pN Strip smallest prefix containing num leading slashes from
            #   files.
            # -d DIR Change the working directory to DIR first.
            # -i PATCHFILE Read patch from PATCHFILE instead of stdin.

            utils.run_cmd(cmd, self.log, error_message="Failed to apply patch")

    def _commit_hirni_patches(self):
        """ commit patches to hirni """

        hirni_path = self.dataset_path/"code"/"hirni-toolbox"
        if not hirni_path.exists():
            return

        # TODO check if there where changes at all

        hirni_dataset = datalad.Dataset(hirni_path)
        # commit inside the submodule
        datalad.save(path=".", dataset=hirni_dataset, message="Patch hirni")
        # commit in the main git repo
        datalad.save(path=hirni_path, dataset=self.dataset,
                     message="Patch hirni")

    def _remove_all(self):
        """ Removes the created dataset

        This is mainly used for testing.
        """
        # datalad remove -r --nocheck -d bids_autoconv
        datalad.remove(
            dataset=self.dataset_path,
            recursive=True,
            check=False
        )
