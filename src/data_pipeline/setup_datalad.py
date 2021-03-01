""" Converts tar ball into bids compatible dataset using datalad and hirni"""

from pathlib import Path

import datalad.api as datalad

import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler


class SetupDatalad():
    """ Set up a datalad dataset and preconfigure it"""

    def __init__(self, project_dir: Path, config: dict):
        """

        Args:
            dataset_path: Absolute path of the dataset
        """

        self.config = self._get_config(config)

        self.dataset_path = Path(project_dir,
                                 self.config["dataset_name"]).expanduser()

        self.log = utils.get_logger(__class__)
        self.dataset = None

    @staticmethod
    def _get_config(config):

        schema = {
            "type": "object",
            "properties": {
                "dataset_name": {"type": "string"},
                "setup_procedure": {"type": "string"},
                "patches": {"type": "array"}
            },
            "required": ["dataset_name", "setup_procedure"]
        }
        # TODO make setup_procedure optional

        ConfigHandler.get_instance().validate(config=config, schema=schema)

        # set default values for optional parameters
        config["patches"] = config.get("patches", [])

        return config

    def run(self):
        "Set up dataset to be used and apply patches"

        if self.dataset_path.exists():
            raise Exception("ERROR: dataset under {} already exists"
                            .format(self.dataset_path))

        self.log.info("Run datalad setup")

        try:
            self.dataset = datalad.create(str(self.dataset_path))
            datalad.run_procedure(spec=self.config["setup_procedure"],
                                  dataset=self.dataset)

            self._apply_patches()
            # commit patches to hirni
            hirni_path = self.dataset_path/"code"/"hirni-toolbox"
            hirni_dataset = datalad.Dataset(hirni_path)
            # commit inside the submodule
            datalad.save(path=".", dataset=hirni_dataset,
                         message="Patch hirni")
            # commit in the main git repo
            datalad.save(path=hirni_path, dataset=self.dataset,
                         message="Patch hirni")

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
