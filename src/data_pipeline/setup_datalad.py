""" Converts tar ball into bids compatible dataset using datalad and hirni"""

from pathlib import Path

import datalad.api as datalad

import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler


class SetupDatalad():
    """ Set up a datalad dataset and preconfigure it"""

    def __init__(self, project_dir):

        self.config = self._get_config()
        self.dataset_name = self.config["dataset_name"]
        self.dataset_path = Path(project_dir,
                                 self.config["dataset_name"]).expanduser()

        self.log = utils.get_logger(__class__)
        self.dataset = None

    @staticmethod
    def _get_config():

        schema = {
            "type": "object",
            "properties": {
                "bids": {
                    "type": "object",
                    "properties": {
                        "dataset_name": {"type": "string"},
                        "patches": {"type": "array"}
                    },
                    "required": ["dataset_name"]
                },
            },
            "required": ["bids"]
        }

        config_handler = ConfigHandler.get_instance()
        config = ConfigHandler.get_instance().get()
        config_handler.validate(config=config, schema=schema)
        return config["bids"]

    def run(self):
        "Set up dataset to be used and apply patches"

        if self.dataset_path.exists():
            raise Exception("ERROR: dataset under {} already exists"
                            .format(self.dataset_path))

        self.log.info("Run datalad setup")

        try:
            self.dataset = datalad.create(str(self.dataset_path))
            datalad.run_procedure(spec="cfg_hirni", dataset=self.dataset)

            self._apply_patches()
            # TODO commit to dataset: orig files

        except Exception:
            self.log.error("Some error occurred", exc_info=True)
            self._remove_all()
            raise

    def _apply_patches(self):
        # apply patches
        patches = self.config.get("patches", [])

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
