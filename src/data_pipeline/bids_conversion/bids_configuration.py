""" Converts tar ball into bids compatible dataset using datalad and hirni"""

from pathlib import Path
import shutil
from typing import Union

import datalad.api as datalad

import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler
from .bids_conversion import BidsConversion


class BidsConfiguration():
    """ Enables configuration of rules to for bids convertions """

    def __init__(self, dataset_path: Union[str, Path]):
        self.dataset_path = Path(dataset_path)

        self.log = utils.get_logger(__class__)  # type: ignore
        self.dataset = utils.get_dataset(self.dataset_path, self.log)

        self.config = ConfigHandler.get_instance().get("bids_conversion")

        self.acqid = self.config["config_acqid"]
        self.anon_subject = self.config["config_anon_subject"]

        self.conversion = BidsConversion(self.dataset_path, self.anon_subject)

    def generate_preview(self, source_dataset: str, active_procedures: dict):
        """ Generade bids conversion and view the result

        Generate bids and display a side by side comparison of the result to
        the original data.

        Args:
            source_dataset: The path to the dataset to install from which bids
                data should be generated
            active_procedures: The procedures to execute in addition, including
                the parameters to run them e.g. addtional procedures for
                getting the event_files. Format is
                {<proc_name>: {"parameters": <parameters>}, ...}
        """

        self.conversion.install_source_dataset(source_dataset)

        # check if data was imported
        if not (self.conversion.install_dataset_path/self.acqid).exists():
            self.log.warning("No dataset was imported. Nothing to convert")
            return

        spec = [
            # "sourcedata/studyspec.json",
            self.conversion.install_dataset_name/"studyspec.json",
            # "sourcedata/*/studyspec.json"
            self.conversion.install_dataset_name/self.acqid/"studyspec.json"
        ]

        # Clean up old bids conversion
        bids_dir = self._get_bids_dir()
        if bids_dir.exists():
            self.log.info("Target directory %s for bids conversion already "
                          "exists. Remove and reuse it.", bids_dir)
            shutil.rmtree(bids_dir)

        self.log.info("Convert to BIDS based on study specification")
        self.conversion.convert(spec)
        self.conversion.run_procedures(active_procedures)

        self._print_preview(bids_dir)

    def _get_bids_dir(self):
        return self.dataset_path/"sub-{}".format(self.anon_subject)

    def _print_preview(self, bids_dir):

        # imported data
        src_data_dir = Path(self.dataset_path,
                            self.conversion.install_dataset_name,
                            self.acqid, "dicoms")
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

    def run_bids_validator(self):
        """ Wrapper around BidsConversion """
        self.conversion.run_bids_validator()

    def cleanup(self):
        """ cleanup generated bids data """

        # uninstall sourcedata
        if self.conversion.install_dataset_path.exists():
            # without the ChangeWorkingDir the command does not operate inside
            # of dataset_path
            with utils.ChangeWorkingDir(self.dataset_path):
                datalad.uninstall(
                    path=self.conversion.install_dataset_name,
                    dataset=self.dataset_path,
                    recursive=True
                )

        # remove bids conversion
        bids_dir = self._get_bids_dir()
        if bids_dir.exists():
            self.log.info("Remove %s", bids_dir)
            shutil.rmtree(bids_dir)
