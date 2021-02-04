""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import argparse
import logging
from pathlib import Path
import subprocess

import datalad.api as datalad
import datalad_hirni as hirni
import jsonschema

import utils


class BidsConfiguration(object):

    def __init__(self, skip_setup=False):
        self.dataset = None
        # in case something goes wrong
        self.mark_dataset_to_be_removed = False

        self.config = self._get_config(filename="config.yaml")
        self.dataset_path = Path(self.config["working_dir"],
                                 self.config["dataset_name"])

        self.log = logging.getLogger(self.__class__.__name__)

        if skip_setup:
            self.dataset = datalad.Dataset(self.dataset_path)
            return

        try:
            self._setup_datalad()
        except Exception:  # pylint: disable=broad-except
            self.log.error("Something went wrong", exc_info=True)
            # in case anything goes wrong in setup remove data to ensure
            # repeatability
            self._remove_all()

    @staticmethod
    def _get_config(filename):

        schema = {
            "type": "object",
            "properties": {
                "bids": {
                    "type": "object",
                    "properties": {
                        "working_dir": {"type": "string"},
                        "dataset_name": {"type": "string"},
                        "patches": {"type": "array"}
                    },
                    "required": ["working_dir", "dataset_name"]
                },
            },
            "required": ["bids"]
        }

        config = utils.get_config(filename=filename)

        jsonschema.validate(config, schema)
        # TODO catch jsonschema.exceptions.ValidationError for proper logging

        return config["bids"]

    def _setup_datalad(self):
        "Set up dataset to be used and apply patches"

        if self.dataset_path.exists():
            self.mark_dataset_to_be_removed = False
            raise Exception("ERROR: dataset under {} already exists"
                            .format(self.dataset_path))

        self.dataset = datalad.create(str(self.dataset_path))

        datalad.run_procedure(spec="cfg_hirni", dataset=self.dataset)

        # apply patches
        patches = self.config.get("patches", [])
        for orig, patch in patches:
            cmd = ["patch", "-u", self.dataset_path/orig, "-i", patch]
            try:
                output = subprocess.run(cmd, capture_output=True, check=True)
            except Exception:
                print("ERROR: failed apply patch")
                self.mark_dataset_to_be_removed = True
                raise

            if output.stdout:
                # no additional newline after output
                # print(output.stdout.decode("utf-8"), end="")
                self.log.info(output.stdout.decode("utf-8"))

        # TODO commit to dataset: orig files

    def import_data(self, anon: str, tarball: str):
        # arguments:
        # path, acqid=None, dataset=None, subject=None,
        # anon_subject=None, properties=None
        hirni.import_dicoms(
            dataset=self.dataset,
            path=tarball,
            anon_subject=anon,
            acqid="sourcedata"
        )
        # creates a subdataset <acqid> under sourcedata/dicoms

    def register_rule(self, rule: str, overwrite: bool=False):

        rule_file = Path("code/costum_rules", rule)

        # reset studyspec to avoid problems with next imported dataset
        # (this dataset: default spec merged with rule, next dataset: only rule)
        with Path("sourcedata/studyspec.json").open() as f:
            f.write("")

        config = self.dataset.config
        if not conig.hast_options("datalad.hirni.dicom2spec", "rules"):

            # reset studyspec to avoid problems with next imported dataset
            # (this dataset: default spec merged with rule,
            #  next dataset: only rule)
            with Path("sourcedata/studyspec.json").open() as f:
                f.write("")

        elif not overwrite:
            print("Already registered rule detected.")
            return

        # get rule file: cp <orig location> rule_fule

        config.add("datalad.hirni.dicom2spec.rules",
                   rule_file, where='dataset')

        # TODO commit in dataset


    def modify_rule(self):
        # cp rule over
        pass

    def generate_preview(self):
        # datalad get sourcedata/dicoms/*

        # datalad hirni-dicom2spec -s sourcedata/studyspec.json sourcedata/dicoms
        # arguments:
        # path=None, spec=None, dataset=None, subject=None,
        # anon_subject=None, acquisition=None, properties=None
        hirni.dicom2spec(
            spec="sourcedata/studyspec.json",
            dataset=self.dataset,
            path="sourcedata/dicoms",
        )

        #datalad hirni-spec2bids --anonymize sourcedata/studyspec.json

    def _remove_all(self):
        """ Removes the created dataset

        This is mainly used for testing.
        """
        if not self.mark_dataset_to_be_removed:
            return

        # datalad remove -r --nocheck -d /home/nela/projects/Antonias_data/try_hirni/bids_autoconv
        datalad.remove(
            dataset=self.dataset_path,
            recursive=True,
            check=False
        )


def _setup_logging(name=""):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()

    fmt = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
#    fmt = ("[%(asctime)s] [%(module)s:%(funcName)s:%(lineno)d] "
#           "[%(name)s] [%(levelname)s] %(message)s")
    formatter = logging.Formatter(fmt)

    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False


def argument_parsing():
    """Parsing command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--import_data",
        help="Sets up a datalad dataset and prepares the convesion",
        action="store_true"
    )

    return parser.parse_args()


if __name__ == "__main__":

    _setup_logging()
    args = argument_parsing()

    #anon = 20
    #tarball = ../../original/sourcedata.tar.gz
    rule = "myrules.py"

    conv = BidsConfiguration()


    #setup_datalad()
    #import_data()

#for debugging: remove dataset again:
#datalad remove -r --nocheck -d bids_autoconv