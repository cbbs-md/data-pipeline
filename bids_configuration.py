""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import argparse
import contextlib
import logging
import os
from pathlib import Path
import shutil
import subprocess

import datalad.api as datalad
import datalad_hirni as hirni
import jsonschema

import utils


class ChangeWorkingDir(contextlib.ContextDecorator):
    """ Change the working directory temporaly """

    def __init__(self, new_wd):
        self.current_wd = None
        self.new_wd = new_wd

    def __enter__(self):
        self.current_wd = Path.cwd()
        os.chdir(self.new_wd)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.current_wd)
        # signal that the exception was handled and the program should continue
        return True


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

    def import_data(self, anon_subject: str, tarball: str):
        """ Import tarball as subdataset

        Args:
            anon_subject: anonymize subject identifier
            tarball: path to tarball to import
        """

        # datalad hirni-import-dcm --anon-subject "$ANON" \
        #   ../../original/sourcedata.tar.gz sourcedata

        datalad.hirni_import_dcm(
            dataset=self.dataset,
            anon_subject=anon_subject,
            # subject=
            path=tarball,
            acqid="bids_config_test_set",
            # properties=
        )

        # creates a subdataset <acqid> under sourcedata/dicoms

    def apply_rule(self, rule: str, overwrite: bool = False):
        """Register datalad hirni rule"""

        rule_dir = Path("code/costum_rules")
        rule_file = Path(rule_dir, rule)
        acqid = "bids_config_test_set"

        config = self.dataset.config
        # cases
        # 1. no rule defined -> add rule, clear studyspec, copy tmp_rule
        # 2. rule definied + same rule file -> clear studyspec, copy tmp_rule
        # 3. rule definied + different rule file:
        #   3.1 overwrite: overwrite rule, clear studyspec, copy tmp_rule
        #   3.2 no overwrite: do nothing

        if not config.has_option("datalad.hirni.dicom2spec", "rules"):
            set_rule = True
        elif config.get("datalad.hirni.dicom2spec.rules") == rule_file:
            set_rule = False
        elif overwrite:
            set_rule = True
        else:
            self.log.error("Already registered rule detected.")
            return

        if set_rule:
            config.set("datalad.hirni.dicom2spec.rules",
                       rule_file, where='dataset')

        # reset studyspec to avoid problems with next imported dataset
        # (this dataset: default spec merged with rule,
        #  next dataset: only rule)
        spec_path = Path(self.dataset_path, acqid, "studyspec.json")
        spec_path.write_text("")
        # TODO this does not work, since the anon_subject is errased as well

        # generate costume rule dir
        abs_rule_dir = Path(self.dataset_path, rule_dir)
        if not abs_rule_dir.exists():
            abs_rule_dir.mkdir(parents=True)

        # get rule file: cp <orig location> rule_file
        source = "tmp_rule.py"
        shutil.move(source, Path(self.dataset_path, rule_file))
        shutil.copy(Path("patches/rules_base.py"),
                    Path(self.dataset_path, rule_dir))

        # TODO commit in dataset: changed .datalad.config, studyspec.json

    def generate_preview(self):
        """ Generade bids converion """
        acqid = "bids_config_test_set"

        # TODO clean up old bids convertion

        # datalad get bids_config_test_set/dicoms/*
        # datalad.get(dataset=str(Path(acqid, "dicoms")))

        spec = str(Path(acqid, "studyspec.json"))

        # datalad hirni-dicom2spec -s bids_config_test_set/studyspec.json \
        #     bids_config_test_set/dicoms
        # FIX since dicom2spec only looks for rule file in current dir and not
        # in dataset dir
        with ChangeWorkingDir(self.dataset_path):
            datalad.hirni_dicom2spec(
                path=str(Path(acqid, "dicoms")),
                spec=spec,
                dataset=self.dataset,
                # subject=
                # anon_subject=
                # acquisition=
                # properties=
            )

        # datalad hirni-spec2bids --anonymize sourcedata/studyspec.json
        datalad.hirni_spec2bids(
            specfile=spec,
            dataset=self.dataset,
            anonymize=True,
            # only_type=
        )

    def _remove_all(self):
        """ Removes the created dataset

        This is mainly used for testing.
        """
        if not self.mark_dataset_to_be_removed:
            return

        # datalad remove -r --nocheck -d bids_autoconv
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
    parser.add_argument(
        "--apply_rule",
        help="Registers and applies datalad hirni rule",
        action="store_true"
    )
    parser.add_argument(
        "--generate_preview",
        help="Generates the BIDS converion",
        action="store_true"
    )

    return parser.parse_args()


if __name__ == "__main__":

    _setup_logging()
    args = argument_parsing()

    rule = "myrules.py"

    if args.import_data:
        conv = BidsConfiguration(skip_setup=False)
        conv.import_data(
            anon_subject=20,
            tarball="/path/to/data/original/sourcedata.tar.gz",
        )
    elif args.apply_rule:
        conv = BidsConfiguration(skip_setup=True)
        conv.apply_rule(rule=rule, overwrite=True)
    elif args.generate_preview:
        conv = BidsConfiguration(skip_setup=True)
        conv.generate_preview()


# for debugging: remove dataset again:
# datalad remove -r --nocheck -d bids_autoconv
