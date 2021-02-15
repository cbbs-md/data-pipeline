""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import json
from pathlib import Path
import shutil
import subprocess

import datalad.api as datalad
import jsonschema
import questionary

import utils


class SetupDatalad(object):
    """ Set up a datalad dataset and preconfigure it"""

    def __init__(self):

        self.config = self._get_config(filename="config.yaml")
        self.dataset_name = self.config["dataset_name"]
        self.dataset_path = Path(self.config["working_dir"],
                                 self.config["dataset_name"]).expanduser()

        self.log = utils.get_logger(__class__)
        self.dataset = None

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

            try:
                # pylint: disable=subprocess-run-check
                output = subprocess.run(cmd, capture_output=True)
            except Exception:
                self.log.error("Failed to apply patch")
                raise

            if output.stdout:
                # no additional newline after output
                # print(output.stdout.decode("utf-8"), end="")
                self.log.info(output.stdout.decode("utf-8"))

            # capture return code explicitely instead of useing subprocess
            # parameter check=True to be able to log error message
            if output.returncode:
                self.log.debug("cmd: %s", " ".join(cmd))
                self.log.error("Failed to apply patch, error was: %s",
                               output.stderr.decode("utf-8"))
                raise Exception("Failed to apply patch")

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


class BidsConfiguration(object):
    """ Enables configuration of rules to for bids convertions """

    def __init__(self, dataset_path: str or Path):
        self.dataset_path = dataset_path

        # in case something goes wrong
        self.mark_dataset_to_be_removed = False

        self.log = utils.get_logger(__class__)

        self.dataset = datalad.Dataset(self.dataset_path)
        # TODO replace with datalad require_dataset?

        self.acqid = "bids_config_test_set"

    def import_data(self, anon_subject: str, tarball: str):
        """ Import tarball as subdataset

        Args:
            anon_subject: anonymize subject identifier
            tarball: path to tarball to import
        """

        # datalad hirni-import-dcm --anon-subject "$ANON" \
        #   ../../original/sourcedata.tar.gz sourcedata

        self.log.info("Import %s ad anon-subject %s and aquisition %s",
                      tarball, anon_subject, self.acqid)

        # creates a subdataset <acqid> under sourcedata/dicoms
        datalad.hirni_import_dcm(
            dataset=self.dataset,
            anon_subject=anon_subject,
            # subject=
            path=tarball,
            acqid=self.acqid,
            # properties=
        )

    def apply_rule(self, rule: str, overwrite: bool = False):
        """Register datalad hirni rule"""

        rule_dir = Path("code/costum_rules")
        rule_file = Path(rule_dir, rule)

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

        self._reset_studyspec()

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

    def _reset_studyspec(self):
        """ reset studyspec to avoid problems with next imported dataset

        dicomseries:all entry is needed for hirni to convert data properly
        (this dataset: default spec merged with rule,
        next dataset: only rule)
        """

        spec_file = Path(self.dataset_path, self.acqid, "studyspec.json")
        spec_list = utils.read_spec(spec_file)
        dicomseries_all = [i for i in spec_list
                           if i["type"] == "dicomseries:all"]

        # write dicomseries:all
        with spec_file.open("w") as f:
            for i in dicomseries_all:
                f.write(json.dumps(i) + "\n")

    def generate_preview(self):
        """ Generade bids converion """

        # TODO clean up old bids convertion

        # datalad get bids_config_test_set/dicoms/*
        # datalad.get(dataset=str(Path(self.acqid, "dicoms")))

        spec = str(Path(self.acqid, "studyspec.json"))
        self.log.info("Generate study specification file")

        # datalad hirni-dicom2spec -s bids_config_test_set/studyspec.json \
        #     bids_config_test_set/dicoms
        # FIX since dicom2spec only looks for rule file in current dir and not
        # in dataset dir
        with utils.ChangeWorkingDir(self.dataset_path):
            datalad.hirni_dicom2spec(
                path=str(Path(self.acqid, "dicoms")),
                spec=spec,
                dataset=self.dataset,
                # subject=
                # anon_subject=
                # acquisition=
                # properties=
            )

        self.log.info("Convert to BIDS based on study specification")

        # datalad hirni-spec2bids --anonymize sourcedata/studyspec.json
        datalad.hirni_spec2bids(
            specfile=spec,
            dataset=self.dataset,
            anonymize=True,
            # only_type=
        )


def configure_bids_conversion():
    """ Sets up a datalad dataset and prepares the convesion """

    answer = questionary.rawselect(
        "What do you want to do?",
        choices=[
            # Sets up a datalad dataset and prepares the convesion
            "Import data",
            # Registers and applies datalad hirni rule
            "Apply rule",
            # Generates the BIDS converion
            "Generate preview"],
    ).ask()

    setup = SetupDatalad()
    if answer == "Import data":
        setup.run()
        # TODO can this be moved down into the other if?

    conv = BidsConfiguration(setup.dataset_path)
    if answer == "Import data":
        conv.import_data(
            anon_subject=20,
            tarball="/home/nela/projects/Antonias_data/"
                    "original/sourcedata.tar.gz",
        )
    elif answer == "Apply rule":
        conv.apply_rule(rule="myrules.py", overwrite=True)
    elif answer == "Generate preview":
        conv.generate_preview()
