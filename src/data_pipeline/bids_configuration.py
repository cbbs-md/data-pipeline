""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import json
from pathlib import Path
import shutil

import click
import datalad.api as datalad
import questionary

from data_pipeline.setup_datalad import SetupDatalad
import data_pipeline.utils as utils


class BidsConfiguration(object):
    """ Enables configuration of rules to for bids convertions """

    def __init__(self, dataset_path: str or Path):
        self.dataset_path = Path(dataset_path)

        # in case something goes wrong
        self.mark_dataset_to_be_removed = False
        self.log = utils.get_logger(__class__)
        self.dataset = datalad.Dataset(self.dataset_path)
        # TODO replace with datalad require_dataset?

        self.acqid = "bids_rule_config"

    def import_data(self, anon_subject: str, tarball: str):
        """ Import tarball as subdataset

        Args:
            anon_subject: anonymize subject identifier
            tarball: path to tarball to import
        """

        # datalad hirni-import-dcm --anon-subject "$ANON" \
        #   ../../original/sourcedata.tar.gz sourcedata

        self.log.info("Import %s as anon-subject %s and aquisition %s",
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

        self.log.info("Structure of imported data:")
        data_path = Path(self.dataset_path, self.acqid, "dicoms", "sourcedata",
                         str(anon_subject))
        utils.run_cmd(["tree", "-L", "1", data_path], self.log)

    def apply_rule(self, rule_dir: str, rule: str,
                   overwrite: bool = False):
        """Register datalad hirni rule"""

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

        abs_rule_file = Path(self.dataset_path, rule_file)
        if not abs_rule_file.exists():
            shutil.copy(Path("patches/custom_rules_template.py"),
                        abs_rule_file)
        click.edit(filename=abs_rule_file)

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


def ask_questions():
    """ Define and ask the questionary for the user"""
    questions = [
        {
            "type": "select",
            "name": "step_select",
            "message": "What do you want to do?",
            "choices": [
                # Sets up a datalad dataset and prepares the convesion
                "Import data",
                # Registers and applies datalad hirni rule
                "Configure and apply rule",
                # Generates the BIDS converion
                "Generate preview",
                questionary.Separator(),
                "Exit"
            ],
            "use_shortcuts": True,
            "default": "Exit",

        },
        {
            "type": "text",
            "name": "anon_subject",
            "message": "Define anon_subject:",
            "when": lambda x: x["step_select"] == "Import data",
        },
        {
            "type": "path",
            "name": "data_path",
            "message": "Path to the data tar ball:",
            "when": lambda x: x["step_select"] == "Import data",
        },
    ]
    return questionary.prompt(questions)


def configure_bids_conversion():
    """ Sets up a datalad dataset and prepares the convesion """

    while True:
        answer = ask_questions()
        if not answer or answer["step_select"] == "Exit":
            break

        mode = answer["step_select"]

        setup = SetupDatalad()
        conv = BidsConfiguration(setup.dataset_path)
        if mode == "Import data":
            setup.run()
            conv.import_data(
                anon_subject=answer["anon_subject"],
                tarball=answer["data_path"],
            )
        elif mode == "Configure and apply rule":
            conv.apply_rule(
                rule_dir=Path("code", "costum_rules"),
                rule="custom_rules.py",
                overwrite=True
            )
        elif mode == "Generate preview":
            conv.generate_preview()
            # run procedures
