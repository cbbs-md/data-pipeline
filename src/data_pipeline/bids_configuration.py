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

        self.log = utils.get_logger(__class__)
        self.dataset = datalad.Dataset(self.dataset_path)
        # TODO replace with datalad require_dataset?

        self.acqid = "bids_rule_config"
        self.spec_file = Path(self.dataset_path, self.acqid, "studyspec.json")
        # hirni will remove underscores form the anon_subject entry
        # e.g. bids_config -> bidsconfig
        self.anon_subject = "bidsconfig"

    def import_data(self, tarball: str):
        """ Import tarball as subdataset

        Args:
            tarball: path to tarball to import
        """

        # datalad hirni-import-dcm --anon-subject "$ANON" \
        #   ../../original/sourcedata.tar.gz sourcedata

        self.log.info("Import %s as anon-subject %s and aquisition %s",
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

        # copy rule_base
        abs_rule_base_file = Path(self.dataset_path, rule_dir, "rules_base.py")
        if not abs_rule_base_file.exists():
            # TODO patch hirni and put rule_base inside of it
            rule_base = Path(Path(__file__).parent.absolute(),
                             "patches/rules_base.py")
            shutil.copy(rule_base, abs_rule_base_file)

        # copy rule template
        abs_rule_file = Path(self.dataset_path, rule_file)
        if not abs_rule_file.exists():
            template = Path(Path(__file__).parent.absolute(),
                            "patches/custom_rules.py")
#           TODO use correct template, this is only for dev
#                           "patches/custom_rules_template.py")
            shutil.copy(template, abs_rule_file)
        click.edit(filename=abs_rule_file)

        # TODO commit in dataset: changed .datalad.config, studyspec.json

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

    def generate_preview(self):
        """ Generade bids converion """

        spec = self.spec_file.relative_to(self.dataset_path)
        self._create_studyspec(spec)

        # Clean up old bids convertion
        bids_dir = self.dataset_path/"sub-{}".format(self.anon_subject)
        if bids_dir.exists():
            self.log.info("Target directory %s for bids conversion already "
                          "exists. Remove and reuse it.", bids_dir)

            shutil.rmtree(bids_dir)

        self.log.info("Convert to BIDS based on study specification")

        # datalad hirni-spec2bids --anonymize sourcedata/studyspec.json
        datalad.hirni_spec2bids(
            specfile=spec,
            dataset=self.dataset,
            anonymize=True,
            # only_type=
        )

        # imported data
        src_data_dir = Path(self.dataset_path, self.acqid, "dicoms",
                            "sourcedata")
        src_tree = utils.run_cmd(["tree", "-d", src_data_dir], self.log)

        # converted data
        bids_tree = utils.run_cmd_piped(
           [["tree", bids_dir], ["sed", "s/-> .*//"]], self.log
        )

        # Generate nice output
        src_tree = src_tree.split("\n")
        src_tree[0] = "source:"
        bids_tree = bids_tree.split("\n")
        bids_tree[0] = "result:"

        self.log.info("Preview:\n %s",
                      self._put_side_by_side(src_tree, bids_tree))

    @staticmethod
    def _put_side_by_side(left: list, right: list) -> str:

        col_width = max(len(line) for line in left) + 2  # padding

        max_len = max(len(left), len(right))
        left.extend([""] * (max_len - len(left)))
        right.extend([""] * (max_len - len(right)))

        result = ""
        for row in zip(left, right):
            result += "".join(word.ljust(col_width) for word in row) + "\n"

        return result

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
                "Configure and register rule",
                # Generates the BIDS converion
                "Generate preview",
                questionary.Separator(),
                "Exit"
            ],
            "use_shortcuts": True,
            "default": "Exit",

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
            conv.import_data(tarball=answer["data_path"])
        elif mode == "Configure and register rule":
            conv.apply_rule(
                rule_dir=Path("code", "costum_rules"),
                rule="custom_rules.py",
                overwrite=True
            )
        elif mode == "Generate preview":
            conv.generate_preview()
            # run procedures
