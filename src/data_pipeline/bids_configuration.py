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

    def register_rule(self):
        """Register datalad hirni rule"""

        rule_dir = Path("code", "costum_rules")
        rule = "custom_rules.py"

        rule_file = Path(rule_dir, rule)

        config = self.dataset.config
        # cases
        # 1. no rule defined -> add rule, clear studyspec, copy tmp_rule
        # 2. rule definied + same rule file -> clear studyspec, copy tmp_rule
        # 3. rule definied + different rule file
        #       -> overwrite rule, clear studyspec, copy tmp_rule

        if (not config.has_option("datalad.hirni.dicom2spec", "rules")
                or config.get("datalad.hirni.dicom2spec.rules") != rule_file):
            config.set("datalad.hirni.dicom2spec.rules",
                       rule_file, where='dataset')

        self._reset_studyspec()

        # generate costume rule dir
        abs_rule_dir = Path(self.dataset_path, rule_dir)
        if not abs_rule_dir.exists():
            abs_rule_dir.mkdir(parents=True)

        # copy rule_base and rule template
        rules_to_copy = [
            # (source file name, target file name)
            ("rules_base.py", "rules_base.py"),
            ("custom_rules.py", rule),
            # TODO use correct template, this is only for dev
            # ("custom_rule_template.py", rule),
        ]

        # TODO patch hirni and put rule_base inside of it
        for src_name, target_name in rules_to_copy:
            target = Path(self.dataset_path, rule_dir, src_name)
            if not target.exists():
                source = Path(Path(__file__).parent.absolute(),
                              "patches", target_name)
                shutil.copy(source, target)

        # edit rule template
        abs_rule_file = Path(self.dataset_path, rule_file)
        self.log.info("Opening %s", abs_rule_file)
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

        # since logging can not be controlled when using the datalad api, the
        # console output will be flooded -> sircument it by using the command
        # line interface
        with utils.ChangeWorkingDir(self.dataset_path):
            utils.run_cmd(
                [
                    "datalad",
                    "hirni-spec2bids",
                    "--anonymize",
                    spec
                ],
                self.log
            )

        # datalad hirni-spec2bids --anonymize sourcedata/studyspec.json
#        datalad.hirni_spec2bids(
#            specfile=spec,
#            dataset=self.dataset,
#            anonymize=True,
#            # only_type=
#        )

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
                      utils.show_side_by_side(src_tree, bids_tree))

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


def ask_questions() -> (dict, dict):
    """ Define and ask the questionary for the user"""

    choices = dict(
        import_data="Import data",
        register_rule="Configure and register rule",
        add_procedures="Add procedure",
        preview="Generate preview",
        cleanup="Cleanup",
    )

    questions = [
        {
            "type": "select",
            "name": "step_select",
            "message": "What do you want to do?",
            "choices": [
                # Sets up a datalad dataset and prepares the convesion
                choices["import_data"],
                # Registers and applies datalad hirni rule
                choices["register_rule"],
                # Create, import or register procedures
                choices["add_procedures"],
                # Generates the BIDS converion
                choices["preview"],
                # Remove imported and converted
                choices["cleanup"],
                questionary.Separator(),
                "Help",
                "Exit"
            ],
            "use_shortcuts": True,
            "default": "Exit",

        },
        {
            "type": "path",
            "name": "data_path",
            "message": "Path to the data tar ball:",
            "when": lambda x: x["step_select"] == choices["import_data"],
        },
    ]
    return questionary.prompt(questions), choices


def configure_bids_conversion():
    """ Sets up a datalad dataset and prepares the convesion """

    while True:
        answer, choices = ask_questions()
        if not answer or answer["step_select"] == "Exit":
            break

        mode = answer["step_select"]

        setup = SetupDatalad()
        conv = BidsConfiguration(setup.dataset_path)
        if mode == choices["import_data"]:
            setup.run()
            conv.import_data(tarball=answer["data_path"])
        elif mode == choices["register_rule"]:
            conv.register_rule()

        elif mode == choices["preview"]:
            conv.generate_preview()
            # run procedures
