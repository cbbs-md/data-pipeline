""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import copy
import os
from pathlib import Path
import shutil
from typing import Union

import datalad.api as datalad

import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler


class BidsConfiguration():
    """ Enables configuration of rules to for bids convertions """

    def __init__(self, dataset_path: Union[str, Path]):
        self.dataset_path = Path(dataset_path)

        self.log = utils.get_logger(__class__)  # type: ignore
        self.dataset = datalad.Dataset(self.dataset_path)
        # TODO replace with datalad require_dataset?

        self.config = ConfigHandler.get_instance().get("bids_conversion")

        self.acqid = self.config["config_acqid"]
        self.anon_subject = self.config["config_anon_subject"]

        # the name under which the source dataset should be installed inside
        # the bids dataset
        self.install_dataset_name = Path("sourcedata")
        self.install_dataset_path = Path(self.dataset_path,
                                         self.install_dataset_name)

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

        self._install_source_dataset(source_dataset)

        # check if data was imported
        if not (self.install_dataset_path/self.anon_subject).exists():
            self.log.warning("No dataset was imported. Nothing to convert")
            return

        spec = [
            # "sourcedata/studyspec.json",
            self.install_dataset_name/"studyspec.json",
            # "sourcedata/*/studyspec.json"
            self.install_dataset_name/self.acqid/"studyspec.json"
        ]

        # Clean up old bids conversion
        bids_dir = self._get_bids_dir()
        if bids_dir.exists():
            self.log.info("Target directory %s for bids conversion already "
                          "exists. Remove and reuse it.", bids_dir)
            shutil.rmtree(bids_dir)

        self.log.info("Convert to BIDS based on study specification")

        # TODO check if heudiconv container already downloaded, otherwise warn
        # user that this might take some time

        # since logging can not be controlled when using the datalad api, the
        # console output will be flooded -> circument it by using the command
        # line interface
        with utils.ChangeWorkingDir(self.dataset_path):
            cmd = ["datalad", "hirni-spec2bids", "--anonymize"] + spec
            utils.run_cmd(cmd, self.log)

        # datalad hirni-spec2bids --anonymize sourcedata/studyspec.json
#        datalad.hirni_spec2bids(
#            specfile=spec,
#            dataset=self.dataset,
#            anonymize=True,
#            # only_type=
#        )

        # run procedures
        for procedure, values in active_procedures.items():
            proc_spec = "{} {}".format(procedure, values["parameters"])
            # replace {anon_subject}, ...
            proc_spec = proc_spec.format(anon_subject=self.anon_subject)
            self.log.info("Execute procedure %s", proc_spec)
            datalad.run_procedure(proc_spec, dataset=self.dataset)

        self._print_preview(bids_dir)

    def _install_source_dataset(self, source_dataset: str):
        """ Install the source dataset to be able to process it """

        is_not_empty = any(self.install_dataset_path.iterdir())
        if self.install_dataset_path.exists() and is_not_empty:
            self.log.info("Source dataset already installed, update it.")
            datalad.update(
                self.install_dataset_name,
                merge=True,
                dataset=self.dataset_path,
                recursive=True
            )
            return

        # using the command line interface since the datalad api behaves
        # differently and is missing the activation of datalad-url
        # Then the procedures of the subdataset are not found
        with utils.ChangeWorkingDir(self.dataset_path):
            cmd = ["datalad", "install",
                   "--dataset", self.dataset_path,
                   "--source", source_dataset,
                   self.install_dataset_name,
                   "--recursive"]
            utils.run_cmd(cmd, self.log)

        # without the ChangeWorkingDir the command does not operate inside of
        # dataset_path
#        with utils.ChangeWorkingDir(self.dataset_path):
#            datalad.install(
#                path=self.install_dataset_name,
#                source=source_dataset,
#                 dataset=self.dataset_path,
#                # get_data=
#                # description=
#                recursive=True
#            )

    def _get_bids_dir(self):
        return self.dataset_path/"sub-{}".format(self.anon_subject)

    def _print_preview(self, bids_dir):

        # imported data
        src_data_dir = Path(self.dataset_path, self.install_dataset_name,
                            self.acqid, "dicoms", "sourcedata")
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
        """ Checks the dataset for bids conformity"""

        name = self.config["validator_container_name"]
        image_url = self.config["validator_image_url"]
        container_dir = Path(self.config["container_dir"])
        if not container_dir.is_absolute():
            container_dir = Path(self.dataset_path, container_dir)

        if not container_dir.exists():
            container_dir.mkdir(parents=True)

        # if no .bids-validator-config.json file exists create it
        utils.copy_template(
            template=Path(self.config["validator_config_template"]),
            target=self.dataset_path/".bids-validator-config.json",
            # use template dir inside of bids_conversion
            this_file_path=Path(__file__)
        )

        container_path = Path(container_dir, name)
        if not container_path.exists():
            # modify environment only for exectued command and not whole
            # process
            environment = copy.deepcopy(os.environ)
            environment["SINGULARITY_PULLFOLDER"] = str(container_dir)

            cmd = ["singularity", "pull", "--name", name, image_url]
            utils.run_cmd(cmd, self.log, env=environment)

        # singularity run --no-home --containall --bind $DIR_TO_CHECK:/data
        #     $CONTAINER_PATH /data
        res = utils.run_cmd(
            [
                "singularity", "run",
                "--no-home",
                "--containall",
                "--bind", "{}:/data".format(self.dataset_path),
                str(container_path), "/data"
            ],
            self.log,
            raise_exception=False,
            surpress_output=True
        )

        self.log.info(res)

    def cleanup(self):
        """ cleanup generated bids data """

        # uninstall sourcedata
        if self.install_dataset_path.exists():
            # without the ChangeWorkingDir the command does not operate inside
            # of dataset_path
            with utils.ChangeWorkingDir(self.dataset_path):
                datalad.uninstall(
                    path=self.install_dataset_name,
                    dataset=self.dataset_path,
                    recursive=True
                )

        # remove bids conversion
        bids_dir = self._get_bids_dir()
        if bids_dir.exists():
            self.log.info("Remove %s", bids_dir)
            shutil.rmtree(bids_dir)
