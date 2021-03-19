""" Converts tar ball into bids compatible dataset using datalad and hirni"""

import copy
import os
from pathlib import Path
from typing import Union

import datalad.api as datalad

import data_pipeline.utils as utils
from data_pipeline.config_handler import ConfigHandler


class SourceHandler():
    """ A basic source dataset """
    # pylint: disable=too-few-public-methods

    def __init__(self, dataset_path: Union[str, Path]):
        self.log = utils.get_logger(__class__)  # type: ignore

        self.dataset_path = Path(dataset_path)
        self.dataset = utils.get_dataset(self.dataset_path, self.log)

    def import_data(self, tarball: str, anon_subject: str, acqid: str):
        """ Import tarball as subdataset

        Args:
            tarball: path to tarball to import
            anon_subject: The anonymous subject id
            acqid: The acquisition identifier for this data
        """

        path = Path(tarball).expanduser().resolve()

        # TODO proper detection if already imported
        if Path(self.dataset_path, acqid, "dicoms").exists():
            self.log.info("Acquisition dataset already imported.")
            return

        # check if tarball exists
        if not path.exists():
            self.log.error("Tarball %s does not exists", path)
            raise ValueError("Tarball {} does not exists".format(path))

        self.log.info("Import %s: anon-subject=%s, aquisition=%s",
                      path, anon_subject, acqid)

        # creates a subdataset <acqid> under sourcedata/dicoms
        # without the ChangeWorkingDir the command does not operate inside of
        # dataset_path and thus does not find the rules file
        with utils.ChangeWorkingDir(self.dataset_path):
            # datalad hirni-import-dcm --anon-subject "$ANON" \
            #   ../../original/sourcedata.tar.gz sourcedata
            datalad.hirni_import_dcm(
                dataset=self.dataset,
                anon_subject=anon_subject,
                # subject=
                path=path,
                acqid=acqid,
                # properties=
            )

    def get_heudiconv_container(self):
        """ load the heudiconv container into the source dataset """

        heudiconv_container = Path(self.dataset_path, "code", "hirni-toolbox",
                                   "converters", "heudiconv", "heudiconv.simg")
        if heudiconv_container.exists():
            return

        self.log.info("Heudiconv container has to be downloaded. This "
                      "might take some time.")
        datalad.get(heudiconv_container, dataset=self.dataset_path)


class BidsConversion():
    """ Install and convert data into bids format """

    def __init__(self, dataset_path, anon_subject):
        self.dataset_path = Path(dataset_path)
        self.anon_subject = anon_subject

        self.log = utils.get_logger(__class__)  # type: ignore
        self.dataset = utils.get_dataset(self.dataset_path, self.log)

        self.config = ConfigHandler.get_instance().get("bids_conversion")

        # the name under which the source dataset should be installed inside
        # the bids dataset
        self.install_dataset_name = Path("sourcedata")
        self.install_dataset_path = Path(self.dataset_path,
                                         self.install_dataset_name)

    def install_source_dataset(self, source_dataset: str):
        """ Install the source dataset to be able to process it """

        if self.install_dataset_path.exists():
            is_not_empty = any(self.install_dataset_path.iterdir())
            if is_not_empty:
                self.log.info("Source dataset already installed, update it.")
                datalad.update(
                    self.install_dataset_name,
                    merge=True,
                    dataset=self.dataset_path,
                    recursive=True
                )
                return

        self.log.info("Install source dataset.")
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

    def convert(self, spec: list):
        """ Converts to bids using datalad hirni

        Args:
            spec: A list of hirni studyspec files to use
        """
        heudiconv_container = Path(self.install_dataset_path, "code",
                                   "hirni-toolbox", "converters", "heudiconv",
                                   "heudiconv.simg")
        if not heudiconv_container.exists():
            # This also triggers if container is downloaded inside the source
            # dataset but get not yet executed to get it from there
            self.log.info("Get heudiconv container")

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

    def run_procedures(self, procedures: dict):
        """ Run a list of procedures procedures

        Args:
            active_procedures: The procedures to run in the form
            {<proc name>: "parameters": <parameters as string>}
        """
        # run procedures
        for procedure, values in procedures.items():
            proc_spec = "{} {}".format(procedure, values["parameters"])
            # replace {anon_subject}, ...
            proc_spec = proc_spec.format(anon_subject=self.anon_subject)
            self.log.info("Execute procedure %s", proc_spec)
            datalad.run_procedure(proc_spec, dataset=self.dataset)

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
            # modify environment only for executed command and not whole
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
            suppress_output=True
        )

        self.log.info(res)
