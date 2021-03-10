""" Runs Bids conversion

Converts a tar ball into bids compatible dataset using datalad and hirni
"""

from data_pipeline.config_handler import ConfigHandler
from data_pipeline.setup_datalad import get_dataset_path
from data_pipeline import utils
from .bids_conversion import SourceHandler, BidsConversion
from .source_configuration import ProcedureHandling


class Conversion():
    """ Import and convert data """
    # pylint: disable=too-few-public-methods

    def __init__(self, source_dataset_path, bids_dataset_path, data_path):
        self.source_dataset_path = source_dataset_path
        self.bids_dataset_path = bids_dataset_path
        self.data_path = data_path

        self.source_handler = None

    def run(self, anon_subject: str, acqid: str):
        """ Run the bids convertion

        Args:
            anon_subject: The anonymous subject identifier
            acqid:  The acquisition identifier
        """

        self._import_data(anon_subject, acqid)
        self._convert(anon_subject, acqid)
        self._cleanup()

    def _import_data(self, anon_subject: str, acqid: str):
        """ import tarball into sourcedata """

        tarball = self.data_path.format(anon_subject=anon_subject, acqid=acqid)

        try:
            self.source_handler = SourceHandler(self.source_dataset_path)
        except utils.UsageError:
            # error was already logged and more traceback is not needed
            return

        self.source_handler.import_data(
            tarball=tarball,
            anon_subject=anon_subject,
            acqid=acqid
        )

    def _convert(self, anon_subject: str, acqid: str):
        try:
            conversion = BidsConversion(self.bids_dataset_path, anon_subject)
        except utils.UsageError:
            # error was already logged and more traceback is not needed
            return

        # to avoid reloading the container after a uninstall
        self.source_handler.get_heudiconv_container()

        # install/update sourcedata into bids
        conversion.install_source_dataset(self.source_dataset_path)

        # spec2bids
        conversion.convert(spec=[
            conversion.install_dataset_name/"studyspec.json",
            conversion.install_dataset_name/acqid/"studyspec.json"
        ])

        # procedures
        active_procedures = (ProcedureHandling(self.source_dataset_path)
                             .get_active_procedures())
        conversion.run_procedures(active_procedures)

        conversion.run_bids_validator()

    def _cleanup(self):
        pass
        # TODO uninstall


def run(project_dir):
    """ Run conversion """

    config = ConfigHandler.get_instance().get()
    subject_config = utils.get_config(filename=config["subject_file"])

    source_dataset_path = get_dataset_path(
        project_dir, config["bids_conversion"]["source"]["dataset_name"]
    )
    bids_dataset_path = get_dataset_path(
        project_dir, config["bids_conversion"]["bids"]["dataset_name"]
    )

    conv = Conversion(source_dataset_path, bids_dataset_path,
                      data_path=subject_config["data_path"])

    for subject in subject_config["subjects"]:
        # get anon_subject, acquid, and tarball
        anon_subject = subject["anon_subject"]
        acqid = subject["acqid"]
        print("Convert acqid={}, anon_subject={}".format(acqid, anon_subject))

        conv.run(anon_subject=anon_subject, acqid=acqid)
