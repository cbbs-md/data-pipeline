"""Custom rules for dicom2spec"""

import abc

class RulesBase(abc.ABC):

    def __init__(self, dicommetadata):
        """

        Parameter
        ----------
        dicommetadata: list of dict
            dicom metadata as extracted by datalad; one dict per image series
        """
        self._dicom_series = dicommetadata

    def __call__(self, subject=None, anon_subject=None, session=None):
        """

        Parameters
        ----------

        Returns
        -------
        list of tuple (dict, bool)
        """
        spec_dicts = []
        for dicom_dict in self._dicom_series:
            rules = self._rules(
                dicom_dict,
                subject=subject,
                anon_subject=anon_subject,
                session=session
            )
            is_valid = self.series_is_valid(dicom_dict)
            spec_dicts.append((rules, is_valid))
        return spec_dicts

    @abc.abstractmethod
    def _rules(self, series_dict, subject=None, anon_subject=None,
               session=None):
        pass

    def series_is_valid(self, series_dict):
        return series_dict['ProtocolName'] != 'ExamCard'
