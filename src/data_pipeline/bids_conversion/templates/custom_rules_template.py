"""Custom rules for dicom2spec"""

from rules_base import RulesBase


class MyDICOM2SpecRules(RulesBase):

    @staticmethod
    def _get_grep_field_mapping_modality(series_dict):
        if "M" in series_dict['ImageType']:
            # will infer magnitude1/magnitude2 automatically
            return "magnitude"

        return "phasediff"

    def _rules(self, series_dict, subject=None, anon_subject=None,
               session=None):

        # please insert your code here

        return {
            'description': series_dict.get("SeriesDescription", ""),
            'comment': 'I actually have no clue',
            'subject': subject or series_dict['PatientID'],
            'anon-subject': anon_subject or None,
            'bids-session': session or None,
            # 'bids-modality': modality,
            # 'bids-task': task,
            # 'bids-run': run,
        }


__datalad_hirni_rules = MyDICOM2SpecRules
