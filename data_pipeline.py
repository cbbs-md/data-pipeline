
import argparse
import logging

from bids_configuration import SetupDatalad, BidsConfiguration


def _setup_logging(name=__name__):

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "[%(name)s] [%(levelname)s] %(message)s"
            },
            "precise": {
                "format": ("[%(asctime)s] [%(name)s] [%(levelname)s] "
                           "%(message)s")
            }
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "simple"
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "precise",
                "filename": "data_pipeline.log",
                "maxBytes": 10485760,
                "backupCount": 3,
            }
        },
        "loggers": {
            "": {
                "handlers": ["file"],
                "level": "INFO",
                "propagate": True
            }
        }
    })


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

    setup = SetupDatalad()
    if args.import_data:
        setup.run()
        # TODO can this be moved down into the other if?

    conv = BidsConfiguration(setup.dataset_path)
    if args.import_data:
        conv.import_data(
            anon_subject=20,
            tarball="/home/nela/projects/Antonias_data/"
                    "original/sourcedata.tar.gz",
        )
    elif args.apply_rule:
        conv.apply_rule(rule="myrules.py", overwrite=True)
    elif args.generate_preview:
        conv.generate_preview()

# for debugging: remove dataset again:
# datalad remove -r --nocheck -d bids_autoconv