""" The main executable to run the tool """
import logging

import click

from data_pipeline.bids_configuration import configure_bids_conversion


def _setup_logging():

    log_level = "INFO"

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "[%(levelname)s] %(message)s"
            },
            "precise": {
                "format": ("[%(asctime)s] [%(name)s] [%(levelname)s] "
                           "%(message)s")
            }
        },
        "handlers": {
            "console": {
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "simple"
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": log_level,
                "formatter": "precise",
                "filename": "data_pipeline.log",
                "maxBytes": 10485760,
                "backupCount": 3,
            }
        },
        "loggers": {
            "": {
                "handlers": ["console", "file"],
                "level": log_level,
                "propagate": True
            }
        }
    })

    # workaround for broken datalad logging
    # if not disabled it will flood the logs
    logging.getLogger("datalad").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


@click.command()
@click.option('--configure', is_flag=True,
              help="Prepares and configure the bids convesion")
def main(configure):
    """ Execute data-pipeline """
    _setup_logging()

    if configure:
        configure_bids_conversion()


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
