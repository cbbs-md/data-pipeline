
import logging

import click

from bids_configuration import configure_bids_conversion


def _setup_logging():

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
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": True
            }
        }
    })

    # workaround for broken datalad logging
    # if not disabled it will flood the logs
    logging.getLogger("datalad").setLevel(logging.WARNING)


@click.command()
@click.option('--configure', is_flag=True,
              help="Prepares and configure the bids convesion")
def _main(configure):
    #_setup_logging()

    if configure:
        configure_bids_conversion()


if __name__ == "__main__":
    _main()  # pylint: disable=no-value-for-parameter
