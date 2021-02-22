""" The main executable to run the tool """
import logging
from pathlib import Path
import sys

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
@click.option("--setup", is_flag=True,
              help="Sets up inside the project dir")
@click.option("--project", type=str, default=Path.cwd(),
              help="Choose the project directory")
@click.option("--configure", is_flag=True,
              help="Prepares and configure the bids convesion")
def main(setup, project, configure):
    """ Execute data-pipeline """
    _setup_logging()

    if project:
        # check that project dir exists
        if not Path(project).exists():
            logging.error("Project dir %s does not exist", project)
            sys.exit(1)

    if setup:
        # create a config file in the project dir
        pass

    if configure:
        configure_bids_conversion(project)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
