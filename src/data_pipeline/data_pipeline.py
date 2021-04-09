""" The main executable to run the tool """
import logging
from pathlib import Path
import shutil
import sys

import click

import data_pipeline.bids_conversion as bids_conversion
from data_pipeline.config_handler import ConfigHandler


def _setup_logging(project):

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
                "filename": Path(project, "data_pipeline.log"),
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
              help="Prepares and configure the BIDS conversion")
@click.option("--run", is_flag=True,
              help="Run the BIDS conversion")
def main(setup, project, configure, run):
    """ Execute data-pipeline """

    # also relative paths like ../<my_project_dir> are allowed
    project = Path(project).resolve()

    if project:
        logging.info("Using project dir: %s", project)
        # check that project dir exists
        if not project.exists():
            logging.error("Project dir %s does not exist", project)
            sys.exit(1)

    _setup_logging(project)

    config_filename = "config.yaml"
    config_path = Path(project, config_filename)

    if setup:
        logging.info("Setting up data_pipeline in %s", project)

        # create a config file in the project dir
        source = Path(Path(__file__).parent.absolute(),
                      "templates", "config_template.yaml")
        shutil.copy(source, config_path)

    try:
        ConfigHandler(config_file=config_path)
    except FileNotFoundError:
        logging.error(
            "Not a correct project dir. Either use an existing project or set "
            "one up. See --help for further information.")
        sys.exit(2)

    if configure:
        # TODO let user choose which modules to run

        bids_conversion.configure(project)

    if run:
        bids_conversion.run(project)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
