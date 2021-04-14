""" Test the high level data_pipeline fuctionality """

import shutil
import unittest.mock as mock
from pathlib import Path

from click.testing import CliRunner

import data_pipeline
from data_pipeline.data_pipeline import main


# @pytest.fixture
# def no_logging(monkeypatch):
#     monkeypatch.delattr("data_pipeline.data_pipeline._setup_logging")


class TestProject:
    """ Collections of tests concering the project option """

    @staticmethod
    def test_not_exist(tmp_path):
        """ Project argument is a non-existing directory """
        not_existing_project = tmp_path / "not_existing_project"

        result = CliRunner().invoke(main, ["--project", not_existing_project])
        assert result.exception
        assert result.exit_code == 1

    @staticmethod
    def test_exist(tmp_path):
        """ Project directory does exist but not properly set up """
        result = CliRunner().invoke(main, ["--project", tmp_path])
        assert result.exception
        assert result.exit_code == 2

    @staticmethod
    def test_proper_project(tmp_path):
        """ Project directory is properly set up """
        config = Path(Path(data_pipeline.__file__).parent.absolute(),
                      "templates", "config_template.yaml")
        shutil.copy(config, tmp_path / "config.yaml")

        result = CliRunner().invoke(main, ["--project", tmp_path])
        assert not result.exception
        assert result.exit_code == 0


def test_logging(tmp_path):
    """ Test that log file is created """
    CliRunner().invoke(main, ["--project", tmp_path])
    assert Path(tmp_path, "data_pipeline.log").exists()


def test_setup(tmp_path):
    """ Test that setup option works """
    result = CliRunner().invoke(main, ["--project", tmp_path, "--setup"])
    assert Path(tmp_path, "config.yaml").exists()
    assert not result.exception
    assert result.exit_code == 0


def test_configure(project):
    """ Test that configure option works """
    with mock.patch("data_pipeline.bids_conversion.configure") as mocked:
        result = CliRunner().invoke(main,
                                    ["--project", project, "--configure"])
        assert not result.exception
        assert result.exit_code == 0
        assert mocked.called


def test_run(project):
    """ Test that run option works """
    with mock.patch("data_pipeline.bids_conversion.run") as mock_run:
        result = CliRunner().invoke(main, ["--project", project, "--run"])
        assert not result.exception
        assert result.exit_code == 0
        assert mock_run.called
