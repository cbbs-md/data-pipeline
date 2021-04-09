""" Test the high level data_pipeline fuctionality """
from importlib import reload
import shutil
from pathlib import Path

from click.testing import CliRunner
import pytest

import data_pipeline
from data_pipeline.data_pipeline import main


@pytest.fixture
def no_logging(monkeypatch):
    monkeypatch.delattr("data_pipeline.data_pipeline._setup_logging")


@pytest.fixture(autouse=True)
def reset_confighandler():
    # disable fixture by using @pytest.mark.noautofixt
    # ConfigHandler is a Singleton
    reload(data_pipeline.config_handler)


class TestProject:

    def test_not_exist(self, tmp_path):
        not_existing_project = tmp_path / "not_existing_project"

        result = CliRunner().invoke(main, ["--project", not_existing_project])
        assert result.exception
        assert result.exit_code == 1

    def test_exist(self, tmp_path):
        result = CliRunner().invoke(main, ["--project", tmp_path])
        assert result.exception
        assert result.exit_code == 2

    def test_proper_project(self, tmp_path):
        config = Path(Path(data_pipeline.__file__).parent.absolute(),
                      "templates", "config_template.yaml")
        shutil.copy(config, tmp_path / "config.yaml")

        result = CliRunner().invoke(main, ["--project", tmp_path])
        assert not result.exception
        assert result.exit_code == 0


def test_logging(tmp_path):
    CliRunner().invoke(main, ["--project", tmp_path])
    assert Path(tmp_path, "data_pipeline.log").exists()


def test_setup(tmp_path):
    CliRunner().invoke(main, ["--project", tmp_path, "--setup"])
    assert Path(tmp_path, "config.yaml").exists()
