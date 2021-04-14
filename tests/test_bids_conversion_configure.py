""" Test bids_conversion configure """

# pylint: disable=missing-function-docstring
# pylint: disable=no-self-use, too-few-public-methods

from prompt_toolkit.input.defaults import create_pipe_input
from prompt_toolkit.output import DummyOutput
import pytest

import data_pipeline
from data_pipeline.bids_conversion import configure
from data_pipeline.bids_conversion.configure_m import _ask_questions
from data_pipeline.config_handler import ConfigHandler


def ask_with_patched_input(question, text):
    inp = create_pipe_input()
    try:
        inp.send_text(text)
        return question(input=inp, output=DummyOutput())
    finally:
        inp.close()


class KeyInputs:  # pylint: disable=missing-class-docstring
    DOWN = "\x1b[B"
    UP = "\x1b[A"
    LEFT = "\x1b[D"
    RIGHT = "\x1b[C"
    ENTER = "\r"
    ESCAPE = "\x1b"
    CONTROLC = "\x03"
    BACK = "\x7f"
    SPACE = " "
    TAB = "\x09"


class TestQuestions:
    """ Test interactive input """

    def test_default(self):
        text = KeyInputs.ENTER + "\r"
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == "Exit"

    def test_exit(self):
        text = "8" + KeyInputs.ENTER + KeyInputs.ENTER + "\r"
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == "Exit"


@pytest.fixture(name="setup_config_handler")
def setup_config_handler_fixture(project):
    ConfigHandler(config_file=project / "config.yaml")


@pytest.fixture(name="mock_ask")
def mock_ask_fixture(monkeypatch):
    def _mock_ask(answer, choice):
        monkeypatch.setattr(data_pipeline.bids_conversion.configure_m,
                            "_ask_questions",
                            lambda: (answer, choice))

    return _mock_ask


class TestConfigure:
    """ Test configure function """

    @pytest.fixture(autouse=True)
    def auto_setup(self, setup_config_handler):
        pass

    def test_exit(self, project, mock_ask):
        mock_ask(answer={"step_select": "Exit"}, choice={})
        configure(project)
        assert (project / "sourcedata").exists()
        assert (project / "bids").exists()
