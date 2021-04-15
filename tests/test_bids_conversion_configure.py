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


@pytest.fixture(name="choices")
def choices_fixture():
    return dict(
        # step_select
        import_data="Import data",
        register_rule="Configure and register rule »",
        add_procedure="Add procedure »",
        preview="Generate preview",
        check="Check for BIDS conformity",
        cleanup="Cleanup",

        # rule_select
        rule_create="Create new rule",
        rule_import="Import rule",

        # procedure_select
        proc_change="Change active procedures",
        proc_create="Create new procedure",
        proc_import="Import procedure",
    )


class Press:  # pylint: disable=missing-class-docstring
    EXIT = "8" + KeyInputs.ENTER
    REGISTER_RULE = "2" + KeyInputs.ENTER
    ADD_PROCEDURE = "3" + KeyInputs.ENTER
    PREVIEW = "4" + KeyInputs.ENTER
    CHECK = "5" + KeyInputs.ENTER
    CLEANUP = "6" + KeyInputs.ENTER
    HELP = "7" + KeyInputs.ENTER


class TestQuestions:
    """ Test interactive input """

    def test_default(self):
        text = KeyInputs.ENTER + "\r"
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == "Exit"

    def test_exit(self):
        text = Press.EXIT + KeyInputs.ENTER + "\r"
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == "Exit"

    def test_import(self):
        pass

    def test_rule_return(self, choices):
        text = Press.REGISTER_RULE + "3" + KeyInputs.ENTER + "\r"
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["register_rule"]
        assert answer["rule_select"] == "Return"

    def test_rule_create(self):
        text = Press.REGISTER_RULE + "1" + KeyInputs.ENTER + "\r"
        answer, _ = ask_with_patched_input(_ask_questions, text)
        #TODO
        # print(answer)

    def test_rule_import(self):
        pass

    def test_procedure_return(self, choices):
        text = Press.ADD_PROCEDURE + "4" + KeyInputs.ENTER + "\r"
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["add_procedure"]
        assert answer["procedure_select"] == "Return"

    def test_procedure_create_shell(self):
        pass

    def test_procedure_create_python(self):
        pass

    def test_procedure_import(self):
        pass

    def test_procedure_change(self):
        pass

    def test_preview(self):
        pass

    def test_check(self):
        pass

    def test_cleanup(self):
        pass

    def test_help(self):
        pass


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
