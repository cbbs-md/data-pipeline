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
        text = KeyInputs.ENTER
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == "Exit"

    def test_exit(self):
        text = Press.EXIT + KeyInputs.ENTER
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == "Exit"

    def test_import(self, choices):
        filename = "test"
        text = "1" + KeyInputs.ENTER + filename + KeyInputs.ENTER
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["import_data"]
        assert answer["data_path"] == filename

    def test_rule_return(self, choices):
        text = Press.REGISTER_RULE + "3" + KeyInputs.ENTER
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["register_rule"]
        assert answer["rule_select"] == "Return"

    def test_rule_create(self, choices):
        text = Press.REGISTER_RULE + "1" + KeyInputs.ENTER
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["register_rule"]
        assert answer["rule_select"] == choices["rule_create"]

    def test_rule_import(self, choices):
        filename = "test"
        text = (Press.REGISTER_RULE
                + "2" + KeyInputs.ENTER
                + filename + KeyInputs.ENTER)
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["register_rule"]
        assert answer["rule_select"] == choices["rule_import"]
        assert answer["rule_file"] == filename

    def test_procedure_return(self, choices):
        text = Press.ADD_PROCEDURE + "4" + KeyInputs.ENTER
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["add_procedure"]
        assert answer["procedure_select"] == "Return"

    def test_procedure_create_shell(self, choices):
        filename = "test"
        text = (Press.ADD_PROCEDURE
                + "2" + KeyInputs.ENTER
                + "1" + KeyInputs.ENTER
                + filename + KeyInputs.ENTER)
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["add_procedure"]
        assert answer["procedure_select"] == choices["proc_create"]
        assert answer["procedure_type"] == "shell"
        assert answer["procedure_name"] == filename

    def test_procedure_create_python(self, choices):
        filename = "test"
        text = (Press.ADD_PROCEDURE
                + "2" + KeyInputs.ENTER
                + "2" + KeyInputs.ENTER
                + filename + KeyInputs.ENTER)
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["add_procedure"]
        assert answer["procedure_select"] == choices["proc_create"]
        assert answer["procedure_type"] == "python"
        assert answer["procedure_name"] == filename

    def test_procedure_import(self, choices):
        filepath = "test_path"
        filename = "test_name"
        text = (Press.ADD_PROCEDURE
                + "3" + KeyInputs.ENTER
                + filepath + KeyInputs.ENTER
                + filename + KeyInputs.ENTER)
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["add_procedure"]
        assert answer["procedure_select"] == choices["proc_import"]
        assert answer["procedure_file"] == filepath
        assert answer["procedure_file_name"] == filename

    def test_procedure_change(self, choices):
        text = Press.ADD_PROCEDURE + "1" + KeyInputs.ENTER
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["add_procedure"]
        assert answer["procedure_select"] == choices["proc_change"]
        # Selection of procedures to activate/deactivate is not done inside
        # of "_ask_questions"

    def test_preview(self, choices):
        text = Press.PREVIEW
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["preview"]

    def test_check(self, choices):
        text = Press.CHECK
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["check"]

    def test_cleanup(self, choices):
        text = Press.CLEANUP
        answer, _ = ask_with_patched_input(_ask_questions, text)
        assert answer["step_select"] == choices["cleanup"]

    def test_help(self):
        # not implemented yet
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
