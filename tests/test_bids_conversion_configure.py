""" Test bids_conversion configure """

# pylint: disable=missing-function-docstring
# pylint: disable=no-self-use, too-few-public-methods

from prompt_toolkit.input.defaults import create_pipe_input
from prompt_toolkit.output import DummyOutput

from data_pipeline.bids_conversion.configure_m import _ask_questions


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
