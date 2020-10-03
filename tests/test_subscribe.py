from contextlib import redirect_stdout
import io
import os
from pathlib import Path

import pytest

from hop import models
from hop import subscribe


# test the subscribe printer for each message format
@pytest.mark.parametrize("message_format", ["voevent", "circular", "blob"])
@pytest.mark.parametrize("json_dump", [True, False])
def test_print_message(message_format, json_dump, message_parameters_dict):

    # load parameters from conftest
    message_parameters = message_parameters_dict[message_format]
    model_name = message_parameters["model_name"]
    test_file = message_parameters["test_file"]

    shared_datadir = Path("tests/data")

    test_content_path = shared_datadir / "test_data" / test_file

    # load model
    model_class = message_parameters["expected_model"]
    if issubclass(model_class, models.MessageModel):
        model = model_class.load_file(test_content_path)
    else:
        model = test_content_path.read_text()

    f = io.StringIO()
    with redirect_stdout(f):
        subscribe.print_message(model, json_dump=json_dump)

    # extract message string from stdout
    test_message_stdout_str = f.getvalue()

    # read in expected stdout text
    expected_basename = os.path.splitext(test_file)[0]
    if json_dump:
        expected_file = "_".join([expected_basename, "json"]) + ".stdout"
    else:
        expected_file = expected_basename + ".stdout"
    expected_message_stdout = (shared_datadir / "expected_data" / expected_file).read_text()

    # verify printed message structure is correct
    print(test_message_stdout_str)
    assert test_message_stdout_str == expected_message_stdout

    f.close()
