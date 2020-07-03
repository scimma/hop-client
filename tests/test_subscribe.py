import io
import codecs
from contextlib import redirect_stdout
from pathlib import Path

from unittest.mock import patch, MagicMock
import pytest

from hop import subscribe


# test the subscribe printer for each message format
@pytest.mark.parametrize("message_format", ["voevent", "circular", "blob"])
def test_print_message(message_format, message_parameters_dict):

    # load parameters from conftest
    message_parameters = message_parameters_dict[message_format]
    model_name = message_parameters["model_name"]
    test_file = message_parameters["test_file"]

    shared_datadir = Path("tests/data")

    test_content = (shared_datadir / "test_data" / test_file).read_text()

    # control the output of the hop.model's print method with mocking
    with patch(f"hop.models.{model_name}", MagicMock()) as mock_model:
        mock_model.__str__.return_value = test_content

        f = io.StringIO()
        with redirect_stdout(f):
            subscribe.print_message(mock_model, json_dump=False)

        # read stdout from beginning
        f.seek(0)

        # extract message string from stdout
        test_message_stdout_list = f.readlines()
        test_message_stdout_str = "".join(test_message_stdout_list)

        # read in expected stdout text
        expected_message_raw = (
            shared_datadir / "expected_data" / (test_file + ".stdout")
        ).read_text()

        # use codec to decode the expected output to leave newline/tab characters intact
        expected_message_stdout = codecs.unicode_escape_decode(expected_message_raw)[0]

        # verify printed message structure is correct
        print(test_message_stdout_str)

        assert test_message_stdout_str == expected_message_stdout
