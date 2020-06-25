#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "a module that tests subscribe utilities"

import io
import codecs
from contextlib import redirect_stdout
from pathlib import Path

from unittest.mock import patch, MagicMock, Mock, create_autospec
from dataclasses import fields
import pytest

from hop import subscribe, models
from hop.models import GCNCircular, VOEvent, MessageBlob

# test the subscribe classifier for each message format

def content_mock(message_model):
    """Mock content to pass during the message_model creation since MagicMock() 
    is unable to mock __init__ of the model dataclass in tests.
    """
    content = {}
    for field in fields(message_model):
        content.update({field.name: "test"})
    return content

@pytest.mark.parametrize("message", [
    {"format":"voevent", "content": content_mock(VOEvent)},
    {"format":"circular", "content": content_mock(GCNCircular)},
    {"format":"blob", "content": content_mock(MessageBlob)},
    {"format":"other", "content": "other"},
    ["wrong_datatype"],
    {"wrong_key":"value"},
    ])
def test_classify_message(message, message_parameters_dict):

    # test a non-dict message
    if not isinstance(message, dict):
        with pytest.raises(ValueError):
            test_model = subscribe.classify_message(message)
        return
    # test a dict message with wrong key values
    elif not (("format" in message) and ("content" in message)):
        with pytest.raises(KeyError):
            test_model = subscribe.classify_message(message)
        return

    message_format = message["format"]
    message_content = message["content"]

    # test an invalid format
    if not message_format in message_parameters_dict:
        with pytest.raises(ValueError):
            test_model = subscribe.classify_message(message)
        return

    # load parameters from conftest for valid formats
    message_parameters = message_parameters_dict[message_format]
    model_name = message_parameters["model_name"]
    expected_model = message_parameters["expected_model"]

    # test valid formats
    with patch(f"hop.models.{model_name}", MagicMock()) as patch_model:
        test_model = subscribe.classify_message(message)

    # verify the message is classified properly
    assert isinstance(test_model, expected_model)


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
