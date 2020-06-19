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
from hop.models import GCNCircular, VOEvent, message_blob

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
    {"format":"blob", "content": content_mock(message_blob)},
    {"format":"other", "content": "other"},
    ])
def test_classify_message(message):
    message_format = message["format"]
    message_content = message["content"]

    if message_format == "voevent":
        expected_model = models.VOEvent
        model_name = "VOEvent"
    elif message_format == "circular":
        expected_model = models.GCNCircular
        model_name = "GCNCircular"
    elif message_format == "blob":
        expected_model = models.message_blob
        model_name = "message_blob"
    else:
        with pytest.raises(ValueError):
            test_model = subscribe.classify_message(message)
        return

    # with patch(f"hop.models.{model_name}", Mock()) as mock_model:
#    m = MagicMock()
#    m = content_mock(expected_model)
#    m.return_value = expected_model
#    m.__init__ = MagicMock(return_value=expected_model)
#    with patch(f"hop.models.{model_name}", return_value=m) as patch_model:
    with patch(f"hop.models.{model_name}", MagicMock()) as patch_model:
#    with patch.object(f"hop.models.{model_name}", "__init__") as patch_model:
#        patch_model.__init__.return_value = expected_model
#        patch_model.return_value = expected_model

        test_model = subscribe.classify_message(message)
        #patch_model.assert_called_with(message_content)

    assert isinstance(test_model, expected_model)


# test the subscribe printer for each message format
@pytest.mark.parametrize("message_format", ["voevent", "circular", "blob"])
def test_print_message(message_format):
    if message_format == "voevent":
        model_type = "VOEvent"
        test_file = "example_voevent.xml"
    elif message_format == "circular":
        model_type = "GCNCircular"
        test_file = "example_gcn.gcn3"
    elif message_format == "blob":
        model_type = "message_blob"
        test_file = "example_blob.txt"

    shared_datadir = Path("tests/data")

    test_content = (shared_datadir / "test_data" / test_file).read_text()

    # control the output of the hop.model's print method with mocking
    with patch(f"hop.models.{model_type}", MagicMock()) as mock_model:
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
