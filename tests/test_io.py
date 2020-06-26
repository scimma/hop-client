#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests the io utilities"


from dataclasses import fields
from unittest.mock import mock_open, patch, MagicMock

import pytest

from hop import io
from hop.models import GCNCircular, VOEvent, MessageBlob


def content_mock(message_model):
    """Mock content to pass during the message_model creation since MagicMock()
    is unable to mock __init__ of the model dataclass in tests.
    """
    content = {}
    for field in fields(message_model):
        content.update({field.name: "test"})
    return content

# test the subscribe classifier for each message format


@pytest.mark.parametrize("message", [
    {"format": "voevent", "content": content_mock(VOEvent)},
    {"format": "circular", "content": content_mock(GCNCircular)},
    {"format": "blob", "content": content_mock(MessageBlob)},
    {"format": "other", "content": "other"},
    ["wrong_datatype"],
    {"wrong_key": "value"},
])
def test_deserialize(message, message_parameters_dict):

    # test a non-dict message
    if not isinstance(message, dict):
        with pytest.raises(ValueError):
            test_model = io.Deserializer.deserialize(message)
        return
    # test a dict message with wrong key values
    elif not (("format" in message) and ("content" in message)):
        with pytest.raises(ValueError):
            test_model = io.Deserializer.deserialize(message)
        return

    message_format = message["format"]
    message_content = message["content"]

    # test an invalid format
    if message_format not in message_parameters_dict:
        with pytest.raises(ValueError):
            test_model = io.Deserializer.deserialize(message)
        return

    # load parameters from conftest for valid formats
    message_parameters = message_parameters_dict[message_format]
    model_name = message_parameters["model_name"]
    expected_model = message_parameters["expected_model"]

    # test valid formats
    with patch(f"hop.models.{model_name}", MagicMock()):
        test_model = io.Deserializer.deserialize(message)

    # verify the message is classified properly
    assert isinstance(test_model, expected_model)


def test_stream(circular_msg, circular_text):
    with patch("hop.io.Stream.open", mock_open()) as mock_stream:
        broker_url = "kafka://hostname:port/gcn"
        persist = False

        stream = io.Stream(persist=persist)

        # verify defaults are stored correctly
        assert stream.persist == persist

        with stream.open(broker_url, "w") as s:
            s.write(circular_msg)

        # verify GCN was processed
        mock_stream.assert_called_with(broker_url, "w")
