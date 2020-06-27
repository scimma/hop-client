#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests the io utilities"


from dataclasses import fields
import json
from unittest.mock import patch, MagicMock, Mock

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


def test_stream_read(circular_msg, circular_text):
    with patch("hop.io._Consumer", MagicMock()) as mock_consumer:
        mock_consumer.stream.return_value = [{'hey', 'you'}]

        broker_url = "kafka://hey@hostname:port/gcn"
        start_at = io.StartPosition.EARLIEST
        persist = False

        stream = io.Stream(persist=persist)

        # verify groupid needs to be set in read-mode
        with pytest.raises(ValueError):
            stream.open("kafka://localhost:9092/topic1", "r")

        with stream.open(broker_url, "r", start_at=start_at) as s:
            for msg in s:
                continue


def test_stream_write(circular_msg, circular_text):
    with patch("hop.io._Producer", MagicMock()) as mock_producer:
        mock_producer.write = Mock()

        broker_url = "kafka://localhost:port/gcn"
        start_at = io.StartPosition.EARLIEST
        persist = False

        stream = io.Stream()

        # verify only 1 topic is allowed in write-mode
        with pytest.raises(ValueError):
            stream.open("kafka://localhost:9092/topic1,topic2", "w")

        # check various warnings when certain settings are set
        with pytest.warns(UserWarning):
            stream.open("kafka://group@localhost:9092/topic1", "w")
        with pytest.warns(UserWarning):
            stream.open(broker_url, "w", start_at=start_at)
        with pytest.warns(UserWarning):
            stream.open(broker_url, "w", persist=persist)

        with stream.open(broker_url, "w") as s:
            s.write(circular_msg)


def test_stream_open():
    stream = io.Stream()

    # verify only read/writes are allowed
    with pytest.raises(ValueError):
        stream.open("kafka://localhost:9092/topic1", "q")


def test_unpack(circular_msg, circular_text):
    wrapped_msg = {"format": "circular", "content": circular_msg}

    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(wrapped_msg).encode("utf-8")

    unpacked_msg = io._Consumer.unpack(kafka_msg)


def test_pack(circular_msg, circular_text):
    # message class
    circular = GCNCircular(**circular_msg)
    packed_msg = io._Producer.pack(circular)

    # unstructured message
    message = {"hey": "you"}
    packed = io._Producer.pack(message)
