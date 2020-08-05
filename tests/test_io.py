from dataclasses import fields
import json
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock

import pytest

from hop.auth import Auth
from hop import io
from hop.models import GCNCircular, VOEvent, MessageBlob

logger = logging.getLogger("hop")


def content_mock(message_model):
    """Mock content to pass during the message_model creation since MagicMock()
    is unable to mock __init__ of the model dataclass in tests.
    """
    content = {}
    for field in fields(message_model):
        content.update({field.name: "test"})
    return content


# test the deserializer for each message format
@pytest.mark.parametrize("message", [
    {"format": "voevent", "content": content_mock(VOEvent)},
    {"format": "circular", "content": content_mock(GCNCircular)},
    {"format": "blob", "content": content_mock(MessageBlob)},
    {"format": "other", "content": "other"},
    ["wrong_datatype"],
    {"wrong_key": "value"},
])
def test_deserialize(message, message_parameters_dict, caplog):

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

    # load parameters from conftest for valid formats
    if message_format in message_parameters_dict:
        message_parameters = message_parameters_dict[message_format]
        model_name = message_parameters["model_name"]
        expected_model = message_parameters["expected_model"]

        # test valid formats
        with patch(f"hop.models.{model_name}", MagicMock()):
            test_model = io.Deserializer.deserialize(message)

        # verify the message is classified properly
        assert isinstance(test_model, expected_model)

    else:  # test an invalid format
        with caplog.at_level(logging.WARNING):
            test_model = io.Deserializer.deserialize(message)

            # verify a message blob was produced with warnings
            output = f"Message format {message_format.upper()} " \
                "not recognized, returning a MessageBlob"
            assert isinstance(test_model, MessageBlob)
            assert output in caplog.text
            assert test_model.missing_schema


def test_stream_read(circular_msg, circular_text):
    with patch("hop.io._Consumer", MagicMock()) as mock_consumer:
        mock_consumer.stream.return_value = [{'hey', 'you'}]

        broker_url1 = "kafka://hostname:port/gcn"
        broker_url2 = "kafka://hey@hostname:port/gcn"
        start_at = io.StartPosition.EARLIEST
        persist = False

        stream = io.Stream(persist=persist, start_at=start_at, auth=False)

        with stream.open(broker_url1, "r") as s:
            for msg in s:
                continue

        with stream.open(broker_url2, "r") as s:
            for msg in s:
                continue


def test_stream_write(circular_msg, circular_text):
    with patch("hop.io._Producer", MagicMock()) as mock_producer:
        mock_producer.write = Mock()

        broker_url = "kafka://localhost:port/gcn"
        auth = Auth("user", "password")
        start_at = io.StartPosition.EARLIEST
        persist = False

        stream = io.Stream(start_at=start_at, persist=persist, auth=auth)

        # verify only 1 topic is allowed in write mode
        with pytest.raises(ValueError):
            stream.open("kafka://localhost:9092/topic1,topic2", "w")

        # verify warning is raised when groupid is set in write mode
        with pytest.warns(UserWarning):
            stream.open("kafka://group@localhost:9092/topic1", "w")

        with stream.open(broker_url, "w") as s:
            s.write(circular_msg)


def test_stream_open():
    stream = io.Stream(auth=False)

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


@pytest.mark.parametrize("message", [
    {"format": "voevent", "content": content_mock(VOEvent)},
    {"format": "circular", "content": content_mock(GCNCircular)},
    {"format": "blob", "content": content_mock(MessageBlob)},
])
def test_pack_unpack_roundtrip(message, message_parameters_dict, caplog):
    format = message["format"]
    content = message["content"]

    # load test data
    shared_datadir = Path("tests/data")
    test_filename = message_parameters_dict[format]["test_file"]
    test_file = shared_datadir / "test_data" / test_filename

    # generate a message
    expected_model = message_parameters_dict[format]["expected_model"]
    if format in ("voevent", "circular"):
        orig_message = expected_model.load_file(test_file)
    else:
        orig_message = test_file.read_text()

    # pack the message
    packed_msg = io._Producer.pack(orig_message)

    # mock a kafka message with value being the packed message
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = packed_msg

    # unpack the message
    unpacked_msg = io._Consumer.unpack(kafka_msg)

    # verify based on format
    if format in ("voevent", "circular"):
        assert isinstance(unpacked_msg, expected_model)
        assert unpacked_msg.asdict() == orig_message.asdict()
    else:
        assert isinstance(unpacked_msg, MessageBlob)
        assert unpacked_msg.content == orig_message
