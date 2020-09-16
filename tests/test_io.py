from dataclasses import fields
import json
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from hop.auth import Auth
from hop import io
from hop.models import GCNCircular, VOEvent, Blob

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
    {"format": "blob", "content": "this is a test message"},
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
                "not recognized, returning a Blob"
            assert isinstance(test_model, Blob)
            assert output in caplog.text
            assert test_model.missing_schema


def test_stream_read(circular_msg, circular_text, mock_broker, mock_consumer):
    topic = "gcn"
    group_id = "test-group"
    start_at = io.StartPosition.EARLIEST

    mock_adc_consumer = mock_consumer(mock_broker, topic, group_id, start_at)
    with patch("hop.io.consumer.Consumer", autospec=True) as mock_instance:
        mock_instance.side_effect = mock_adc_consumer
        mock_adc_consumer.stream.return_value = [{'hey', 'you'}]

        broker_url1 = f"kafka://hostname:port/{topic}"
        broker_url2 = f"kafka://{group_id}@hostname:port/{topic}"
        persist = False

        stream = io.Stream(persist=persist, start_at=start_at, auth=False)

        with stream.open(broker_url1, "r") as s:
            for msg in s:
                continue

        with stream.open(broker_url2, "r") as s:
            for msg in s:
                continue


def test_stream_write(circular_msg, circular_text, mock_broker, mock_producer):
    topic = "gcn"
    mock_adc_producer = mock_producer(mock_broker, topic)
    with patch("hop.io.producer.Producer", autospec=True) as mock_instance:
        mock_instance.side_effect = mock_adc_producer

        broker_url = f"kafka://localhost:port/{topic}"
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
    {"format": "blob", "content": "this is a test message"},
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
        assert isinstance(unpacked_msg, type(unpacked_msg))
        assert unpacked_msg == orig_message


def test_metadata(mock_kafka_message):
    metadata = io.Metadata.from_message(mock_kafka_message)

    # verify all properties are populated and match raw message
    assert metadata._raw == mock_kafka_message
    assert metadata.topic == mock_kafka_message.topic()
    assert metadata.partition == mock_kafka_message.partition()
    assert metadata.offset == mock_kafka_message.offset()
    assert metadata.timestamp == mock_kafka_message.timestamp()[1]
    assert metadata.key == mock_kafka_message.key()
