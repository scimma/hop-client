from dataclasses import fields
import json
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from hop.auth import Auth
from hop import io
from hop.models import GCNCircular, VOEvent, Blob

from conftest import temp_environ, temp_config

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


def test_stream_read(circular_msg):
    topic = "gcn"
    group_id = "test-group"
    start_at = io.StartPosition.EARLIEST

    message_data = {"format": "circular", "content": circular_msg}
    fake_message = MagicMock()
    fake_message.value = MagicMock(return_value=json.dumps(message_data).encode("utf-8"))
    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[fake_message])
    stream = io.Stream(persist=False, start_at=start_at, auth=False)
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
        broker_url1 = f"kafka://hostname:port/{topic}"
        broker_url2 = f"kafka://{group_id}@hostname:port/{topic}"

        messages = 0
        with stream.open(broker_url1, "r") as s:
            for msg in s:
                messages += 1
        assert messages == 1

        messages = 0
        with stream.open(broker_url2, "r") as s:
            for msg in s:
                messages += 1
        assert messages == 1


def test_stream_write(circular_msg, circular_text, mock_broker, mock_producer):
    topic = "gcn"
    mock_adc_producer = mock_producer(mock_broker, topic)
    expected_msg = json.dumps(Blob(circular_msg).serialize()).encode("utf-8")
    with patch("hop.io.producer.Producer", autospec=True, return_value=mock_adc_producer):

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

        mock_broker.reset()
        with stream.open(broker_url, "w") as s:
            s.write(circular_msg)
            assert mock_broker.has_message(topic, expected_msg)

        # repeat, but with a manual close instead of context management
        mock_broker.reset()
        s = stream.open(broker_url, "w")
        s.write(circular_msg)
        s.close()
        assert mock_broker.has_message(topic, expected_msg)


def test_stream_auth(auth_config, tmpdir):
    # turning off authentication should give None for the auth property
    s1 = io.Stream(auth=False)
    assert s1.auth is None

    # turning on authentication should give an auth object with the data read from the default file
    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        s2 = io.Stream(auth=True)
        a2 = s2.auth
        assert a2._config["sasl.username"] == "username"
        assert a2._config["sasl.password"] == "password"
        assert a2.username == "username"

    # turning on authentication should fail when the default file does not exist
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)), pytest.raises(FileNotFoundError):
        s3 = io.Stream(auth=True)
        a3 = s3.auth

    # anything other than a bool passed to the Stream constructor should get handed back unchanged
    s4 = io.Stream(auth="blarg")
    assert s4.auth == "blarg"


def test_stream_open(auth_config, tmpdir):
    stream = io.Stream(auth=False)

    # verify only read/writes are allowed
    with pytest.raises(ValueError) as err:
        stream.open("kafka://localhost:9092/topic1", "q")
    assert "mode must be either 'w' or 'r'" in err.value.args

    # verify that URLs with no scheme are rejected
    with pytest.raises(ValueError) as err:
        stream.open("bad://example.com/topic", "r")
    assert "invalid kafka URL: must start with 'kafka://'" in err.value.args

    # verify that URLs with no topic are rejected
    with pytest.raises(ValueError) as err:
        stream.open("kafka://example.com/", "r")
    assert "no topic(s) specified in kafka URL" in err.value.args

    # verify that complete URLs are accepted
    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("adc.consumer.Consumer.subscribe", MagicMock()) as subscribe:
        stream = io.Stream()
        # opening a valid URL for reading should succeed
        consumer = stream.open("kafka://example.com/topic", "r")
        # an appropriate consumer group name should be derived from the username in the auth
        assert consumer._consumer.conf.group_id.startswith(stream.auth.username)
        # the target topic should be subscribed to
        subscribe.assert_called_once_with("topic")

        # opening a valid URL for writing should succeed
        producer = stream.open("kafka://example.com/topic", "w")
        producer.write("data")


def test_unpack(circular_msg, circular_text):
    wrapped_msg = {"format": "circular", "content": circular_msg}

    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(wrapped_msg).encode("utf-8")

    unpacked_msg = io.Consumer._unpack(kafka_msg)

    unpacked_msg2, metadata = io.Consumer._unpack(kafka_msg, metadata=True)
    assert unpacked_msg2 == unpacked_msg


def test_mark_done(circular_msg):
    start_at = io.StartPosition.EARLIEST
    message_data = {"format": "circular", "content": circular_msg}

    mock_message = MagicMock()
    mock_message.value = MagicMock(return_value=json.dumps(message_data).encode("utf-8"))
    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[mock_message])
    stream = io.Stream(persist=False, start_at=start_at, auth=False)

    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
        with stream.open("kafka://hostname:port/gcn", "r") as s:
            for msg, metadata in s.read(metadata=True):
                s.mark_done(metadata)
                mock_instance.mark_done.assert_called()


def test_pack(circular_msg, circular_text):
    # message class
    circular = GCNCircular(**circular_msg)
    packed_msg = io.Producer._pack(circular)

    # unstructured message
    message = {"hey": "you"}
    packed = io.Producer._pack(message)


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
    packed_msg = io.Producer._pack(orig_message)

    # mock a kafka message with value being the packed message
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = packed_msg

    # unpack the message
    unpacked_msg = io.Consumer._unpack(kafka_msg)

    # verify based on format
    if format in ("voevent", "circular"):
        assert isinstance(unpacked_msg, expected_model)
        assert unpacked_msg.asdict() == orig_message.asdict()
    else:
        assert isinstance(unpacked_msg, type(unpacked_msg))
        assert unpacked_msg == orig_message


def test_pack_unpack_roundtrip_unstructured():
    # objects (of types that json.loads happens to produce) should remain unchanged by the process
    # of packing and unpacking
    for orig_message in ["a string", ["a", "B", "c"], {"dict": True, "other_data": [5, 17]}]:
        packed_msg = io.Producer._pack(orig_message)
        kafka_msg = MagicMock()
        kafka_msg.value.return_value = packed_msg
        unpacked_msg = io.Consumer._unpack(kafka_msg)
        assert unpacked_msg == orig_message

    # non-serializable objects should raise an error
    with pytest.raises(TypeError):
        # note that we are not trying to pack a string, but the string class itself
        packed_msg = io.Producer._pack(str)


@pytest.mark.parametrize("message", [
    {"format": "voevent", "content": content_mock(VOEvent)},
    {"format": "circular", "content": content_mock(GCNCircular)}
])
def test_load_load_file_equivalence(message, message_parameters_dict):
    format = message["format"]
    content = message["content"]

    # load test data
    shared_datadir = Path("tests/data")
    test_filename = message_parameters_dict[format]["test_file"]
    test_file = shared_datadir / "test_data" / test_filename

    deserializer = io.Deserializer[format.upper()]
    from_file = deserializer.load_file(test_file)

    with open(test_file, "r") as df:
        raw_data = df.read()
    from_mem = deserializer.load(raw_data)
    assert from_mem == from_file


def test_metadata(mock_kafka_message):
    metadata = io.Metadata.from_message(mock_kafka_message)

    # verify all properties are populated and match raw message
    assert metadata._raw == mock_kafka_message
    assert metadata.topic == mock_kafka_message.topic()
    assert metadata.partition == mock_kafka_message.partition()
    assert metadata.offset == mock_kafka_message.offset()
    assert metadata.timestamp == mock_kafka_message.timestamp()[1]
    assert metadata.key == mock_kafka_message.key()


def test_plugin_loading(caplog):
    # plugins which fail during loading should trigger a warning
    import pluggy
    pm1 = pluggy.PluginManager("hop")

    def raise_ex(ignored):
        raise Exception("Things are bad")
    lse_mock = MagicMock(side_effect=raise_ex)
    pm1.load_setuptools_entrypoints = lse_mock

    with patch("pluggy.PluginManager", MagicMock(return_value=pm1)), \
            caplog.at_level(logging.WARNING):
        registered = io._load_deserializer_plugins()
        assert "Could not load external message plugins" in caplog.text
        # but built-in models should still be available
        assert len(registered) == 3
        assert "VOEVENT" in registered
        assert "CIRCULAR" in registered
        assert "BLOB" in registered
        # while we're here, make sure the documented interface is being used
        lse_mock.assert_called_with("hop_plugin")

    # users should be warned if plugins collide
    pm2 = pluggy.PluginManager("hop")
    gm_mock = MagicMock(return_value=[{
                        "foo": None,
                        "bar": None,
                        }, {"Foo": str}])
    # If we don't make this property explicitly false, the Mock will helpfully create it as another
    # Mock object which will then look true-ish to pluggy, leading to problems
    gm_mock.spec.warn_on_impl = False
    pm2.hook.get_models = gm_mock
    with patch("pluggy.PluginManager", MagicMock(return_value=pm2)), \
            caplog.at_level(logging.WARNING):
        registered = io._load_deserializer_plugins()
        assert "Identified duplicate message plugin" in caplog.text

    # additional plugins should appear in the resulting list
    pm3 = pluggy.PluginManager("hop")

    def fake_lse(ignored):
        original_get_models = pm3.hook.get_models

        def add_extra_model():
            models = original_get_models()
            models.append({"Foo": None})
            return models
        pm3.hook.get_models = add_extra_model
    pm3.load_setuptools_entrypoints = fake_lse
    with patch("pluggy.PluginManager", MagicMock(return_value=pm3)):
        registered = io._load_deserializer_plugins()
        print(registered)
        assert len(registered) == 4
        assert "FOO" in registered
        assert "VOEVENT" in registered
        assert "CIRCULAR" in registered
        assert "BLOB" in registered
