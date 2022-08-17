from collections import Counter
from dataclasses import asdict, fields
import json
import logging
from pathlib import Path
import time
from unittest.mock import patch, MagicMock

import pytest

from hop.auth import Auth
from hop import io
from hop.models import AvroBlob, Blob, GCNCircular, JSONBlob, VOEvent, format_name
import confluent_kafka

from conftest import temp_environ, temp_config, message_parameters_dict_data

logger = logging.getLogger("hop")


def content_mock(message_model):
    """Mock content to pass during the message_model creation since MagicMock()
    is unable to mock __init__ of the model dataclass in tests.
    """
    content = {}
    for field in fields(message_model):
        content.update({field.name: "test"})
    return content


def make_message(content, headers=[], topic="test-topic", partition=0, offset=0):
    message = MagicMock()
    message.value.return_value = content
    message.headers.return_value = headers
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    message.timestamp.return_value = (0, 1234567890)
    message.key.return_value = "test-key"
    return message


def make_message_standard(message, **kwags):
    raw = message.serialize()
    return make_message(raw["content"],
                        headers=[("_format", raw["format"].encode("utf-8"))], **kwags)


# only applicable to old message models which are JSON-compatible
def old_style_message(message):
    raw = {"format": format_name(type(message)), "content": asdict(message)}
    return json.dumps(raw).encode("utf-8")


def get_model_data(model_name):
    return message_parameters_dict_data[model_name]["model_text"]


# test the deserializer for each message format
@pytest.mark.parametrize("message", [
    # properly formatted, new-style messages
    {"format": "voevent",
        "content": make_message_standard(VOEvent.load(get_model_data("voevent")))},
    {"format": "circular",
        "content": make_message_standard(GCNCircular.load(get_model_data("circular")))},
    {"format": "json", "content": make_message_standard(JSONBlob.load(get_model_data("json")))},
    {"format": "avro", "content": make_message_standard(AvroBlob.load(get_model_data("avro")))},
    {"format": "blob", "content": make_message_standard(Blob(b"some data"))},
    # a new-style message in some user-defined format we don't have loaded
    {"format": "other", "content": make_message(b"other", headers=[("_format", b"other")])},
    # valid, old-style messages
    {"format": "voevent",
        "content": make_message(old_style_message(VOEvent.load(get_model_data("voevent"))))},
    {"format": "circular",
        "content": make_message(old_style_message(GCNCircular.load(get_model_data("circular"))))},
    # an old-style message with the old JSON label
    {"format": "json",
        "content": make_message(b'{"format":"blob", "content":{"foo":"bar", "baz":5}}')},
    # messages produced by foreign clients which don't apply our format labels
    {"format": "json", "content": make_message(b'{"foo":"bar", "baz":5}')},
    {"format": "blob", "content": make_message(b'some arbitrary data\0that hop can\'t read')},
])
def test_deserialize(message, message_parameters_dict, caplog):

    message_format = message["format"]
    message_content = message["content"]

    # load parameters from conftest for valid formats
    if message_format in message_parameters_dict:
        message_parameters = message_parameters_dict[message_format]
        model_name = message_parameters["model_name"]
        expected_model = message_parameters["expected_model"]

        # test valid formats
        test_model = io.Deserializer.deserialize(message_content)

        # verify the message is classified properly
        assert isinstance(test_model, expected_model)

    else:  # test an invalid format
        with caplog.at_level(logging.WARNING):
            test_model = io.Deserializer.deserialize(message_content)

            # verify a message blob was produced with warnings
            output = f"Message format {message_format.upper()} " \
                "not recognized; returning a Blob"
            assert isinstance(test_model, Blob)
            assert output in caplog.text


def test_stream_read(circular_msg):
    topic = "gcn"
    group_id = "test-group"
    start_at = io.StartPosition.EARLIEST

    fake_message = make_message(
        GCNCircular.load(get_model_data("circular")).serialize()["content"],
        headers=[("_format", b"circular")])

    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[fake_message])
    stream = io.Stream(start_at=start_at, until_eos=True, auth=False)
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


def test_stream_read_test_channel(circular_msg):
    start_at = io.StartPosition.EARLIEST
    message_data = {"format": "circular", "content": circular_msg}
    fake_message = make_message_standard(circular_msg)

    def test_headers():
        return [('_test', b'true')]

    fake_message.headers = test_headers
    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[fake_message])
    stream = io.Stream(start_at=start_at, until_eos=True, auth=False)
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
        broker_url = "kafka://hostname:port/test-topic"
        messages = 0
        with stream.open(broker_url, "r") as s:
            for msg in s:
                messages += 1
        assert messages == 0

        messages = 0
        with stream.open(broker_url, "r", ignoretest=False) as s:
            for msg in s:
                messages += 1
        assert messages == 1


def test_stream_read_multiple(circular_msg):
    group_id = "test-group"
    topic1 = "gcn1"
    topic2 = "gcn2"
    start_at = io.StartPosition.EARLIEST

    message_data = {"format": "circular", "content": circular_msg}
    topic1_message = make_message_standard(circular_msg, topic=topic1)
    topic2_message = make_message_standard(circular_msg, topic=topic2)
    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[topic1_message, topic2_message])
    stream = io.Stream(start_at=start_at, until_eos=True, auth=False)
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
        broker_url = f"kafka://{group_id}@hostname:port/{topic1},{topic2}"

        messages = Counter()
        with stream.open(broker_url, "r") as s:
            for msg, meta in s.read(metadata=True):
                messages[meta.topic] += 1

        assert messages[topic1] == 1
        assert messages[topic2] == 1


def test_stream_write(circular_msg, circular_text, mock_broker, mock_producer):
    topic = "gcn"
    mock_adc_producer = mock_producer(mock_broker, topic)
    expected_msg = make_message_standard(circular_msg)

    headers = {"some header": b"some value", "another header": b"other value"}
    canonical_headers = [("some header", b"some value"),
                         ("another header", b"other value"),
                         ("_format", b"circular")]
    test_headers = canonical_headers.copy()
    test_headers.insert(2, ('_test', b'true'))
    none_test_headers = [('_test', b"true"), ("_format", b"circular")]

    with patch("hop.io.producer.Producer", autospec=True, return_value=mock_adc_producer):

        broker_url = f"kafka://localhost:port/{topic}"
        auth = Auth("user", "password")
        start_at = io.StartPosition.EARLIEST
        until_eos = True

        stream = io.Stream(start_at=start_at, until_eos=until_eos, auth=auth)

        # verify only 1 topic is allowed in write mode
        with pytest.raises(ValueError):
            stream.open("kafka://localhost:9092/topic1,topic2", "w")

        # verify warning is raised when groupid is set in write mode
        with pytest.warns(UserWarning):
            stream.open("kafka://localhost:9092/topic1", "w", group_id="group")

        mock_broker.reset()
        with stream.open(broker_url, "w") as s:
            s.write(circular_msg, headers)
            assert mock_broker.has_message(topic, expected_msg.value(), canonical_headers)

        mock_broker.reset()
        with stream.open(broker_url, "w") as s:
            s.write(circular_msg, headers, test=True)
            assert mock_broker.has_message(topic, expected_msg.value(), test_headers)

        mock_broker.reset()
        with stream.open(broker_url, "w") as s:
            s.write(circular_msg, headers=None, test=True)
            assert mock_broker.has_message(topic, expected_msg.value(), none_test_headers)

        # repeat, but with a manual close instead of context management
        mock_broker.reset()
        s = stream.open(broker_url, "w")
        s.write(circular_msg, headers)
        s.close()
        assert mock_broker.has_message(topic, expected_msg.value(), canonical_headers)


def test_stream_write_raw(circular_msg, circular_text, mock_broker, mock_producer):
    topic = "gcn"
    mock_adc_producer = mock_producer(mock_broker, topic)
    encoded_msg = io.Producer.pack(circular_msg)
    headers = {"some header": "some value"}
    canonical_headers = list(headers.items())
    with patch("hop.io.producer.Producer", autospec=True, return_value=mock_adc_producer):
        stream = io.Stream(auth=False)

        broker_url = f"kafka://localhost:9092/{topic}"

        mock_broker.reset()
        with stream.open(broker_url, "w") as s:
            s.write_raw(encoded_msg, canonical_headers)
            assert mock_broker.has_message(topic, encoded_msg, canonical_headers)

        # repeat, but with a manual close instead of context management
        mock_broker.reset()
        s = stream.open(broker_url, "w")
        s.write_raw(encoded_msg, canonical_headers)
        s.close()
        assert mock_broker.has_message(topic, encoded_msg, canonical_headers)


def test_stream_auth(auth_config, tmpdir):
    # turning off authentication should give None for the auth property
    s1 = io.Stream(auth=False)
    assert s1.auth is None

    # turning on authentication should give an auth object with the data read from the default file
    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        s2 = io.Stream(auth=True)
        a2 = s2.auth[0]
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

    # verify that URLs with too many hostnames
    with pytest.raises(ValueError) as err:
        stream.open("kafka://example.com,example.net/topic", "r")
        assert "Multiple broker addresses are not supported" in err.value.args

    # verify that complete URLs are accepted
    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("adc.consumer.Consumer.subscribe", MagicMock()) as subscribe:
        stream = io.Stream()
        # opening a valid URL for reading should succeed
        consumer = stream.open("kafka://example.com/topic", "r")
        # an appropriate consumer group name should be derived from the username in the auth
        assert consumer._consumer.conf.group_id.startswith(stream.auth[0].username)
        # the target topic should be subscribed to
        subscribe.assert_called_once_with(["topic"])

        # opening a valid URL for writing should succeed
        producer = stream.open("kafka://example.com/topic", "w")
        producer.write("data")


def test_unpack(circular_msg):
    kafka_msg = make_message_standard(circular_msg)

    unpacked_msg = io.Consumer._unpack(kafka_msg)
    assert unpacked_msg == circular_msg

    unpacked_msg2, metadata = io.Consumer._unpack(kafka_msg, metadata=True)
    assert unpacked_msg2 == unpacked_msg


def test_mark_done(circular_msg):
    start_at = io.StartPosition.EARLIEST
    message_data = {"format": "circular", "content": circular_msg}

    mock_message = make_message_standard(circular_msg)
    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[mock_message])
    stream = io.Stream(until_eos=True, start_at=start_at, auth=False)

    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
        with stream.open("kafka://hostname:port/gcn", "r") as s:
            for msg, metadata in s.read(metadata=True):
                s.mark_done(metadata)
                mock_instance.mark_done.assert_called()


def test_pack(circular_msg, circular_text):
    # message class
    packed_msg, headers = io.Producer.pack(circular_msg)

    # unstructured message
    message = {"hey": "you"}
    packed, headers = io.Producer.pack(message)


@pytest.mark.parametrize("message", [
    {"format": "voevent", "read-mode": "rb"},
    {"format": "circular", "read-mode": "r"},
    {"format": "blob", "read-mode": "rb"},
    {"format": "json", "read-mode": "rb"},
    {"format": "avro", "read-mode": "rb"},
])
def test_pack_unpack_roundtrip(message, message_parameters_dict, caplog):
    format = message["format"]

    # load test data
    shared_datadir = Path("tests/data")
    test_filename = message_parameters_dict[format]["test_file"]
    test_file = shared_datadir / "test_data" / test_filename

    # generate a message
    expected_model = message_parameters_dict[format]["expected_model"]
    with open(test_file, message["read-mode"]) as f:
        orig_message = expected_model.load(f)

    # pack the message
    packed_msg, headers = io.Producer.pack(orig_message)

    # mock a kafka message with value being the packed message
    kafka_msg = make_message(packed_msg, headers)

    # unpack the message
    unpacked_msg = io.Consumer._unpack(kafka_msg)

    # verify based on format
    if format in ("voevent", "circular"):
        assert isinstance(unpacked_msg, expected_model)
        assert unpacked_msg == orig_message
    else:
        assert isinstance(unpacked_msg, type(unpacked_msg))
        assert unpacked_msg == orig_message


def test_pack_unpack_roundtrip_unstructured():
    # objects (of types that json.loads happens to produce, and bytes) should remain unchanged
    # by the process of packing and unpacking
    for orig_message in [
            "a string",
            ["a", "B", "c"],
            {"dict": True, "other_data": [5, 17]},
            b"A\x00B\x04"]:
        packed_msg, headers = io.Producer.pack(orig_message)
        print(f"pasked_msg: {packed_msg}")
        kafka_msg = make_message(packed_msg, headers)
        unpacked_msg = io.Consumer._unpack(kafka_msg)
        assert unpacked_msg.content == orig_message

    # non-serializable objects should raise an error
    with pytest.raises(TypeError):
        # note that we are not trying to pack a string, but the string class itself
        packed_msg, _ = io.Producer.pack(str)


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
    assert metadata.headers == mock_kafka_message.headers()


def test_plugin_loading(caplog):
    # plugins which fail during loading should trigger a warning
    import pluggy
    pm1 = pluggy.PluginManager("hop")

    def raise_ex(ignored):
        raise Exception("Things are bad")
    lse_mock = MagicMock(side_effect=raise_ex)
    pm1.load_setuptools_entrypoints = lse_mock

    builtin_models = ["VOEVENT", "CIRCULAR", "JSON", "AVRO", "BLOB"]

    with patch("pluggy.PluginManager", MagicMock(return_value=pm1)), \
            caplog.at_level(logging.WARNING):
        registered = io._load_deserializer_plugins()
        assert "Could not load external message plugins" in caplog.text
        # but built-in models should still be available
        assert len(registered) == len(builtin_models)
        for expected_model in builtin_models:
            assert expected_model in registered
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
        assert len(registered) == len(builtin_models) + 1
        for expected_model in builtin_models:
            assert expected_model in registered
        assert "FOO" in registered


def test_stream_flush():
    # flush is pretty trivial; it should just call the underlying flush
    with patch("adc.producer.Producer.flush", MagicMock()) as flush:
        broker_url = "kafka://example.com:9092/topic"
        stream = io.Stream(auth=False).open(broker_url, "w")
        stream.flush()
        flush.assert_called()


def test_is_test(circular_msg):
    fake_message = MagicMock()

    def fake_headers_none():
        return None

    def fake_headers_list_test():
        return [('_test', b'true')]

    def fake_headers_list():
        return [('foo', b'bar')]

    fake_message.headers = fake_headers_none
    ret = io.Consumer.is_test(fake_message)
    assert ret is False
    fake_message.headers = fake_headers_list_test
    ret = io.Consumer.is_test(fake_message)
    assert ret is True
    fake_message.headers = fake_headers_list
    ret = io.Consumer.is_test(fake_message)
    assert ret is False


def make_mock_listing_consumer(topics=[]):
    """Create a mock object suitable for replacing confluent_kafka.Consumer
        which has a list_topics method which acts as if a predetermined set of
        topics exist.
    """
    def get_topics(topic=None, timeout=None):
        nonlocal topics
        result = {}
        if topic is None:
            for topic in topics:
                result[topic] = MagicMock()  # don't care much what value is
                result[topic].error = None  # but it should claim no errors
        elif topic in topics:
            result[topic] = MagicMock()
            result[topic].error = None
        # wrap in a fake metadata object
        metadata = MagicMock()
        metadata.topics = result
        return metadata
    consumer = MagicMock()
    consumer.list_topics = get_topics
    return MagicMock(return_value=consumer)


def test_list_topics():
    # when there are some topics, they are all listed
    with patch("confluent_kafka.Consumer", make_mock_listing_consumer(["foo", "bar"])) as Consumer:
        listing = io.list_topics("kafka://example.com", auth=False)
        assert(len(listing) == 2)
        assert("foo" in listing)
        assert("bar" in listing)

        # when auth=False, no auth related properties are set
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert(len(cons_args) == 1)
        assert("sasl.username" not in cons_args[0])
        assert("sasl.password" not in cons_args[0])

    # when there are no topics, the result is empty
    with patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        listing = io.list_topics("kafka://example.com", auth=False)
        assert(len(listing) == 0)

    # result topics should be the intersection of ones which exist and which were requested
    with patch("confluent_kafka.Consumer", make_mock_listing_consumer(["foo", "bar"])) as Consumer:
        listing = io.list_topics("kafka://example.com/bar,baz", auth=False)
        assert(len(listing) == 1)
        assert("bar" in listing)


def test_list_topics_too_many_brokers():
    # If the URL specifies too many brokers, an error is raised
    with patch("confluent_kafka.Consumer", make_mock_listing_consumer([])):
        with pytest.raises(ValueError):
            listing = io.list_topics("kafka://example.com,example.net", auth=False)


def test_list_topics_auth(auth_config, tmpdir):
    # when auth=True, auth related properties should be set
    with temp_config(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        listing = io.list_topics("kafka://example.com", auth=True)

        # when auth=True, auth related properties should be set
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert(len(cons_args) == 1)
        assert("sasl.username" in cons_args[0])
        assert("sasl.password" in cons_args[0])
        assert(cons_args[0]["sasl.username"] == "username")
        assert(cons_args[0]["sasl.password"] == "password")

    # when an Auth object is set, it should take precedence over automatic lookup
    with temp_config(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        auth = Auth("someone_else", "other_password")
        listing = io.list_topics("kafka://example.com", auth=auth)

        # when auth=Auth(...), auth related properties should be set correctly
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert(len(cons_args) == 1)
        assert("sasl.username" in cons_args[0])
        assert("sasl.password" in cons_args[0])
        assert(cons_args[0]["sasl.username"] == auth.username)
        assert(cons_args[0]["sasl.password"] == auth.password)

    # when an Auth object is set, it should take precedence over automatic lookup,
    # even with userinfo in the URL
    with temp_config(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        auth = Auth("someone_else", "other_password")
        listing = io.list_topics("kafka://username@example.com", auth=auth)

        # when auth=Auth(...), auth related properties should be set correctly
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert(len(cons_args) == 1)
        assert("sasl.username" in cons_args[0])
        assert("sasl.password" in cons_args[0])
        assert(cons_args[0]["sasl.username"] == auth.username)
        assert(cons_args[0]["sasl.password"] == auth.password)

    # when auth=True and userinfo, the correct credential should be automatically
    # looked up
    multi_cred = """
        auth = [{
            username="user1",
            password="pass1"
            },
            {
            username="user2",
            password="pass2"
            }]
        """
    with temp_config(tmpdir, multi_cred) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        listing = io.list_topics("kafka://user2@example.com", auth=True)

        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert(len(cons_args) == 1)
        assert("sasl.username" in cons_args[0])
        assert("sasl.password" in cons_args[0])
        assert(cons_args[0]["sasl.username"] == "user2")
        assert(cons_args[0]["sasl.password"] == "pass2")


def test_list_topics_timeout():
    cred = Auth("user", "pass")
    for timeout in [0.5, 1.0, 1.5]:
        start = time.time()
        with pytest.raises(confluent_kafka.KafkaException) as err:
            io.list_topics("kafka://not-a-valid-broker.scimma.org", auth=cred, timeout=timeout)
        stop = time.time()
        assert(abs((stop - start) - timeout) < 0.1)
