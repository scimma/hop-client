from collections import Counter
from collections.abc import Collection, Mapping
from dataclasses import asdict, fields
from datetime import timedelta
import json
import logging
from pathlib import Path
import time
from unittest.mock import patch, MagicMock
from uuid import uuid4
import bson

import pytest

from hop.auth import Auth, AmbiguousCredentialError
from hop import io
from hop.models import (AvroBlob, Blob, ExternalMessage, GCNCircular, GCNTextNotice, JSONBlob,
                        VOEvent)
from adc.errors import KafkaException
import confluent_kafka

from conftest import (temp_environ, temp_auth, message_parameters_dict_data, PhonyConnection,
                      mock_pool_manager)

logger = logging.getLogger("hop")


def content_mock(message_model):
    """Mock content to pass during the message_model creation since MagicMock()
    is unable to mock __init__ of the model dataclass in tests.
    """
    content = {}
    for field in fields(message_model):
        content.update({field.name: "test"})
    return content


def make_message(content, headers=[], topic="test-topic", partition=0, offset=0, key="test-key"):
    message = MagicMock()
    message.value.return_value = content
    message.headers.return_value = headers
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    message.timestamp.return_value = (0, 1234567890)
    message.key.return_value = key
    return message


def make_message_standard(message, **kwags):
    raw = message.serialize()
    return make_message(raw["content"],
                        headers=[("_format", raw["format"].encode("utf-8"))], **kwags)


# only applicable to old message models which are JSON-compatible
def old_style_message(format_name, message):
    raw = {"format": format_name, "content": message}
    return make_message(json.dumps(raw).encode("utf-8"))


def get_model_data(model_name, key_name="model_text"):
    return message_parameters_dict_data[model_name][key_name]


# test the deserializer for each message format
@pytest.mark.parametrize("message", [
    # properly formatted, new-style messages
    {"format": "voevent",
        "content": make_message_standard(VOEvent.load(get_model_data("voevent")))},
    {"format": "gcntextnotice",
        "content": make_message_standard(GCNTextNotice.load(get_model_data("gcntextnotice")))},
    {"format": "circular",
        "content": make_message_standard(GCNCircular.load(get_model_data("circular")))},
    {"format": "json", "content": make_message_standard(JSONBlob.load(get_model_data("json")))},
    {"format": "avro", "content": make_message_standard(AvroBlob.load(get_model_data("avro")))},
    {"format": "blob", "content": make_message_standard(Blob(b"some data"))},
    # a new-style message in some user-defined format we don't have loaded
    {"format": "other", "content": make_message(b"other", headers=[("_format", b"other")])},
    # valid, old-style messages
    {"format": "voevent-encoded-as-json",
        "content": make_message(get_model_data("voevent-encoded-as-json").encode("utf-8"),
                                headers=[("_format", b"voevent")])},
    {"format": "voevent-encoded-as-json",
        "content": old_style_message("voevent",
                                     json.loads(get_model_data("voevent-encoded-as-json")))},
    {"format": "circular",
        "content": old_style_message("circular",
                                     asdict(GCNCircular.load(get_model_data("circular"))))},
    # an old-style message with the old JSON label
    {"format": "json",
        "content": make_message(b'{"format":"blob", "content":{"foo":"bar", "baz":5}}')},
    # messages produced by foreign clients which don't apply our format labels
    {"format": "json", "content": make_message(b'{"foo":"bar", "baz":5}')},
    {"format": "blob",
        "content": make_message(b'some arbitrary data\xDE\xAD\xBE\xEFthat hop can\'t read')},
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


def test_get_offload_server():
    # everything working as it should
    fake_message = make_message(b'{"LargeMessageUploadEndpoint":"https://example.com"}')
    mock_consumer = MagicMock(stream=MagicMock(return_value=[fake_message]))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint == "https://example.com"
        mock_consumer.close.assert_called_once()

    # topic does not exist
    mock_consumer = MagicMock(subscribe=MagicMock(side_effect=ValueError("topic does not exist")))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint is None
        mock_consumer.close.assert_called_once()

    # no message on topic
    mock_consumer = MagicMock(stream=MagicMock(return_value=[]))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint is None
        mock_consumer.close.assert_called_once()

    # message not UTF-8
    fake_message = make_message(b"\xF0")
    mock_consumer = MagicMock(stream=MagicMock(return_value=[fake_message]))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint is None
        mock_consumer.close.assert_called_once()

    # message UTF-8 but not JSON
    fake_message = make_message(b"\t\vThis is not JSON\v\t")
    mock_consumer = MagicMock(stream=MagicMock(return_value=[fake_message]))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint is None
        mock_consumer.close.assert_called_once()

    # endpoint key missing
    fake_message = make_message(b'{"foo":"bar", "baz":"quux"}')
    mock_consumer = MagicMock(stream=MagicMock(return_value=[fake_message]))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint is None
        mock_consumer.close.assert_called_once()

    # endpoint key wrong type
    fake_message = make_message(b'{"LargeMessageUploadEndpoint":7}')
    mock_consumer = MagicMock(stream=MagicMock(return_value=[fake_message]))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint is None
        mock_consumer.close.assert_called_once()

    # unencrypted endpoint
    fake_message = make_message(b'{"LargeMessageUploadEndpoint":"http://example.com"}')
    mock_consumer = MagicMock(stream=MagicMock(return_value=[fake_message]))
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)):
        with pytest.warns(UserWarning):
            endpoint = io._get_offload_server("my-broker.net", auth=None)
        assert endpoint == "http://example.com"
        mock_consumer.close.assert_called_once()


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
                assert isinstance(msg, GCNCircular), \
                    "Message should be deserialized to the expected model type"
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


def test_stream_read_raw(circular_msg):
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
            for msg in s.read_raw():
                print(msg)
                # Since the message should be raw, we should be able to
                # deserialize it using the correct model class.
                des = GCNCircular.deserialize(msg)
                assert des
                messages += 1
        assert messages == 1

        messages = 0
        with stream.open(broker_url2, "r") as s:
            for result in s.read_raw(metadata=True):
                assert len(result) == 2, "Result should be a 2-tuple"
                msg = result[0]
                metadata = result[1]
                des = GCNCircular.deserialize(msg)
                assert des
                messages += 1
        assert messages == 1


def test_stream_read_test_raw(circular_msg):
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
            for msg in s.read_raw():
                messages += 1
        assert messages == 0

        messages = 0
        with stream.open(broker_url, "r", ignoretest=False) as s:
            for msg in s.read_raw():
                messages += 1
        assert messages == 1


def test_http_error_to_kafka():
    expected_errors = {
        400: confluent_kafka.KafkaError.INVALID_REQUEST,
        401: confluent_kafka.KafkaError.SASL_AUTHENTICATION_FAILED,
        403: confluent_kafka.KafkaError.TOPIC_AUTHORIZATION_FAILED,
        404: confluent_kafka.KafkaError.RESOURCE_NOT_FOUND,
        413: confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE,
        450: confluent_kafka.KafkaError.INVALID_REQUEST,
        500: confluent_kafka.KafkaError.UNKNOWN,
        502: confluent_kafka.KafkaError.NETWORK_EXCEPTION,
        505: confluent_kafka.KafkaError.UNSUPPORTED_VERSION,
    }
    for http_code in expected_errors:
        msg = f"error {http_code}"
        kerr = io._http_error_to_kafka(http_code, msg)
        assert kerr.code() == expected_errors[http_code], f"for HTTP error code {http_code}"
        assert kerr.str() == msg


class FakeRead:
    def __init__(self, data):
        self.data = data
        self.counter = 0

    def __call__(self, *args):
        self.counter += 1
        if self.counter == 1:
            return self.data
        return None


def test_consumer_fetch_external_no_auth():
    orig_data = b"datadatadata"
    orig_id = b'|\xdc\xfa\xa0v\x94Iu\x8c\xcd\xeaJ\x7f6\xf5r'
    orig_timestamp = 172000
    payload = bson.dumps({"message": orig_data,
                          "metadata": {"headers": [("_id", orig_id)],
                                       "timestamp": orig_timestamp}
                          })

    # success, no metadata requested
    url = "https://example.com/msg/1234"
    reference_message = make_message_standard(ExternalMessage(url=url))
    response = MagicMock(status=200, read=FakeRead(payload))
    del response.stream
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic")
        output = c._fetch_external(url, False, reference_message)
        assert isinstance(output, Blob)
        assert output.content == orig_data

    # success, with metadata requested
    response = MagicMock(status=200, read=FakeRead(payload))
    del response.stream
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic")
        output = c._fetch_external(url, True, reference_message)
        assert len(output) == 2
        assert isinstance(output[0], Blob)
        assert output[0].content == orig_data
        assert isinstance(output[1], io.Metadata)
        assert output[1].topic == "test-topic"
        assert output[1].timestamp == orig_timestamp
        assert ("_id", orig_id) in output[1].headers

    # HTTP failure, no error callback
    response = MagicMock(status=500, read=FakeRead(b"Error!"))
    del response.stream
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic")
        m = c._fetch_external(url, True, reference_message)
        assert m.error() is not None
        assert m.error().code() == confluent_kafka.KafkaError.UNKNOWN

    # HTTP failure, with error callback
    response = MagicMock(status=500, read=FakeRead(b"Error!"))
    del response.stream
    ecallback = MagicMock()
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic", error_callback=ecallback)
        m = c._fetch_external(url, True, reference_message)
        assert m.error() is not None
        assert m.error().code() == confluent_kafka.KafkaError.UNKNOWN
        ecallback.assert_called_once()
        assert len(ecallback.call_args.args) == 1
        assert ecallback.call_args.args[0].code() == confluent_kafka.KafkaError.UNKNOWN

    # Data not BSON, no error callback
    response = MagicMock(status=200, read=FakeRead(b"Not valid BSON"))
    del response.stream
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic")
        m = c._fetch_external(url, True, reference_message)
        assert m.error() is not None
        assert m.error().code() == confluent_kafka.KafkaError._VALUE_DESERIALIZATION

    # Data not BSON, with error callback
    response = MagicMock(status=200, read=FakeRead(b"Not valid BSON"))
    del response.stream
    ecallback = MagicMock()
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic", error_callback=ecallback)
        m = c._fetch_external(url, True, reference_message)
        assert m.error() is not None
        assert m.error().code() == confluent_kafka.KafkaError._VALUE_DESERIALIZATION
        ecallback.assert_called_once()
        assert len(ecallback.call_args.args) == 1
        assert ecallback.call_args.args[0].code() \
            == confluent_kafka.KafkaError._VALUE_DESERIALIZATION

    # Valid BSON but malformed message record, no error callback
    response = MagicMock(status=200, read=FakeRead(bson.dumps({1: 2, 3: 4})))
    del response.stream
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic")
        m = c._fetch_external(url, True, reference_message)
        assert m.error() is not None
        assert m.error().code() == confluent_kafka.KafkaError._VALUE_DESERIALIZATION

    # Valid BSON but malformed message record, with error callback
    response = MagicMock(status=200, read=FakeRead(bson.dumps({1: 2, 3: 4})))
    del response.stream
    ecallback = MagicMock()
    with patch("requests.adapters.PoolManager", mock_pool_manager(PhonyConnection([response]))), \
            patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic", error_callback=ecallback)
        m = c._fetch_external(url, True, reference_message)
        assert m.error() is not None
        assert m.error().code() == confluent_kafka.KafkaError._VALUE_DESERIALIZATION
        ecallback.assert_called_once()
        assert len(ecallback.call_args.args) == 1
        assert ecallback.call_args.args[0].code() \
            == confluent_kafka.KafkaError._VALUE_DESERIALIZATION


def test_consumer_fetch_external_with_auth():
    orig_data = b"datadatadata"
    orig_id = b'|\xdc\xfa\xa0v\x94Iu\x8c\xcd\xeaJ\x7f6\xf5r'
    orig_timestamp = 172000
    payload = bson.dumps({"message": orig_data,
                          "metadata": {"headers": [("_id", orig_id)],
                                       "timestamp": orig_timestamp}
                          })
    auth = Auth("user", "pencil")

    # use auth, endpoint not cached, matches trusted
    url = "https://example.com/msg/1234"
    reference_message = make_message_standard(ExternalMessage(url=url))
    response = MagicMock(status=200, read=FakeRead(payload))
    del response.stream
    conn = PhonyConnection([response])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("hop.io.consumer.Consumer", MagicMock()), \
            patch("hop.io._get_offload_server",
                  MagicMock(return_value="https://example.com")):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic", auth=auth)
        output = c._fetch_external(url, False, reference_message)
        assert isinstance(output, Blob)
        assert output.content == orig_data
        assert len(conn.requests) > 0
        assert "Authorization" in conn.requests[0]["headers"]

    # use auth, endpoint cached
    response = MagicMock(status=200, read=FakeRead(payload))
    del response.stream
    conn = PhonyConnection([response])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("hop.io.consumer.Consumer", MagicMock()), \
            patch("hop.io._get_offload_server", MagicMock()) as go:
        # Re-use existing c
        output = c._fetch_external(url, False, reference_message)
        assert isinstance(output, Blob)
        assert output.content == orig_data
        assert len(conn.requests) > 0
        assert "Authorization" in conn.requests[0]["headers"]
        go.assert_not_called()

    # use auth, endpoint not cached, does not match trusted
    url = "https://example.com/msg/1234"
    reference_message = make_message_standard(ExternalMessage(url=url))
    response = MagicMock(status=200, read=FakeRead(payload))
    del response.stream
    conn = PhonyConnection([response])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("hop.io.consumer.Consumer", MagicMock()), \
            patch("hop.io._get_offload_server",
                  MagicMock(return_value="https://example.net")):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic", auth=auth)
        output = c._fetch_external(url, False, reference_message)
        assert isinstance(output, Blob)
        assert output.content == orig_data
        assert len(conn.requests) > 0
        assert "Authorization" not in conn.requests[0]["headers"]


def test_consumer_fetch_external_disabled():
    url = "https://example.com/msg/1234"
    reference_message = make_message_standard(ExternalMessage(url=url))

    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[reference_message])

    def should_not_be_called(*args):
        assert False, "Consumer._fetch_external should not be called in this context"

    # it should be possible to epxlicitly disable automatic fetching of external messages
    with patch("hop.io.Consumer._fetch_external", should_not_be_called), \
            patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
        c = io.Consumer("cID", ["example.com:9092"], "test-topic", fetch_external=False)
        for output in c.read(False):
            print(output)
            assert isinstance(output, ExternalMessage)
            assert output.url == url


def test_consumer_check_bson_message_structure():
    url = "https://example.com/some_message"
    valid = {"message": b"datadatadata",
             "metadata": {"headers": [("_id", b'|\xdc\xfa\xa0v\x94Iu\x8c\xcd\xeaJ\x7f6\xf5r')],
                          "timestamp": 172000}
             }
    io.Consumer._check_bson_message_structure(valid, url)

    # wrong top-level type
    err = io.Consumer._check_bson_message_structure("a_string", url)
    assert err == f"Message data from {url} is not a mapping"

    no_message = {"metadata": {"headers": [("_id", b'|\xdc\xfa\xa0v\x94Iu\x8c\xcd\xeaJ\x7f6\xf5r')],
                               "timestamp": 172000}
                  }
    err = io.Consumer._check_bson_message_structure(no_message, url)
    assert err == f"Original message data not present in message data from {url}"

    wrong_message_type = {"message": 22,
                          "metadata": {"headers": [("foo", b"bar")],
                                       "timestamp": 172000}
                          }
    err = io.Consumer._check_bson_message_structure(wrong_message_type, url)
    assert err == f"Original message data not present in message data from {url}"

    no_metadata = {"message": b"datadatadata"}
    err = io.Consumer._check_bson_message_structure(no_metadata, url)
    assert err == f"Metadata not present or malformed in message data from {url}"

    wrong_metadata_type = {"message": b"datadatadata", "metadata": 3.14}
    err = io.Consumer._check_bson_message_structure(wrong_metadata_type, url)
    assert err == f"Metadata not present or malformed in message data from {url}"

    no_headers = {"message": b"datadatadata", "metadata": {"timestamp": 172000}}
    err = io.Consumer._check_bson_message_structure(no_headers, url)
    assert err == "Original message headers not present or malformed in message data" \
                  f" from {url}"

    wrong_headers_type = {"message": b"datadatadata",
                          "metadata": {"headers": 99,
                                       "timestamp": 172000}
                          }
    err = io.Consumer._check_bson_message_structure(wrong_headers_type, url)
    assert err == "Original message headers not present or malformed in message data" \
                  f" from {url}"

    wrong_header_type = {"message": b"datadatadata",
                         "metadata": {"headers": ["foo"],
                                      "timestamp": 172000}
                         }
    err = io.Consumer._check_bson_message_structure(wrong_header_type, url)
    assert err == f"Malformed original message header in message data from {url}"

    wrong_header_key_type = {"message": b"datadatadata",
                             "metadata": {"headers": [(9, b"bar")],
                                          "timestamp": 172000}
                             }
    err = io.Consumer._check_bson_message_structure(wrong_header_key_type, url)
    assert err == f"Malformed original message header in message data from {url}"

    wrong_header_value_type = {"message": b"datadatadata",
                               "metadata": {"headers": [("foo", 11.5)],
                                            "timestamp": 172000}
                               }
    err = io.Consumer._check_bson_message_structure(wrong_header_value_type, url)
    assert err == f"Malformed original message header in message data from {url}"

    no_timestamp = {"message": b"datadatadata",
                    "metadata": {"headers": [("foo", b"bar")]}
                    }
    err = io.Consumer._check_bson_message_structure(no_timestamp, url)
    assert err == f"Timestamp not present or malformed in message data from {url}"

    wrong_timestamp_type = {"message": b"datadatadata",
                            "metadata": {"headers": [("foo", b"bar")],
                                         "timestamp": "lunchtime"}
                            }
    err = io.Consumer._check_bson_message_structure(wrong_timestamp_type, url)
    assert err == f"Timestamp not present or malformed in message data from {url}"


def test_stream_stop(circular_msg):
    start_at = io.StartPosition.EARLIEST
    fake_message = make_message_standard(circular_msg)

    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[fake_message])
    mock_instance.stop = MagicMock()
    stream = io.Stream(start_at=start_at, until_eos=True, auth=False)
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
        broker_url = "kafka://hostname:port/gcn"

        with stream.open(broker_url, "r") as s:
            for msg in s:
                assert isinstance(msg, GCNCircular), \
                    "Message should be deserialized to the expected model type"
            s.stop()
        mock_instance.stop.assert_called_once()


def test_stream_write(circular_msg, circular_text, mock_broker, mock_producer, mock_admin_client):
    topic = "gcn"
    expected_msg = make_message_standard(circular_msg)
    fixed_uuid = uuid4()

    auth = Auth("user", "password")
    headers = {"some header": b"some value", "another header": b"other value"}
    canonical_headers = [("some header", b"some value"),
                         ("another header", b"other value"),
                         ("_id", fixed_uuid.bytes),
                         ("_sender", auth.username.encode("utf-8")),
                         ("_format", b"circular")]
    test_headers = canonical_headers.copy()
    test_headers.insert(4, ('_test', b'true'))
    none_test_headers = [("_id", fixed_uuid.bytes), ("_sender", auth.username.encode("utf-8")),
                         ('_test', b"true"), ("_format", b"circular")]
    test_key = "testkey"

    mb = mock_broker
    with patch("hop.io.adc_producer.Producer", side_effect=lambda c: mock_producer(mb, c.topic)), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mb)), \
            patch("hop.io.uuid.uuid4", MagicMock(return_value=fixed_uuid)):

        broker_url = f"kafka://localhost:port/{topic}"
        start_at = io.StartPosition.EARLIEST
        until_eos = True

        stream = io.Stream(start_at=start_at, until_eos=until_eos, auth=auth)

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

        mock_broker.reset()
        with stream.open(broker_url, "w") as s:
            s.write(circular_msg, headers, key=test_key)
            assert mock_broker.has_message(topic, expected_msg.value(), canonical_headers, test_key)

        # repeat, but with a manual close instead of context management
        mock_broker.reset()
        s = stream.open(broker_url, "w")
        s.write(circular_msg, headers)
        s.close()
        assert mock_broker.has_message(topic, expected_msg.value(), canonical_headers)

        mock_broker.reset()
        # more than one topics should now be allowed in write mode
        with stream.open("kafka://localhost:9092/topic1,topic2", "w") as s:
            with pytest.raises(Exception):
                # however, a topic must be specified when calling write with multiple topics
                # specified on construction
                s.write(circular_msg, headers)

            # selecting a topic explicitly when calling write should work
            s.write(circular_msg, headers, topic="topic1")
            assert mock_broker.has_message("topic1", expected_msg.value(), canonical_headers)
            s.write(circular_msg, headers, topic="topic2")
            assert mock_broker.has_message("topic2", expected_msg.value(), canonical_headers)

        mock_broker.reset()
        # no topic can also be specified in write mode
        with stream.open("kafka://localhost:9092/", "w") as s:
            with pytest.raises(Exception):
                # however, a topic must be specified when calling write
                s.write(circular_msg, headers)

            s.write(circular_msg, headers, topic="topic1")
            assert mock_broker.has_message("topic1", expected_msg.value(), canonical_headers)
            s.write(circular_msg, headers, topic="topic2")
            assert mock_broker.has_message("topic2", expected_msg.value(), canonical_headers)


def test_stream_write_raw(circular_msg, circular_text, mock_broker, mock_producer,
                          mock_admin_client):
    topic = "gcn"
    encoded_msg = io.Producer.pack(circular_msg)
    headers = {"some header": "some value"}
    canonical_headers = list(headers.items())
    mb = mock_broker
    with patch("hop.io.adc_producer.Producer", side_effect=lambda c: mock_producer(mb, c.topic)), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mock_broker)):
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

        with stream.open("kafka://localhost:9092/topic1,topic2", "w") as s:
            with pytest.raises(Exception):
                s.write_raw(encoded_msg, canonical_headers)
            s.write_raw(encoded_msg, canonical_headers, topic="topic1")
            assert mock_broker.has_message("topic1", encoded_msg, canonical_headers)
            s.write_raw(encoded_msg, canonical_headers, topic="topic2")
            assert mock_broker.has_message("topic2", encoded_msg, canonical_headers)


def test_stream_write_message_too_large_no_offload(mock_broker, mock_producer, mock_admin_client):
    mb = mock_broker
    topic = "topic"
    broker_address = "localhost:9092"
    broker_url = f"kafka://{broker_address}/{topic}"
    mb.set_topic_max_message_size(topic, 32)
    encoded_msg = io.Producer.pack(Blob(content=b'm' * 64))
    mock_consumer = MagicMock(subscribe=MagicMock(side_effect=ValueError("topic does not exist")))

    # With no offload endpoint supplied by the broker, attempting to send an over-large message
    # should fail with the usual error
    def producer_factory(c):
        return mock_producer(mb, c.topic)
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_consumer)), \
            patch("hop.io.adc_producer.Producer", side_effect=producer_factory), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mb)):
        stream = io.Stream(auth=Auth("user", "pencil", method="SCRAM-SHA-1"))
        with stream.open(broker_url, "w") as s:
            with pytest.raises(KafkaException):
                s.write_raw(encoded_msg, [])
            assert not mock_broker.has_message(topic, encoded_msg, [])
            assert s.offload_url is None

            # with no delivery callback, errors currently vanish into the void
            s.write_raw(encoded_msg, [], delivery_callback=None)
            assert not mock_broker.has_message(topic, encoded_msg, [])


def test_write_with_large_mesage_offload(mock_broker, mock_producer, mock_admin_client):
    mb = mock_broker
    topic = "topic"
    broker_address = "localhost:9092"
    broker_url = f"kafka://{broker_address}/{topic}"
    offload_url = "http://localhost:8000"
    mb.set_topic_max_message_size(topic, 32)
    encoded_msg = io.Producer.pack(Blob(content=b'm' * 64))
    mock_get_offload = MagicMock(return_value=offload_url)
    mock_offload_args = []

    def make_mock_offload(placeholder_message, placeholder_headers=[], err=None):
        def mock_offload(obj, message, headers, topic, key=None):
            mock_offload_args.append((message, headers, topic, key))
            return (placeholder_message, placeholder_headers, err)
        return mock_offload

    def producer_factory(c):
        return mock_producer(mb, c.topic)

    mo = make_mock_offload(b'{"url":"dummy"}')
    with patch("hop.io._get_offload_server", mock_get_offload), \
            patch("hop.io.adc_producer.Producer", side_effect=producer_factory), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mb)), \
            patch("hop.io.Producer._offload_message", mo):
        stream = io.Stream(auth=Auth("user", "pencil", method="SCRAM-SHA-1"))
        with stream.open(broker_url, "w") as s:
            s.write_raw(*encoded_msg)
            mock_get_offload.assert_called_once()
            assert len(mock_offload_args) == 1, "_offload_message should be called once"
            assert mock_offload_args[0][0] == encoded_msg[0]
            assert mock_offload_args[0][1] == encoded_msg[1]
            assert mock_offload_args[0][2] == topic
            assert not mock_broker.has_message(topic, encoded_msg, [])
            assert hasattr(s, "offload_url")
            assert s.offload_url == offload_url

            # a second write should not cause the offload endpoint to be looked up again
            s.write_raw(*encoded_msg)
            mock_get_offload.assert_called_once()

    test_error = confluent_kafka.KafkaError(confluent_kafka.KafkaError._BAD_MSG, "Test Error",
                                            False, False, False)
    mo = make_mock_offload(None, None, test_error)
    with patch("hop.io._get_offload_server", mock_get_offload), \
            patch("hop.io.adc_producer.Producer", side_effect=producer_factory), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mb)), \
            patch("hop.io.Producer._offload_message", mo):
        stream = io.Stream(auth=Auth("user", "pencil", method="SCRAM-SHA-1"))
        with stream.open(broker_url, "w") as s:
            with pytest.raises(KafkaException) as ex:
                s.write_raw(*encoded_msg)
            assert ex.value.error == test_error
            assert not mock_broker.has_message(topic, encoded_msg, [])

            # with no delivery callback, errors currently vanish into the void
            s.write_raw(*encoded_msg, delivery_callback=None)
            assert not mock_broker.has_message(topic, encoded_msg, [])


def test_write_offload_disabled(mock_broker, mock_producer, mock_admin_client):
    mb = mock_broker
    topic = "topic"
    broker_address = "localhost:9092"
    broker_url = f"kafka://{broker_address}/{topic}"
    offload_url = "http://localhost:8000"
    mb.set_topic_max_message_size(topic, 32)
    encoded_msg = io.Producer.pack(Blob(content=b'm' * 64))
    mock_get_offload = MagicMock(return_value=offload_url)

    def producer_factory(c):
        return mock_producer(mb, c.topic)

    # it should be possible to explicitly disable offloading
    def should_not_be_called(*args):
        assert False, "_offload_message should not becalled in this context"
    with patch("hop.io._get_offload_server", mock_get_offload), \
            patch("hop.io.adc_producer.Producer", side_effect=producer_factory), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mb)), \
            patch("hop.io.Producer._offload_message", should_not_be_called):
        stream = io.Stream(auth=Auth("user", "pencil", method="SCRAM-SHA-1"))
        with stream.open(broker_url, "w", automatic_offload=False) as s:
            with pytest.raises(KafkaException) as ex:
                s.write_raw(*encoded_msg)
            mock_get_offload.assert_not_called()
            assert "Unable to send message" in str(ex) or "MSG_SIZE_TOO_LARGE" in str(ex)


def check_outgoing_bson_message(data):
    if not isinstance(data, Mapping):
        return "Data is not a mapping"
    if "message" not in data or not isinstance(data["message"], bytes):
        return "Message data not present"
    if "headers" not in data or \
            not isinstance(data["headers"], Collection):
        return "Message headers not present or malformed"
    for header in data["headers"]:
        if not isinstance(header, Collection) or len(header) != 2 \
                or not isinstance(header[0], str) or not isinstance(header[1], bytes):
            return "Malformed message header"
    if "key" in data:
        if not isinstance(data["key"], bytes) and not isinstance(data["key"], str):
            return "Malformed message key"

    return None


def test_offload_message():
    fixed_uuid = uuid4()

    test_nonce = "fyko+d2lbbFgONRv9qkxdawL"
    test_sid = "AAAABBBBCCCCDDDD"
    test_server_first_data = "cj1meWtvK2QybGJiRmdPTlJ2OXFreGRhd0wzcmZjTkhZSlkxWlZ2V1ZzN" \
                             "2oscz1RU1hDUitRNnNlazhiZjkyLGk9NDA5Ng=="
    test_server_first = f"SCRAM-SHA-1 sid={test_sid},data={test_server_first_data}"
    resp1 = MagicMock(status=401, headers={"WWW-Authenticate": test_server_first},
                      read=FakeRead(b""))
    test_server_final = f"sid={test_sid},data=dj1ybUY5cHFWOFM3c3VBb1pXamE0ZEpSa0ZzS1E9"
    resp2 = MagicMock(status=200, headers={"Authentication-Info": test_server_final},
                      read=FakeRead(b""))

    # offloading a message with everything in order should work
    conn = PhonyConnection([resp1, resp2])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        prod = io.Producer("example.com:9092", [], None)
        prod.offload_url = "http://example.com/8000"
        prod.auth = Auth("user", "pencil", method="SCRAM-SHA-1")
        result = prod._offload_message(b"data", [("_id", fixed_uuid.bytes)], "topic")
        assert result[2] is None
        assert len(conn.requests) > 0
        # the body that was sent should be valid BSON
        payload = bson.loads(conn.requests[-1]["body"])
        # we happen to have a tool, covered by other tests for verifying the structure of
        # offloaded message payloads
        print(payload)
        assert check_outgoing_bson_message(payload) is None
        rmessage = result[0]
        assert isinstance(rmessage, bytes)
        decoded_rmessage = ExternalMessage.load(rmessage)
        assert decoded_rmessage.url.startswith(prod.offload_url)
        assert decoded_rmessage.url.endswith(str(fixed_uuid))
        rheaders = result[1]
        assert isinstance(rheaders, list)
        id_header = None
        for header in rheaders:
            assert header[0] != "_test", "If the original message was not marked as a test, " \
                                         "the offloaded version also should not be"
            if header[0] == "_id":
                id_header = header[1]
        assert id_header is not None
        assert id_header != fixed_uuid.hex, "Reference message should hace a distinct ID"

    # offloading a message with the test flag set should work and preserve the flag
    conn = PhonyConnection([resp1, resp2])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        prod = io.Producer("example.com:9092", [], None)
        prod.offload_url = "http://example.com/8000"
        prod.auth = Auth("user", "pencil", method="SCRAM-SHA-1")
        result = prod._offload_message(b"data", [("_id", fixed_uuid.bytes), ("_test", b"true")],
                                       "topic")
        assert result[2] is None
        rmessage = result[0]
        assert isinstance(rmessage, bytes)
        decoded_rmessage = ExternalMessage.load(rmessage)
        assert decoded_rmessage.url.startswith(prod.offload_url)
        assert decoded_rmessage.url.endswith(str(fixed_uuid))
        rheaders = result[1]
        assert isinstance(rheaders, list)
        id_header = None
        test_header = None
        for header in rheaders:
            if header[0] == "_id":
                id_header = header[1]
            elif header[0] == "_test":
                test_header = header[1]
        assert id_header is not None
        assert id_header != fixed_uuid.hex, "Reference message should hace a distinct ID"
        assert test_header is not None
        assert test_header == b"true", "If the original message was marked as a test, " \
                                       "the offloaded version also should be"

    # offloading a message with a key should also pass that through
    conn = PhonyConnection([resp1, resp2])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        prod = io.Producer("example.com:9092", [], None)
        prod.offload_url = "http://example.com/8000"
        prod.auth = Auth("user", "pencil", method="SCRAM-SHA-1")
        result = prod._offload_message(b"data", [("_id", fixed_uuid.bytes)], "topic", key="a_key")
        assert result[2] is None
        assert len(conn.requests) > 0
        payload = bson.loads(conn.requests[-1]["body"])
        assert check_outgoing_bson_message(payload) is None
        assert payload["key"] == "a_key"

    # malformed _id headers should be ignored
    conn = PhonyConnection([resp1, resp2])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        prod = io.Producer("example.com:9092", [], None)
        prod.offload_url = "http://example.com/8000"
        prod.auth = Auth("user", "pencil", method="SCRAM-SHA-1")
        result = prod._offload_message(b"data", [("_id", b"not-valid"), ("_id", fixed_uuid.bytes)],
                                       "topic")
        assert result[2] is None
        rmessage = result[0]
        assert isinstance(rmessage, bytes)
        decoded_rmessage = ExternalMessage.load(rmessage)
        assert decoded_rmessage.url.startswith(prod.offload_url)
        assert decoded_rmessage.url.endswith(str(fixed_uuid))
        rheaders = result[1]
        assert isinstance(rheaders, list)
        id_header = None
        test_header = None
        for header in rheaders:
            if header[0] == "_id":
                id_header = header[1]
        assert id_header is not None
        assert id_header != fixed_uuid.hex, "Reference message should hace a distinct ID"

    # lack of an _id header is an error
    prod = io.Producer("example.com:9092", [], None)
    prod.offload_url = "http://example.com/8000"
    result = prod._offload_message(b"data", [], "topic")
    assert result[0] is None
    assert result[1] is None
    assert result[2] is not None
    assert result[2].code() == confluent_kafka.KafkaError._BAD_MSG
    assert "Message has no ID ('_id' header)" in result[2].str()

    # failing to offload a message, e.g. because of lack of write permissions,
    # should produce an error
    resp2_not_permitted = MagicMock(
        status=403, headers={"Authentication-Info": test_server_final},
        read=FakeRead(b"Operation not permitted"))
    conn = PhonyConnection([resp1, resp2_not_permitted])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        prod = io.Producer("example.com:9092", [], None)
        prod.offload_url = "http://example.com/8000"
        prod.auth = Auth("user", "pencil", method="SCRAM-SHA-1")
        result = prod._offload_message(b"data", [("_id", fixed_uuid.bytes)], "topic")
        assert result[0] is None
        assert result[1] is None
        assert result[2] is not None
        assert result[2].code() == confluent_kafka.KafkaError.TOPIC_AUTHORIZATION_FAILED
        assert "Failed to send large message to offload server" in result[2].str()

    # if the offload endpoint does not handshake correctly, an error should be produced
    print("Begin incorrect SCRAM handshake")
    server_first_no_sid = MagicMock(
        status=401, headers={"WWW-Authenticate": f"SCRAM-SHA-1 data={test_server_first_data}"},
        read=FakeRead(b""))
    conn = PhonyConnection([server_first_no_sid])
    with patch("requests.adapters.PoolManager", mock_pool_manager(conn)), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        prod = io.Producer("example.com:9092", [], None)
        prod.offload_url = "http://example.com/8000"
        prod.auth = Auth("user", "pencil", method="SCRAM-SHA-1")
        result = prod._offload_message(b"data", [("_id", fixed_uuid.bytes)], "topic")
        assert result[0] is None
        assert result[1] is None
        assert result[2] is not None
        assert result[2].code() == confluent_kafka.KafkaError.SASL_AUTHENTICATION_FAILED
        assert "Failed to send large message to offload server" in result[2].str()


def test_stream_auth(auth_config, tmpdir):
    # turning off authentication should give None for the auth property
    s1 = io.Stream(auth=False)
    assert s1.auth is None

    # turning on authentication should give an auth object with the data read from the default file
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
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


def test_stream_open(auth_config, mock_broker, mock_producer, mock_admin_client, tmpdir):
    mb = mock_broker
    stream = io.Stream(auth=False)

    # verify only read/writes are allowed
    with pytest.raises(ValueError) as err:
        stream.open("kafka://localhost:9092/topic1", "q")
    assert "mode must be either 'w' or 'r'" in err.value.args

    # verify that URLs with no scheme are rejected
    with pytest.raises(ValueError) as err:
        stream.open("bad://example.com/topic", "r")
    assert "invalid kafka URL: must start with 'kafka://'" in err.value.args

    # verify that URLs with no topic are rejected when reading
    with pytest.raises(ValueError) as err:
        stream.open("kafka://example.com/", "r")
    assert "no topic(s) specified in kafka URL" in err.value.args

    # verify that URLs with too many hostnames
    with pytest.raises(ValueError) as err:
        stream.open("kafka://example.com,example.net/topic", "r")
        assert "Multiple broker addresses are not supported" in err.value.args

    def producer_factory(c):
        return mock_producer(mb, c.topic)
    # verify that complete URLs are accepted
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("hop.io.adc_producer.Producer", side_effect=producer_factory), \
            patch("adc.consumer.Consumer.subscribe", MagicMock()) as subscribe, \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mock_broker)):
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


def test_stream_open_ambiguous_creds():
    creds = [
        Auth("user1", "pass1", host="example.com"),
        Auth("user2", "pass2", host="example.com"),
    ]
    stream = io.Stream(auth=creds)
    with pytest.raises(AmbiguousCredentialError) as err:
        stream.open("kafka://example.com/topic")
    assert "To select a specific credential" in err.value.args[0]


def test_unpack(circular_msg):
    kafka_msg = make_message_standard(circular_msg)

    with patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("group", "kafka://example.com", ["topic"])
        unpacked_msg = c._unpack(kafka_msg)
        assert unpacked_msg == circular_msg

        unpacked_msg2, metadata = c._unpack(kafka_msg, metadata=True)
        assert unpacked_msg2 == unpacked_msg


def test_unpack_external():
    url = "https://external.service/msg"
    kafka_msg = make_message_standard(ExternalMessage(url=url))

    with patch("hop.io.consumer.Consumer", MagicMock()), \
            patch("hop.io.Consumer._fetch_external", MagicMock()):
        c = io.Consumer("group", "kafka://example.com", ["topic"])
        unpacked_msg = c._unpack(kafka_msg)
        c._fetch_external.assert_called_with(url, False, kafka_msg)


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


def test_producer_check_topic_settings(mock_broker, mock_admin_client):
    default_size = 1024 * 1024
    non_default_size = 178
    mock_broker.set_default_topic_max_message_size(default_size)
    mock_broker.set_topic_max_message_size("topic2", non_default_size)

    with patch("hop.io.AdminClient", return_value=mock_admin_client(mock_broker)):
        prod = io.Producer("example.com:9092", [], None)
        results = prod._check_topic_settings(["topic1", "topic2"])
        assert len(results) == 2, "Number of results should match number of requested topics"
        assert "topic1" in results, "Each requested topic should appear in results"
        assert "topic2" in results, "Each requested topic should appear in results"
        assert "max.message.bytes" in results["topic1"], "Results should contain max.message.bytes"
        assert "max.message.bytes" in results["topic2"], "Results should contain max.message.bytes"
        assert results["topic1"]["max.message.bytes"].value == default_size
        assert results["topic2"]["max.message.bytes"].value == non_default_size


def test_producer_check_topic_settings_inaccessible():
    inaccessible_error = confluent_kafka.KafkaError(
        confluent_kafka.KafkaError.TOPIC_AUTHORIZATION_FAILED,
        "Topic authorization failed.", False, False, False)
    ex = confluent_kafka.KafkaException(inaccessible_error)
    inaccessible_future = MagicMock()
    inaccessible_future.result = MagicMock(side_effect=ex)
    ConfigResource = confluent_kafka.admin.ConfigResource

    def describe_configs_disallowed(queries):
        return {ConfigResource(q.restype, q.name): inaccessible_future for q in queries}

    MockAdminClient = MagicMock()
    MockAdminClient.describe_configs = describe_configs_disallowed

    with patch("hop.io.AdminClient", return_value=MockAdminClient):
        prod = io.Producer("example.com:9092", [], None)
        with pytest.warns(UserWarning):
            results = prod._check_topic_settings(["topic"])
        assert len(results) == 1
        assert "topic" in results


def test_producer_check_topic_settings_general_kafka_error():
    network_error = confluent_kafka.KafkaError(
        confluent_kafka.KafkaError.NETWORK_EXCEPTION,
        "Network error", False, False, False)
    ex = confluent_kafka.KafkaException(network_error)
    error_future = MagicMock()
    error_future.result = MagicMock(side_effect=ex)
    ConfigResource = confluent_kafka.admin.ConfigResource

    def describe_configs_disallowed(queries):
        return {ConfigResource(q.restype, q.name): error_future for q in queries}

    MockAdminClient = MagicMock()
    MockAdminClient.describe_configs = describe_configs_disallowed

    with patch("hop.io.AdminClient", return_value=MockAdminClient):
        prod = io.Producer("example.com:9092", [], None)
        with pytest.raises(confluent_kafka.KafkaException):
            prod._check_topic_settings(["topic"])


def test_producer_set_producer_for_topic():
    prod = io.Producer("example.com:9092", [], None)

    assert len(prod.topics) == 0, "No topic records should be initially created with no topics"
    assert len(prod.producers) == 0, "No producers should be initially created with no topics"

    ConfigEntry = confluent_kafka.admin.ConfigEntry

    # configure a first topic
    with patch("hop.io.adc_producer.Producer", MagicMock) as prod_impl, \
            patch("hop.io.time.time", MagicMock(return_value=1.0)):
        t1_settings = {"max.message.bytes": ConfigEntry("max.message.bytes", 128)}
        prod._set_producer_for_topic("topic1", t1_settings)
        # there should be one topic record, and one producer record
        # the topic record should reflect the 'current' time and correct max size
        # the producer should be set to the correct max size, and have a refcount of 1
        assert len(prod.topics) == 1
        assert "topic1" in prod.topics
        assert prod.topics["topic1"].last_check_time == 1.0
        assert prod.topics["topic1"].max_message_size == 128
        assert len(prod.producers) == 1
        assert 128 in prod.producers
        assert prod.producers[128].n_users == 1

    # configure a second topic with the same size
    with patch("hop.io.adc_producer.Producer", MagicMock) as prod_impl, \
            patch("hop.io.time.time", MagicMock(return_value=2.0)):
        t2_settings = {"max.message.bytes": ConfigEntry("max.message.bytes", 128)}
        prod._set_producer_for_topic("topic2", t2_settings)
        # there should now be two topic records, and still one producer record
        # the producer should now have a refcount of 2
        assert len(prod.topics) == 2
        assert "topic2" in prod.topics
        assert prod.topics["topic2"].last_check_time == 2.0
        assert prod.topics["topic2"].max_message_size == 128
        assert len(prod.producers) == 1
        assert 128 in prod.producers
        assert prod.producers[128].n_users == 2

    # configure a third topic with a distinct size
    with patch("hop.io.adc_producer.Producer", MagicMock) as prod_impl, \
            patch("hop.io.time.time", MagicMock(return_value=3.0)):
        t3_settings = {"max.message.bytes": ConfigEntry("max.message.bytes", 256)}
        prod._set_producer_for_topic("topic3", t3_settings)
        # there should now be three topic records, and two producer records
        # the new producer should now have a refcount of 1
        assert len(prod.topics) == 3
        assert "topic3" in prod.topics
        assert prod.topics["topic3"].last_check_time == 3.0
        assert prod.topics["topic3"].max_message_size == 256
        assert len(prod.producers) == 2
        assert 256 in prod.producers
        assert prod.producers[128].n_users == 2
        assert prod.producers[256].n_users == 1


def test_producer_record_for_topic(mock_broker, mock_admin_client):
    default_size = 128
    non_default_size = 256
    non_default_size2 = 512
    mock_broker.set_default_topic_max_message_size(default_size)
    mock_broker.set_topic_max_message_size("topic3", non_default_size)

    prod = io.Producer("example.com:9092", [], None, topic_check_period=timedelta(seconds=50))

    assert len(prod.topics) == 0, "No topic records should be initially created with no topics"
    assert len(prod.producers) == 0, "No producers should be initially created with no topics"

    mac = mock_admin_client(mock_broker)

    # the first use of a topic shold trigger a check of its settings and allocate a producer
    with patch("hop.io.AdminClient", return_value=mac), \
            patch("hop.io.adc_producer.Producer", MagicMock(return_value=MagicMock())), \
            patch("hop.io.time.time", MagicMock(return_value=1.0)):
        rec1 = prod._record_for_topic("topic1")
        print(mock_broker._max_sizes)
        assert rec1.last_check_time == 1.0
        assert rec1.max_message_size == default_size
        p1 = rec1.producer
        assert len(prod.topics) == 1
        assert len(prod.producers) == 1
        assert mac.describe_configs_queries == 1
    mac.reset_query_counter()

    # the second use of the topic (within the TTL) should return the same record with the
    # same producer
    with patch("hop.io.AdminClient", return_value=mac), \
            patch("hop.io.adc_producer.Producer", MagicMock(return_value=MagicMock())), \
            patch("hop.io.time.time", MagicMock(return_value=2.0)):
        rec2 = prod._record_for_topic("topic1")
        assert rec2 is rec1
        assert rec2.producer is p1
        assert len(prod.topics) == 1
        assert len(prod.producers) == 1
        assert prod.producers[default_size].n_users == 1
        assert mac.describe_configs_queries == 0, \
            "no new settings query should be issued"
    mac.reset_query_counter()

    # using a second topic with the same maximum size should yield a new topic record with the same
    # producer object
    with patch("hop.io.AdminClient", return_value=mac), \
            patch("hop.io.adc_producer.Producer", MagicMock(return_value=MagicMock())), \
            patch("hop.io.time.time", MagicMock(return_value=2.0)):
        rec3 = prod._record_for_topic("topic2")
        assert rec3 is not rec1
        assert rec3.producer is p1
        assert len(prod.topics) == 2
        assert len(prod.producers) == 1
        assert prod.producers[default_size].n_users == 2
    mac.reset_query_counter()

    # using a topic with a distinct maximum message size should cause the allocation of another
    # producer object
    with patch("hop.io.AdminClient", return_value=mac), \
            patch("hop.io.adc_producer.Producer", MagicMock(return_value=MagicMock())), \
            patch("hop.io.time.time", MagicMock(return_value=3.0)):
        rec4 = prod._record_for_topic("topic3")
        assert rec4 is not rec1
        assert rec4 is not rec3
        assert rec4.producer is not p1
        assert len(prod.topics) == 3
        assert len(prod.producers) == 2
        assert prod.producers[default_size].n_users == 2
        assert prod.producers[non_default_size].n_users == 1
    mac.reset_query_counter()

    # After enough time has elapsed, sttings should be requeried
    # If the resulting settings are the same, no changes should be made
    with patch("hop.io.AdminClient", return_value=mac), \
            patch("hop.io.adc_producer.Producer", MagicMock(return_value=MagicMock())), \
            patch("hop.io.time.time", MagicMock(return_value=200.0)):
        rec5 = prod._record_for_topic("topic1")
        assert rec5 is rec1
        assert rec5.producer is p1
        assert len(prod.topics) == 3
        assert len(prod.producers) == 2
        assert prod.producers[default_size].n_users == 2
        assert mac.describe_configs_queries == 1, \
            "a new settings query should be issued"
    mac.reset_query_counter()

    # If a periodic re-check encounters new settings, new producer objects should be created as
    # needed, and refcounts updated
    mock_broker.set_topic_max_message_size("topic1", non_default_size2)
    with patch("hop.io.AdminClient", return_value=mac), \
            patch("hop.io.adc_producer.Producer", MagicMock(return_value=MagicMock())), \
            patch("hop.io.time.time", MagicMock(return_value=300.0)):
        rec6 = prod._record_for_topic("topic1")
        assert rec6 is not rec1
        assert rec6.producer is not p1
        assert len(prod.topics) == 3
        assert len(prod.producers) == 3
        assert prod.producers[default_size].n_users == 1
        assert prod.producers[non_default_size2].n_users == 1
        assert mac.describe_configs_queries == 1, \
            "a new settings query should be issued"
    mac.reset_query_counter()

    # If a producer's refcount goes to zero it should be deallocated
    mock_broker.set_topic_max_message_size("topic2", non_default_size)
    with patch("hop.io.AdminClient", return_value=mac), \
            patch("hop.io.adc_producer.Producer", MagicMock(return_value=MagicMock())), \
            patch("hop.io.time.time", MagicMock(return_value=400.0)):
        rec7 = prod._record_for_topic("topic2")
        assert rec7 is not rec3
        assert rec7.producer is not p1
        assert len(prod.topics) == 3
        assert len(prod.producers) == 2
        assert default_size not in prod.producers
        assert prod.producers[non_default_size].n_users == 2
        assert mac.describe_configs_queries == 1, \
            "a new settings query should be issued"
        print(p1)
        p1.flush.assert_called()
        p1.close.assert_called()
    mac.reset_query_counter()


def test_producer_estimate_message_size():
    estimate = io.Producer._estimate_message_size

    for length in range(2, 10, 2):
        assert estimate(b'p' * length, []) >= length

    assert estimate(b'p' * 64, [], b'k' * 16) == 16 + estimate(b'p' * 64, [])

    assert estimate(b'p' * 64, [(b'k', b'v')]) > estimate(b'p' * 64, [])
    assert estimate(b'p' * 64, [(b'k', b'v'), ('k', 'v')]) > estimate(b'p' * 64, [(b'k', b'v')])

    assert estimate(b'p' * 64, {b'k': b'v'}) > estimate(b'p' * 64, [])
    assert estimate(b'p' * 64, {b'k': b'v', 'k1': 'v1'}) > estimate(b'p' * 64, {b'k': b'v'})


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
    with patch("hop.io.consumer.Consumer", MagicMock()):
        c = io.Consumer("group", "kafka://example.com", ["topic"])
        unpacked_msg = c._unpack(kafka_msg)

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
        with patch("hop.io.consumer.Consumer", MagicMock()):
            c = io.Consumer("group", "kafka://example.com", ["topic"])
            unpacked_msg = c._unpack(kafka_msg)
            assert unpacked_msg.content == orig_message

    # non-serializable objects should raise an error
    with pytest.raises(TypeError):
        # note that we are not trying to pack a string, but the string class itself
        packed_msg, _ = io.Producer.pack(str)


def test_producer_context_keyboard_interrupt():
    with io.Producer("kafka://example.com:9092", [], None) as s:
        raise KeyboardInterrupt()


def test_producer_context_normal_exit_unsent(mock_broker, mock_producer, mock_admin_client):
    topic = "some_topic"
    mb = mock_broker
    mp = mock_producer(mb, topic)
    with patch("hop.io.adc_producer.Producer", side_effect=lambda c: mp), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mb)):
        mp.delay_sending()
        with pytest.raises(Exception) as ex:
            with io.Producer("kafka://example.com:9092", [topic], None,
                             produce_timeout=timedelta(milliseconds=1)) as s:
                s.write("abc")
                s.write("def")
                assert len(mp) == 2, "The two messages should be 'unsent'"
        assert "2 messages remain unsent, some data may have been lost" in str(ex)


def test_producer_context_normal_exit_no_timeout(mock_broker, mock_producer, mock_admin_client):
    topic = "some_topic"
    mb = mock_broker
    mp = mock_producer(mb, topic)
    with patch("hop.io.adc_producer.Producer", side_effect=lambda c: mp), \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mb)):
        mp.delay_sending()
        with io.Producer("kafka://example.com:9092", [topic], None,
                         produce_timeout=timedelta(seconds=0)) as s:
            s.write("abc")
            s.write("def")
            assert len(mp) == 2, "The two messages should be 'unsent'"
        # no exception should be raised, as the context manager should flush all messages
        assert len(mp) == 0, "No messages should remain 'unsent'"


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

    builtin_models = ["VOEVENT", "GCNTEXTNOTICE", "CIRCULAR", "JSON", "AVRO", "BLOB", "EXTERNAL"]

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


def test_stream_flush(mock_broker, mock_admin_client):
    # flush is pretty trivial; it should just call the underlying flush
    with patch("adc.producer.Producer.flush", MagicMock()) as flush, \
            patch("hop.io.AdminClient", return_value=mock_admin_client(mock_broker)):
        broker_url = "kafka://example.com:9092/topic"
        stream = io.Stream(auth=False).open(broker_url, "w")
        stream.flush()
        flush.assert_called_once()


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
        assert len(listing) == 2
        assert "foo" in listing
        assert "bar" in listing

        # when auth=False, no auth related properties are set
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert len(cons_args) == 1
        assert "sasl.username" not in cons_args[0]
        assert "sasl.password" not in cons_args[0]

    # when there are no topics, the result is empty
    with patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        listing = io.list_topics("kafka://example.com", auth=False)
        assert len(listing) == 0

    # result topics should be the intersection of ones which exist and which were requested
    with patch("confluent_kafka.Consumer", make_mock_listing_consumer(["foo", "bar"])) as Consumer:
        listing = io.list_topics("kafka://example.com/bar,baz", auth=False)
        assert len(listing) == 1
        assert "bar" in listing


def test_list_topics_too_many_brokers():
    # If the URL specifies too many brokers, an error is raised
    with patch("confluent_kafka.Consumer", make_mock_listing_consumer([])):
        with pytest.raises(ValueError):
            listing = io.list_topics("kafka://example.com,example.net", auth=False)


def test_list_topics_auth(auth_config, tmpdir):
    # when auth=True, auth related properties should be set
    with temp_auth(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        listing = io.list_topics("kafka://example.com", auth=True)

        # when auth=True, auth related properties should be set
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert len(cons_args) == 1
        assert "sasl.username" in cons_args[0]
        assert "sasl.password" in cons_args[0]
        assert cons_args[0]["sasl.username"] == "username"
        assert cons_args[0]["sasl.password"] == "password"

    # when an Auth object is set, it should take precedence over automatic lookup
    with temp_auth(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        auth = Auth("someone_else", "other_password")
        listing = io.list_topics("kafka://example.com", auth=auth)

        # when auth=Auth(...), auth related properties should be set correctly
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert len(cons_args) == 1
        assert "sasl.username" in cons_args[0]
        assert "sasl.password" in cons_args[0]
        assert cons_args[0]["sasl.username"] == auth.username
        assert cons_args[0]["sasl.password"] == auth.password

    # when an Auth object is set, it should take precedence over automatic lookup,
    # even with userinfo in the URL
    with temp_auth(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        auth = Auth("someone_else", "other_password")
        listing = io.list_topics("kafka://username@example.com", auth=auth)

        # when auth=Auth(...), auth related properties should be set correctly
        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert len(cons_args) == 1
        assert "sasl.username" in cons_args[0]
        assert "sasl.password" in cons_args[0]
        assert cons_args[0]["sasl.username"] == auth.username
        assert cons_args[0]["sasl.password"] == auth.password

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
    with temp_auth(tmpdir, multi_cred) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_mock_listing_consumer([])) as Consumer:
        listing = io.list_topics("kafka://user2@example.com", auth=True)

        Consumer.assert_called_once()
        cons_args = Consumer.call_args[0]
        assert len(cons_args) == 1
        assert "sasl.username" in cons_args[0]
        assert "sasl.password" in cons_args[0]
        assert cons_args[0]["sasl.username"] == "user2"
        assert cons_args[0]["sasl.password"] == "pass2"


def test_list_topics_timeout():
    cred = Auth("user", "pass")
    for timeout in [0.5, 1.0, 1.5]:
        start = time.time()
        with pytest.raises(confluent_kafka.KafkaException) as err:
            io.list_topics("kafka://not-a-valid-broker.scimma.org", auth=cred, timeout=timeout)
        stop = time.time()
        assert abs((stop - start) - timeout) < 0.1
