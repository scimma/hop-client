from unittest.mock import patch, mock_open, MagicMock
import sys
import pytest
import json
from io import BytesIO, StringIO
from uuid import uuid4

from hop import __version__, configure, io, models
from conftest import temp_auth, temp_config, temp_environ


@pytest.mark.script_launch_mode("subprocess")
def test_cli_hop(script_runner, auth_config, tmpdir):
    ret = script_runner.run(["hop", "--help"])
    assert ret.success

    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop", "--version"])
        assert ret.success

        assert f"hop version {__version__}\n" in ret.stdout
        assert ret.stderr == ""


@pytest.mark.script_launch_mode("subprocess")
def test_cli_hop_module(script_runner, auth_config, tmpdir):
    ret = script_runner.run(["python", "-m", "hop", "--help"])
    assert ret.success

    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["python", "-m", "hop", "--version"])
        assert ret.success

        assert f"hop version {__version__}\n" in ret.stdout
        assert ret.stderr == ""


def assert_called_with_nonexclusive(mock: MagicMock, *expected_args, **expected_kwargs):
    """unittest.mock.Mock.assert_called_with requires that there be no arguments besides those
    specified, which makes tests brittle. This work-around checks that the specified arguments were
    present, but ignores additional arguments.

    Args:
        mock: the mock object to check
        expected_args: required positional arguments
        expected_kwargs: required keyword arguments
    """
    if not mock.called:
        raise AssertionError(f"{mock} was not called")
    actual_args = mock.call_args.args
    if len(actual_args) < len(expected_args):
        raise AssertionError(f"{mock} was called with too few positional arguments")
    for i in range(0, len(expected_args)):
        if actual_args[i] != expected_args[i]:
            raise AssertionError(f"{mock} was called with unexpected positional argument {i}: "
                                 f"Called with {actual_args[i]} instead of {expected_args[i]}")
    actual_kwargs = mock.call_args.kwargs
    for kw in expected_kwargs.keys():
        if kw not in actual_kwargs:
            raise AssertionError("f{mock} was not called with expected keyword argument {kw}")
        if actual_kwargs[kw] != expected_kwargs[kw]:
            raise AssertionError(f"{mock} was called with unexpected keyword argument {kw}: "
                                 f"Called with {actual_kwargs[kw]} instead of "
                                 f"{expected_kwargs[kw]}")


@pytest.mark.parametrize("message_format", ["voevent", "circular", "blob"])
def test_cli_publish(script_runner, message_format, message_parameters_dict):
    if sys.version_info < (3, 7, 4):
        if message_format == "voevent":
            pytest.skip("requires python3.7.4 or higher")

    ret = script_runner.run(["hop", "publish", "--help"])
    assert ret.success

    # load parameters from conftest
    message_parameters = message_parameters_dict[message_format]

    test_file = message_parameters["test_file"]
    model_text = message_parameters["model_text"]

    # test publishing files
    message_mock = mock_open(read_data=model_text)
    with patch("hop.models.open", message_mock) as mock_file, patch(
        "hop.io.Stream.open", mock_open()
    ) as mock_stream:

        broker_url = "kafka://hostname:port/message"
        ret = script_runner.run([
            "hop", "publish", broker_url, test_file, "--quiet",
            "-f", message_format.upper(), "--no-auth",
        ])

        # verify CLI output
        assert ret.success
        assert ret.stderr == ""

        # verify message was processed
        if message_format in ["voevent", "blob"]:
            assert_called_with_nonexclusive(mock_file, test_file, "rb")
        else:
            assert_called_with_nonexclusive(mock_file, test_file, "r")

        assert_called_with_nonexclusive(mock_stream, broker_url, "w")

    # test publishing from stdin
    with patch("hop.io.Stream.open", mock_open()) as mock_stream:
        ret = script_runner.run(["hop", "publish", "-f", message_format.upper(), broker_url],
                                stdin=StringIO('"message1"\n"message2"'))
        if message_format == "blob":
            assert ret.success
        else:  # only the blob format is supported, others should trigger an error
            assert not ret.success
            assert "piping/redirection only allowed for BLOB and JSON formats" in ret.stderr


def test_cli_publish_blob_msgs(mock_broker, mock_producer, mock_consumer, mock_admin_client):
    from hop import publish, io, models
    args = MagicMock()
    args.url = "kafka://hostname:port/topic"
    args.format = io.Deserializer.BLOB.name
    args.test = False
    args.timeout = 10.0
    start_at = io.StartPosition.EARLIEST
    read_url = "kafka://group@hostname:port/topic"
    fixed_uuid = uuid4()
    fake_uuid = MagicMock()
    fake_uuid.uuid4 = MagicMock(return_value=fixed_uuid)

    mock_adc_producer = mock_producer(mock_broker, "topic")
    mock_adc_consumer = mock_consumer(mock_broker, "topic", "group")
    msgs = [b"a string", b"\x10\x00\x20\x0B"]
    for msg in msgs:
        with patch("sys.stdin", BytesIO(msg)) as mock_stdin, \
                patch("hop.io.adc_producer.Producer", return_value=mock_adc_producer), \
                patch("hop.io.consumer.Consumer", return_value=mock_adc_consumer), \
                patch("hop.io.AdminClient", return_value=mock_admin_client(mock_broker)), \
                patch("hop.io.uuid", fake_uuid):
            publish._main(args)

            # each published message should be on the broker
            encoded = models.Blob(msg).serialize()
            expected_msg = {
                "message": encoded["content"],
                "headers": [("_id", fixed_uuid.bytes),
                            ("_format", encoded["format"].encode("utf-8"))],
            }
            assert mock_broker.has_message("topic", **expected_msg)

            # reading from the broker should yield messages which match the originals
            with io.Stream(start_at=None, auth=False).open(read_url, "r") as s:
                extracted_msgs = []
                for extracted_msg in s:
                    extracted_msgs.append(extracted_msg)
                # there should be one new message
                assert len(extracted_msgs) == 1
                # and it should be the one we published
                assert msg in [msg.content for msg in extracted_msgs]


def test_cli_publish_json_blob_msgs(mock_broker, mock_producer, mock_consumer, mock_admin_client):
    from hop import publish, io, models
    args = MagicMock()
    args.url = "kafka://hostname:port/topic"
    args.format = io.Deserializer.JSON.name
    args.test = False
    args.timeout = 10.0
    start_at = io.StartPosition.EARLIEST
    read_url = "kafka://group@hostname:port/topic"
    fixed_uuid = uuid4()
    fake_uuid = MagicMock()
    fake_uuid.uuid4 = MagicMock(return_value=fixed_uuid)

    mock_adc_producer = mock_producer(mock_broker, "topic")
    mock_adc_consumer = mock_consumer(mock_broker, "topic", "group")
    msgs = ["a string", ["a", "list", "of", "values"],
            {"a": "dict", "with": ["multiple", "values"]}]
    for msg in msgs:
        with patch("sys.stdin", StringIO(json.dumps(msg))) as mock_stdin, \
                patch("hop.io.adc_producer.Producer", return_value=mock_adc_producer), \
                patch("hop.io.consumer.Consumer", return_value=mock_adc_consumer), \
                patch("hop.io.AdminClient", return_value=mock_admin_client(mock_broker)), \
                patch("hop.io.uuid", fake_uuid):
            publish._main(args)

            # each published message should be on the broker
            encoded = models.JSONBlob(msg).serialize()
            expected_msg = {
                "message": encoded["content"],
                "headers": [("_id", fixed_uuid.bytes),
                            ("_format", encoded["format"].encode("utf-8"))],
            }
            assert mock_broker.has_message("topic", **expected_msg)

            # reading from the broker should yield messages which match the originals
            with io.Stream(start_at=None, auth=False).open(read_url, "r") as s:
                extracted_msgs = []
                for extracted_msg in s:
                    extracted_msgs.append(extracted_msg)
                # there should be one new message
                assert len(extracted_msgs) == 1
                # and it should be the one we published
                assert msg in [msg.content for msg in extracted_msgs]


def test_cli_publish_bad_blob(mock_broker, mock_producer, mock_admin_client):
    # ensure that invalid JSON causes an exception to be raised
    from hop import publish, io

    args = MagicMock()
    args.url = "kafka://hostname:port/topic"
    args.format = io.Deserializer.JSON.name
    args.test = False
    args.timeout = 10.0

    mock_adc_producer = mock_producer(mock_broker, "topic")
    msgs = ["not quoted", '{"unclosed:"brace"',
            "invalid\tcharacters\\\b"]
    for msg in msgs:
        # note that we do not serialize the messages as JSON
        with patch("sys.stdin", StringIO(msg)) as mock_stdin, \
                patch("hop.io.adc_producer.Producer", return_value=mock_adc_producer), \
                patch("hop.io.AdminClient", return_value=mock_admin_client(mock_broker)), \
                pytest.raises(ValueError):
            publish._main(args)


# create a mock output stream which supports writing binary data
def make_mock_stream():
    # this will be the underlying object which holds the data
    buffer = BytesIO()
    # Prepare wrapper routines which take strings like a TextIOBase.
    # Currently only acting as an output stream is supported.
    encoding = "utf-8"

    def write(s, /):
        encoded = s.encode(encoding)
        buffer.write(encoded)
        return len(encoded)
    # this will return bytes, not str, because the data is not constrained by any string encoding

    def getvalue():
        return buffer.getvalue()
    stream = MagicMock()
    stream.buffer = buffer
    stream.write = write
    stream.getvalue = getvalue
    return stream


def test_cli_subscribe(script_runner):
    with patch("pytest_console_scripts.StreamMock", make_mock_stream):
        ret = script_runner.run(["hop", "subscribe", "--help"])
        assert ret.success

        with patch("hop.io.Stream.open", mock_open()) as mock_stream:

            broker_url = "kafka://hostname:port/message"
            ret = script_runner.run(["hop", "subscribe", broker_url, "--no-auth"])

            # verify CLI output
            assert ret.success
            assert len(ret.stderr) == 0

            # verify broker url was processed
            mock_stream.assert_called_with(broker_url, "r", group_id=None, ignoretest=True)

            ret = script_runner.run(["hop", "subscribe", broker_url, "--no-auth", "--group-id",
                                     "group"])

            # verify CLI output
            assert ret.success
            assert len(ret.stderr) == 0

            # verify consumer group ID was used
            mock_stream.assert_called_with(broker_url, "r", group_id="group", ignoretest=True)

        message_body = "some-message"
        message_model = models.Blob(message_body.encode("utf-8"))
        message = io.Consumer.ExternalMessage(*io.Producer.pack(message_model), "a_topic",
                                              0, 0, 0, None)
        mock_instance = MagicMock()
        mock_instance.stream = MagicMock(return_value=[message])
        with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
            ret = script_runner.run(["hop", "--debug", "subscribe", broker_url, "--no-auth",
                                     "--quiet"])
            assert ret.success
            assert len(ret.stderr) == 0
            assert message.value() in ret.stdout

        message = io.Consumer.ExternalMessage(*io.Producer.pack(message_model, test=True),
                                              "a_topic", 0, 0, 0, None)
        mock_instance.stream = MagicMock(return_value=[message])

        with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
            ret = script_runner.run(["hop", "subscribe", broker_url, "--no-auth", "--quiet"])
            assert ret.success
            assert len(ret.stderr) == 0
            assert len(ret.stdout) == 0

        with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)):
            ret = script_runner.run(["hop", "subscribe", broker_url, "--no-auth", "--quiet",
                                     "--test"])
            assert ret.success
            assert len(ret.stderr) == 0
            assert message.value() in ret.stdout


def test_cli_subscribe_logging(script_runner):
    broker_url = "kafka://hostname:port/message"
    message_body = "some-message"
    message_model = models.Blob(message_body.encode("utf-8"))
    message = io.Consumer.ExternalMessage(*io.Producer.pack(message_model), "a_topic",
                                          0, 0, 0, None)
    mock_instance = MagicMock()
    mock_instance.stream = MagicMock(return_value=[message])
    with patch("hop.io.consumer.Consumer", MagicMock(return_value=mock_instance)), \
            patch("pytest_console_scripts.StreamMock", make_mock_stream):
        # check logging with --quiet (only warnings/errors)
        ret = script_runner.run(["hop", "--debug", "subscribe", broker_url, "--no-auth", "--quiet"])
        assert ret.success
        assert b"DEBUG" not in ret.stderr
        assert b"INFO" not in ret.stderr

        # check default logging (INFO)
        ret = script_runner.run(["hop", "--debug", "subscribe", broker_url, "--no-auth"])
        assert ret.success
        assert b"INFO" in ret.stderr
        assert b"DEBUG" not in ret.stderr

        # check verbose logging (DEBUG)
        ret = script_runner.run(["hop", "--debug", "subscribe", broker_url, "--no-auth", "-v"])
        assert ret.success
        assert b"INFO" in ret.stderr
        assert b"DEBUG" in ret.stderr


def test_cli_interrupt(script_runner):
    mock_consumer = MagicMock()
    interrupt = MagicMock(side_effect=KeyboardInterrupt())
    mock_consumer.__enter__ = interrupt
    mock_consumer.__iter__ = interrupt
    mock_consumer.__exit__ = interrupt
    with patch("hop.io.Stream.open", MagicMock(return_value=mock_consumer)):
        broker_url = "kafka://hostname:port/topic"
        ret = script_runner.run(["hop", "subscribe", broker_url, "--no-auth"])
        assert ret.success
        assert "INFO" in ret.stderr
        assert "received keyboard interrupt, closing" in ret.stderr


def dummy_topic_info(topic, error=None):
    info = MagicMock()
    info.error = error
    info.partitions = {}
    if error is None:
        info.partitions[0] = MagicMock()
    return info


def make_consumer_mock(expected_topics):
    metadata = MagicMock()
    metadata.topics = expected_topics
    list_topics = MagicMock(return_value=metadata)
    consumer = MagicMock()
    consumer.list_topics = list_topics
    return MagicMock(return_value=consumer)


def test_cli_list_topics(script_runner, auth_config, tmpdir):
    ret = script_runner.run(["hop", "list-topics", "--help"])
    assert ret.success

    broker_url = "kafka://hostname:port/"

    # general listing when no topics are returned
    with patch("confluent_kafka.Consumer", make_consumer_mock({})) as mock_consumer:
        ret = script_runner.run(["hop", "list-topics", broker_url, "--no-auth"])

        assert ret.success
        assert ret.stderr == ""
        assert "No accessible topics" in ret.stdout

        mock_consumer.assert_called()
        mock_consumer.return_value.list_topics.assert_called_with(timeout=-1.)

    expected_topics = ["foo", "bar"]
    unexpected_topics = ["baz"]
    topic_results = {}
    for topic in expected_topics:
        topic_results[topic] = dummy_topic_info(topic)
    for topic in unexpected_topics:
        topic_results[topic] = dummy_topic_info(topic, "an error")

    # general listing when some topics are returned
    with patch("confluent_kafka.Consumer", make_consumer_mock(topic_results)) as mock_consumer:
        ret = script_runner.run(["hop", "--debug", "list-topics", broker_url, "--no-auth"])

        assert ret.success
        assert ret.stderr == ""
        assert "Accessible topics" in ret.stdout
        for topic in expected_topics:
            assert topic in ret.stdout
        for topic in unexpected_topics:
            assert topic not in ret.stdout

        mock_consumer.assert_called()
        mock_consumer.return_value.list_topics.assert_called_with(timeout=-1.)

    query_topics = ["foo", "bar", "baz"]
    # listing of specific topics, none of which exist
    with patch("confluent_kafka.Consumer", make_consumer_mock({})) as mock_consumer:
        ret = script_runner.run(["hop", "list-topics", broker_url + ",".join(query_topics),
                                 "--no-auth"])

        assert ret.success
        assert ret.stderr == ""
        assert "No accessible topics" in ret.stdout

        mock_consumer.assert_called()
        for topic in query_topics:
            mock_consumer.return_value.list_topics.assert_any_call(topic=topic, timeout=-1.)

    # listing of specific topics, some of which exist and some of which do not
    with patch("confluent_kafka.Consumer", make_consumer_mock(topic_results)) as mock_consumer:
        ret = script_runner.run(["hop", "list-topics", broker_url + ",".join(query_topics),
                                 "--no-auth"])

        assert ret.success
        assert ret.stderr == ""
        assert "Accessible topics" in ret.stdout
        for topic in expected_topics:
            assert topic in ret.stdout
        for topic in unexpected_topics:
            assert topic not in ret.stdout

        mock_consumer.assert_called()
        for topic in query_topics:
            mock_consumer.return_value.list_topics.assert_any_call(topic=topic, timeout=-1.)

    # general listing with authentication
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir), \
            patch("confluent_kafka.Consumer", make_consumer_mock(topic_results)) as mock_consumer:
        ret = script_runner.run(["hop", "list-topics", broker_url])

        assert ret.success
        assert ret.stderr == ""
        assert "Accessible topics" in ret.stdout
        for topic in expected_topics:
            assert topic in ret.stdout
        for topic in unexpected_topics:
            assert topic not in ret.stdout

        mock_consumer.assert_called()
        mock_consumer.return_value.list_topics.assert_called_with(timeout=-1.)

    # attempting to use multiple brokers should provoke an error
    ret = script_runner.run(["hop", "list-topics", "kafka://example.com,example.net"])
    assert not ret.success
    assert "Multiple broker addresses are not supported" in ret.stderr


def test_cli_configure_locate(script_runner, general_config, tmpdir):
    with temp_config(tmpdir, general_config) as conf_dir, temp_environ(XDG_CONFIG_HOME=conf_dir):
        ret1 = script_runner.run(["hop", "configure", "--help"])
        assert ret1.success
        assert ret1.stderr == ""

        ret = script_runner.run(["hop", "configure", "locate"])
        assert ret.success
        assert conf_dir in ret.stdout
        assert ret.stderr == ""


def test_cli_configure_show(script_runner, general_config, tmpdir):
    with temp_config(tmpdir, general_config) as conf_dir, temp_environ(XDG_CONFIG_HOME=conf_dir):
        ret = script_runner.run(["hop", "configure", "show"])
        assert ret.success
        assert "fetch_external: False" in ret.stdout
        assert "automatic_offload: False" in ret.stdout
        assert ret.stderr == ""


def test_cli_configure_set(script_runner, general_config, tmpdir):
    with temp_config(tmpdir, general_config) as conf_dir, temp_environ(XDG_CONFIG_HOME=conf_dir):
        ret = script_runner.run(["hop", "configure", "set", "fetch_external", "true"])
        assert ret.success
        config = configure.load_config()
        assert config.fetch_external

    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        ret = script_runner.run(["hop", "configure", "set", "not-a-setting", "18"])
        assert not ret.success
        assert "not-a-setting" in ret.stderr

    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        ret = script_runner.run(["hop", "configure", "set", "fetch_external", "warbler"])
        assert not ret.success
        assert "Invalid boolean value 'warbler'" in ret.stderr


def test_cli_auth(script_runner, auth_config, tmpdir):
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret1 = script_runner.run(["hop", "auth", "--help"])
        assert ret1.success
        assert ret1.stderr == ""

        ret = script_runner.run(["hop", "auth", "locate"])
        assert ret.success
        assert config_dir in ret.stdout
        assert ret.stderr == ""


def test_list_credentials(script_runner, auth_config, tmpdir):
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop", "auth", "list"])
        assert ret.success
        assert "username" in ret.stdout
        assert ret.stderr == ""


def test_add_credential(script_runner, auth_config, tmpdir):
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        csv_file = str(tmpdir) + "/new_cred.csv"
        with open(csv_file, "w") as f:
            f.write("username,password\nnew_user,new_pass")
        ret = script_runner.run(["hop", "auth", "add", csv_file])
        assert ret.success
        assert "Wrote configuration to" in ret.stderr


def test_add_credential_overwrite(script_runner, auth_config, tmpdir):
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        csv_file = str(tmpdir) + "/new_cred.csv"
        with open(csv_file, "w") as f:
            f.write("username,password\nnew_user,new_pass")
        ret = script_runner.run(["hop", "auth", "add", csv_file])
        assert ret.success
        assert "Wrote configuration to" in ret.stderr

        with open(csv_file, "w") as f:
            f.write("username,password\nnew_user,other_pass")

        # try to overwrite the credential, without forcing
        ret = script_runner.run(["hop", "auth", "add", csv_file])
        assert ret.success
        assert "Credential already exists; overwrite with --force" in ret.stderr

        # try again, with force
        ret = script_runner.run(["hop", "auth", "add", "--force", csv_file])
        assert ret.success
        assert "Wrote configuration to" in ret.stderr


def test_delete_credential(script_runner, auth_config, tmpdir):
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop", "auth", "remove", "username"])
        assert ret.success
        assert "Wrote configuration to" in ret.stderr


def test_cli_version(script_runner, auth_config, tmpdir):
    with temp_auth(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop", "version", "--help"])
        assert ret.success
        assert ret.stderr == ""

        ret = script_runner.run(["hop", "version"])
        assert ret.success
        assert f"hop-client=={__version__}\n" in ret.stdout
        assert ret.stderr == ""


def test_error_verbosity(script_runner):
    simple = script_runner.run(["hop", "subscribe", "BAD-URL"])
    assert not simple.success
    assert simple.stdout == ""
    assert "Traceback (most recent call last)" not in simple.stderr
    assert simple.stderr.startswith("hop: ")

    detailed = script_runner.run(["hop", "--debug", "subscribe", "BAD-URL"])
    assert not detailed.success
    assert detailed.stdout == ""
    assert "Traceback (most recent call last)" in detailed.stderr


def test_config_advice(script_runner, auth_config, tmpdir):
    advice_tag = "No valid credential data found"
    # nonexistent config file
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        ret = script_runner.run(["hop"])
        assert advice_tag in ret.stdout

    # wrong credential file permissions
    import stat
    with temp_auth(tmpdir, "", stat.S_IROTH) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop"])
        assert advice_tag in ret.stdout
        assert "unsafe permissions" in ret.stderr

    # syntactically invalid TOML in credential file
    garbage = "JVfwteouh '652b"
    with temp_auth(tmpdir, garbage) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop"])
        assert advice_tag in ret.stdout
        assert "not configured correctly" in ret.stderr

    # syntactically valid TOML without an [auth] section
    toml_no_auth = """title = "TOML Example"
    [owner]
    name = "Tom Preston-Werner"
    dob = 1979-05-27T07:32:00-08:00
    """
    with temp_auth(tmpdir, toml_no_auth) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop"])
        assert advice_tag in ret.stdout
        assert "configuration file has no auth section" in ret.stderr

    # syntactically valid TOML an incomplete [auth] section
    toml_bad_auth = """[auth]
    foo = "bar"
    """
    with temp_auth(tmpdir, toml_bad_auth) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        ret = script_runner.run(["hop"])
        assert advice_tag in ret.stdout
        assert "missing auth property" in ret.stderr
