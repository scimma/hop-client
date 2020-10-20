from unittest.mock import patch, mock_open, MagicMock
import sys
import pytest
from io import StringIO
import io

from hop import __version__


@pytest.mark.script_launch_mode("subprocess")
def test_cli_hop(script_runner):
    ret = script_runner.run("hop", "--help")
    assert ret.success

    ret = script_runner.run("hop", "--version")
    assert ret.success

    assert ret.stdout == f"hop version {__version__}\n"
    assert ret.stderr == ""


@pytest.mark.parametrize("message_format", ["voevent", "circular", "blob"])
def test_cli_publish(script_runner, message_format, message_parameters_dict):
    if sys.version_info < (3, 7, 4):
        if message_format == "voevent":
            pytest.skip("requires python3.7.4 or higher")

    ret = script_runner.run("hop", "publish", "--help")
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
        ret = script_runner.run(
            "hop", "publish", broker_url, test_file, "-f", message_format.upper(), "--no-auth",
        )

        # verify CLI output
        assert ret.success
        assert ret.stderr == ""

        # verify message was processed
        if message_format == "voevent":
            mock_file.assert_called_with(test_file, "rb")
        else:
            mock_file.assert_called_with(test_file, "r")

        mock_stream.assert_called_with(broker_url, "w")

    # test publishing from stdin
    with patch("hop.io.Stream.open", mock_open()) as mock_stream:
        ret = script_runner.run("hop", "publish", "-f", message_format.upper(), broker_url,
                                stdin=io.StringIO('"message1"\n"message2"'))
        if message_format == "blob":
            assert ret.success
        else:  # only the blob format is supported, others should trigger an error
            assert not ret.success
            assert "piping/redirection only allowed for BLOB formats" in ret.stderr


def test_cli_publish_blob_types(mock_broker, mock_producer, mock_consumer):
    from hop import publish, io, models
    import json
    args = MagicMock()
    args.url = "kafka://hostname:port/topic"
    args.format = io.Deserializer.BLOB.name
    start_at = io.StartPosition.EARLIEST
    read_url = "kafka://group@hostname:port/topic"

    mock_adc_producer = mock_producer(mock_broker, "topic")
    mock_adc_consumer = mock_consumer(mock_broker, "topic", "group")
    msgs = ["a string", ["a", "list", "of", "values"],
            {"a": "dict", "with": ["multiple", "values"]}]
    for msg in msgs:
        with patch("sys.stdin", StringIO(json.dumps(msg))) as mock_stdin, \
                patch("hop.io.producer.Producer", return_value=mock_adc_producer), \
                patch("hop.io.consumer.Consumer", return_value=mock_adc_consumer):
            publish._main(args)

            # each published message should be on the broker
            expected_msg = json.dumps(models.Blob(msg).serialize()).encode("utf-8")
            assert mock_broker.has_message("topic", expected_msg)

            # reading from the broker should yield messages which match the originals
            with io.Stream(persist=False, start_at=None, auth=False).open(read_url, "r") as s:
                extracted_msgs = []
                for extracted_msg in s:
                    extracted_msgs.append(extracted_msg)
                # there should be one new message
                assert len(extracted_msgs) == 1
                # and it should be the one we published
                assert msg in extracted_msgs


def test_cli_publish_bad_blob(mock_broker, mock_producer):
    # ensure that invalid JSON causes an exception to be raised
    from hop import publish, io

    args = MagicMock()
    args.url = "kafka://hostname:port/topic"
    args.format = io.Deserializer.BLOB.name

    mock_adc_producer = mock_producer(mock_broker, "topic")
    msgs = ["not quoted", '{"unclosed:"brace"',
            "invalid\tcharacters\\\b"]
    for msg in msgs:
        # note that we do not serialize the messages as JSON
        with patch("sys.stdin", StringIO(msg)) as mock_stdin, \
                patch("hop.io.producer.Producer", return_value=mock_adc_producer), \
                pytest.raises(ValueError):
            publish._main(args)


def test_cli_subscribe(script_runner):
    ret = script_runner.run("hop", "subscribe", "--help")
    assert ret.success

    with patch("hop.io.Stream.open", mock_open()) as mock_stream:

        broker_url = "kafka://hostname:port/message"
        ret = script_runner.run("hop", "subscribe", broker_url, "--no-auth")

        # verify CLI output
        assert ret.success
        assert ret.stderr == ""

        # verify broker url was processed
        mock_stream.assert_called_with(broker_url, "r")


def test_cli_auth(script_runner):
    ret1 = script_runner.run("hop", "configure", "--help")
    assert ret1.success
    assert ret1.stderr == ""

    ret = script_runner.run("hop", "configure", "locate")
    assert ret.success
    assert ret.stderr == ""


def test_cli_version(script_runner):
    ret = script_runner.run("hop", "version", "--help")
    assert ret.success
    assert ret.stderr == ""

    ret = script_runner.run("hop", "version")
    assert ret.success
    assert ret.stderr == ""


def test_error_verbosity(script_runner):
    simple = script_runner.run("hop", "subscribe", "BAD-URL")
    assert not simple.success
    assert simple.stdout == ""
    assert "Traceback (most recent call last)" not in simple.stderr
    assert simple.stderr.startswith("hop: ")

    detailed = script_runner.run("hop", "--debug", "subscribe", "BAD-URL")
    assert not detailed.success
    assert detailed.stdout == ""
    assert "Traceback (most recent call last)" in detailed.stderr
