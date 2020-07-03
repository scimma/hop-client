from unittest.mock import patch, mock_open
import sys

import pytest

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
    if sys.version_info < (3, 7):
        if message_format == "voevent":
            pytest.skip("requires python3.7 or higher")

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
            "hop", "publish", broker_url, "-f", message_format.upper(), test_file, "--no-auth",
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
    ret1 = script_runner.run("hop", "auth", "--help")
    assert ret1.success
    assert ret1.stderr == ""

    ret = script_runner.run("hop", "auth", "locate")
    assert ret.success
    assert ret.stderr == ""


def test_cli_version(script_runner):
    ret = script_runner.run("hop", "version", "--help")
    assert ret.success
    assert ret.stderr == ""

    ret = script_runner.run("hop", "version")
    assert ret.success
    assert ret.stderr == ""
