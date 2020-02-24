#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests entry points"


from unittest.mock import patch, mock_open
import pytest

from scimma.client import __version__

from test_publish import GCN_CIRCULAR


@pytest.mark.script_launch_mode("subprocess")
def test_cli_scimma(script_runner):
    ret = script_runner.run("scimma", "--help")
    assert ret.success

    ret = script_runner.run("scimma", "--version")
    assert ret.success

    assert ret.stdout == f"scimma-client version {__version__}\n"
    assert ret.stderr == ""


def test_cli_publish(script_runner):
    ret = script_runner.run("scimma", "publish", "--help")
    assert ret.success

    gcn_mock = mock_open(read_data=GCN_CIRCULAR)
    with patch("scimma.client.publish.open", gcn_mock) as mock_file, patch(
        "scimma.client.publish.stream.open", mock_open()
    ) as mock_stream:

        gcn_file = "example.gcn3"
        broker_url = "kafka://hostname:port/gcn"
        ret = script_runner.run("scimma", "publish", "-b", broker_url, gcn_file)

        # verify CLI output
        assert ret.success
        assert ret.stderr == ""

        # verify GCN was processed
        mock_file.assert_called_with(gcn_file, "r")
        mock_stream.assert_called_with(broker_url, "w", format="json", config=None)
