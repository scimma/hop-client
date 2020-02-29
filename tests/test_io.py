#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests the io utilities"


from unittest.mock import patch, mock_open

from scimma.client import Stream


def test_stream(circular_msg, circular_text):
    gcn_mock = mock_open(read_data=circular_text)
    with patch("scimma.client.io.streaming.open", mock_open()) as mock_stream:
        broker_url = "kafka://hostname:port/gcn"
        format = "json"

        stream = Stream(format="json", start_at="beginning", config={})

        # verify defaults are stored correctly
        assert stream._options["format"] == "json"
        assert stream._options["start_at"] == "beginning"
        assert stream._options["config"] == {}

        with stream.open(broker_url, "w") as s:
            s.write(circular_msg)

        # verify GCN was processed
        mock_stream.assert_called_with(broker_url, "w")
