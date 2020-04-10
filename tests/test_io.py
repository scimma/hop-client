#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests the io utilities"


from unittest.mock import patch, mock_open

from hop import Stream


def test_stream(circular_msg, circular_text):
    with patch("hop.io.streaming.open", mock_open()) as mock_stream:
        broker_url = "kafka://hostname:port/gcn"
        format = "json"
        start_at = "beginning"
        config = {}

        stream = Stream(format=format, start_at=start_at, config=config)

        # verify defaults are stored correctly
        assert stream._options["format"] == format
        assert stream._options["start_at"] == start_at
        assert stream._options["config"] == config

        with stream.open(broker_url, "w") as s:
            s.write(circular_msg)

        # verify GCN was processed
        mock_stream.assert_called_with(
            broker_url, "w", format=format, start_at=start_at, config=config
        )
