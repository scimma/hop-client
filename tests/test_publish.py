#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests the publish utilities"


from unittest.mock import patch, mock_open

from hop import publish


def test_read_parse_gcn(circular_text, circular_msg):
    with patch("builtins.open", mock_open(read_data=circular_text)) as mock_file:
        gcn_file = "example.gcn3"
        gcn = publish.read_parse_gcn(gcn_file)

        # verify GCN was read in
        assert open(gcn_file).read() == circular_text
        mock_file.assert_called_with(gcn_file)

        # verify parsed GCN structure is correct
        assert gcn.header["title"] == circular_msg["header"]["title"]
        assert gcn.header["number"] == circular_msg["header"]["number"]
        assert gcn.header["subject"] == circular_msg["header"]["subject"]
        assert gcn.header["date"] == circular_msg["header"]["date"]
        assert gcn.header["from"] == circular_msg["header"]["from"]

        assert gcn.body == circular_msg["body"]
