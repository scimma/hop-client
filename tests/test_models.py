#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests models"


from unittest.mock import patch, mock_open

from hop import models


def test_voevent(voevent_fileobj):
    voevent = models.VOEvent.from_xml(voevent_fileobj)

    # check a few attributes
    assert voevent.ivorn == "ivo://gwnet/LVC#S200302c-1-Preliminary"
    assert voevent.role == "observation"
    assert voevent.version == "2.0"

    assert voevent.Who["Date"] == "2020-03-02T02:00:09"
    assert voevent.Description == "Report of a candidate gravitational wave event"
    assert (
        voevent.WhereWhen["ObsDataLocation"]["ObservatoryLocation"]["id"]
        == "LIGO Virgo"
    )


def test_gcn_circular(circular_text, circular_msg):
    with patch("builtins.open", mock_open(read_data=circular_text)) as mock_file:
        gcn_file = "example.gcn3"
        with open(gcn_file, "r") as f:
            gcn = models.GCNCircular.from_email(f)

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
