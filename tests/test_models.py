from unittest.mock import patch, mock_open

from hop import models


def test_voevent(voevent_fileobj):
    voevent = models.VOEvent.load(voevent_fileobj)

    # check a few attributes
    assert voevent.ivorn == "ivo://gwnet/LVC#S200302c-1-Preliminary"
    assert voevent.role == "observation"
    assert voevent.version == "2.0"

    assert voevent.Who["Date"] == "2020-03-02T02:00:09"
    assert voevent.Description == "Report of a candidate gravitational wave event"
    assert voevent.WhereWhen["ObsDataLocation"]["ObservatoryLocation"]["id"] == "LIGO Virgo"

    # verify wrapper format is correct
    assert voevent.serialize()["format"] == "voevent"


def test_gcn_circular(circular_text, circular_msg):
    with patch("builtins.open", mock_open(read_data=circular_text)):
        gcn_file = "example.gcn3"
        with open(gcn_file, "r") as f:
            gcn = models.GCNCircular.load(f)

        # verify parsed GCN structure is correct
        assert gcn.header["title"] == circular_msg["header"]["title"]
        assert gcn.header["number"] == circular_msg["header"]["number"]
        assert gcn.header["subject"] == circular_msg["header"]["subject"]
        assert gcn.header["date"] == circular_msg["header"]["date"]
        assert gcn.header["from"] == circular_msg["header"]["from"]

        assert gcn.body == circular_msg["body"]

        # verify wrapper format is correct
        assert gcn.serialize()["format"] == "circular"


def test_blob(blob_text, blob_msg):
    with patch("builtins.open", mock_open(read_data=blob_text)):
        blob_file = "example_blob.txt"
        with open(blob_file, "r") as f:
            blob = models.MessageBlob.load(f)

        # verify blob text is correct
        assert blob.content == blob_msg["content"]

        # verify wrapper format is correct
        assert blob.serialize()["format"] == "blob"
