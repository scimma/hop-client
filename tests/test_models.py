from unittest.mock import patch, mock_open
import json
import pytest

from hop import models


def test_MessageModel():
    # derived classes must implement their own load() logic, and the base implementation has no
    # useful functionality
    with pytest.raises(NotImplementedError):
        models.MessageModel.load(None)


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

    # conversion to a string should produce valid JSON which should parse to match the original
    # event data
    as_string = str(voevent)
    reparsed = json.loads(as_string)
    assert reparsed["ivorn"] == voevent.ivorn
    assert reparsed["role"] == voevent.role
    assert reparsed["version"] == voevent.version
    assert reparsed["Who"]["Date"] == voevent.Who["Date"]
    assert reparsed["Description"] == voevent.Description
    assert reparsed["WhereWhen"]["ObsDataLocation"]["ObservatoryLocation"]["id"] \
        == voevent.WhereWhen["ObsDataLocation"]["ObservatoryLocation"]["id"]


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

        # conversion to a string should produce email formatted data which should parse to match the
        # original event data
        as_string = str(gcn)
        reparsed = models.GCNCircular.load(as_string)
        assert reparsed.header == gcn.header
        assert reparsed.body == gcn.body


def test_blob(blob_text, blob_msg):
    with patch("builtins.open", mock_open(read_data=blob_text)):
        blob_file = "example_blob.txt"
        with open(blob_file, "r") as f:
            blob = models.Blob.load(f)

        # verify blob text is correct
        assert blob.content == blob_msg["content"]

        # verify wrapper format is correct
        assert blob.serialize()["format"] == "blob"

        # if the blob was holding a string, conversion to a string should just return that
        as_string = str(blob)
        assert as_string == blob.content
        # round-tripping should work iff the blob was holding a string all along
        assert models.Blob.load(as_string) == blob

    # conversion to a dictionary should just result in a key 'content' under which the original
    # contents are stored, regardless of whether the missing_schema flag is set
    blob_ms = models.Blob("foo", True)
    blob_ms_dict = blob_ms.asdict()
    assert type(blob_ms_dict) is dict
    assert blob_ms_dict["content"] == blob_ms.content
    # if the flag was set, this should separately be include in the resulting dictionary
    assert blob_ms_dict["missing_schema"] is True

    blob_ws = models.Blob("bar")
    blob_ws_dict = blob_ws.asdict()
    assert type(blob_ws_dict) is dict
    assert blob_ws_dict["content"] == blob_ws.content
