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


def test_gcn_circular(circular_text, circular_msg, circular_data_raw):
    with patch("builtins.open", mock_open(read_data=circular_text)):
        gcn_file = "example.gcn3"
        with open(gcn_file, "r") as f:
            gcn = models.GCNCircular.load(f)

        # verify parsed GCN structure is correct
        assert gcn.header["title"] == circular_data_raw["header"]["title"]
        assert gcn.header["number"] == circular_data_raw["header"]["number"]
        assert gcn.header["subject"] == circular_data_raw["header"]["subject"]
        assert gcn.header["date"] == circular_data_raw["header"]["date"]
        assert gcn.header["from"] == circular_data_raw["header"]["from"]

        assert gcn.body == circular_data_raw["body"]

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


def test_avro(avro_data_raw, avro_data, avro_msg):
    with patch("builtins.open", mock_open(read_data=avro_data)):
        avro = models.AvroBlob.load(avro_data)

    # verify data is correct after deserializing
    assert avro.content == avro_data_raw

    # verify wrapper format is correct
    assert avro.serialize()["format"] == "avro"

    # verify that serializing again produces the same result
    serialized = avro.serialize()["content"]
    assert models.AvroBlob.load(serialized) == avro

    # test that serializing the same object tree without a schema produces equivalent results
    avro2 = models.AvroBlob(avro_data_raw)
    serialized2 = avro2.serialize()["content"]
    # serialized2 may not be the same as avro_data because the schemas can differ
    assert models.AvroBlob.load(serialized2) == avro


def test_avro_invalid_data_type_serialization():
    # data must be a sequence of records
    with pytest.raises(TypeError) as te:
        models.AvroBlob({1: "abc", False: "def"}).serialize()
    assert "AvroBlob requires content to be a sequence of records" in str(te.value)

    # dictionary keys can only be strings
    with pytest.raises(ValueError) as ve:
        models.AvroBlob([{1: "abc", False: "def"}]).serialize()
    assert "Dictionaries with non-string keys cannot be represented as Avro" in str(ve.value)

    # arbitrary, user-defined types are not supported in Avro
    with pytest.raises(ValueError) as ve:
        models.AvroBlob([models.Blob(b"somedata")]).serialize()
    assert "Unable to assign an Avro type to value of type <class 'hop.models.Blob'>" \
        in str(ve.value)


def test_avro_invalid_data_type_deserialization():
    # only bytes objects are accepted, not strings
    with pytest.raises(TypeError):
        models.AvroBlob.load("some string")


def test_avro_operators(avro_data, avro_data_raw):
    avro_with_schema = models.AvroBlob.load(avro_data)
    avro_no_schema = models.AvroBlob(avro_data_raw)

    # equality should not consider schemas
    assert avro_with_schema == avro_no_schema
    # not equal to objects of any other type
    assert avro_with_schema != avro_data_raw
    # hashing is not implemented because python fails to implement it for dict
    with pytest.raises(NotImplementedError):
        hash(avro_with_schema)
