from unittest.mock import patch, mock_open
from io import BytesIO
import json
from pathlib import Path
import pytest

import fastavro

from hop import models
from conftest import message_parameters_dict_data


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


def test_voevent_deserialization():
    mpdd = message_parameters_dict_data

    v2 = models.VOEvent.deserialize(mpdd["voevent"]["model_text"])
    assert isinstance(v2, models.VOEvent)

    v1 = models.VOEvent.deserialize(mpdd["voevent-encoded-as-json"]["model_text"])
    assert isinstance(v1, models.VOEvent)

    with pytest.raises(Exception) as ex:
        vx = models.VOEvent.deserialize(b"\tNot XML or JSON\t")


def test_voevent_dict_factory():
    d1 = {"foo": 1, "bar": 2, "baz": 3}
    o1 = models.VOEvent.dict_factory(d1.items())
    assert o1 == d1

    d2 = {"foo": 1, "_bar": 2, "baz": 3, "_quux": 4}
    o2 = models.VOEvent.dict_factory(d2.items())
    assert len(o2) == 2
    assert "foo" in o2
    assert "_bar" not in o2
    assert "baz" in o2
    assert "_quux" not in o2


VOEvent_attribute_names = {"cite", "coord_system_id", "dataType", "expires", "id", "importance",
                           "ivorn", "meaning", "mimetype", "name", "probability", "relation",
                           "role", "type", "ucd", "unit", "uri", "utype", "value", "version"}


VOEvent_element_names = {"AstroCoordSystem", "AstroCoords", "Author", "AuthorIVORN", "C1", "C2",
                         "C3", "Citations", "Concept", "Data", "Date", "Description", "Error",
                         "Error2Radius", "EventIVORN", "Field", "Group", "How", "ISOTime",
                         "Inference", "Name", "Name1", "Name2", "Name3", "ObsDataLocation",
                         "ObservationLocation", "ObservatoryLocation", "Param", "Position2D",
                         "Position3D", "Reference", "TD", "TR", "Table", "Time", "TimeInstant",
                         "TimeOffset", "TimeScale", "VOEvent", "Value", "Value2", "Value3",
                         "What", "WhereWhen", "Who", "Why", "contactEmail", "contactName",
                         "contactPhone", "contributor", "logoURL", "shortName", "title"}


def test_voevent_label_attributes_list():
    ta = [{n: 0} for n in VOEvent_attribute_names]
    models.VOEvent.label_attributes_list(ta)
    for entry in ta:
        for key in entry.keys():
            assert key.startswith('@')
            assert key[1:] in VOEvent_attribute_names

    nta = [[{n: 0} for n in VOEvent_attribute_names]]
    models.VOEvent.label_attributes_list(nta)
    assert len(nta) == 1
    for entry in nta[0]:
        for key in entry.keys():
            assert key.startswith('@')
            assert key[1:] in VOEvent_attribute_names

    te = [{n: 0} for n in VOEvent_element_names]
    models.VOEvent.label_attributes_list(te)
    for entry in te:
        for key in entry.keys():
            assert not key.startswith('@')
            assert key in VOEvent_element_names

    nte = [[{n: 0} for n in VOEvent_element_names]]
    models.VOEvent.label_attributes_list(nte)
    assert len(nte) == 1
    for entry in nte[0]:
        for key in entry.keys():
            assert not key.startswith('@')
            assert key in VOEvent_element_names


def test_voevent_label_attributes_dict():
    ta = {n: 0 for n in VOEvent_attribute_names}
    models.VOEvent.label_attributes_dict(ta)
    for key in ta.keys():
        assert key.startswith('@')
        assert key[1:] in VOEvent_attribute_names

    te = {n: 0 for n in VOEvent_element_names}
    models.VOEvent.label_attributes_dict(te)
    for key in te.keys():
        assert not key.startswith('@')
        assert key in VOEvent_element_names


def test_voevent_ensure_bytes():
    b = b"abcdef\x00\x01ghij"
    assert models.VOEvent.ensure_bytes(b) == b

    s = "foo bar baz"
    assert models.VOEvent.ensure_bytes(s) == s.encode("utf-8")

    f = BytesIO(b)
    assert models.VOEvent.ensure_bytes(f) == b

    assert models.VOEvent.ensure_bytes(c.to_bytes(1, byteorder="big") for c in b) == b


def test_gcn_text_notice(gcn_text_notice_fileobj, gcn_text_notice_data):
    text_notice = models.GCNTextNotice.load(gcn_text_notice_fileobj)

    # the object should retain the raw data unchanged for lossless round-tripping
    assert text_notice.raw == gcn_text_notice_data

    # check the data was parsed sensibly
    expected_fields = ["title", "notice_date", "notice_type", "stream", "run_num", "event_num",
                       "src_ra", "src_dec", "src_error", "src_error50", "discovery_date",
                       "discovery_time", "revision", "energy", "signalness", "far", "sun_postn",
                       "sun_dist", "moon_postn", "moon_dist", "gal_coords", "ecl_coords",
                       "comments"]
    for field in expected_fields:
        assert field in text_notice.fields

    expected_title = "GCN/AMON NOTICE"
    assert text_notice.fields["title"] == expected_title
    expected_energy = "1.2126e+02 [TeV]"
    assert text_notice.fields["energy"] == expected_energy
    # multi-line values should be accumulated
    expected_src_ra = \
        "103.7861d {+06h 55m 09s} (J2000),\n" \
        "104.1106d {+06h 56m 27s} (current),\n" \
        "103.1176d {+06h 52m 28s} (1950)"
    assert text_notice.fields["src_ra"] == expected_src_ra
    # repeated keys should have their values accumulated
    expected_comments = \
        "IceCube Bronze event.\n" \
        "The position error is statistical only, there is no systematic added."
    assert text_notice.fields["comments"] == expected_comments


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
        blob = models.Blob.load_file(blob_file)

        # verify blob text is correct
        assert blob.content == blob_msg["content"]

        # verify wrapper format is correct
        assert blob.serialize()["format"] == "blob"

        assert bytes(blob) == blob_msg["content"]
        assert models.Blob.load(bytes(blob)) == blob


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

    # test single_record = False codepath
    with patch("builtins.open", mock_open(read_data=avro_data)):
        avro2 = models.AvroBlob.load(avro_data, single_record=False)

    # verify data is correct after deserializing
    assert avro2.content == [avro_data_raw]

    # verify wrapper format is correct
    assert avro2.serialize()["format"] == "avro"

    # verify that serializing again produces the same result
    serialized2 = avro2.serialize()["content"]
    assert models.AvroBlob.load(serialized2, single_record=False) == avro2

    # test that serializing the same object tree without a schema produces equivalent results
    avro3 = models.AvroBlob(avro_data_raw)
    serialized3 = avro3.serialize()["content"]

    # serialized2 may not be the same as avro_data because the schemas can differ
    assert models.AvroBlob.load(serialized3) == avro3


def test_avro_invalid_data_type_serialization():
    # data must be a sequence of records if single_record = False
    with pytest.raises(TypeError) as te:
        models.AvroBlob({1: "abc", False: "def"}, single_record=False).serialize()
    assert "AvroBlob requires content to be a sequence of records when " \
           "single_record = False" in str(te.value)

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


def test_avro_multi_record_deserialization():
    original = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    outio = BytesIO()
    schema = {'type': 'array', 'items': 'long'}
    fastavro.writer(outio, schema, original)
    raw = outio.getvalue()

    # default behavior should be to require a single record
    with pytest.raises(TypeError) as te:
        loaded = models.AvroBlob.load(raw)
    assert "AvroBlob requires content to be a single record" in str(te.value)

    # allowing multiple records should load as such
    loaded = models.AvroBlob.load(raw, single_record=False)
    assert loaded.content == original


def test_externalmessage():
    inputs = [
        '{"url": "http://example.com"}',
        b'{"url": "http://example.com"}',
    ]
    for input in inputs:
        for read in [True, False]:
            if read:
                with patch("builtins.open", mock_open(read_data=input)):
                    m = models.ExternalMessage.load(open("notarealfile"))
            else:
                m = models.ExternalMessage.load(input)
            assert m.url == "http://example.com"

    with pytest.raises(json.JSONDecodeError):
        models.ExternalMessage.load("\tnot valid JSON\t")


@pytest.mark.parametrize("model", [
    models.Blob,
    models.JSONBlob,
    models.AvroBlob,
    models.GCNTextNotice,
    models.GCNCircular,
    models.VOEvent,
])
def test_model_roundtrip(model, tmpdir):
    model_name = models.format_name(model)
    shared_datadir = Path("tests/data")
    orig_path = shared_datadir / "test_data" / message_parameters_dict_data[model_name]["test_file"]
    instance1 = model.load_file(orig_path)
    test_path = tmpdir.join("rewritten.dat")
    with open(test_path, "wb") as out_file:
        out_file.write(bytes(instance1))
    instance2 = model.load_file(test_path)
    assert instance1 == instance2
