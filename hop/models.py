from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
import email
import fastavro
from inspect import isgenerator
from io import BytesIO
import json
import re
from typing import Any, Dict, List, Union
import collections.abc

import xmltodict

from . import plugins
from . import avro_utils

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


def format_name(cls):
    if hasattr(cls, "format_name"):
        return cls.format_name
    return cls.__name__.lower()


@dataclass
class MessageModel(ABC):
    """An abstract message model.

    """

    def __bytes__(self):
        """Produce the canonical representation of this message.
        """
        # by default, encode using JSON
        return json.dumps(asdict(self)).encode("utf-8")

    def serialize(self):
        """Wrap the message with its format and content, for transmission to Kafka.

        Returns:
            A dictionary with "format" and "content" keys.
            The value stored under "format" is the format label.
            The value stored under "content" is the actual encoded data.

        """
        return {"format": format_name(type(self)),
                "content": bytes(self),
                }

    @classmethod
    def deserialize(cls, data):
        """Unwrap a message produced by serialize() (the "content" value).

        Returns:
            An instance of the model class.

        """
        # corresponding to the default serialize implementation, upack from JSON
        return cls(**json.loads(data.decode("utf-8")))

    @classmethod
    def load_file(cls, filename):
        """Create a new message model from a file.

        Args:
            filename: The path to a file.

        Returns:
            The message model.

        """
        with open(filename, "r") as f:
            return cls.load(f)

    @classmethod
    @abstractmethod
    def load(cls, input_):
        """Create a new message model from a file object or string.
        This base implementation has no functionality and should not be called.

        Args:
            input_: A file object or string.

        Returns:
            The message model.

        """
        raise NotImplementedError("MessageModel.load() should not be called")


@dataclass
class VOEvent(MessageModel):
    """Defines a VOEvent 2.0 structure.

    Implements the schema defined by:
        http://www.ivoa.net/Documents/VOEvent/20110711/

    """

    ivorn: str
    role: str = field(default_factory=lambda: "observation")
    version: str = field(default_factory=lambda: "2.0")

    Who: dict = field(default_factory=dict)
    What: dict = field(default_factory=dict)
    WhereWhen: dict = field(default_factory=dict)
    How: dict = field(default_factory=dict)
    Why: dict = field(default_factory=dict)
    Citations: dict = field(default_factory=dict)
    Description: dict = field(default_factory=dict)
    Reference: dict = field(default_factory=dict)

    _raw: bytes = field(default_factory=bytes)

    @staticmethod
    def dict_factory(input: list):
        """Create a dictionary from a list of tuples, filtering out keys which begin with
        underscores.

        Args:
            input: An existing dictionary.

        Returns:
            A copy of input without any keys beginning with underscores.
        """
        return {k: v for (k, v) in input if not k.startswith('_')}

    @staticmethod
    def is_attribute(key: str):
        """Identify keys which are properly XML attributes, as opposed to sub-elements.
        This works because in the VOEvent v2.0 schema there are no elements and attributes which
        share names.

        Args:
            key: The key to check.

        Returns:
            True if the key corresponds to an attribute, False if it matches an element type.
        """
        # extracted from http://www.ivoa.net/xml/VOEvent/VOEvent-v2.0.xsd
        known_attributes = {"cite", "coord_system_id", "dataType", "expires", "id", "importance",
                            "ivorn", "meaning", "mimetype", "name", "probability", "relation",
                            "role", "type", "ucd", "unit", "uri", "utype", "value", "version"}
        return key in known_attributes

    @classmethod
    def label_attributes_list(cls, d: list, attr_prefix: str = "@"):
        """Recursively add a prefix to dictionary keys so that xmltodict.unparse can restore
        attributes correctly.

        Args:
            d: A list in which to patch attribute names.
            attr_prefix: The prefix to add to keys which should be attributes.

        Returns:
            None; d is modified in-place.
        """
        for item in d:
            if isinstance(item, dict):
                cls.label_attributes_dict(item, attr_prefix=attr_prefix)
            elif isinstance(item, list):
                cls.label_attributes_list(item, attr_prefix=attr_prefix)

    @classmethod
    def label_attributes_dict(cls, d: dict, attr_prefix: str = "@"):
        """Recursively add a prefix to dictionary keys so that xmltodict.unparse can restore
        attributes correctly.

        Args:
            d: A dictionary in which to patch attribute names.
            attr_prefix: The prefix to add to keys which should be attributes.

        Returns:
            None; d is modified in-place.
        """
        to_fix = set()
        for k, v in d.items():
            if isinstance(v, dict):
                cls.label_attributes_dict(v, attr_prefix=attr_prefix)
            elif isinstance(v, list):
                cls.label_attributes_list(v, attr_prefix=attr_prefix)

            if cls.is_attribute(k):
                to_fix.add(k)
        for k in to_fix:
            d[attr_prefix + k] = d[k]
            del d[k]

    def __str__(self):
        return json.dumps(asdict(self, dict_factory=self.dict_factory), indent=2)

    def __bytes__(self):
        return self._raw

    @staticmethod
    def ensure_bytes(data_source):
        """Turn a string, a file-like object, or a generator into bytes. This is useful if the data
        needs to be used more than once, e.g. parsing and being stored, when the data source may be
        single-pass only.

        Args:
            data_source: A source of data, which may already be bytes, or a string, file-like
                         object, or a generator.

        Returns:
            The bytes extracted or converted from data_source.
        """
        if isinstance(data_source, str):
            return data_source.encode("utf-8")
        elif hasattr(data_source, 'read'):
            return data_source.read()
        elif isgenerator(data_source):
            input_data = BytesIO()
            for chunk in data_source:
                input_data.write(chunk)
            return input_data.getvalue()
        return data_source

    @classmethod
    def deserialize(cls, raw):
        """Unwrap a message produced by serialize() (the "content" value).

        Returns:
            An instance of the model class.

        """
        raw = cls.ensure_bytes(raw)
        try:
            return cls.load(raw)
        except Exception as e:
            try:  # old versions converted the XML to JSON, so see if we can undo that
                result = cls(**json.loads(raw.decode("utf-8")))
                rd = asdict(result, dict_factory=cls.dict_factory)
                cls.label_attributes_dict(rd)
                result._raw = xmltodict.unparse({"voe:VOEvent": rd}, attr_prefix="@",
                                                short_empty_elements=True).encode("utf-8")
                return result
            except Exception:
                # if something went wrong with the fall-back approach, report the original error
                raise e

    @classmethod
    def load(cls, xml_input):
        """Create a new VOEvent from an XML-formatted VOEvent.

        Args:
            xml_input: A file object, string, or generator.

        Returns:
            The VOEvent.

        """
        xml_input = cls.ensure_bytes(xml_input)

        vo = xmltodict.parse(xml_input, attr_prefix="")

        # enter root and remove XML-specific namespaces
        data = {k: v for k, v in vo["voe:VOEvent"].items() if ":" not in k}
        data["_raw"] = xml_input
        return cls(**data)

    @classmethod
    def load_file(cls, filename):
        """Create a new VOEvent from an XML-formatted VOEvent file.

        Args:
            filename: Name of the VOEvent file.

        Returns:
            The VOEvent.

        """
        with open(filename, "rb") as f:
            return cls.load(f)


@dataclass
class GCNTextNotice(MessageModel):
    """A GCN Notice in the plain text format.

    The original message data is stored in the raw property,
    and the parsed message is available via the fields property,
    which is a dictionary.
    """

    raw: bytes
    fields: dict

    def __bytes__(self):
        return self.raw

    @classmethod
    def deserialize(cls, data):
        fields = {}
        line_pattern = re.compile("([^:]*): *(.*)")
        last_key = None
        for line in data.decode("utf-8").strip().splitlines():
            m = re.match(line_pattern, line)
            if m is None:
                # if the line does not match the "key: value" format,
                # we assume it is a continuation of a previous value
                if last_key is not None:
                    fields[last_key] += '\n' + line.lstrip()
            else:
                # if the key is repeated,
                # we treat the new value as a continuation of the previous one
                last_key = m.group(1).strip().lower()
                value = m.group(2).strip()
                if last_key in fields:
                    fields[last_key] += '\n' + value
                else:
                    fields[last_key] = value

        return cls(raw=data, fields=fields)

    @classmethod
    def load(cls, input):
        if hasattr(input, "read"):
            raw = input.read()
        else:
            raw = input
        return cls.deserialize(raw)

    @classmethod
    def load_file(cls, filename):
        with open(filename, "rb") as f:
            return cls.load(f)


@dataclass
class GCNCircular(MessageModel):
    """Defines a GCN Circular structure.

    The parsed GCN circular is formatted as a dictionary with
    the following schema:

        {'headers': {'title': ..., 'number': ..., ...}, 'body': ...}

    """

    header: dict
    body: str
    format_name = "circular"

    def __str__(self):
        headers = [(name.upper() + ":").ljust(9) + val for name, val in self.header.items()]
        return "\n".join(headers + ["", self.body])

    def __bytes__(self):
        return str(self).encode("utf-8")

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" keys.
            The value stored under "format" is the format label.
            The value stored under "content" is the actual encoded data.

        """
        # by default, encode using JSON
        return {"format": format_name(type(self)),
                "content": json.dumps(asdict(self)).encode("utf-8"),
                }

    @classmethod
    def load(cls, email_input):
        """Create a new GCNCircular from an RFC 822 formatted circular.

        Args:
            email_input: A file object or string.

        Returns:
            The GCNCircular.

        """
        if hasattr(email_input, "read"):
            message = email.message_from_file(email_input)
        else:
            message = email.message_from_string(email_input)

        # format gcn circular into header/body
        return cls(
            header={title.lower(): content for title, content in message.items()},
            body=message.get_payload(),
        )


@dataclass
class Blob(MessageModel):
    """Defines an opaque message blob.

    """

    content: bytes

    def __str__(self):
        return str(self.content)

    def __bytes__(self):
        return self.content

    @classmethod
    def deserialize(cls, data):
        return cls(content=data)

    @classmethod
    def load(cls, blob_input):
        """Create a blob message from input data.

        Args:
            blob_input: The unstructured message data (bytes) or file object.

        Returns:
            The Blob.

        """
        if hasattr(blob_input, "read"):
            raw = blob_input.read()
        else:
            raw = blob_input
        return cls(content=raw)

    @classmethod
    def load_file(cls, filename):
        """Create a new message model from a file.

        Args:
            filename: The path to a file.

        Returns:
            The message model.

        """
        # arbitrary data should not be subjected to line-ending conversion, etc.
        with open(filename, "rb") as f:
            return cls.load(f)


@dataclass
class JSONBlob(MessageModel):
    """Defines an unformatted message blob.

    """

    content: JSONType
    format_name = "json"

    def __str__(self):
        return str(self.content)

    def __bytes__(self):
        return json.dumps(self.content).encode("utf-8")

    @classmethod
    def deserialize(cls, data):
        return cls(content=json.loads(data.decode("utf-8")))

    @classmethod
    def load(cls, blob_input):
        """Create a blob message from input text.

        Args:
            blob_input: The unstructured message text or file object.

        Returns:
            The Blob.

        """
        if hasattr(blob_input, "read"):
            raw = blob_input.read()
        else:
            raw = blob_input
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls(content=json.loads(raw))


@dataclass
class AvroBlob(MessageModel):
    """Defines an unformatted message blob.

    """

    # serializing as Avro supports essentially the same types as JSON
    content: Union[JSONType, List[JSONType]]
    schema: dict = None
    format_name = "avro"
    single_record: bool = True

    def __init__(self, content: Union[JSONType, List[JSONType]], schema: dict = None,
                 single_record: bool = True):
        if not single_record and not isinstance(content, collections.abc.Sequence):
            raise TypeError("AvroBlob requires content to be a sequence of records when "
                            "single_record = False")
        self.content = content
        self.schema = schema
        self.single_record = single_record

    def __str__(self):
        return str(self.content)

    def __bytes__(self):
        if self.single_record:
            records = [self.content]
        else:
            records = self.content

        if self.schema is None:  # make up an ad-hoc schema
            self.schema = avro_utils.SchemaGenerator().find_common_type(records)

        buffer = BytesIO()
        fastavro.writer(buffer,
                        self.schema,
                        records
                        )
        return buffer.getvalue()

    @classmethod
    def _read_avro(cls, stream, single_record=True):
        extracted = []
        reader = fastavro.reader(stream)
        for record in reader:
            extracted.append(record)
        if single_record:
            if len(extracted) > 1:
                raise TypeError("AvroBlob requires content to be a single "
                                "record when single_record = True, number of "
                                "records in message being deserialized is "
                                f"{len(extracted)}.")
            content = extracted[0]
        else:
            content = extracted
        return cls(content=content, schema=reader.writer_schema, single_record=single_record)

    @classmethod
    def deserialize(cls, data, single_record=True):
        return cls._read_avro(BytesIO(data), single_record=single_record)

    @classmethod
    def load(cls, blob_input, single_record=True):
        """Create a blob message from input avro data.

        Args:
            blob_input: The encoded Avro data or file object.
            single_record: True if input avro data only contains one record.
              True by default.

        Returns:
            The Blob.

        """
        if hasattr(blob_input, "read"):
            raw = blob_input
        else:
            if not isinstance(blob_input, bytes):
                raise TypeError
            raw = BytesIO(blob_input)
        return cls._read_avro(raw, single_record=single_record)

    @classmethod
    def load_file(cls, filename, single_record=True):
        """Create a new message model from a file.

        Args:
            filename: The path to a file.
            single_record: True if input avro data only contains one record.
              True by default.

        Returns:
            The message model.

        """
        with open(filename, "rb") as f:
            return cls.load(f, single_record=single_record)

    def __eq__(self, other):
        if type(self) is not type(other):
            return False
        # compare only content, not schemas
        return self.content == other.content

    def __hash__(self):
        raise NotImplementedError("AvroBlob objects are not hashable")


@dataclass
class ExternalMessage(MessageModel):
    """Defines a message which refers to data stored externally at some URL
    """

    url: str
    format_name = "external"

    @classmethod
    def load(cls, input):
        """Create a blob message from input text.

        Args:
            blob_input: The unstructured message text or file object.

        Returns:
            The Blob.

        """
        if hasattr(input, "read"):
            raw = input.read()
        else:
            raw = input
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        decoded = json.loads(raw)
        return cls(url=decoded["url"])


@plugins.register
def get_models():
    model_classes = [
        VOEvent,
        GCNTextNotice,
        GCNCircular,
        Blob,
        JSONBlob,
        AvroBlob,
        ExternalMessage,
    ]
    return {format_name(cls): cls for cls in model_classes}
