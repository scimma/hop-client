from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
import email
import fastavro
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
        return json.dumps(asdict(self)).encode("utf-8")

    def serialize(self):
        """Wrap the message with its format and content, for transmission of Kafka.

        Returns:
            A dictionary with "format" and "content" keys.
            The value stored under "format" is the format label.
            The value stored under "content" is the actual encoded data.

        """
        # by default, encode using JSON
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
    role: str = "observation"
    version: str = "2.0"

    Who: dict = field(default_factory=dict)
    What: dict = field(default_factory=dict)
    WhereWhen: dict = field(default_factory=dict)
    How: dict = field(default_factory=dict)
    Why: dict = field(default_factory=dict)
    Citations: dict = field(default_factory=dict)
    Description: dict = field(default_factory=dict)
    Reference: dict = field(default_factory=dict)

    def __str__(self):
        return json.dumps(asdict(self), indent=2)

    def __bytes__(self):
        # TODO: this isn't really suitable, as the output should be the original XML format
        # That requires some massaging of the data to restore information discarded by xmltodict.
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
    def load(cls, xml_input):
        """Create a new VOEvent from an XML-formatted VOEvent.

        Args:
            xml_input: A file object, string, or generator.

        Returns:
            The VOEvent.

        """
        vo = xmltodict.parse(xml_input, attr_prefix="")

        # enter root and remove XML-specific namespaces
        return cls(**{k: v for k, v in vo["voe:VOEvent"].items() if ":" not in k})

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
