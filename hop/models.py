from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
import email
import fastavro
from io import BytesIO
import json
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

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" keys.
            The value stored under "format" is the format label.
            The value stored under "content" is the actual encoded data.

        """
        # by default, encode using JSON
        return {"format": format_name(type(self)),
                "content": json.dumps(asdict(self)).encode("utf-8")
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

    def serialize(self):
        return {"format": format_name(type(self)), "content": self.content}

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


@dataclass
class JSONBlob(MessageModel):
    """Defines an unformatted message blob.

    """

    content: JSONType
    format_name = "json"

    def __str__(self):
        return str(self.content)

    def serialize(self):
        return {"format": format_name(type(self)),
                "content": json.dumps(self.content).encode("utf-8")
                }

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

    content: List[JSONType]  # serializing as Avro supports essentially the same types as JSON
    schema: dict = None
    format_name = "avro"

    def __init__(self, content: List[JSONType], schema: dict = None):
        if not isinstance(content, collections.abc.Sequence):
            raise TypeError("AvroBlob requires content to be a sequence of records")
        self.content = content
        self.schema = schema

    def __str__(self):
        return str(self.content)

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" keys.

        """
        if self.schema is None:  # make up an ad-hoc schema
            self.schema = avro_utils.SchemaGenerator().find_common_type(self.content)

        stringio = BytesIO()
        fastavro.writer(stringio,
                        self.schema,
                        self.content
                        )
        return {"format": format_name(type(self)), "content": stringio.getvalue()}

    @classmethod
    def _read_avro(cls, stream):
        extracted = []
        reader = fastavro.reader(stream)
        for record in reader:
            extracted.append(record)
        return cls(content=extracted, schema=reader.writer_schema)

    @classmethod
    def deserialize(cls, data):
        return cls._read_avro(BytesIO(data))

    @classmethod
    def load(cls, blob_input):
        """Create a blob message from input avro data.

        Args:
            blob_input: The encoded Avro data or file object.

        Returns:
            The Blob.

        """
        if hasattr(blob_input, "read"):
            raw = blob_input
        else:
            if not isinstance(blob_input, bytes):
                raise TypeError
            raw = BytesIO(blob_input)
        return cls._read_avro(raw)

    @classmethod
    def load_file(cls, filename):
        """Create a new message model from a file.

        Args:
            filename: The path to a file.

        Returns:
            The message model.

        """
        with open(filename, "rb") as f:
            return cls.load(f)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        # compare only content, not schemas
        return self.content == other.content

    def __hash__(self):
        raise NotImplementedError("AvroBlob objects are not hashable")


@plugins.register
def get_models():
    model_classes = [
        VOEvent,
        GCNCircular,
        Blob,
        JSONBlob,
        AvroBlob,
    ]
    return {format_name(cls): cls for cls in model_classes}
