from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
import email
import json
from typing import Any, Dict, List, Union

import xmltodict

from . import plugins

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


@dataclass
class MessageModel(ABC):
    """An abstract message model.

    """

    def asdict(self):
        """Represents the message model as a dictionary.

        """
        return asdict(self)

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" keys.

        """
        return {"format": type(self).__name__.lower(), "content": self.asdict()}

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
        return json.dumps(self.asdict(), indent=2)

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

    def __str__(self):
        headers = [(name.upper() + ":").ljust(9) + val for name, val in self.header.items()]
        return "\n".join(headers + ["", self.body])

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" key-value pairs.

        """

        wrapped_message = {"format": "circular", "content": self.asdict()}
        return wrapped_message

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
    """Defines an unformatted message blob.

    """

    content: JSONType
    missing_schema: bool = False

    def __str__(self):
        return str(self.content)

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" keys.

        """
        return {"format": type(self).__name__.lower(), "content": self.content}

    def asdict(self):
        """Represents the message as a dictionary.

        Returns:
            The dictionary representation of the message.

        """
        return asdict(self) if self.missing_schema else {"content": self.content}

    @classmethod
    def load(cls, blob_input):
        """Create a blob message from input text.

        Args:
            blob_input: The unstructured message text or file object.

        Returns:
            The Blob.

        """
        if hasattr(blob_input, "read"):
            return cls(content=blob_input.read())
        else:
            return cls(content=blob_input)


@plugins.register
def get_models():
    return {
        "voevent": VOEvent,
        "circular": GCNCircular,
        "blob": Blob,
    }
