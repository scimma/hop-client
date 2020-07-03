from dataclasses import asdict, dataclass, field
import email
import json

import xmltodict


@dataclass
class VOEvent(object):
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

    def asdict(self):
        """Represents the VOEvent as a dictionary.

        Returns:
            A dictionary representation of the VOEvent.

        """
        return asdict(self)

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" key-value pairs.

        """

        wrapped_message = {"format": "voevent", "content": self.asdict()}
        return wrapped_message

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
class GCNCircular(object):
    """Defines a GCN Circular structure.

    The parsed GCN circular is formatted as a dictionary with
    the following schema:

        {'headers': {'title': ..., 'number': ..., ...}, 'body': ...}

    """

    header: dict
    body: str

    def asdict(self):
        """Represents the GCN Circular as a dictionary.

        Returns:
            The dictionary representation of the Circular.

        """
        return asdict(self)

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
            A dictionary with "format" and "content" key-value pairs.

        """

        wrapped_message = {"format": "circular", "content": self.asdict()}
        return wrapped_message

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

    @classmethod
    def load_file(cls, filename):
        """Create a new GCNCircular from an RFC 822 formatted circular file.

        Args:
            filename: The GCN filename.

        Returns:
            The GCNCircular.

        """
        with open(filename, "r") as f:
            return cls.load(f)


@dataclass
class MessageBlob(object):
    """Defines an unformatted message structure.

    This is included to mirror the implementation of structured formats.

    """

    content: str
    missing_schema: bool = False

    def asdict(self):
        """Represents the message as a dictionary.

        Returns:
            The dictionary representation of the message.

        """
        return asdict(self) if self.missing_schema else {"content": self.content}

    def serialize(self):
        """Wrap the message with its format and content.

        Returns:
           A dictionary with "format" and "content" key-value pairs

        """

        wrapped_message = {"format": "blob", "content": self.content}
        return wrapped_message

    def __str__(self):
        return str(self.content)

    @classmethod
    def load(cls, blob_input):
        """Create a blob message from input text.

        Args:
            blob_input: The unstructured message text or file object.

        Returns:
            The Blob.

        """
        if hasattr(blob_input, "read"):
            return cls(blob_input.read())
        else:
            return cls(blob_input)

    @classmethod
    def load_file(cls, filename):
        """Create a blob message from an input file.

        Args:
            filename: A filename.

        Returns:
            The Blob.

        """
        with open(filename, "r") as f:
            return cls(f.read())
