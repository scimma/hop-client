#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module to define common message formats"


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
            dict: the dict representation of the VOEvent.

        """
        return asdict(self)

    def wrap_message(self):
        """Wrap the message with its format and content.

        Returns:
           A dictionary with "format" and "content" key-value pairs

        """

        wrapped_message = {"format": "voevent", "content": self.asdict()}
        return wrapped_message

    def __str__(self):
        return json.dumps(self.asdict(), indent=2)

    @classmethod
    def from_xml(cls, xml_input):
        """Create a new VOEvent from an XML-formatted VOEvent.

        Args:
            xml_input: a file object, string, or generator

        Returns:
            The VOEvent.

        """
        vo = xmltodict.parse(xml_input, attr_prefix="")

        # enter root and remove XML-specific namespaces
        return cls(**{k: v for k, v in vo["voe:VOEvent"].items() if ":" not in k})

    @classmethod
    def from_xml_file(cls, filename):
        """Create a new VOEvent from an XML-formatted VOEvent file.

        Args:
            filename: name of the VOEvent file

        Returns:
            The VOEvent

        """
        with open(filename, "rb") as f:
            return cls.from_xml(f)


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
            dict: the dict representation of the Circular.

        """
        return asdict(self)

    def wrap_message(self):
        """Wrap the message with its format and content.

        Returns:
           A dictionary with "format" and "content" key-value pairs

        """

        wrapped_message = {"format": "circular", "content": self.asdict()}
        return wrapped_message

    def __str__(self):
        headers = [(name.upper() + ":").ljust(9) + val for name, val in self.header.items()]
        return "\n".join(headers + ["", self.body])

    @classmethod
    def from_email(cls, email_input):
        """Create a new GCNCircular from an RFC 822 formatted circular.

        Args:
            email_input: a file object or string

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
    def from_email_file(cls, filename):
        """Create a new GCNCircular from an RFC 822 formatted circular file.

        Args:
            filename: the GCN filename

        Returns:
            The GCNCircular.

        """

        with open(filename, "r") as f:
            return cls.from_email(f)


@dataclass
class MessageBlob(object):
    """Defines an unformatted message structure.

    This is included as a dataclass to mirror the implementation of structured formats.

    """

    content: str

    def asdict(self):
        """Represents the message as a dictionary.

        Returns:
            dict: the dict representation of the message

        """
        return asdict(self)

    def wrap_message(self):
        """Wrap the message with its format and content.

        Returns:
           A dictionary with "format" and "content" key-value pairs

        """

        wrapped_message = {"format": "blob", "content": self.asdict()}
        return wrapped_message

    def __str__(self):
        return self.content

    @classmethod
    def from_text(cls, blob_input):
        """Create a blob message from input text or file

        Args:
            blob_input: a file or string

        Returns:
            The Blob.

        """
        try:
            with open(blob_input, "r") as f:
                return cls(f.read())
        except FileNotFoundError:
            return cls(blob_input)
