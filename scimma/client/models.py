#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module to define common message types"


from dataclasses import asdict, dataclass, field


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


@dataclass
class GCNCircular(object):
    """Defines a GCN Circular structure.

    """

    header: dict
    body: str

    def asdict(self):
        """Represents the GCN Circular as a dictionary.

        Returns:
            dict: the dict representation of the Circular.

        """
        return asdict(self)
