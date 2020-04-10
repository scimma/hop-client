#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module for i/o utilities"


from adc import streaming


class Stream(object):
    """Defines an event stream.

    Sets up defaults used within the genesis client, so that when a
    stream connection is opened, it will use defaults specified here.

    Args:
      format: the message serialization format
      start_at: the message offset to start at
      config: librdkafka style options, either a `dict` or a file path

    """

    def __init__(self, format=None, start_at=None, config=None):
        self._options = {}
        if format is not None:
            self._options["format"] = format
        if start_at is not None:
            self._options["start_at"] = start_at
        if config is not None:
            self._options["config"] = config

    def open(self, *args, **kwargs):
        """Opens a connection to an event stream.

        Args:
          broker_url: sets the broker URL to connect to

        Kwargs:
          mode: read ('r') or write ('w') from the stream
          format: the message serialization format
          start_at: the message offset to start at
          config: librdkafka style options, either a `dict` or a file path

        """
        opts = {**self._options, **kwargs}
        return streaming.open(*args, **opts)
