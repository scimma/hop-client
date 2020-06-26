#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish messages"


import argparse
import warnings

from . import cli
from . import io
from .models import GCNCircular, VOEvent, MessageBlob


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_url_opts(parser)
    parser.add_argument(
        "message", metavar="MESSAGE", nargs="+", help="One or more messages to publish.",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=io.Deserializer.__members__,
        default=str(io.Deserializer.BLOB),
        help="Specify the message format. Defaults to BLOB for an unstructured message.",
    )


def _main(args=None):
    """Parse and publish messages.

    """

    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # set up stream and message loader
    stream = io.Stream()
    loader = io.Deserializer[args.format]

    with stream.open(args.url, "w") as s:
        for message_file in args.message:
            message = loader.load_file(message_file)
            s.write(message)
