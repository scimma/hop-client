#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish GCNs"


import argparse
import os
import warnings

from . import cli
from .io import Stream
from .models import GCNCircular, VOEvent, message_blob


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_url_opts(parser)
    cli.add_config_opts(parser)

    parser.add_argument(
        "-m",
        "--message",
        action="append",
        nargs=2,
        metavar=("format", "content"),
        help="Specify the format (circular, voevent, blob) and content of a message to publish",
    )


def _main(args=None):
    """Parse and publish GCNs.

    """

    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    if not args.message:
        raise NameError("Error: no message specified. Specify one or more messages to publish.")
        
    # load config if specified
    config = cli.load_config(args)

    stream = Stream(format="json", config=config)
    with stream.open(args.url, "w") as s:
        for message in args.message:

            msg_format = message[0].lower()
            msg_content = message[1]

            if msg_format == "circular":
                with open(msg_content, "r") as f:
                    gcn = GCNCircular.from_email(f)
            elif msg_format == "voevent":
                with open(msg_content, "rb") as f:
                    gcn = VOEvent.from_xml(f)
            elif msg_format == "blob":
                with open(msg_content, "r") as f:
                    gcn = message_blob.from_file(f)
            else:
                warnings.warn("Warning: format not recognized. Sending as blob")
                with open(msg_content, "r") as f:
                    gcn = message_blob.from_file(f)

            s.write(gcn.wrap_msg())
