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
    # parser.add_argument(
    #     "gcn", metavar="GCN", nargs="+", help="One or more GCNs to publish.",
    # )
    cli.add_config_opts(parser)

    parser.add_argument(
        "-m",
        "--message",
        action="append",
        nargs=2,
        metavar=("format", "content"),
        help="The format (gcn, voevent, blob) and content of a message to publish",
    )

    # parser.add_argument(
    #     "-f",
    #     "--format",
    #     type=str,
    #     default="blob",
    #     nargs="+",
    #     help="Specifies the format of the message, such as gcn or voevent. Default: 'blob'.",
    # )


def _main(args=None):
    """Parse and publish GCNs.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # load config if specified
    config = cli.load_config(args)

    stream = Stream(format="json", config=config)
    with stream.open(args.url, "w") as s:
        for message in args.message:

            msg_format = message[0].lower()
            msg_content = message[1]

            if msg_format == "gcn":
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
