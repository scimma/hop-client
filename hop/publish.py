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
    parser.add_argument(
        "message", metavar="MESSAGE", nargs="+", help="One or more messages to publish.",
    )
    cli.add_config_opts(parser)

    parser.add_argument(
        "-f",
        "--format",
        type=str,
        default="blob",
        help="Specifies the format of the message, such as 'circular' or 'voevent'. "
        "Specify 'blob' if sending an unstructured message. Default: 'blob'.",
    )

def _main(args=None):
    """Parse and publish messages.

    """

    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # if not args.message:
    #     raise NameError("Error: no message specified. Specify one or more messages to publish.")
        
    # load config if specified
    config = cli.load_config(args)

    # get format of the message(s) if specified
    message_format = "blob" if not args.format else args.format

    stream = Stream(format="json", config=config)
    with stream.open(args.url, "w") as s:
        for message_file in args.message:

            if message_format == "circular":
                with open(message_file, "r") as f:
                    message_model = GCNCircular.from_email(f)
            elif message_format == "voevent":
                with open(message_file, "rb") as f:
                    message_model = VOEvent.from_xml(f)
            elif message_format == "blob":
                with open(message_file, "r") as f:
                    message_model = message_blob.from_file(f)
            else:
                warnings.warn("Warning: format not recognized. Sending as unstructured blob")
                with open(message_file, "r") as f:
                    message_model = message_blob.from_file(f)

            s.write(message_model.wrap_message())
