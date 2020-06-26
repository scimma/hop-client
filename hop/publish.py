#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish messages"


import argparse
import warnings

from . import cli
from .io import Stream
from .models import GCNCircular, VOEvent, MessageBlob


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
        help="Specifies the format of the message(s), such as 'circular' or 'voevent'. "
        "Specify 'blob' if sending an unstructured message. Default: 'blob'.",
    )


def _main(args=None):
    """Parse and publish messages.

    """

    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # load config if specified
    config = cli.load_config(args)

    stream = Stream()
    with stream.open(args.url, "w") as s:
        if args.format in model_loader:
            loader = model_loader[args.format]
        else:
            warnings.warn(
                "Warning: format not recognized. Sending as unstructured blob")
            loader = model_loader["blob"]

        for message_file in args.message:
            message_model = loader(message_file)
            s.write(message_model.wrap_message())
