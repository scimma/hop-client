#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish GCNs"


import argparse
import email
import os

from .io import Stream
from .models import GCNCircular, VOEvent


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    parser.add_argument(
        "url",
        metavar="URL",
        help="Sets the URL (kafka://host[:port]/topic) to publish GCNs to.",
    )
    parser.add_argument(
        "gcn", metavar="GCN", nargs="+", help="One or more GCNs to publish.",
    )

    # configuration options
    config = parser.add_mutually_exclusive_group()
    config.add_argument(
        "-F", "--config-file", help="Set client configuration from file.",
    )
    config.add_argument(
        "-X",
        "--config",
        action="append",
        help="Set client configuration via prop=val. Can be specified multiple times.",
    )


def _main(args=None):
    """Parse and publish GCNs.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # load config if specified
    if args.config_file:
        config = args.config_file
    elif args.config:
        config = {opt[0]: opt[1] for opt in (kv.split("=") for kv in args.config)}
    else:
        config = None

    stream = Stream(format="json", config=config)
    with stream.open(args.url, "w") as s:
        for gcn in args.gcn:
            extension = os.path.splitext(gcn)[1]

            # parse based on GCN type
            if extension == ".gcn3":
                with open(gcn, "r") as f:
                    gcn = GCNCircular.from_email(f)
            elif extension == ".xml":
                with open(gcn, "rb") as f:
                    gcn = VOEvent.from_xml(f)
            else:
                raise ValueError(f"File extension {extension} not recognized")

            # publish GCN
            s.write(gcn.asdict())
