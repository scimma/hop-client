#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish GCNs"


import argparse
import os

from . import cli
from .io import Stream
from .models import GCNCircular, VOEvent


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_url_opts(parser)
    parser.add_argument(
        "gcn", metavar="GCN", nargs="+", help="One or more GCNs to publish.",
    )
    cli.add_config_opts(parser)


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
