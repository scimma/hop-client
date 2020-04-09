#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "tools to receive and parse GCN circulars"


import argparse
import json

from . import cli
from .io import Stream
from .models import GCNCircular


def print_gcn(gcn_dict, json_dump=False):
    """Parse a gcn dictionary and print to stdout.

    Args:
      gcn_dict:  the dictionary object containing a GCN-formatted message

    Returns:
      None

    """
    if json_dump:
        print(json.dumps(gcn_dict))
    else:
        gcn = GCNCircular(**gcn_dict)
        for line in str(gcn).splitlines():
            print(line)


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_url_opts(parser)
    cli.add_config_opts(parser)

    # consumer options
    parser.add_argument(
        "-j", "--json", help="Request gcn output as raw json", action="store_true",
    )
    parser.add_argument(
        "-e",
        "--earliest",
        help="Request to stream from the earliest available Kafka offset",
        action="store_true",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=10,
        help="Specifies the time (in seconds) to wait for new messages. Default: 10s",
    )


def _main(args=None):
    """Receive and parse GCN circulars.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # load config if specified
    config = cli.load_config(args)

    # set offset
    start_offset = "earliest" if args.earliest else "latest"

    # read from topic

    # assume json format for the gcn
    gcn_format = "json"

    stream = Stream(format=gcn_format, config=config, start_at=start_offset)
    with stream.open(args.url, "r") as s:
        for gcn_dict in s(timeout=args.timeout):
            print_gcn(gcn_dict, args.json)
