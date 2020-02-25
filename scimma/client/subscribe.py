#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "tools to receive and parse GCN circulars"


import argparse
import email

from genesis import streaming as stream

# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    parser.add_argument(
        "-b",
        "--broker-url",
        required=True,
        help="Sets the broker URL (kafka://host[:port]/topic) to receive GCNs from.",
    )
    parser.add_argument(
        "-j", "--json", help="A test argument for raw json printing",
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
    """Receive and parse GCN circulars.

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

