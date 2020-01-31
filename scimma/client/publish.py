#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish GCN circulars"


import argparse
import email

from genesis import streaming as stream


def read_parse_gcn(gcn_file):
    """Reads and parses a GCN circular file.

    The parsed GCN circular is formatted as a dictionary with
    the following schema:

        {'headers': {'title': ..., 'number': ..., ...}, 'body': ...}

    Args:
      gcn_file: the path to an RFC 822 formatted GCN circular

    Returns:
        the parsed GCN circular

    """
    with open(gcn_file, "r") as f:
        msg = email.message_from_file(f)

    # format gcn circular into header/body
    return {
        "header": {title.lower(): content for title, content in msg.items()},
        "body": msg.get_payload(),
    }


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    parser.add_argument("gcn", nargs="+", help="One or more GCNs to publish.")
    parser.add_argument(
        "-b",
        "--broker-url",
        required=True,
        help="Sets the broker URL to publish GCNs to.",
    )


def _main(args=None):
    """Parse and publish GCN circulars.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    with stream.open(args.broker_url, "w", format="json") as s:
        for gcn_file in args.gcn:
            gcn = read_parse_gcn(gcn_file)
            s.write(gcn)
