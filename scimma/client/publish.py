#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish GCN circulars"


import argparse
import email


def read_parse_gcn(gcn_file):
    """Reads and parses a GCN circular file.

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


def main(args=None):
    """Parse and publish GCN circulars.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _parser_add_arguments(parser)
        args = parser.parse_args()
