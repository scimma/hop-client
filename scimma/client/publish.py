#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish GCN circulars"


import argparse


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
