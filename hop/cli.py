#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module for CLI utilities"


def add_client_opts(parser):
    parser.add_argument(
        "url",
        metavar="URL",
        help="Sets the URL (kafka://host[:port]/topic) to publish messages to.",
    )
    parser.add_argument(
        "--no-auth", action="store_true", help="If set, disable authentication."
    )
